# Copyright (c) 2017 Eayun, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy
# of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""
Schema:
  'k': monitor_key :: six.text_type
  'p': project :: six.text_type
  't': monitor_type :: six.text_type
  'v': monitor_value :: dict
"""

from zaqar.storage import base
from zaqar.storage import errors
from zaqar.storage.mongodb import utils

MONITORS_INDEX = [
    ('k', 1),
]

MONITORS_LIST_INDEX = [
    ('k', 1),
    ('p', 1),
    ('t', 1),
]

OMIT_FIELDS = (('_id', False),)

QUEUE_MONITOR = {
    'mc': 'msg_counts',
    'mb': 'msg_bytes',
    'bmc': 'bulk_msg_counts',
    'bmb': 'bulk_msg_bytes',
    'cmc': 'consume_msg_counts',
    'cmb': 'consume_msg_bytes',
    }

TOPIC_MONITOR = {
    'mc': 'msg_counts',
    'mb': 'msg_bytes',
    'bmc': 'bulk_msg_counts',
    'bmb': 'bulk_msg_bytes',
    'tsmc': 'total_sub_msg_counts',
    'tsmb': 'total_sub_msg_bytes',
    'smc': 'sub_msg_counts',
    'smb': 'sub_msg_bytes',
}


def _field_spec():
    return dict(OMIT_FIELDS)


class MonitorController(base.MonitorBase):

    def __init__(self, *args, **kwargs):
        super(MonitorController, self).__init__(*args, **kwargs)
        self._msg_ctrl = self.driver.message_controller
        self._col = self.driver.monitor_database.monitors
        self._col.ensure_index(MONITORS_INDEX,
                               background=True,
                               name='monitors_key',
                               unique=True)
        self._col.ensure_index(MONITORS_LIST_INDEX,
                               background=True,
                               name='monitors_list_key',
                               unique=True)

    def _calc_queue_items(self, name, project, res):
        active_msgs = self._msg_ctrl._count(name, project=project)
        inactive_msgs = self._msg_ctrl._claimed_or_delay_count(name,
                                                               project=project,
                                                               claimed=True)
        delayed_msgs = self._msg_ctrl._claimed_or_delay_count(name,
                                                              project=project,
                                                              delayed=True)
        for k in res:
            res[k]['active_msgs'] = active_msgs
            res[k]['inactive_msgs'] = inactive_msgs
            res[k]['delayed_msgs'] = delayed_msgs
            res[k]['deleted_msgs'] = (res[k]['bulk_msg_counts'] +
                                      res[k]['msg_counts']) - \
                                     (active_msgs + delayed_msgs +
                                      inactive_msgs)
        return res

    @utils.raises_conn_error
    def list(self, m_type=None, project=None,
             marker=None, limit=10, all_project=False):
        query = {}
        if marker is not None:
            query['k'] = {'$gt': marker}
        if m_type is not None:
            query['t'] = m_type
        if project is not None and not all_project:
            query['p'] = project

        cursor = self._col.find(query, projection=_field_spec(),
                                limit=limit).sort('k', 1)
        marker_key = {}

        def normalizer(monitor):
            marker_key['next'] = monitor['k']
            res = _normalize(monitor)
            i_project, i_type, i_name = monitor['k'].split('/')
            if i_type == 'queues':
                res = self._calc_queue_items(i_name, i_project, res)
            return res

        yield utils.HookedCursor(cursor, normalizer)
        yield marker_key and marker_key['next']

    @utils.raises_conn_error
    def get(self, name, m_type, project):
        key = '%s/%s/%s' % (project, m_type, name)
        res = self._col.find_one({'k': key},
                                 _field_spec())

        if not res:
            raise errors.MonitorDoesNotExist(key)

        res = _normalize(res)

        if m_type == 'queues':
            res = self._calc_queue_items(name, project, res)

        return res

    @utils.raises_conn_error
    def create(self, name, m_type, project):
        key = '%s/%s/%s' % (project, m_type, name)
        one = self._col.find_one({'k': key}, _field_spec())
        if one:
            raise errors.MonitorAlreadyExist(key)

        self._col.insert({'k': key, 't': m_type, 'p': project,
                          'v': _init_value(m_type)})

    @utils.raises_conn_error
    def _update(self, body):
        one = self._col.find_one({'k': body['k']},
                                 _field_spec())
        if not one:
            self.create(body['n'], body['t'], body['p'])
            one = self._col.find_one({'k': body['k']},
                                     _field_spec())

        monitor_values = one.get('v', {})
        for i in monitor_values:
            monitor_values[i] = str(int(monitor_values[i]) +
                                    body['v'].get(i, 0))

        res = self._col.update({'k': body['k']},
                               {'$set': {'v': monitor_values}},
                               upsert=False)

        if not res['updatedExisting']:
            raise errors.MonitorDoesNotExist(body['k'])

    def update(self, messages, name, project,
               count_type, success=None):
        msg_bytes = 0
        for msg in messages:
            msg_bytes += len(str(msg['body']))
        msg_counts = len(messages)
        m_type = 'topics'
        if count_type in ['send_messages',
                          'consume_messages']:
            m_type = 'queues'
        key = '%s/%s/%s' % (project, m_type, name)
        body = {
            'k': key,
            't': m_type,
            'p': project,
            'n': name,
            'v': {}
        }
        if count_type in ['send_messages',
                          'publish_messages']:
            if msg_counts > 1:
                body['v']['bmc'] = msg_counts
                body['v']['bmb'] = msg_bytes
            else:
                body['v']['mc'] = msg_counts
                body['v']['mb'] = msg_bytes
        elif count_type == 'consume_messages':
            body['v']['cmc'] = msg_counts
            body['v']['cmb'] = msg_bytes
        elif count_type == 'subscribe_messages':
            if success:
                body['v']['smc'] = msg_counts
                body['v']['smb'] = msg_bytes
            else:
                body['v']['tsmc'] = msg_counts
                body['v']['tsmb'] = msg_bytes

        self._update(body)


def _normalize(monitor):
    ret = {
        monitor['k']: {}
    }
    MONITOR = {'queues': QUEUE_MONITOR, 'topics': TOPIC_MONITOR}
    for mk in MONITOR:
        for k in MONITOR[mk]:
            if monitor['t'] == mk:
                ret[monitor['k']][MONITOR[mk][k]] = \
                        int(monitor['v'].get(k, 0)) \
                        if 'bytes' not in MONITOR[mk][k] \
                        else float(monitor['v'].get(k, 0))/1024
    return ret


def _init_value(m_type):
    value = {}
    if m_type == 'queues':
        for key in QUEUE_MONITOR:
            value[key] = 0
    elif m_type == 'topics':
        for key in TOPIC_MONITOR:
            value[key] = 0
    return value
