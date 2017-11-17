# Copyright (c) 2017 Eayun, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_log import log as logging

from zaqar.i18n import _LE
from zaqar.notification import tasks

LOG = logging.getLogger(__name__)


class QueueTask(object):

    def execute(self, subscription, messages, **kwargs):
        queue_name = subscription.get('subscriber', '').split(':')[-1]
        client_uuid = kwargs.get('client_uuid', None)
        message_controller = kwargs.get('message_controller', None)
        queue_controller = kwargs.get('queue_controller', None)
        monitor_controller = kwargs.get('monitor_controller', None)
        project_id = kwargs.get('project', None)
        conf = kwargs.get('conf', None)
        try:
            queue_meta = queue_controller.get_metadata(queue_name,
                                                       project_id)
        except Exception as e:
            LOG.error(_LE('Queue task got exception: %s.') % str(e))

        queue_default_ttl = queue_meta.get('_default_message_ttl',
                                           3600)
        delay_ttl = queue_meta.get('delay_ttl', 0)
        for msg in messages:
            msg['ttl'] = queue_default_ttl
            msg['delay_ttl'] = delay_ttl

        @tasks.notifier_retry_policy(conf, messages, subscription)
        def _post_msg():
            message_ids = message_controller.post(queue_name,
                                                  messages=messages,
                                                  project=project_id,
                                                  client_uuid=client_uuid)
            LOG.debug('Messages: %s publish for Subscription:'
                      '%s Success. Message id is: %s ' %
                      (messages, subscription, message_ids))

            try:
                monitor_controller.update(messages, subscription['source'],
                                          project_id, 'subscribe_messages',
                                          success=True)

                monitor_controller.update(messages, queue_name,
                                          project_id, 'send_messages')
            except Exception as ex:
                LOG.exception(ex)

        _post_msg()

    def register(self, subscriber, options, ttl, project_id, request_data):
        pass
