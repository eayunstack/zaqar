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

import falcon
from oslo_log import log
import six

from zaqar.common import decorators
from zaqar.storage import errors
from zaqar.transport import acl
from zaqar.transport import utils as transport_utils
from zaqar.transport.wsgi import errors as wsgi_errors

LOG = log.getLogger(__name__)


class CollectionResource(object):
    """A resource to list registered monitors

    :param monitor_controller: means to interact with storage
    """

    def __init__(self, monitor_controller, validate):
        self.monitor_ctrl = monitor_controller
        self._validate = validate

    @decorators.TransportLog("Monitors collection")
    @acl.enforce("monitors:get_all")
    def on_get(self, request, response, project_id):

        LOG.debug(u'LIST monitors for project_id %s' % project_id)

        store = {}
        request.get_param('marker', store=store)
        request.get_param_as_int('limit', store=store)
        store['all_project'] = request.get_param_as_bool('all')
        request.get_param('m_type', store=store)

        cursor = self.monitor_ctrl.list(project=project_id, **store)
        monitors = list(next(cursor))

        results = {
            'monitors': monitors
        }

        response.body = transport_utils.to_json(results)
        response.status = falcon.HTTP_200


class ItemResource(object):
    """A handler for individual monitor.

    :param monitor_controller: means to interact with storage
    """

    def __init__(self, monitor_controller):
        self.monitor_ctrl = monitor_controller

    @decorators.TransportLog("Monitors item")
    @acl.enforce("monitors:get")
    def on_get(self, request, response, project_id, m_type, name):

        LOG.debug(u'GET monitor - project: %s, m_type: %s, name: %s' %
                  (project_id, m_type, name))
        data = None

        try:
            data = self.monitor_ctrl.get(name, m_type, project_id)
        except errors.MonitorDoesNotExist as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPNotFound(six.text_type(ex))

        response.body = transport_utils.to_json(data)
