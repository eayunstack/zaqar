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
from oslo_log import log as logging
import six

from zaqar.common import decorators
from zaqar.i18n import _
from zaqar.transport import acl
from zaqar.transport import utils
from zaqar.transport import validation
from zaqar.transport.wsgi import errors as wsgi_errors

LOG = logging.getLogger(__name__)


class CollectionResource(object):

    __slots__ = ('_topic_controller', '_validate')

    def __init__(self, validate, topic_controller):
        self._topic_controller = topic_controller
        self._validate = validate

    @decorators.TransportLog("Topics collection")
    @acl.enforce("topics:get_all")
    def on_get(self, req, resp, project_id):
        kwargs = {}

        req.get_param('marker', store=kwargs)
        req.get_param_as_int('limit', store=kwargs)
        req.get_param_as_bool('detailed', store=kwargs)

        try:
            self._validate.topic_listing(**kwargs)
            results = self._topic_controller.list(project=project_id, **kwargs)

            # Buffer list of topics
            topics = list(next(results))

        except validation.ValidationFailed as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPBadRequestAPI(six.text_type(ex))

        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Topics could not be listed.')
            raise wsgi_errors.HTTPServiceUnavailable(description)

        # Got some. Prepare the response.
        kwargs['marker'] = next(results) or kwargs.get('marker', '')
        for each_topic in topics:
            each_topic['href'] = req.path + '/' + each_topic['name']

        links = []
        if topics:
            links = [
                {
                    'rel': 'next',
                    'href': req.path + falcon.to_query_str(kwargs)
                }
            ]

        response_body = {
            'topics': topics,
            'links': links
        }

        resp.body = utils.to_json(response_body)
        # status defaults to 200
