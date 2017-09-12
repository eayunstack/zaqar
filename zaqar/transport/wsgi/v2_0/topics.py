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
from zaqar.storage import errors as storage_errors
from zaqar.transport import acl
from zaqar.transport import utils
from zaqar.transport import validation
from zaqar.transport.wsgi import errors as wsgi_errors
from zaqar.transport.wsgi import utils as wsgi_utils

LOG = logging.getLogger(__name__)


class ItemResource(object):

    __slots__ = ('_validate', '_topic_controller', '_message_controller',
                 '_reserved_metadata')

    def __init__(self, validate, topic_controller, message_controller):
        self._validate = validate
        self._topic_controller = topic_controller
        self._message_controller = message_controller
        self._reserved_metadata = ['max_messages_post_size',
                                   'default_message_ttl']

    def _get_reserved_metadata(self):
        reserved_metadata = {
            '_%s' % meta:
                self._validate.get_limit_conf_value(meta)
            for meta in self._reserved_metadata
        }
        return reserved_metadata

    @decorators.TransportLog("Topics item")
    @acl.enforce("topics:get")
    def on_get(self, req, resp, project_id, topic_name):
        try:
            resp_dict = self._topic_controller.get(topic_name,
                                                   project=project_id)
        except storage_errors.DoesNotExist as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPNotFound(six.text_type(ex))

        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Topic metadata could not be retrieved.')
            raise wsgi_errors.HTTPServiceUnavailable(description)

        resp.body = utils.to_json(resp_dict)
        # status defaults to 200

    @decorators.TransportLog("Topics item")
    @acl.enforce("topics:create")
    def on_put(self, req, resp, project_id, topic_name):
        try:
            # Deserialize topic metadata
            metadata = {}
            if req.content_length:
                document = wsgi_utils.deserialize(req.stream,
                                                  req.content_length)
                metadata = wsgi_utils.sanitize(document, spec=None)
            self._validate.queue_metadata_putting(metadata)
        except validation.ValidationFailed as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPBadRequestAPI(six.text_type(ex))

        try:
            for meta in self._reserved_metadata:
                if meta not in metadata:
                    metadata[meta] = self._validate.get_limit_conf_value(meta)
            created = self._topic_controller.create(topic_name,
                                                    metadata=metadata,
                                                    project=project_id)
        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Topic could not be created.')
            raise wsgi_errors.HTTPServiceUnavailable(description)

        resp.status = falcon.HTTP_201 if created else falcon.HTTP_204
        resp.location = req.path

    @decorators.TransportLog("Topics item")
    @acl.enforce("topics:delete")
    def on_delete(self, req, resp, project_id, topic_name):
        LOG.debug(u'Topic item DELETE - topic: %(topic)s, '
                  u'project: %(project)s',
                  {'topic': topic_name, 'project': project_id})
        try:
            self._topic_controller.delete(topic_name, project=project_id)

        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Topic could not be deleted.')
            raise wsgi_errors.HTTPServiceUnavailable(description)

        resp.status = falcon.HTTP_204


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
