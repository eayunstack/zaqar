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

    __slots__ = ('_validate', '_topic_controller',
                 '_message_controller', '_monitor_controller',
                 '_reserved_metadata')

    def __init__(self, validate, topic_controller,
                 message_controller, monitor_controller):
        self._validate = validate
        self._topic_controller = topic_controller
        self._message_controller = message_controller
        self._monitor_controller = monitor_controller
        self._reserved_metadata = ['max_messages_post_size',
                                   'default_message_ttl']

    def _get_reserved_metadata(self):
        reserved_metadata = {
            '_%s' % meta:
                self._validate.get_limit_conf_value(meta)
            for meta in self._reserved_metadata
        }
        return reserved_metadata

    def _init_metadata(self, metadata):
        default_key = {
            '_max_messages_post_size': 'default_message_size',
            '_default_message_ttl': 'default_topic_message_ttl'}

        for key in default_key.keys():
            if key not in metadata:
                metadata[key] = self._validate. \
                    get_limit_conf_value(default_key[key])

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
            self._init_metadata(metadata)
            self._validate.queue_metadata_putting(metadata)
        except validation.ValidationFailed as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPBadRequestAPI(six.text_type(ex))

        try:
            created = self._topic_controller.create(topic_name,
                                                    metadata=metadata,
                                                    project=project_id)
        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Topic could not be created.')
            raise wsgi_errors.HTTPServiceUnavailable(description)

        try:
            self._monitor_controller.create(topic_name, 'topics', project_id)
        except storage_errors.MonitorAlreadyExist as ex:
            LOG.debug(ex)
        except Exception as ex:
            LOG.exception(ex)

        resp.status = falcon.HTTP_201 if created else falcon.HTTP_204
        resp.location = req.path

    @decorators.TransportLog("Topics item")
    @acl.enforce("topics:update")
    def on_patch(self, req, resp, project_id, topic_name):
        """Allows one to update a topic's metadata.

        This method expects the user to submit a JSON object. There is also
        strict format checking through the use of
        jsonschema. Appropriate errors are returned in each case for
        badly formatted input.

        :returns: HTTP | 200,400,409,503
        """
        LOG.debug(u'PATCH topic - name: %s', topic_name)

        # NOTE(flwang): See below link to get more details about draft 10,
        # tools.ietf.org/html/draft-ietf-appsawg-json-patch-10
        content_types = {
            'application/openstack-messaging-v2.0-json-patch': 10,
        }

        if req.content_type not in content_types:
            headers = {'Accept-Patch':
                       ', '.join(sorted(content_types.keys()))}
            msg = _("Accepted media type for PATCH: %s.")
            LOG.debug(msg % headers)
            raise wsgi_errors.HTTPUnsupportedMediaType(msg % headers)

        if req.content_length:
            try:
                changes = utils.read_json(req.stream, req.content_length)
                changes = wsgi_utils.sanitize(changes,
                                              spec=None, doctype=list)
            except utils.MalformedJSON as ex:
                LOG.debug(ex)
                description = _(u'Request body could not be parsed.')
                raise wsgi_errors.HTTPBadRequestBody(description)

            except utils.OverflowedJSONInteger as ex:
                LOG.debug(ex)
                description = _(u'JSON contains integer that is too large.')
                raise wsgi_errors.HTTPBadRequestBody(description)

            except Exception as ex:
                # Error while reading from the network/server
                LOG.exception(ex)
                description = _(u'Request body could not be read.')
                raise wsgi_errors.HTTPServiceUnavailable(description)
        else:
            msg = _("PATCH body could not be empty for update.")
            LOG.debug(msg)
            raise wsgi_errors.HTTPBadRequestBody(msg)

        try:
            changes = self._validate.queue_patching(req, changes)

            # NOTE(Eva-i): using 'get_metadata' instead of 'get', so
            # TopicDoesNotExist error will be thrown in case of non-existent
            # topic.
            metadata = self._topic_controller.get_metadata(topic_name,
                                                           project=project_id)
            reserved_metadata = self._get_reserved_metadata()
            for change in changes:
                change_method_name = '_do_%s' % change['op']
                change_method = getattr(self, change_method_name)
                change_method(req, metadata, reserved_metadata, change)

            self._validate.queue_metadata_putting(metadata)

            self._topic_controller.set_metadata(topic_name,
                                                metadata,
                                                project_id)
        except storage_errors.DoesNotExist as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPNotFound(six.text_type(ex))
        except validation.ValidationFailed as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPBadRequestBody(six.text_type(ex))
        except wsgi_errors.HTTPConflict as ex:
            raise ex
        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Topic could not be updated.')
            raise wsgi_errors.HTTPServiceUnavailable(description)
        resp.body = utils.to_json(metadata)

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

    @staticmethod
    def _do_replace(req, metadata, reserved_metadata, change):
        path = change['path']
        path_child = path[1]
        value = change['value']
        if path_child in metadata or path_child in reserved_metadata:
            metadata[path_child] = value
        else:
            msg = _("Can't replace non-existent object %s.")
            raise wsgi_errors.HTTPConflict(msg % path_child)

    @staticmethod
    def _do_add(req, metadata, reserved_metadata, change):
        path = change['path']
        path_child = path[1]
        value = change['value']
        metadata[path_child] = value

    @staticmethod
    def _do_remove(req, metadata, reserved_metadata, change):
        path = change['path']
        path_child = path[1]
        if path_child in metadata:
            metadata.pop(path_child)
        elif path_child not in reserved_metadata:
            msg = _("Can't remove non-existent object %s.")
            raise wsgi_errors.HTTPConflict(msg % path_child)


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
