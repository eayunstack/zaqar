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
import six
import falcon

from zaqar.common import decorators
from zaqar.i18n import _
from zaqar.storage import errors as storage_errors
from zaqar.transport import acl
from zaqar.transport import utils
from zaqar.transport.wsgi import errors as wsgi_errors
from zaqar.transport import validation
from zaqar.transport.wsgi import utils as wsgi_utils

LOG = logging.getLogger(__name__)


class CollectionResource(object):

    __slots__ = (
        '_message_controller',
        '_queue_controller',
        '_wsgi_conf',
        '_validate',
        '_message_post_spec',
        '_claim_controller',
    )

    def __init__(self, wsgi_conf, validate,
                 message_controller, queue_controller, claim_controller):

        self._wsgi_conf = wsgi_conf
        self._validate = validate
        self._message_controller = message_controller
        self._queue_controller = queue_controller
        self._claim_controller = claim_controller

    @decorators.TransportLog("Messages consume item")
    @acl.enforce("messages:consume")
    def on_get(self, req, resp, project_id, queue_name):
        auto_delete = req.get_param_as_int('auto_delete')
        limit = req.get_param_as_int('limit')
        claim_options = {} if limit is None else {'limit': limit}

        queue_meta = None
        try:
            queue_meta = self._queue_controller.get_metadata(queue_name,
                                                             project_id)
        except storage_errors.DoesNotExist as ex:
            self._validate.identification(queue_name, project_id)
            self._queue_controller.create(queue_name, project=project_id)
            queue_meta = {}

        queue_claim_ttl = queue_meta.get('claim_ttl', 1)
        metadata = {'grace': 0}
        if queue_claim_ttl:
            metadata['ttl'] = queue_claim_ttl

        # Claim some messages
        try:
            self._validate.claim_creation(metadata, limit=limit)

            cid, msgs = self._claim_controller.create(
                queue_name,
                metadata=metadata,
                project=project_id,
                **claim_options)

            # Buffer claimed messages
            # TODO(kgriffs): optimize, along with serialization (below)
            resp_msgs = list(msgs)
            for msg in resp_msgs:
                if auto_delete:
                    self._message_controller.\
                        consume_delete(queue_name, msg['handle'],
                                       project=project_id)
        except validation.ValidationFailed as ex:
            LOG.debug(ex)
            raise wsgi_errors.HTTPBadRequestAPI(six.text_type(ex))

        except Exception as ex:
            LOG.exception(ex)
            description = _(u'Consume message failed.')
            raise wsgi_errors.HTTPServiceUnavailable(description)

        # Serialize claimed messages, if any. This logic assumes
        # the storage driver returned well-formed messages.
        if len(resp_msgs) != 0:
            base_path = req.path.rpartition('/')[0]
            resp_msgs = [wsgi_utils.format_message_v1_1(msg, base_path, cid)
                         for msg in resp_msgs]

            resp.location = req.path + '/' + cid
            resp.body = utils.to_json({'messages': resp_msgs})
            resp.status = falcon.HTTP_201
        else:
            resp.status = falcon.HTTP_204
