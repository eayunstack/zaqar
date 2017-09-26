# Copyright (c) 2015 Catalyst IT Ltd
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

import json
from oslo_log import log as logging
import requests

from zaqar.notification import tasks

LOG = logging.getLogger(__name__)


class WebhookTask(object):

    def execute(self, subscription, messages, headers=None, **kwargs):
        if headers is None:
            headers = {'Content-Type': 'application/json'}
        headers.update(subscription['options'].get('post_headers', {}))
        monitor_controller = kwargs.get('monitor_controller', None)
        conf = kwargs.get('conf', None)
        project = kwargs.get('project', None)

        @tasks.notifier_retry_policy(conf, messages, subscription)
        def _post_msg():
            for msg in messages:
                # NOTE(Eva-i): Unfortunately this will add 'queue_name' key to
                # our original messages(dicts) which will be later consumed in
                # the storage controller. It seems safe though.
                msg['queue_name'] = subscription['source']
                if 'post_data' in subscription['options']:
                    data = subscription['options']['post_data']
                    data = data.replace('"$zaqar_message$"', json.dumps(msg))
                else:
                    data = json.dumps(msg)
                requests.post(subscription['subscriber'],
                              data=data,
                              headers=headers)

            LOG.debug('Messages: %s publish for Subscription:'
                      '%s Success.' % (messages, subscription))

            try:
                monitor_controller.update(messages,
                                          subscription['source'],
                                          project,
                                          'subscribe_messages',
                                          success=True)
            except Exception as ex:
                LOG.exception(ex)

        _post_msg()

    def register(self, subscriber, options, ttl, project_id, request_data):
        pass
