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

import functools
import time
import random
from oslo_log import log as logging

from zaqar.i18n import _LE

LOG = logging.getLogger(__name__)

BACKOFF_RETRY_TIMES = 3
BACKOFF_RETRY_MIN_SECONDS = 10
BACKOFF_RETRY_MAX_SECONDS = 20
EXP_DECAY_MAX_SLEEP_TIME = 512
BACKOFF_RETRY = 'BACKOFF_RETRY'
EXPONENTIAL_DECAY_RETRY = 'EXPONENTIAL_DECAY_RETRY'


def notifier_retry_policy(conf, messages, subscription):

    def _retry_policy(f, retry_policy):
        retry_times = 0
        if retry_policy == BACKOFF_RETRY:
            retry_times = BACKOFF_RETRY_TIMES
        elif retry_policy == EXPONENTIAL_DECAY_RETRY:
            retry_times = conf.notification.max_notifier_retries

        for i in range(retry_times):
            if retry_policy == BACKOFF_RETRY:
                sleep_time = random.randint(BACKOFF_RETRY_MIN_SECONDS,
                                            BACKOFF_RETRY_MAX_SECONDS)
            elif retry_policy == EXPONENTIAL_DECAY_RETRY:
                sleep_time = 2**i
                if sleep_time > EXP_DECAY_MAX_SLEEP_TIME:
                    sleep_time = EXP_DECAY_MAX_SLEEP_TIME

            time.sleep(sleep_time)

            LOG.debug('Retry_policy[%s]: retry times: %s,'
                      'sleep time: %s, The subscription is: %s,'
                      'The messages is: %s,' %
                      (retry_policy, i + 1, sleep_time,
                       subscription, messages))
            try:
                f()
                break
            except Exception as e:
                LOG.debug(_LE('Notifier task retry got exception: %s.')
                          % str(e))

    def decorator(f):
        @functools.wraps(f)
        def wrapper():
            try:
                f()
            except Exception as e:
                LOG.exception(_LE('Notifier task got exception: %s.') %
                              str(e))
                retry_policy = subscription['options'].get('push_policy',
                                                           None)

                _retry_policy(f, retry_policy)
        return wrapper
    return decorator
