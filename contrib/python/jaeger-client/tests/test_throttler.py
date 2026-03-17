# Copyright (c) 2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
import time

from jaeger_client.throttler import RemoteThrottler


def test_throttler_simple():
    channel = mock.MagicMock()
    throttler = RemoteThrottler(channel, 'test-service')
    allowed = throttler.is_allowed('test-operation')
    assert not allowed
    allowed = throttler.is_allowed('test-operation')
    assert not allowed


def test_throttler_no_io_loop():
    channel = mock.MagicMock()
    channel.io_loop = None
    throttler = RemoteThrottler(channel, 'test-service')
    assert throttler


def test_throttler_credits():
    channel = mock.MagicMock()
    throttler = RemoteThrottler(channel, 'test-service')
    throttler.credits['test-operation'] = 3.0
    allowed = throttler.is_allowed('test-operation')
    assert allowed
    assert throttler.credits['test-operation'] == 2.0


def test_throttler_init_polling():
    channel = mock.MagicMock()
    throttler = RemoteThrottler(channel, 'test-service')
    # noinspection PyProtectedMember
    throttler._init_polling()
    assert channel.io_loop.call_later.call_count == 1
    throttler.close()
    # noinspection PyProtectedMember
    throttler._init_polling()
    assert channel.io_loop.call_later.call_count == 1


def test_throttler_delayed_polling():
    channel = mock.MagicMock()
    channel.io_loop.time = time.time
    channel.io_loop._next_timeout = 1
    throttler = RemoteThrottler(channel, 'test-service')
    throttler.credits = {'test-operation': 0}
    # noinspection PyProtectedMember
    throttler._delayed_polling()
    assert channel.request_throttling_credits.call_count == 1
    assert throttler.periodic
    throttler.close()
    throttler.periodic = None
    throttler._delayed_polling()
    assert throttler.periodic is None


def test_throttler_request_callback():
    channel = mock.MagicMock()
    throttler = RemoteThrottler(channel, 'test-service')
    throttler.error_reporter = mock.MagicMock()
    # noinspection PyProtectedMember
    future = mock.MagicMock()
    future.exception = mock.MagicMock()
    future.exception.return_value = Exception()
    throttler._request_callback(future)
    assert throttler.error_reporter.error.call_count == 1
    future.exception.return_value = None
    future.result.return_value = mock.MagicMock()
    future.result.return_value.body = """
        {
            \"balances\": [
                {
                    \"operation\": \"test-operation\",
                    \"balance\": 2.0
                }
            ]
        }
    """
    throttler._request_callback(future)
    assert throttler.credits['test-operation'] == 2.0

    future.result.return_value.body = '{ "bad": "json" }'
    throttler._request_callback(future)
    assert throttler.error_reporter.error.call_count == 2
