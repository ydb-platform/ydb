# Copyright (c) 2017 Uber Technologies, Inc.
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

import time
import mock

from jaeger_client.rate_limiter import RateLimiter


def test_rate_limiting_sampler():
    rate_limiter = RateLimiter(2, 2)
    assert rate_limiter.balance <= 2.0
    rate_limiter.balance = 2.0
    # stop time by overwriting timestamp() function to always return
    # the same time
    ts = time.time()
    rate_limiter.last_tick = ts
    with mock.patch('jaeger_client.rate_limiter.RateLimiter.timestamp') \
            as mock_time:
        mock_time.side_effect = lambda: ts  # always return same time
        assert rate_limiter.timestamp() == ts
        assert rate_limiter.check_credit(1), 'initial balance allows first item'
        assert rate_limiter.check_credit(1), 'initial balance allows second item'
        assert not rate_limiter.check_credit(1), 'initial balance exhausted'

        # move time 250ms forward, not enough credits to pay for one sample
        mock_time.side_effect = lambda: ts + 0.25
        assert not rate_limiter.check_credit(1), 'not enough time passed for full item'

        # move time 500ms forward, now enough credits to pay for one sample
        mock_time.side_effect = lambda: ts + 0.5
        assert rate_limiter.check_credit(1), 'enough time for new item'
        assert not rate_limiter.check_credit(1), 'no more balance'

        # move time 5s forward, enough to accumulate credits for 10 samples,
        # but it should still be capped at 2
        rate_limiter.last_tick = ts  # reset the timer
        mock_time.side_effect = lambda: ts + 5
        assert rate_limiter.check_credit(1), 'enough time for new item'
        assert rate_limiter.check_credit(1), 'enough time for second new item'
        for i in range(0, 8):
            assert not rate_limiter.check_credit(1), 'but no further, since time is stopped'

    # Test with rate limit of greater than 1 second
    rate_limiter = RateLimiter(0.1, 1.0)
    assert rate_limiter.balance <= 1.0
    rate_limiter.balance = 1.0
    ts = time.time()
    rate_limiter.last_tick = ts
    with mock.patch('jaeger_client.rate_limiter.RateLimiter.timestamp') \
            as mock_time:
        mock_time.side_effect = lambda: ts  # always return same time
        assert rate_limiter.timestamp() == ts
        assert rate_limiter.check_credit(1), 'initial balance allows first item'
        assert not rate_limiter.check_credit(1), 'initial balance exhausted'

        # move time 11s forward, enough credits to pay for one sample
        mock_time.side_effect = lambda: ts + 11
        assert rate_limiter.check_credit(1)

    # Test update
    rate_limiter = RateLimiter(3.0, 3.0)
    assert rate_limiter.balance <= 3.0
    rate_limiter.balance = 3.0
    ts = time.time()
    rate_limiter.last_tick = ts
    with mock.patch('jaeger_client.rate_limiter.RateLimiter.timestamp') \
            as mock_time:
        mock_time.side_effect = lambda: ts  # always return same time
        assert rate_limiter.timestamp() == ts
        assert rate_limiter.check_credit(1)
        rate_limiter.update(2.0, 2.0)
        assert rate_limiter.balance == 4.0 / 3.0
