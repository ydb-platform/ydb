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

import random
import time


class RateLimiter(object):
    """
    RateLimiter is based on leaky bucket algorithm, formulated in terms
    of a credits balance that is replenished every time check_credit()
    method is called (tick) by the amount proportional to the time
    elapsed since the last tick, up to the max_balance. A call
    to check_credit() takes a cost of an item we want to pay with the
    balance. If the balance exceeds the cost of the item, the item is
    "purchased" and the balance reduced, indicated by returned value of
    true. Otherwise the balance is unchanged and return false.

    This can be used to limit a rate of messages emitted by a service
    by instantiating the Rate Limiter with the max number of messages a
    service is allowed to emit per second, and calling check_credit(1.0)
    for each message to determine if the message is within the rate limit.

    It can also be used to limit the rate of traffic in bytes, by setting
    credits_per_second to desired throughput as bytes/second, and calling
    check_credit() with the actual message size.
    """

    def __init__(self, credits_per_second, max_balance):
        self.credits_per_second = credits_per_second
        self.max_balance = max_balance
        self.balance = self.max_balance * random.random()
        self.last_tick = self.timestamp()

    @staticmethod
    def timestamp():
        return time.time()

    def update(self, credits_per_second, max_balance):
        self._update_balance()
        self.credits_per_second = credits_per_second
        # The new balance should be proportional to the old balance.
        self.balance = max_balance * self.balance / self.max_balance
        self.max_balance = max_balance

    def check_credit(self, item_cost):
        self._update_balance()
        if self.balance >= item_cost:
            self.balance -= item_cost
            return True
        return False

    def _update_balance(self):
        current_time = self.timestamp()
        elapsed_time = current_time - self.last_tick
        self.last_tick = current_time
        self.balance += elapsed_time * self.credits_per_second
        if self.balance > self.max_balance:
            self.balance = self.max_balance
