# Copyright (c) The OpenTracing Authors.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import absolute_import

from concurrent.futures import ThreadPoolExecutor
from unittest import TestCase

import asyncio

from opentracing.scope_managers.asyncio import AsyncioScopeManager
from opentracing.harness.scope_check import ScopeCompatibilityCheckMixin


class AsyncioCompabilityCheck(TestCase, ScopeCompatibilityCheckMixin):

    def scope_manager(self):
        return AsyncioScopeManager()

    def run_test(self, test_fn):
        async def async_test_fn():
            test_fn()
        asyncio.get_event_loop().run_until_complete(async_test_fn())

    def test_no_event_loop(self):
        # no event loop exists by default in
        # new threads, so make sure we don't fail there.
        def test_fn():
            manager = self.scope_manager()
            assert manager.active is None

        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(test_fn).result()
