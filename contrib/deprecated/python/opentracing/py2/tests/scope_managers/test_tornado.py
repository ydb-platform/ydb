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
import pytest
from unittest import TestCase

from tornado import ioloop, version_info
try:
    from opentracing.scope_managers.tornado import TornadoScopeManager
    from opentracing.scope_managers.tornado import tracer_stack_context
except ImportError:
    pass
from opentracing.harness.scope_check import ScopeCompatibilityCheckMixin


# We don't need run tests in case Tornado>=6, because it became
# asyncio-based framework and `stack_context` was deprecated.
@pytest.mark.skipif(version_info >= (6, 0, 0, 0),
                    reason='skip Tornado >= 6')
class TornadoCompabilityCheck(TestCase, ScopeCompatibilityCheckMixin):
    def scope_manager(self):
        return TornadoScopeManager()

    def run_test(self, test_fn):
        with tracer_stack_context():
            ioloop.IOLoop.current().run_sync(test_fn)
