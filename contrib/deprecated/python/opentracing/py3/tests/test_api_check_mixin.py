# Copyright The OpenTracing Authors
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

from __future__ import absolute_import
import unittest
from opentracing import Tracer
from opentracing.harness.api_check import APICompatibilityCheckMixin


class VerifyAPICompatibilityCheck(unittest.TestCase):
    def test_tracer_exception(self):
        api_check = APICompatibilityCheckMixin()
        with self.assertRaises(NotImplementedError):
            api_check.tracer()

    def test_default_baggage_check_mode(self):
        api_check = APICompatibilityCheckMixin()
        assert api_check.check_baggage_values() is True

    def test_default_scope_manager_check_mode(self):
        api_check = APICompatibilityCheckMixin()
        assert api_check.check_scope_manager() is True

    def test_baggage_check_works(self):
        api_check = APICompatibilityCheckMixin()
        setattr(api_check, 'tracer', lambda: Tracer())

        # no-op tracer does not store baggage, so the test with default
        # value of `check_baggage_values()` should fail.
        with self.assertRaises(AssertionError):
            api_check.test_span_baggage()

        # second check that assert on empty baggage will fail too
        with self.assertRaises(AssertionError):
            api_check.test_context_baggage()

    def test_scope_manager_check_works(self):
        api_check = APICompatibilityCheckMixin()
        setattr(api_check, 'tracer', lambda: Tracer())

        # these tests are expected to succeed
        api_check.test_start_active_span_ignore_active_span()
        api_check.test_start_span_propagation_ignore_active_span()

        # no-op tracer has a no-op ScopeManager implementation,
        # which means no *actual* propagation is done,
        # so these tests are expected to work, but asserts to fail
        with self.assertRaises(AssertionError):
            api_check.test_start_active_span()

        with self.assertRaises(AssertionError):
            api_check.test_start_active_span_parent()

        with self.assertRaises(AssertionError):
            api_check.test_start_span_propagation()

        with self.assertRaises(AssertionError):
            api_check.test_tracer_start_active_span_scope()

        with self.assertRaises(AssertionError):
            api_check.test_tracer_start_span_scope()

        with self.assertRaises(AssertionError):
            api_check.test_start_active_span_finish_on_close()
