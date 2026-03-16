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
import unittest
from opentracing import ScopeManager
from opentracing.harness.scope_check import ScopeCompatibilityCheckMixin


class VerifyScopeCompatibilityCheck(unittest.TestCase):
    def test_scope_manager_exception(self):
        scope_check = ScopeCompatibilityCheckMixin()
        with self.assertRaises(NotImplementedError):
            scope_check.scope_manager()

    def test_missing_active_works(self):
        scope_check = ScopeCompatibilityCheckMixin()
        setattr(scope_check, 'scope_manager', lambda: ScopeManager())

        with self.assertRaises(AssertionError):
            scope_check.test_missing_active()

        with self.assertRaises(AssertionError):
            scope_check.test_missing_active_external()

    def test_activate_works(self):
        scope_check = ScopeCompatibilityCheckMixin()
        setattr(scope_check, 'scope_manager', lambda: ScopeManager())

        with self.assertRaises(AssertionError):
            scope_check.test_activate()

        with self.assertRaises(AssertionError):
            scope_check.test_activate_external()

        with self.assertRaises(AssertionError):
            scope_check.test_activate_finish_on_close()

        with self.assertRaises(AssertionError):
            scope_check.test_activate_nested()

        with self.assertRaises(AssertionError):
            scope_check.test_activate_finish_on_close_nested()

    def test_close_wrong_order(self):
        scope_check = ScopeCompatibilityCheckMixin()
        setattr(scope_check, 'scope_manager', lambda: ScopeManager())

        # this test is expected to succeed.
        scope_check.test_close_wrong_order()
