# Copyright (c) 2015-2019, Activision Publishing, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""Assertion library for python unit testing with a fluent API"""

from __future__ import print_function
import os
import contextlib
import inspect
import logging
import sys
import types
from .base import BaseMixin
from .collection import CollectionMixin
from .contains import ContainsMixin
from .date import DateMixin
from .dict import DictMixin
from .dynamic import DynamicMixin
from .extracting import ExtractingMixin
from .exception import ExceptionMixin
from .file import FileMixin
from .helpers import HelpersMixin
from .numeric import NumericMixin
from .snapshot import SnapshotMixin
from .string import StringMixin

__version__ = '1.1'

__tracebackhide__ = True  # clean tracebacks via py.test integration
contextlib.__tracebackhide__ = True  # monkey patch contextlib with clean py.test tracebacks


# soft assertions
_soft_ctx = 0
_soft_err = []


@contextlib.contextmanager
def soft_assertions():
    """Create a soft assertion context.

    Normally, any assertion failure will halt test execution immediately by raising an error.
    Soft assertions are way to collect assertion failures (and failure messages) together, to be
    raised all at once at the end, without halting your test.

    Examples:
        Create a soft assertion context, and some failing tests::

            from assertpy import assert_that, soft_assertions

            with soft_assertions():
                assert_that('foo').is_length(4)
                assert_that('foo').is_empty()
                assert_that('foo').is_false()
                assert_that('foo').is_digit()
                assert_that('123').is_alpha()

        When the context ends, any assertion failures are collected together and a single
        ``AssertionError`` is raised::

            AssertionError: soft assertion failures:
            1. Expected <foo> to be of length <4>, but was <3>.
            2. Expected <foo> to be empty string, but was not.
            3. Expected <False>, but was not.
            4. Expected <foo> to contain only digits, but did not.
            5. Expected <123> to contain only alphabetic chars, but did not.

    Note:
        The soft assertion context only collects *assertion* failures, other errors such as
        ``TypeError`` or ``ValueError`` are always raised immediately.  Triggering an explicit test
        failure with :meth:`fail` will similarly halt execution immediately.  If you need more
        forgiving behavior, use :meth:`soft_fail` to add a failure message without halting test
        execution.
    """
    global _soft_ctx
    global _soft_err

    # init ctx
    if _soft_ctx == 0:
        _soft_err = []
    _soft_ctx += 1

    try:
        yield
    finally:
        # reset ctx
        _soft_ctx -= 1

    if _soft_err and _soft_ctx == 0:
        out = 'soft assertion failures:'
        for i, msg in enumerate(_soft_err):
            out += '\n%d. %s' % (i+1, msg)
        # reset msg, then raise
        _soft_err = []
        raise AssertionError(out)


# factory methods
def assert_that(val, description=''):
    """Set the value to be tested, plus an optional description, and allow assertions to be called.

    This is a factory method for the :class:`AssertionBuilder`, and the single most important
    method in all of assertpy.

    Args:
        val: the value to be tested (aka the actual value)
        description (str, optional): the extra error message description. Defaults to ``''``
            (aka empty string)

    Examples:
        Just import it once at the top of your test file, and away you go...::

            from assertpy import assert_that

            def test_something():
                assert_that(1 + 2).is_equal_to(3)
                assert_that('foobar').is_length(6).starts_with('foo').ends_with('bar')
                assert_that(['a', 'b', 'c']).contains('a').does_not_contain('x')
    """
    global _soft_ctx
    if _soft_ctx:
        return _builder(val, description, 'soft')
    return _builder(val, description)


def assert_warn(val, description='', logger=None):
    """Set the value to be tested, and optional description and logger, and allow assertions to be
    called, but never fail, only log warnings.

    This is a factory method for the :class:`AssertionBuilder`, but unlike :meth:`assert_that` an
    `AssertionError` is never raised, and execution is never halted.  Instead, any assertion failures
    results in a warning message being logged. Uses the given logger, or defaults to a simple logger
    that prints warnings to ``stdout``.


    Args:
        val: the value to be tested (aka the actual value)
        description (str, optional): the extra error message description. Defaults to ``''``
            (aka empty string)
        logger (Logger, optional): the logger for warning message on assertion failure. Defaults to ``None``
            (aka use the default simple logger that prints warnings to ``stdout``)

    Examples:
        Usage::

            from assertpy import assert_warn

            assert_warn('foo').is_length(4)
            assert_warn('foo').is_empty()
            assert_warn('foo').is_false()
            assert_warn('foo').is_digit()
            assert_warn('123').is_alpha()

        Even though all of the above assertions fail, ``AssertionError`` is never raised and
        test execution is never halted.  Instead, the failed assertions merely log the following
        warning messages to ``stdout``::

            2019-10-27 20:00:35 WARNING [test_foo.py:23]: Expected <foo> to be of length <4>, but was <3>.
            2019-10-27 20:00:35 WARNING [test_foo.py:24]: Expected <foo> to be empty string, but was not.
            2019-10-27 20:00:35 WARNING [test_foo.py:25]: Expected <False>, but was not.
            2019-10-27 20:00:35 WARNING [test_foo.py:26]: Expected <foo> to contain only digits, but did not.
            2019-10-27 20:00:35 WARNING [test_foo.py:27]: Expected <123> to contain only alphabetic chars, but did not.

    Tip:
        Use :meth:`assert_warn` if and only if you have a *really* good reason to log assertion
        failures instead of failing.
    """
    return _builder(val, description, 'warn', logger=logger)


def fail(msg=''):
    """Force immediate test failure with the given message.

    Args:
        msg (str, optional): the failure message.  Defaults to ``''``

    Examples:
        Fail a test::

            from assertpy import assert_that, fail

            def test_fail():
                fail('forced fail!')

        If you wanted to test for a known failure, here is a useful pattern::

            import operator

            def test_adder_bad_arg():
                try:
                    operator.add(1, 'bad arg')
                    fail('should have raised error')
                except TypeError as e:
                    assert_that(str(e)).contains('unsupported operand')
    """
    raise AssertionError('Fail: %s!' % msg if msg else 'Fail!')


def soft_fail(msg=''):
    """Within a :meth:`soft_assertions` context, append the failure message to the soft error list,
    but do not halt test execution.

    Otherwise, outside the context, acts identical to :meth:`fail` and forces immediate test
    failure with the given message.

    Args:
        msg (str, optional): the failure message.  Defaults to ``''``

    Examples:
        Failing soft assertions::

            from assertpy import assert_that, soft_assertions, soft_fail

            with soft_assertions():
                assert_that(1).is_equal_to(2)
                soft_fail('my message')
                assert_that('foo').is_equal_to('bar')

        Fails, and outputs the following soft error list::

            AssertionError: soft assertion failures:
            1. Expected <1> to be equal to <2>, but was not.
            2. Fail: my message!
            3. Expected <foo> to be equal to <bar>, but was not.

    """
    global _soft_ctx
    if _soft_ctx:
        global _soft_err
        _soft_err.append('Fail: %s!' % msg if msg else 'Fail!')
        return
    fail(msg)


# assertion extensions
_extensions = {}


def add_extension(func):
    """Add a new user-defined custom assertion to assertpy.

    Once the assertion is registered with assertpy, use it like any other assertion.  Pass val to
    :meth:`assert_that`, and then call it.

    Args:
        func: the assertion function (to be added)

    Examples:
        Usage::

            from assertpy import add_extension

            def is_5(self):
                if self.val != 5:
                    self.error(f'{self.val} is NOT 5!')
                return self

            add_extension(is_5)

            def test_5():
                assert_that(5).is_5()

            def test_6():
                assert_that(6).is_5()  # fails
                # 6 is NOT 5!
    """
    if not callable(func):
        raise TypeError('func must be callable')
    _extensions[func.__name__] = func


def remove_extension(func):
    """Remove a user-defined custom assertion.

    Args:
        func: the assertion function (to be removed)

    Examples:
        Usage::

            from assertpy import remove_extension

            remove_extension(is_5)
    """
    if not callable(func):
        raise TypeError('func must be callable')
    if func.__name__ in _extensions:
        del _extensions[func.__name__]


def _builder(val, description='', kind=None, expected=None, logger=None):
    """Internal helper to build a new :class:`AssertionBuilder` instance and glue on any extension methods."""
    ab = AssertionBuilder(val, description, kind, expected, logger)
    if _extensions:
        # glue extension method onto new builder instance
        for name, func in _extensions.items():
            meth = types.MethodType(func, ab)
            setattr(ab, name, meth)
    return ab


# warnings
class WarningLoggingAdapter(logging.LoggerAdapter):
    """Logging adapter to unwind the stack to get the correct callee filename and line number."""

    def process(self, msg, kwargs):
        def _unwind(frame, fn='assert_warn'):
            if frame and fn in frame.f_code.co_names:
                return frame
            return _unwind(frame.f_back, fn)

        frame = _unwind(inspect.currentframe())
        lineno = frame.f_lineno
        filename = os.path.basename(frame.f_code.co_filename)
        return '[%s:%d]: %s' % (filename, lineno, msg), kwargs


_logger = logging.getLogger('assertpy')
_handler = logging.StreamHandler(sys.stdout)
_handler.setLevel(logging.WARNING)
_format = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
_handler.setFormatter(_format)
_logger.addHandler(_handler)
_default_logger = WarningLoggingAdapter(_logger, None)


class AssertionBuilder(
    StringMixin,
    SnapshotMixin,
    NumericMixin,
    HelpersMixin,
    FileMixin,
    ExtractingMixin,
    ExceptionMixin,
    DynamicMixin,
    DictMixin,
    DateMixin,
    ContainsMixin,
    CollectionMixin,
    BaseMixin,
    object
):
    """The main assertion class.  Never call the constructor directly, always use the
    :meth:`assert_that` helper instead.  Or if you just want warning messages, use the
    :meth:`assert_warn` helper.

    Args:
        val: the value to be tested (aka the actual value)
        description (str, optional): the extra error message description.  Defaults to ``''``
            (aka empty string)
        kind (str, optional): the kind of assertions, one of ``None``, ``soft``, or ``warn``.
            Defaults to ``None``
        expected (Error, optional): the expected exception.  Defaults to ``None``
        logger (Logger, optional): the logger for warning messages.  Defaults to ``None``
    """

    def __init__(self, val, description='', kind=None, expected=None, logger=None):
        """Never call this constructor directly."""
        self.val = val
        self.description = description
        self.kind = kind
        self.expected = expected
        self.logger = logger if logger else _default_logger

    def builder(self, val, description='', kind=None, expected=None, logger=None):
        """Helper to build a new :class:`AssertionBuilder` instance. Use this only if not chaining to ``self``.

        Args:
            val: the value to be tested (aka the actual value)
            description (str, optional): the extra error message description.  Defaults to ``''``
                (aka empty string)
            kind (str, optional): the kind of assertions, one of ``None``, ``soft``, or ``warn``.
                Defaults to ``None``
            expected (Error, optional): the expected exception.  Defaults to ``None``
            logger (Logger, optional): the logger for warning messages.  Defaults to ``None``
        """
        return _builder(val, description, kind, expected, logger)

    def error(self, msg):
        """Helper to raise an ``AssertionError`` with the given message.

        If an error description is set by :meth:`~assertpy.base.BaseMixin.described_as`, then that
        description is prepended to the error message.

        Args:
            msg: the error message

        Examples:
            Used to fail an assertion::

                if self.val != other:
                    self.error('Expected <%s> to be equal to <%s>, but was not.' % (self.val, other))

        Raises:
            AssertionError: always raised unless ``kind`` is ``warn`` (as set when using an
                :meth:`assert_warn` assertion) or ``kind`` is ``soft`` (as set when inside a
                :meth:`soft_assertions` context).
        """
        out = '%s%s' % ('[%s] ' % self.description if len(self.description) > 0 else '', msg)
        if self.kind == 'warn':
            self.logger.warning(out)
            return self
        elif self.kind == 'soft':
            global _soft_err
            _soft_err.append(out)
            return self
        else:
            raise AssertionError(out)
