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

import sys
import collections

if sys.version_info[0] == 3:
    Iterable = collections.abc.Iterable
else:
    Iterable = collections.Iterable

__tracebackhide__ = True


class ExceptionMixin(object):
    """Expected exception mixin."""

    def raises(self, ex):
        """Asserts that val is callable and set the expected exception.

        Just sets the expected exception, but never calls val, and therefore never failes. You must
        chain to :meth:`~when_called_with` to invoke ``val()``.

        Args:
            ex: the expected exception

        Examples:
            Usage::

                assert_that(some_func).raises(RuntimeError).when_called_with('foo')

        Returns:
            AssertionBuilder: returns a new instance (now with the given expected exception) to chain to the next assertion
        """
        if not callable(self.val):
            raise TypeError('val must be callable')
        if not issubclass(ex, BaseException):
            raise TypeError('given arg must be exception')

        # chain on with ex as the expected exception
        return self.builder(self.val, self.description, self.kind, ex)

    def when_called_with(self, *some_args, **some_kwargs):
        """Asserts that val, when invoked with the given args and kwargs, raises the expected exception.

        Invokes ``val()`` with the given args and kwargs.  You must first set the expected
        exception with :meth:`~raises`.

        Args:
            *some_args: the args to call ``val()``
            **some_kwargs: the kwargs to call ``val()``

        Examples:
            Usage::

                def some_func(a):
                    raise RuntimeError('some error!')

                assert_that(some_func).raises(RuntimeError).when_called_with('foo')

        Returns:
            AssertionBuilder: returns a new instance (now with the captured exception error message as the val) to chain to the next assertion

        Raises:
            AssertionError: if val does **not** raise the expected exception
            TypeError: if expected exception not set via :meth:`raises`
        """
        if not self.expected:
            raise TypeError('expected exception not set, raises() must be called first')
        try:
            self.val(*some_args, **some_kwargs)
        except BaseException as e:
            if issubclass(type(e), self.expected):
                # chain on with error message
                return self.builder(str(e), self.description, self.kind)
            else:
                # got exception, but wrong type, so raise
                self.error('Expected <%s> to raise <%s> when called with (%s), but raised <%s>.' % (
                    self.val.__name__,
                    self.expected.__name__,
                    self._fmt_args_kwargs(*some_args, **some_kwargs),
                    type(e).__name__))

        # didn't fail as expected, so raise
        self.error('Expected <%s> to raise <%s> when called with (%s).' % (
            self.val.__name__,
            self.expected.__name__,
            self._fmt_args_kwargs(*some_args, **some_kwargs)))
