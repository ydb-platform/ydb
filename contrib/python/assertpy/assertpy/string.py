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
import re
import collections

if sys.version_info[0] == 3:
    str_types = (str,)
    unicode = str
    Iterable = collections.abc.Iterable
else:
    str_types = (basestring,)
    unicode = unicode
    Iterable = collections.Iterable

__tracebackhide__ = True


class StringMixin(object):
    """String assertions mixin."""

    def is_equal_to_ignoring_case(self, other):
        """Asserts that val is a string and is case-insensitive equal to other.

        Checks actual is equal to expected using the ``==`` operator and ``str.lower()``.

        Args:
            other: the expected value

        Examples:
            Usage::

                assert_that('foo').is_equal_to_ignoring_case('FOO')
                assert_that('FOO').is_equal_to_ignoring_case('foo')
                assert_that('fOo').is_equal_to_ignoring_case('FoO')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if actual is **not** case-insensitive equal to expected
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if not isinstance(other, str_types):
            raise TypeError('given arg must be a string')
        if self.val.lower() != other.lower():
            self.error('Expected <%s> to be case-insensitive equal to <%s>, but was not.' % (self.val, other))
        return self

    def contains_ignoring_case(self, *items):
        """Asserts that val is string and contains the given item or items.

        Walks val and checks for item or items using the ``==`` operator and ``str.lower()``.

        Args:
            *items: the item or items expected to be contained

        Examples:
            Usage::

                assert_that('foo').contains_ignoring_case('F', 'oO')
                assert_that(['a', 'B']).contains_ignoring_case('A', 'b')
                assert_that({'a': 1, 'B': 2}).contains_ignoring_case('A', 'b')
                assert_that({'a', 'B'}).contains_ignoring_case('A', 'b')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contain the case-insensitive item or items
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        if isinstance(self.val, str_types):
            if len(items) == 1:
                if not isinstance(items[0], str_types):
                    raise TypeError('given arg must be a string')
                if items[0].lower() not in self.val.lower():
                    self.error('Expected <%s> to case-insensitive contain item <%s>, but did not.' % (self.val, items[0]))
            else:
                missing = []
                for i in items:
                    if not isinstance(i, str_types):
                        raise TypeError('given args must all be strings')
                    if i.lower() not in self.val.lower():
                        missing.append(i)
                if missing:
                    self.error('Expected <%s> to case-insensitive contain items %s, but did not contain %s.' % (
                        self.val, self._fmt_items(items), self._fmt_items(missing)))
        elif isinstance(self.val, Iterable):
            missing = []
            for i in items:
                if not isinstance(i, str_types):
                    raise TypeError('given args must all be strings')
                found = False
                for v in self.val:
                    if not isinstance(v, str_types):
                        raise TypeError('val items must all be strings')
                    if i.lower() == v.lower():
                        found = True
                        break
                if not found:
                    missing.append(i)
            if missing:
                self.error('Expected <%s> to case-insensitive contain items %s, but did not contain %s.' % (
                    self.val, self._fmt_items(items), self._fmt_items(missing)))
        else:
            raise TypeError('val is not a string or iterable')
        return self

    def starts_with(self, prefix):
        """Asserts that val is string or iterable and starts with prefix.

        Args:
            prefix: the prefix

        Examples:
            Usage::

                assert_that('foo').starts_with('fo')
                assert_that(['a', 'b', 'c']).starts_with('a')
                assert_that((1, 2, 3)).starts_with(1)
                assert_that(((1, 2), (3, 4), (5, 6))).starts_with((1, 2))

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** start with prefix
        """
        if prefix is None:
            raise TypeError('given prefix arg must not be none')
        if isinstance(self.val, str_types):
            if not isinstance(prefix, str_types):
                raise TypeError('given prefix arg must be a string')
            if len(prefix) == 0:
                raise ValueError('given prefix arg must not be empty')
            if not self.val.startswith(prefix):
                self.error('Expected <%s> to start with <%s>, but did not.' % (self.val, prefix))
        elif isinstance(self.val, Iterable):
            if len(self.val) == 0:
                raise ValueError('val must not be empty')
            first = next(iter(self.val))
            if first != prefix:
                self.error('Expected %s to start with <%s>, but did not.' % (self.val, prefix))
        else:
            raise TypeError('val is not a string or iterable')
        return self

    def ends_with(self, suffix):
        """Asserts that val is string or iterable and ends with suffix.

        Args:
            suffix: the suffix

        Examples:
            Usage::

                assert_that('foo').ends_with('oo')
                assert_that(['a', 'b', 'c']).ends_with('c')
                assert_that((1, 2, 3)).ends_with(3)
                assert_that(((1, 2), (3, 4), (5, 6))).ends_with((5, 6))

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** end with suffix
        """
        if suffix is None:
            raise TypeError('given suffix arg must not be none')
        if isinstance(self.val, str_types):
            if not isinstance(suffix, str_types):
                raise TypeError('given suffix arg must be a string')
            if len(suffix) == 0:
                raise ValueError('given suffix arg must not be empty')
            if not self.val.endswith(suffix):
                self.error('Expected <%s> to end with <%s>, but did not.' % (self.val, suffix))
        elif isinstance(self.val, Iterable):
            if len(self.val) == 0:
                raise ValueError('val must not be empty')
            last = None
            for last in self.val:
                pass
            if last != suffix:
                self.error('Expected %s to end with <%s>, but did not.' % (self.val, suffix))
        else:
            raise TypeError('val is not a string or iterable')
        return self

    def matches(self, pattern):
        """Asserts that val is string and matches the given regex pattern.

        Args:
            pattern (str): the regular expression pattern, as raw string (aka prefixed with ``r``)

        Examples:
            Usage::

                assert_that('foo').matches(r'\\w')
                assert_that('123-456-7890').matches(r'\\d{3}-\\d{3}-\\d{4}')

            Match is partial unless anchored, so these assertion pass::

                assert_that('foo').matches(r'\\w')
                assert_that('foo').matches(r'oo')
                assert_that('foo').matches(r'\\w{2}')

            To match the entire string, just use an anchored regex pattern where ``^`` and ``$``
            match the start and end of line and ``\\A`` and ``\\Z`` match the start and end of string::

                assert_that('foo').matches(r'^\\w{3}$')
                assert_that('foo').matches(r'\\A\\w{3}\\Z')

            And regex flags, such as ``re.MULTILINE`` and ``re.DOTALL``, can only be applied via
            *inline modifiers*, such as ``(?m)`` and ``(?s)``::

                s = '''bar
                foo
                baz'''

                # using multiline (?m)
                assert_that(s).matches(r'(?m)^foo$')

                # using dotall (?s)
                assert_that(s).matches(r'(?s)b(.*)z')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** match pattern

        Tip:
            Regular expressions are tricky.  Be sure to use raw strings (aka prefixed with ``r``).
            Also, note that the :meth:`matches` assertion passes for partial matches (as does the
            underlying ``re.match`` method).  So, if you need to match the entire string, you must
            include anchors in the regex pattern.
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if not isinstance(pattern, str_types):
            raise TypeError('given pattern arg must be a string')
        if len(pattern) == 0:
            raise ValueError('given pattern arg must not be empty')
        if re.search(pattern, self.val) is None:
            self.error('Expected <%s> to match pattern <%s>, but did not.' % (self.val, pattern))
        return self

    def does_not_match(self, pattern):
        """Asserts that val is string and does not match the given regex pattern.

        Args:
            pattern (str): the regular expression pattern, as raw string (aka prefixed with ``r``)

        Examples:
            Usage::

                assert_that('foo').does_not_match(r'\\d+')
                assert_that('123').does_not_match(r'\\w+')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** match pattern

        See Also:
            :meth:`matches` - for more about regex patterns
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if not isinstance(pattern, str_types):
            raise TypeError('given pattern arg must be a string')
        if len(pattern) == 0:
            raise ValueError('given pattern arg must not be empty')
        if re.search(pattern, self.val) is not None:
            self.error('Expected <%s> to not match pattern <%s>, but did.' % (self.val, pattern))
        return self

    def is_alpha(self):
        """Asserts that val is non-empty string and all characters are alphabetic (using ``str.isalpha()``).

        Examples:
            Usage::

                assert_that('foo').is_lower()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** lowercase
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if len(self.val) == 0:
            raise ValueError('val is empty')
        if not self.val.isalpha():
            self.error('Expected <%s> to contain only alphabetic chars, but did not.' % self.val)
        return self

    def is_digit(self):
        """Asserts that val is non-empty string and all characters are digits (using ``str.isdigit()``).

        Examples:
            Usage::

                assert_that('1234567890').is_digit()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** digits
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if len(self.val) == 0:
            raise ValueError('val is empty')
        if not self.val.isdigit():
            self.error('Expected <%s> to contain only digits, but did not.' % self.val)
        return self

    def is_lower(self):
        """Asserts that val is non-empty string and all characters are lowercase (using ``str.lower()``).

        Examples:
            Usage::

                assert_that('foo').is_lower()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** lowercase
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if len(self.val) == 0:
            raise ValueError('val is empty')
        if self.val != self.val.lower():
            self.error('Expected <%s> to contain only lowercase chars, but did not.' % self.val)
        return self

    def is_upper(self):
        """Asserts that val is non-empty string and all characters are uppercase (using ``str.upper()``).

        Examples:
            Usage::

                assert_that('FOO').is_upper()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** uppercase
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a string')
        if len(self.val) == 0:
            raise ValueError('val is empty')
        if self.val != self.val.upper():
            self.error('Expected <%s> to contain only uppercase chars, but did not.' % self.val)
        return self

    def is_unicode(self):
        """Asserts that val is a unicode string.

        Examples:
            Usage::

                assert_that(u'foo').is_unicode()  # python 2
                assert_that('foo').is_unicode()   # python 3

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** a unicode string
        """
        if type(self.val) is not unicode:
            self.error('Expected <%s> to be unicode, but was <%s>.' % (self.val, type(self.val).__name__))
        return self
