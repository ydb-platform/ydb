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

if sys.version_info[0] == 3:
    str_types = (str,)
    xrange = range
else:
    str_types = (basestring,)
    xrange = xrange

__tracebackhide__ = True


class ContainsMixin(object):
    """Containment assertions mixin."""

    def contains(self, *items):
        """Asserts that val contains the given item or items.

        Checks if the collection contains the given item or items using ``in`` operator.

        Args:
            *items: the item or items expected to be contained

        Examples:
            Usage::

                assert_that('foo').contains('f')
                assert_that('foo').contains('f', 'oo')
                assert_that(['a', 'b']).contains('b', 'a')
                assert_that((1, 2, 3)).contains(3, 2, 1)
                assert_that({'a': 1, 'b': 2}).contains('b', 'a')  # checks keys
                assert_that({'a', 'b'}).contains('b', 'a')
                assert_that([1, 2, 3]).is_type_of(list).contains(1, 2).does_not_contain(4, 5)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contain the item or items

        Tip:
            Use the :meth:`~assertpy.dict.DictMixin.contains_key` alias when working with
            *dict-like* objects to be self-documenting.

        See Also:
            :meth:`~assertpy.string.StringMixin.contains_ignoring_case` - for case-insensitive string contains
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        elif len(items) == 1:
            if items[0] not in self.val:
                if self._check_dict_like(self.val, return_as_bool=True):
                    self.error('Expected <%s> to contain key <%s>, but did not.' % (self.val, items[0]))
                else:
                    self.error('Expected <%s> to contain item <%s>, but did not.' % (self.val, items[0]))
        else:
            missing = []
            for i in items:
                if i not in self.val:
                    missing.append(i)
            if missing:
                if self._check_dict_like(self.val, return_as_bool=True):
                    self.error('Expected <%s> to contain keys %s, but did not contain key%s %s.' % (
                        self.val, self._fmt_items(items), '' if len(missing) == 0 else 's', self._fmt_items(missing)))
                else:
                    self.error('Expected <%s> to contain items %s, but did not contain %s.' % (self.val, self._fmt_items(items), self._fmt_items(missing)))
        return self

    def does_not_contain(self, *items):
        """Asserts that val does not contain the given item or items.

        Checks if the collection excludes the given item or items using ``in`` operator.

        Args:
            *items: the item or items expected to be excluded

        Examples:
            Usage::

                assert_that('foo').does_not_contain('x')
                assert_that(['a', 'b']).does_not_contain('x', 'y')
                assert_that((1, 2, 3)).does_not_contain(4, 5)
                assert_that({'a': 1, 'b': 2}).does_not_contain('x', 'y')  # checks keys
                assert_that({'a', 'b'}).does_not_contain('x', 'y')
                assert_that([1, 2, 3]).is_type_of(list).contains(1, 2).does_not_contain(4, 5)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** contain the item or items

        Tip:
            Use the :meth:`~assertpy.dict.DictMixin.does_not_contain_key` alias when working with
            *dict-like* objects to be self-documenting.
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        elif len(items) == 1:
            if items[0] in self.val:
                self.error('Expected <%s> to not contain item <%s>, but did.' % (self.val, items[0]))
        else:
            found = []
            for i in items:
                if i in self.val:
                    found.append(i)
            if found:
                self.error('Expected <%s> to not contain items %s, but did contain %s.' % (self.val, self._fmt_items(items), self._fmt_items(found)))
        return self

    def contains_only(self, *items):
        """Asserts that val contains *only* the given item or items.

        Checks if the collection contains only the given item or items using ``in`` operator.

        Args:
            *items: the *only* item or items expected to be contained

        Examples:
            Usage::

                assert_that('foo').contains_only('f', 'o')
                assert_that(['a', 'a', 'b']).contains_only('a', 'b')
                assert_that((1, 1, 2)).contains_only(1, 2)
                assert_that({'a': 1, 'a': 2, 'b': 3}).contains_only('a', 'b')
                assert_that({'a', 'a', 'b'}).contains_only('a', 'b')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val contains anything **not** item or items
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        else:
            extra = []
            for i in self.val:
                if i not in items:
                    extra.append(i)
            if extra:
                self.error('Expected <%s> to contain only %s, but did contain %s.' % (self.val, self._fmt_items(items), self._fmt_items(extra)))

            missing = []
            for i in items:
                if i not in self.val:
                    missing.append(i)
            if missing:
                self.error('Expected <%s> to contain only %s, but did not contain %s.' % (self.val, self._fmt_items(items), self._fmt_items(missing)))
        return self

    def contains_sequence(self, *items):
        """Asserts that val contains the given ordered sequence of items.

        Checks if the collection contains the given sequence of items using ``in`` operator.

        Args:
            *items: the sequence of items expected to be contained

        Examples:
            Usage::

                assert_that('foo').contains_sequence('f', 'o')
                assert_that('foo').contains_sequence('o', 'o')
                assert_that(['a', 'b', 'c']).contains_sequence('b', 'c')
                assert_that((1, 2, 3)).contains_sequence(1, 2)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contains the given sequence of items
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        else:
            try:
                for i in xrange(len(self.val) - len(items) + 1):
                    for j in xrange(len(items)):
                        if self.val[i+j] != items[j]:
                            break
                    else:
                        return self
            except TypeError:
                raise TypeError('val is not iterable')
        self.error('Expected <%s> to contain sequence %s, but did not.' % (self.val, self._fmt_items(items)))

    def contains_duplicates(self):
        """Asserts that val is iterable and *does* contain duplicates.

        Examples:
            Usage::

                assert_that('foo').contains_duplicates()
                assert_that(['a', 'a', 'b']).contains_duplicates()
                assert_that((1, 1, 2)).contains_duplicates()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contain any duplicates
        """
        try:
            if len(self.val) != len(set(self.val)):
                return self
        except TypeError:
            raise TypeError('val is not iterable')
        self.error('Expected <%s> to contain duplicates, but did not.' % self.val)

    def does_not_contain_duplicates(self):
        """Asserts that val is iterable and *does not* contain any duplicates.

        Examples:
            Usage::

                assert_that('fox').does_not_contain_duplicates()
                assert_that(['a', 'b', 'c']).does_not_contain_duplicates()
                assert_that((1, 2, 3)).does_not_contain_duplicates()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** contain duplicates
        """
        try:
            if len(self.val) == len(set(self.val)):
                return self
        except TypeError:
            raise TypeError('val is not iterable')
        self.error('Expected <%s> to not contain duplicates, but did.' % self.val)

    def is_empty(self):
        """Asserts that val is empty.

        Examples:
            Usage::

                assert_that('').is_empty()
                assert_that([]).is_empty()
                assert_that(()).is_empty()
                assert_that({}).is_empty()
                assert_that(set()).is_empty()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** empty
        """
        if len(self.val) != 0:
            if isinstance(self.val, str_types):
                self.error('Expected <%s> to be empty string, but was not.' % self.val)
            else:
                self.error('Expected <%s> to be empty, but was not.' % self.val)
        return self

    def is_not_empty(self):
        """Asserts that val is *not* empty.

        Examples:
            Usage::

                assert_that('foo').is_not_empty()
                assert_that(['a', 'b']).is_not_empty()
                assert_that((1, 2, 3)).is_not_empty()
                assert_that({'a': 1, 'b': 2}).is_not_empty()
                assert_that({'a', 'b'}).is_not_empty()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **is** empty
        """
        if len(self.val) == 0:
            if isinstance(self.val, str_types):
                self.error('Expected not empty string, but was empty.')
            else:
                self.error('Expected not empty, but was empty.')
        return self

    def is_in(self, *items):
        """Asserts that val is equal to one of the given items.

        Args:
            *items: the items expected to contain val

        Examples:
            Usage::

                assert_that('foo').is_in('foo', 'bar', 'baz')
                assert_that(1).is_in(0, 1, 2, 3)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** in the given items
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        else:
            for i in items:
                if self.val == i:
                    return self
        self.error('Expected <%s> to be in %s, but was not.' % (self.val, self._fmt_items(items)))

    def is_not_in(self, *items):
        """Asserts that val is not equal to one of the given items.

        Args:
            *items: the items expected to exclude val

        Examples:
            Usage::

                assert_that('foo').is_not_in('bar', 'baz', 'box')
                assert_that(1).is_not_in(-1, -2, -3)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **is** in the given items
        """
        if len(items) == 0:
            raise ValueError('one or more args must be given')
        else:
            for i in items:
                if self.val == i:
                    self.error('Expected <%s> to not be in %s, but was.' % (self.val, self._fmt_items(items)))
        return self
