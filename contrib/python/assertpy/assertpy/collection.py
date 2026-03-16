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


class CollectionMixin(object):
    """Collection assertions mixin."""

    def is_iterable(self):
        """Asserts that val is iterable collection.

        Examples:
            Usage::

                assert_that('foo').is_iterable()
                assert_that(['a', 'b']).is_iterable()
                assert_that((1, 2, 3)).is_iterable()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** iterable
        """
        if not isinstance(self.val, Iterable):
            self.error('Expected iterable, but was not.')
        return self

    def is_not_iterable(self):
        """Asserts that val is not iterable collection.

        Examples:
            Usage::

                assert_that(1).is_not_iterable()
                assert_that(123.4).is_not_iterable()
                assert_that(True).is_not_iterable()
                assert_that(None).is_not_iterable()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **is** iterable
        """
        if isinstance(self.val, Iterable):
            self.error('Expected not iterable, but was.')
        return self

    def is_subset_of(self, *supersets):
        """Asserts that val is iterable and a subset of the given superset (or supersets).

        Args:
            *supersets: the expected superset (or supersets)

        Examples:
            Usage::

                assert_that('foo').is_subset_of('abcdefghijklmnopqrstuvwxyz')
                assert_that(['a', 'b']).is_subset_of(['a', 'b', 'c'])
                assert_that((1, 2, 3)).is_subset_of([1, 2, 3, 4])
                assert_that({'a': 1, 'b': 2}).is_subset_of({'a': 1, 'b': 2, 'c': 3})
                assert_that({'a', 'b'}).is_subset_of({'a', 'b', 'c'})

                # or multiple supersets (as comma-separated args)
                assert_that('aBc').is_subset_of('abc', 'ABC')
                assert_that((1, 2, 3)).is_subset_of([1, 3, 5], [2, 4, 6])

                assert_that({'a': 1, 'b': 2}).is_subset_of({'a': 1, 'c': 3})  # fails
                # Expected <{'a': 1, 'b': 2}> to be subset of <{'a': 1, 'c': 3}>, but <{'b': 2}> was missing.

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** subset of given superset (or supersets)
        """
        if not isinstance(self.val, Iterable):
            raise TypeError('val is not iterable')
        if len(supersets) == 0:
            raise ValueError('one or more superset args must be given')

        missing = []
        if hasattr(self.val, 'keys') and callable(getattr(self.val, 'keys')) and hasattr(self.val, '__getitem__'):
            # flatten superset dicts
            superdict = {}
            for l, j in enumerate(supersets):
                self._check_dict_like(j, check_values=False, name='arg #%d' % (l+1))
                for k in j.keys():
                    superdict.update({k: j[k]})

            for i in self.val.keys():
                if i not in superdict:
                    missing.append({i: self.val[i]})  # bad key
                elif self.val[i] != superdict[i]:
                    missing.append({i: self.val[i]})  # bad val
            if missing:
                self.error('Expected <%s> to be subset of %s, but %s %s missing.' % (
                    self.val, self._fmt_items(superdict), self._fmt_items(missing), 'was' if len(missing) == 1 else 'were'))
        else:
            # flatten supersets
            superset = set()
            for j in supersets:
                try:
                    for k in j:
                        superset.add(k)
                except Exception:
                    superset.add(j)

            for i in self.val:
                if i not in superset:
                    missing.append(i)
            if missing:
                self.error('Expected <%s> to be subset of %s, but %s %s missing.' % (
                    self.val, self._fmt_items(superset), self._fmt_items(missing), 'was' if len(missing) == 1 else 'were'))

        return self

    def is_sorted(self, key=lambda x: x, reverse=False):
        """Asserts that val is iterable and is sorted.

        Args:
            key (function): the one-arg function to extract the sort comparison key.  Defaults to
                ``lambda x: x`` to just compare items directly.
            reverse (bool): if ``True``, then comparison key is reversed.  Defaults to ``False``.

        Examples:
            Usage::

                assert_that(['a', 'b', 'c']).is_sorted()
                assert_that((1, 2, 3)).is_sorted()

                # with a key function
                assert_that('aBc').is_sorted(key=str.lower)

                # reverse order
                assert_that(['c', 'b', 'a']).is_sorted(reverse=True)
                assert_that((3, 2, 1)).is_sorted(reverse=True)

                assert_that((1, 2, 3, 4, -5, 6)).is_sorted()  # fails
                # Expected <(1, 2, 3, 4, -5, 6)> to be sorted, but subset <4, -5> at index 3 is not.

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val is **not** sorted
        """
        if not isinstance(self.val, Iterable):
            raise TypeError('val is not iterable')

        for i, x in enumerate(self.val):
            if i > 0:
                if reverse:
                    if key(x) > key(prev):
                        self.error('Expected <%s> to be sorted reverse, but subset %s at index %s is not.' % (self.val, self._fmt_items([prev, x]), i-1))
                else:
                    if key(x) < key(prev):
                        self.error('Expected <%s> to be sorted, but subset %s at index %s is not.' % (self.val, self._fmt_items([prev, x]), i-1))
            prev = x

        return self
