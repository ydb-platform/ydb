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

__tracebackhide__ = True


class DictMixin(object):
    """Dict assertions mixin."""

    def contains_key(self, *keys):
        """Asserts the val is a dict and contains the given key or keys.  Alias for :meth:`~assertpy.contains.ContainsMixin.contains`.

        Checks if the dict contains the given key or keys using ``in`` operator.

        Args:
            *keys: the key or keys expected to be contained

        Examples:
            Usage::

                assert_that({'a': 1, 'b': 2}).contains_key('a')
                assert_that({'a': 1, 'b': 2}).contains_key('a', 'b')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contain the key or keys
        """
        self._check_dict_like(self.val, check_values=False, check_getitem=False)
        return self.contains(*keys)

    def does_not_contain_key(self, *keys):
        """Asserts the val is a dict and does not contain the given key or keys.  Alias for :meth:`~assertpy.contains.ContainsMixin.does_not_contain`.

        Checks if the dict excludes the given key or keys using ``in`` operator.

        Args:
            *keys: the key or keys expected to be excluded

        Examples:
            Usage::

                assert_that({'a': 1, 'b': 2}).does_not_contain_key('x')
                assert_that({'a': 1, 'b': 2}).does_not_contain_key('x', 'y')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** contain the key or keys
        """
        self._check_dict_like(self.val, check_values=False, check_getitem=False)
        return self.does_not_contain(*keys)

    def contains_value(self, *values):
        """Asserts that val is a dict and contains the given value or values.

        Checks if the dict contains the given value or values in *any* key.

        Args:
            *values: the value or values expected to be contained

        Examples:
            Usage::

                assert_that({'a': 1, 'b': 2}).contains_value(1)
                assert_that({'a': 1, 'b': 2}).contains_value(1, 2)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contain the value or values
        """
        self._check_dict_like(self.val, check_getitem=False)
        if len(values) == 0:
            raise ValueError('one or more value args must be given')
        missing = []
        for v in values:
            if v not in self.val.values():
                missing.append(v)
        if missing:
            self.error('Expected <%s> to contain values %s, but did not contain %s.' % (self.val, self._fmt_items(values), self._fmt_items(missing)))
        return self

    def does_not_contain_value(self, *values):
        """Asserts that val is a dict and does not contain the given value or values.

        Checks if the dict excludes the given value or values across *all* keys.

        Args:
            *values: the value or values expected to be excluded

        Examples:
            Usage::

                assert_that({'a': 1, 'b': 2}).does_not_contain_value(3)
                assert_that({'a': 1, 'b': 2}).does_not_contain_value(3, 4)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** contain the value or values
        """
        self._check_dict_like(self.val, check_getitem=False)
        if len(values) == 0:
            raise ValueError('one or more value args must be given')
        else:
            found = []
            for v in values:
                if v in self.val.values():
                    found.append(v)
            if found:
                self.error('Expected <%s> to not contain values %s, but did contain %s.' % (self.val, self._fmt_items(values), self._fmt_items(found)))
        return self

    def contains_entry(self, *args, **kwargs):
        """Asserts that val is a dict and contains the given entry or entries.

        Checks if the dict contains the given key-value pair or pairs.

        Args:
            *args: the entry or entries expected to be contained (as ``{k: v}`` args)
            **kwargs: the entry or entries expected to be contained (as ``k=v`` kwargs)

        Examples:
            Usage::

                # using args
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1})
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, {'b': 2})
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, {'b': 2}, {'c': 3})

                # using kwargs
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry(a=1)
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry(a=1, b=2)
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry(a=1, b=2, c=3)

                # or args and kwargs
                assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'c': 3}, a=1, b=2)


        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** contain the entry or entries
        """
        self._check_dict_like(self.val, check_values=False)
        entries = list(args) + [{k: v} for k, v in kwargs.items()]
        if len(entries) == 0:
            raise ValueError('one or more entry args must be given')
        missing = []
        for e in entries:
            if type(e) is not dict:
                raise TypeError('given entry arg must be a dict')
            if len(e) != 1:
                raise ValueError('given entry args must contain exactly one key-value pair')
            k = next(iter(e))
            if k not in self.val:
                missing.append(e)  # bad key
            elif self.val[k] != e[k]:
                missing.append(e)  # bad val
        if missing:
            self.error('Expected <%s> to contain entries %s, but did not contain %s.' % (self.val, self._fmt_items(entries), self._fmt_items(missing)))
        return self

    def does_not_contain_entry(self, *args, **kwargs):
        """Asserts that val is a dict and does not contain the given entry or entries.

        Checks if the dict excludes the given key-value pair or pairs.

        Args:
            *args: the entry or entries expected to be excluded (as ``{k: v}`` args)
            **kwargs: the entry or entries expected to be excluded (as ``k=v`` kwargs)

        Examples:
            Usage::

                # using args
                assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 2})
                assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 2}, {'x': 4})

                # using kwargs
                assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry(a=2)
                assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry(a=2, x=4)

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** contain the entry or entries
        """
        self._check_dict_like(self.val, check_values=False)
        entries = list(args) + [{k: v} for k, v in kwargs.items()]
        if len(entries) == 0:
            raise ValueError('one or more entry args must be given')
        found = []
        for e in entries:
            if type(e) is not dict:
                raise TypeError('given entry arg must be a dict')
            if len(e) != 1:
                raise ValueError('given entry args must contain exactly one key-value pair')
            k = next(iter(e))
            if k in self.val and e[k] == self.val[k]:
                found.append(e)
        if found:
            self.error('Expected <%s> to not contain entries %s, but did contain %s.' % (self.val, self._fmt_items(entries), self._fmt_items(found)))
        return self
