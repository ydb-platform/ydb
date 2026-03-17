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
    str_types = (str,)
    Iterable = collections.abc.Iterable
else:
    str_types = (basestring,)
    Iterable = collections.Iterable

__tracebackhide__ = True


class ExtractingMixin(object):
    """Collection flattening mixin.

    It is often necessary to test collections of objects.  Use the ``extracting()`` helper to
    reduce the collection on a given attribute.  Reduce a list of objects::

        alice = Person('Alice', 'Alpha')
        bob = Person('Bob', 'Bravo')
        people = [alice, bob]

        assert_that(people).extracting('first_name').is_equal_to(['Alice', 'Bob'])
        assert_that(people).extracting('first_name').contains('Alice', 'Bob')
        assert_that(people).extracting('first_name').does_not_contain('Charlie')

    Additionally, the ``extracting()`` helper can accept a list of attributes to be extracted, and
    will flatten them into a list of tuples.  Reduce a list of objects on multiple attributes::

        assert_that(people).extracting('first_name', 'last_name').contains(('Alice', 'Alpha'), ('Bob', 'Bravo'))

    Also, ``extracting()`` works on not just attributes, but also properties, and even
    zero-argument methods.  Reduce a list of object on properties and zero-arg methods::

        assert_that(people).extracting('name').contains('Alice Alpha', 'Bob Bravo')
        assert_that(people).extracting('say_hello').contains('Hello, Alice!', 'Hello, Bob!')

    And ``extracting()`` even works on *dict-like* objects.  Reduce a list of dicts on key::

        alice = {'first_name': 'Alice', 'last_name': 'Alpha'}
        bob = {'first_name': 'Bob', 'last_name': 'Bravo'}
        people = [alice, bob]

        assert_that(people).extracting('first_name').contains('Alice', 'Bob')

    **Filtering**

    The ``extracting()`` helper can include a *filter* to keep only those items for which the given
    *filter* is truthy.  For example::

        users = [
            {'user': 'Alice', 'age': 36, 'active': True},
            {'user': 'Bob', 'age': 40, 'active': False},
            {'user': 'Charlie', 'age': 13, 'active': True}
        ]

        # filter the active users
        assert_that(users).extracting('user', filter='active').is_equal_to(['Alice', 'Charlie'])

    The *filter* can be a *dict-like* object and the extracted items are kept if and only if all
    corresponding key-value pairs are equal::

        assert_that(users).extracting('user', filter={'active': False}).is_equal_to(['Bob'])
        assert_that(users).extracting('user', filter={'age': 36, 'active': True}).is_equal_to(['Alice'])

    Or a *filter* can be any function (including an in-line ``lambda``) that accepts as its single
    argument each item in the collection, and the extracted items are kept if the function
    evaluates to ``True``::

        assert_that(users).extracting('user', filter=lambda x: x['age'] > 20)
            .is_equal_to(['Alice', 'Bob'])

    **Sorting**

    The ``extracting()`` helper can include a *sort* to enforce order on the extracted items.

    The *sort* can be the name of a key (or attribute, or property, or zero-argument method) and
    the extracted items are ordered by the corresponding values::

        assert_that(users).extracting('user', sort='age').is_equal_to(['Charlie', 'Alice', 'Bob'])

    The *sort* can be an ``iterable`` of names and the extracted items are ordered by
    corresponding value of the first name, ties are broken by the corresponding values of the
    second name, and so on::

        assert_that(users).extracting('user', sort=['active', 'age']).is_equal_to(['Bob', 'Charlie', 'Alice'])

    The *sort* can be any function (including an in-line ``lambda``) that accepts as its single
    argument each item in the collection, and the extracted items are ordered by the corresponding
    function return values::

        assert_that(users).extracting('user', sort=lambda x: -x['age']).is_equal_to(['Bob', 'Alice', 'Charlie'])
    """

    def extracting(self, *names, **kwargs):
        """Asserts that val is iterable, then extracts the named attributes, properties, or
        zero-arg methods into a list (or list of tuples if multiple names are given).

        Args:
            *names: the attribute to be extracted (or property or zero-arg method)
            **kwargs: see below

        Keyword Args:
            filter: extract only those items where filter is truthy
            sort: order the extracted items by the sort key

        Examples:
            Usage::

                alice = User('Alice', 20, True)
                bob = User('Bob', 30, False)
                charlie = User('Charlie', 10, True)
                users = [alice, bob, charlie]

                assert_that(users).extracting('user').contains('Alice', 'Bob', 'Charlie')

            Works with *dict-like* objects too::

                users = [
                    {'user': 'Alice', 'age': 20, 'active': True},
                    {'user': 'Bob', 'age': 30, 'active': False},
                    {'user': 'Charlie', 'age': 10, 'active': True}
                ]

                assert_that(people).extracting('user').contains('Alice', 'Bob', 'Charlie')

            Filter::

                assert_that(users).extracting('user', filter='active').is_equal_to(['Alice', 'Charlie'])

            Sort::

                assert_that(users).extracting('user', sort='age').is_equal_to(['Charlie', 'Alice', 'Bob'])

        Returns:
            AssertionBuilder: returns a new instance (now with the extracted list as the val) to chain to the next assertion
        """
        if not isinstance(self.val, Iterable):
            raise TypeError('val is not iterable')
        if isinstance(self.val, str_types):
            raise TypeError('val must not be string')
        if len(names) == 0:
            raise ValueError('one or more name args must be given')

        def _extract(x, name):
            if self._check_dict_like(x, check_values=False, return_as_bool=True):
                if name in x:
                    return x[name]
                else:
                    raise ValueError('item keys %s did not contain key <%s>' % (list(x.keys()), name))
            elif isinstance(x, Iterable):
                self._check_iterable(x, name='item')
                return x[name]
            elif hasattr(x, name):
                attr = getattr(x, name)
                if callable(attr):
                    try:
                        return attr()
                    except TypeError:
                        raise ValueError('val method <%s()> exists, but is not zero-arg method' % name)
                else:
                    return attr
            else:
                raise ValueError('val does not have property or zero-arg method <%s>' % name)

        def _filter(x):
            if 'filter' in kwargs:
                if isinstance(kwargs['filter'], str_types):
                    return bool(_extract(x, kwargs['filter']))
                elif self._check_dict_like(kwargs['filter'], check_values=False, return_as_bool=True):
                    for k in kwargs['filter']:
                        if isinstance(k, str_types):
                            if _extract(x, k) != kwargs['filter'][k]:
                                return False
                    return True
                elif callable(kwargs['filter']):
                    return kwargs['filter'](x)
                return False
            return True

        def _sort(x):
            if 'sort' in kwargs:
                if isinstance(kwargs['sort'], str_types):
                    return _extract(x, kwargs['sort'])
                elif isinstance(kwargs['sort'], Iterable):
                    items = []
                    for k in kwargs['sort']:
                        if isinstance(k, str_types):
                            items.append(_extract(x, k))
                    return tuple(items)
                elif callable(kwargs['sort']):
                    return kwargs['sort'](x)
            return 0

        extracted = []
        for i in sorted(self.val, key=lambda x: _sort(x)):
            if _filter(i):
                items = [_extract(i, name) for name in names]
                extracted.append(tuple(items) if len(items) > 1 else items[0])

        # chain on with _extracted_ list (don't chain to self!)
        return self.builder(extracted, self.description, self.kind)
