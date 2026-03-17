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

from assertpy import assert_that, fail


def test_is_length():
    assert_that(['a', 'b', 'c']).is_length(3)
    assert_that((1, 2, 3, 4)).is_length(4)
    assert_that({'a': 1, 'b': 2}).is_length(2)
    assert_that(set(['a', 'b'])).is_length(2)


def test_is_length_failure():
    try:
        assert_that(['a', 'b', 'c']).is_length(4)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to be of length <4>, but was <3>.")


def test_is_length_bad_arg_failure():
    try:
        assert_that(['a', 'b', 'c']).is_length('bar')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be an int')


def test_is_length_negative_arg_failure():
    try:
        assert_that(['a', 'b', 'c']).is_length(-1)
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a positive int')


def test_contains():
    assert_that(['a', 'b', 'c']).contains('a')
    assert_that(['a', 'b', 'c']).contains('c', 'b', 'a')
    assert_that((1, 2, 3, 4)).contains(1, 2, 3)
    assert_that((1, 2, 3, 4)).contains(4)
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains('a')
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains('a', 'b')
    assert_that(set(['a', 'b', 'c'])).contains('a')
    assert_that(set(['a', 'b', 'c'])).contains('c', 'b')

    fred = Person('fred')
    joe = Person('joe')
    bob = Person('bob')
    assert_that([fred, joe, bob]).contains(joe)


def test_contains_single_item_failure():
    try:
        assert_that(['a', 'b', 'c']).contains('x')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to contain item <x>, but did not.")


def test_contains_multi_item_failure():
    try:
        assert_that(['a', 'b', 'c']).contains('a', 'x', 'z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to contain items <'a', 'x', 'z'>, but did not contain <'x', 'z'>.")


def test_contains_multi_item_single_failure():
    try:
        assert_that(['a', 'b', 'c']).contains('a', 'b', 'z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to contain items <'a', 'b', 'z'>, but did not contain <z>.")


def test_does_not_contain():
    assert_that(['a', 'b', 'c']).does_not_contain('x')
    assert_that(['a', 'b', 'c']).does_not_contain('x', 'y')
    assert_that((1, 2, 3, 4)).does_not_contain(5)
    assert_that((1, 2, 3, 4)).does_not_contain(5, 6)
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain('x')
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain('x', 'y')
    assert_that(set(['a', 'b', 'c'])).does_not_contain('x')
    assert_that(set(['a', 'b', 'c'])).does_not_contain('x', 'y')

    fred = Person('fred')
    joe = Person('joe')
    bob = Person('bob')
    assert_that([fred, joe]).does_not_contain(bob)


def test_does_not_contain_single_item_failure():
    try:
        assert_that(['a', 'b', 'c']).does_not_contain('a')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to not contain item <a>, but did.")


def test_does_not_contain_list_item_failure():
    try:
        assert_that(['a', 'b', 'c']).does_not_contain('x', 'y', 'a')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to not contain items <'x', 'y', 'a'>, but did contain <a>.")


def test_does_not_contain_list_multi_item_failure():
    try:
        assert_that(['a', 'b', 'c']).does_not_contain('x', 'a', 'b')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b', 'c']> to not contain items <'x', 'a', 'b'>, but did contain <'a', 'b'>.")


def test_contains_only():
    assert_that(['a', 'b', 'c']).contains_only('a', 'b', 'c')
    assert_that(['a', 'b', 'c']).contains_only('c', 'b', 'a')
    assert_that(['a', 'a', 'b']).contains_only('a', 'b')
    assert_that(['a', 'a', 'a']).contains_only('a')
    assert_that((1, 2, 3, 4)).contains_only(1, 2, 3, 4)
    assert_that((1, 2, 3, 1)).contains_only(1, 2, 3)
    assert_that((1, 2, 2, 1)).contains_only(1, 2)
    assert_that((1, 1, 1, 1)).contains_only(1)
    assert_that('foobar').contains_only('f', 'o', 'b', 'a', 'r')


def test_contains_only_no_args_failure():
    try:
        assert_that([1, 2, 3]).contains_only()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_contains_only_failure():
    try:
        assert_that([1, 2, 3]).contains_only(1, 2)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <[1, 2, 3]> to contain only <1, 2>, but did contain <3>.')


def test_contains_only_multi_failure():
    try:
        assert_that([1, 2, 3]).contains_only(1, 4)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <[1, 2, 3]> to contain only <1, 4>, but did contain <2, 3>.')


def test_contains_only_superlist_failure():
    try:
        assert_that([1, 2, 3]).contains_only(1, 2, 3, 4)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <[1, 2, 3]> to contain only <1, 2, 3, 4>, but did not contain <4>.')


def test_contains_sequence():
    assert_that(['a', 'b', 'c']).contains_sequence('a')
    assert_that(['a', 'b', 'c']).contains_sequence('b')
    assert_that(['a', 'b', 'c']).contains_sequence('c')
    assert_that(['a', 'b', 'c']).contains_sequence('a', 'b')
    assert_that(['a', 'b', 'c']).contains_sequence('b', 'c')
    assert_that(['a', 'b', 'c']).contains_sequence('a', 'b', 'c')
    assert_that((1, 2, 3, 4)).contains_sequence(1)
    assert_that((1, 2, 3, 4)).contains_sequence(2)
    assert_that((1, 2, 3, 4)).contains_sequence(3)
    assert_that((1, 2, 3, 4)).contains_sequence(4)
    assert_that((1, 2, 3, 4)).contains_sequence(1, 2)
    assert_that((1, 2, 3, 4)).contains_sequence(2, 3)
    assert_that((1, 2, 3, 4)).contains_sequence(3, 4)
    assert_that((1, 2, 3, 4)).contains_sequence(1, 2, 3)
    assert_that((1, 2, 3, 4)).contains_sequence(2, 3, 4)
    assert_that((1, 2, 3, 4)).contains_sequence(1, 2, 3, 4)
    assert_that('foobar').contains_sequence('o', 'o', 'b')

    fred = Person('fred')
    joe = Person('joe')
    bob = Person('bob')
    assert_that([fred, joe, bob]).contains_sequence(fred, joe)


def test_contains_sequence_failure():
    try:
        assert_that([1, 2, 3]).contains_sequence(4, 5)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <[1, 2, 3]> to contain sequence <4, 5>, but did not.')


def test_contains_sequence_bad_val_failure():
    try:
        assert_that(123).contains_sequence(1, 2)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not iterable')


def test_contains_sequence_no_args_failure():
    try:
        assert_that([1, 2, 3]).contains_sequence()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_contains_duplicates():
    assert_that(['a', 'b', 'c', 'a']).contains_duplicates()
    assert_that(('a', 'b', 'c', 'a')).contains_duplicates()
    assert_that([1, 2, 3, 3]).contains_duplicates()
    assert_that((1, 2, 3, 3)).contains_duplicates()
    assert_that('foobar').contains_duplicates()

    fred = Person('fred')
    joe = Person('joe')
    assert_that([fred, joe, fred]).contains_duplicates()


def test_contains_duplicates_failure():
    try:
        assert_that([1, 2, 3]).contains_duplicates()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <[1, 2, 3]> to contain duplicates, but did not.')


def test_contains_duplicates_bad_val_failure():
    try:
        assert_that(123).contains_duplicates()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not iterable')


def test_does_not_contain_duplicates():
    assert_that(['a', 'b', 'c']).does_not_contain_duplicates()
    assert_that(('a', 'b', 'c')).does_not_contain_duplicates()
    assert_that(set(['a', 'b', 'c'])).does_not_contain_duplicates()
    assert_that([1, 2, 3]).does_not_contain_duplicates()
    assert_that((1, 2, 3)).does_not_contain_duplicates()
    assert_that(set([1, 2, 3])).does_not_contain_duplicates()
    assert_that('fobar').does_not_contain_duplicates()

    fred = Person('fred')
    joe = Person('joe')
    assert_that([fred, joe]).does_not_contain_duplicates()


def test_does_not_contain_duplicates_failure():
    try:
        assert_that([1, 2, 3, 3]).does_not_contain_duplicates()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <[1, 2, 3, 3]> to not contain duplicates, but did.')


def test_does_not_contain_duplicates_bad_val_failure():
    try:
        assert_that(123).does_not_contain_duplicates()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not iterable')


def test_is_empty():
    assert_that([]).is_empty()
    assert_that(()).is_empty()
    assert_that({}).is_empty()


def test_is_empty_failure():
    try:
        assert_that(['a', 'b']).is_empty()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['a', 'b']> to be empty, but was not.")


def test_is_not_empty():
    assert_that(['a', 'b']).is_not_empty()
    assert_that((1, 2)).is_not_empty()
    assert_that({'a': 1, 'b': 2}).is_not_empty()
    assert_that(set(['a', 'b'])).is_not_empty()


def test_is_not_empty_failure():
    try:
        assert_that([]).is_not_empty()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected not empty, but was empty.')


def test_starts_with():
    assert_that(['a', 'b', 'c']).starts_with('a')
    assert_that((1, 2, 3)).starts_with(1)

    if sys.version_info[0] == 3:
        ordered = collections.OrderedDict([('z', 9), ('x', 7), ('y', 8)])
        assert_that(ordered.keys()).starts_with('z')
        assert_that(ordered.values()).starts_with(9)
        assert_that(ordered.items()).starts_with(('z', 9))


def test_starts_with_failure():
    try:
        assert_that(['a', 'b', 'c']).starts_with('d')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected ['a', 'b', 'c'] to start with <d>, but did not.")


def test_starts_with_bad_val_failure():
    try:
        assert_that([]).starts_with('a')
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val must not be empty')


def test_starts_with_bad_prefix_failure():
    try:
        assert_that(['a', 'b', 'c']).starts_with('a', 'b')
        fail('should have raised error')
    except TypeError as ex:
        if sys.version_info[0] == 3:
            assert_that(str(ex)).contains('starts_with() takes 2 positional arguments but 3 were given')
        else:
            assert_that(str(ex)).contains('starts_with() takes exactly 2 arguments (3 given)')


def test_ends_with():
    assert_that(['a', 'b', 'c']).ends_with('c')
    assert_that((1, 2, 3)).ends_with(3)

    if sys.version_info[0] == 3:
        ordered = collections.OrderedDict([('z', 9), ('x', 7), ('y', 8)])
        assert_that(ordered.keys()).ends_with('y')
        assert_that(ordered.values()).ends_with(8)
        assert_that(ordered.items()).ends_with(('y', 8))


def test_ends_with_failure():
    try:
        assert_that(['a', 'b', 'c']).ends_with('d')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected ['a', 'b', 'c'] to end with <d>, but did not.")


def test_ends_with_bad_val_failure():
    try:
        assert_that([]).ends_with('a')
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val must not be empty')


def test_ends_with_bad_prefix_failure():
    try:
        assert_that(['a', 'b', 'c']).ends_with('b', 'c')
        fail('should have raised error')
    except TypeError as ex:
        if sys.version_info[0] == 3:
            assert_that(str(ex)).contains('ends_with() takes 2 positional arguments but 3 were given')
        else:
            assert_that(str(ex)).contains('ends_with() takes exactly 2 arguments (3 given)')


def test_chaining():
    assert_that(['a', 'b', 'c']).is_type_of(list).is_length(3).contains('a').does_not_contain('x')
    assert_that(['a', 'b', 'c']).is_type_of(list).is_length(3).contains('a', 'b').does_not_contain('x', 'y')


def test_list_of_lists():
    l = [[1, 2, 3], ['a', 'b', 'c'], (4, 5, 6)]
    assert_that(l).is_length(3)
    assert_that(l).is_equal_to([[1, 2, 3], ['a', 'b', 'c'], (4, 5, 6)])

    assert_that(l).contains([1, 2, 3])
    assert_that(l).contains(['a', 'b', 'c'])
    assert_that(l).contains((4, 5, 6))

    assert_that(l).starts_with([1, 2, 3])
    assert_that(l).ends_with((4, 5, 6))

    assert_that(l[0]).is_equal_to([1, 2, 3])
    assert_that(l[2]).is_equal_to((4, 5, 6))

    assert_that(l[0][0]).is_equal_to(1)
    assert_that(l[2][2]).is_equal_to(6)


def test_list_of_dicts():
    l = [{'a': 1}, {'b': 2}, {'c': 3}]
    assert_that(l).is_length(3)
    assert_that(l).is_equal_to([{'a': 1}, {'b': 2}, {'c': 3}])

    assert_that(l).contains({'a': 1})
    assert_that(l).contains({'b': 2})
    assert_that(l).contains({'c': 3})

    assert_that(l).starts_with({'a': 1})
    assert_that(l).ends_with({'c': 3})

    assert_that(l[0]).is_equal_to({'a': 1})
    assert_that(l[2]).is_equal_to({'c': 3})

    assert_that(l[0]['a']).is_equal_to(1)
    assert_that(l[2]['c']).is_equal_to(3)


class Person(object):
    def __init__(self, name):
        self.name = name
