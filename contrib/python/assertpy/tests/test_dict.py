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
    assert_that({'a': 1, 'b': 2}).is_length(2)


def test_is_length_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).is_length(4)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to be of length <4>, but was <3>.')


def test_contains():
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains('a')
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains('a', 'b')

    if sys.version_info[0] == 3:
        ordered = collections.OrderedDict([('z', 9), ('x', 7), ('y', 8)])
        assert_that(ordered).contains('x')


def test_contains_empty_arg_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_contains_single_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains('x')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to contain key <x>, but did not.')


def test_contains_single_item_dict_like_failure():
    if sys.version_info[0] == 3:
        ordered = collections.OrderedDict([('z', 9), ('x', 7), ('y', 8)])
        try:
            assert_that(ordered).contains('a')
            fail('should have raised error')
        except AssertionError as ex:
            assert_that(str(ex)).ends_with('to contain key <a>, but did not.')


def test_contains_multi_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains('a', 'x', 'z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain keys <'a', 'x', 'z'>, but did not contain keys <'x', 'z'>.")


def test_contains_multi_item_single_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains('a', 'b', 'z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain keys <'a', 'b', 'z'>, but did not contain keys <z>.")


def test_contains_only():
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_only('a', 'b', 'c')
    assert_that(set(['a', 'b', 'c'])).contains_only('a', 'b', 'c')


def test_contains_only_failure():
    try:
        assert_that({'a': 1, 'b': 2}).contains_only('a', 'x')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain only <'a', 'x'>, but did contain <b>.")


def test_contains_only_multi_failure():
    try:
        assert_that({'a': 1, 'b': 2}).contains_only('x', 'y')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain only <'x', 'y'>, but did contain <'")


def test_contains_key():
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_key('a')
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_key('a', 'b')

    if sys.version_info[0] == 3:
        ordered = collections.OrderedDict([('z', 9), ('x', 7), ('y', 8)])
        assert_that(ordered).contains_key('x')


def test_contains_key_bad_val_failure():
    try:
        assert_that(123).contains_key(1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('is not dict-like')


def test_does_not_contain_key():
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_key('x')
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_key('x', 'y')


def test_does_not_contain_key_bad_val_failure():
    try:
        assert_that(123).does_not_contain_key(1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('is not dict-like')


def test_contains_key_single_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_key('x')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).ends_with('to contain key <x>, but did not.')


def test_contains_key_multi_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_key('a', 'x', 'z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).ends_with("to contain keys <'a', 'x', 'z'>, but did not contain keys <'x', 'z'>.")


def test_does_not_contain():
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain('x')
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain('x', 'y')


def test_does_not_contain_empty_arg_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_does_not_contain_single_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain('a')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to not contain item <a>, but did.')


def test_does_not_contain_list_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain('x', 'y', 'a')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to not contain items <'x', 'y', 'a'>, but did contain <a>.")


def test_is_empty():
    assert_that({}).is_empty()


def test_is_empty_failure():
    try:
        assert_that({'a': 1, 'b': 2}).is_empty()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to be empty, but was not.')


def test_is_not_empty():
    assert_that({'a': 1, 'b': 2}).is_not_empty()
    assert_that(set(['a', 'b'])).is_not_empty()


def test_is_not_empty_failure():
    try:
        assert_that({}).is_not_empty()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected not empty, but was empty.')


def test_contains_value():
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_value(1)
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_value(1, 2)


def test_contains_value_empty_arg_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_value()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more value args must be given')


def test_contains_value_bad_val_failure():
    try:
        assert_that('foo').contains_value('x')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('is not dict-like')


def test_contains_value_single_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_value(4)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to contain values <4>, but did not contain <4>.')


def test_contains_value_multi_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_value(1, 4, 5)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to contain values <1, 4, 5>, but did not contain <4, 5>.')


def test_does_not_contain_value():
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_value(4)
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_value(4, 5)


def test_does_not_contain_value_bad_val_failure():
    try:
        assert_that(123).does_not_contain_value(1)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('is not dict-like')


def test_does_not_contain_value_empty_arg_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_value()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more value args must be given')


def test_does_not_contain_value_single_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_value(1)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to not contain values <1>, but did contain <1>.')


def test_does_not_contain_value_list_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_value(4, 5, 1)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to not contain values <4, 5, 1>, but did contain <1>.')


def test_does_not_contain_value_list_multi_item_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_value(4, 1, 2)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('to not contain values <4, 1, 2>, but did contain <1, 2>.')


def test_contains_entry():
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1})
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, {'b': 2})
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, {'b': 2}, {'c': 3})
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry(a=1)
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry(a=1, b=2)
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry(a=1, b=2, c=3)
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, b=2)
    assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'b': 2}, a=1, c=3)


def test_contains_entry_bad_val_failure():
    try:
        assert_that('foo').contains_entry({'a': 1})
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('is not dict-like')


def test_contains_entry_empty_arg_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more entry args must be given')


def test_contains_entry_bad_arg_type_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry('x')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given entry arg must be a dict')


def test_contains_entry_bad_arg_too_big_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1, 'b': 2})
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given entry args must contain exactly one key-value pair')


def test_contains_entry_bad_key_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'x': 1})
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain entries <{'x': 1}>, but did not contain <{'x': 1}>.")


def test_contains_entry_bad_value_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 2})
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain entries <{'a': 2}>, but did not contain <{'a': 2}>.")


def test_contains_entry_bad_keys_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, {'x': 2})
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain entries <{'a': 1}, {'x': 2}>, but did not contain <{'x': 2}>.")


def test_contains_entry_bad_values_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).contains_entry({'a': 1}, {'b': 4})
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to contain entries <{'a': 1}, {'b': 4}>, but did not contain <{'b': 4}>.")


def test_does_not_contain_entry():
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 2})
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 2}, {'b': 1})
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry(a=2)
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry(a=2, b=3)
    assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'x': 4}, y=5, z=6)


def test_does_not_contain_entry_bad_val_failure():
    try:
        assert_that('foo').does_not_contain_entry({'a': 1})
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).contains('is not dict-like')


def test_does_not_contain_entry_empty_arg_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more entry args must be given')


def test_does_not_contain_entry_bad_arg_type_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry('x')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given entry arg must be a dict')


def test_does_not_contain_entry_bad_arg_too_big_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 1, 'b': 2})
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given entry args must contain exactly one key-value pair')


def test_does_not_contain_entry_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 1})
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to not contain entries <{'a': 1}>, but did contain <{'a': 1}>.")


def test_does_not_contain_entry_multiple_failure():
    try:
        assert_that({'a': 1, 'b': 2, 'c': 3}).does_not_contain_entry({'a': 2}, {'b': 2})
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains("to not contain entries <{'a': 2}, {'b': 2}>, but did contain <{'b': 2}>.")


def test_dynamic_assertion():
    fred = {'first_name': 'Fred', 'last_name': 'Smith', 'shoe_size': 12}
    assert_that(fred).is_type_of(dict)

    assert_that(fred['first_name']).is_equal_to('Fred')
    assert_that(fred['last_name']).is_equal_to('Smith')
    assert_that(fred['shoe_size']).is_equal_to(12)

    assert_that(fred).has_first_name('Fred')
    assert_that(fred).has_last_name('Smith')
    assert_that(fred).has_shoe_size(12)


def test_dynamic_assertion_failure_str():
    fred = {'first_name': 'Fred', 'last_name': 'Smith', 'shoe_size': 12}

    try:
        assert_that(fred).has_first_name('Foo')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('Expected <Fred> to be equal to <Foo> on key <first_name>, but was not.')


def test_dynamic_assertion_failure_int():
    fred = {'first_name': 'Fred', 'last_name': 'Smith', 'shoe_size': 12}

    try:
        assert_that(fred).has_shoe_size(34)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).contains('Expected <12> to be equal to <34> on key <shoe_size>, but was not.')


def test_dynamic_assertion_bad_key_failure():
    fred = {'first_name': 'Fred', 'last_name': 'Smith', 'shoe_size': 12}

    try:
        assert_that(fred).has_foo('Fred')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected key <foo>, but val has no key <foo>.')

def test_dynamic_assertion_on_reserved_word():
    fred = {'def': 'Fred'}
    assert_that(fred).is_type_of(dict)
    assert_that(fred['def']).is_equal_to('Fred')
    assert_that(fred).has_def('Fred')

def test_dynamic_assertion_on_dict_method():
    fred = {'update': 'Foo'}
    fred.update({'update': 'Fred'})
    assert_that(fred).is_type_of(dict)
    assert_that(fred['update']).is_equal_to('Fred')
    assert_that(fred).has_update('Fred')
