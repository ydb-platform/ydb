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

from assertpy import assert_that, fail

import sys

if sys.version_info[0] == 3:
    unicode = str
else:
    unicode = unicode


def test_is_length():
    assert_that('foo').is_length(3)


def test_is_length_failure():
    try:
        assert_that('foo').is_length(4)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to be of length <4>, but was <3>.')


def test_contains():
    assert_that('foo').contains('f')
    assert_that('foo').contains('o')
    assert_that('foo').contains('fo', 'o')
    assert_that('fred').contains('d')
    assert_that('fred').contains('fr', 'e', 'd')


def test_contains_single_item_failure():
    try:
        assert_that('foo').contains('x')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to contain item <x>, but did not.')


def test_contains_multi_item_failure():
    try:
        assert_that('foo').contains('f', 'x', 'z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <foo> to contain items <'f', 'x', 'z'>, but did not contain <'x', 'z'>.")


def test_contains_multi_item_single_failure():
    try:
        assert_that('foo').contains('f', 'o', 'x')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <foo> to contain items <'f', 'o', 'x'>, but did not contain <x>.")


def test_contains_ignoring_case():
    assert_that('foo').contains_ignoring_case('f')
    assert_that('foo').contains_ignoring_case('F')
    assert_that('foo').contains_ignoring_case('Oo')
    assert_that('foo').contains_ignoring_case('f', 'o', 'F', 'O', 'Fo', 'Oo', 'FoO')


def test_contains_ignoring_case_type_failure():
    try:
        assert_that(123).contains_ignoring_case('f')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string or iterable')


def test_contains_ignoring_case_missinge_item_failure():
    try:
        assert_that('foo').contains_ignoring_case()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_contains_ignoring_case_single_item_failure():
    try:
        assert_that('foo').contains_ignoring_case('X')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to case-insensitive contain item <X>, but did not.')


def test_contains_ignoring_case_single_item_type_failure():
    try:
        assert_that('foo').contains_ignoring_case(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a string')


def test_contains_ignoring_case_multi_item_failure():
    try:
        assert_that('foo').contains_ignoring_case('F', 'X', 'Z')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <foo> to case-insensitive contain items <'F', 'X', 'Z'>, but did not contain <'X', 'Z'>.")


def test_contains_ignoring_case_multi_item_type_failure():
    try:
        assert_that('foo').contains_ignoring_case('F', 12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given args must all be strings')


def test_contains_ignoring_case_list():
    assert_that(['foo']).contains_ignoring_case('Foo')
    assert_that(['foo', 'bar', 'baz']).contains_ignoring_case('Foo')
    assert_that(['foo', 'bar', 'baz']).contains_ignoring_case('Foo', 'bAr')
    assert_that(['foo', 'bar', 'baz']).contains_ignoring_case('Foo', 'bAr', 'baZ')


def test_contains_ignoring_case_list_elem_type_failure():
    try:
        assert_that([123]).contains_ignoring_case('f')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val items must all be strings')


def test_contains_ignoring_case_list_multi_elem_type_failure():
    try:
        assert_that(['foo', 123]).contains_ignoring_case('f')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val items must all be strings')


def test_contains_ignoring_case_list_missinge_item_failure():
    try:
        assert_that(['foo']).contains_ignoring_case()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_contains_ignoring_case_list_single_item_failure():
    try:
        assert_that(['foo']).contains_ignoring_case('X')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['foo']> to case-insensitive contain items <X>, but did not contain <X>.")


def test_contains_ignoring_case_list_single_item_type_failure():
    try:
        assert_that(['foo']).contains_ignoring_case(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given args must all be strings')


def test_contains_ignoring_case_list_multi_item_failure():
    try:
        assert_that(['foo', 'bar']).contains_ignoring_case('Foo', 'X', 'Y')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <['foo', 'bar']> to case-insensitive contain items <'Foo', 'X', 'Y'>, but did not contain <'X', 'Y'>.")


def test_contains_ignoring_case_list_multi_item_type_failure():
    try:
        assert_that(['foo', 'bar']).contains_ignoring_case('F', 12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given args must all be strings')


def test_does_not_contain():
    assert_that('foo').does_not_contain('x')
    assert_that('foo').does_not_contain('x', 'y')


def test_does_not_contain_single_item_failure():
    try:
        assert_that('foo').does_not_contain('f')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to not contain item <f>, but did.')


def test_does_not_contain_list_item_failure():
    try:
        assert_that('foo').does_not_contain('x', 'y', 'f')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <foo> to not contain items <'x', 'y', 'f'>, but did contain <f>.")


def test_does_not_contain_list_multi_item_failure():
    try:
        assert_that('foo').does_not_contain('x', 'f', 'o')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to("Expected <foo> to not contain items <'x', 'f', 'o'>, but did contain <'f', 'o'>.")


def test_is_empty():
    assert_that('').is_empty()


def test_is_empty_failure():
    try:
        assert_that('foo').is_empty()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to be empty string, but was not.')


def test_is_not_empty():
    assert_that('foo').is_not_empty()


def test_is_not_empty_failure():
    try:
        assert_that('').is_not_empty()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected not empty string, but was empty.')


def test_is_equal_ignoring_case():
    assert_that('FOO').is_equal_to_ignoring_case('foo')
    assert_that('foo').is_equal_to_ignoring_case('FOO')
    assert_that('fOO').is_equal_to_ignoring_case('foo')


def test_is_equal_ignoring_case_failure():
    try:
        assert_that('foo').is_equal_to_ignoring_case('bar')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to be case-insensitive equal to <bar>, but was not.')


def test_is_equal_ignoring_case_bad_value_type_failure():
    try:
        assert_that(123).is_equal_to_ignoring_case(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_is_equal_ignoring_case_bad_arg_type_failure():
    try:
        assert_that('fred').is_equal_to_ignoring_case(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a string')


def test_starts_with():
    assert_that('fred').starts_with('f')
    assert_that('fred').starts_with('fr')
    assert_that('fred').starts_with('fred')


def test_starts_with_failure():
    try:
        assert_that('fred').starts_with('bar')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <fred> to start with <bar>, but did not.')


def test_starts_with_bad_value_type_failure():
    try:
        assert_that(123).starts_with(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string or iterable')


def test_starts_with_bad_arg_none_failure():
    try:
        assert_that('fred').starts_with(None)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given prefix arg must not be none')


def test_starts_with_bad_arg_type_failure():
    try:
        assert_that('fred').starts_with(123)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given prefix arg must be a string')


def test_starts_with_bad_arg_empty_failure():
    try:
        assert_that('fred').starts_with('')
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given prefix arg must not be empty')


def test_ends_with():
    assert_that('fred').ends_with('d')
    assert_that('fred').ends_with('ed')
    assert_that('fred').ends_with('fred')


def test_ends_with_failure():
    try:
        assert_that('fred').ends_with('bar')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <fred> to end with <bar>, but did not.')


def test_ends_with_bad_value_type_failure():
    try:
        assert_that(123).ends_with(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string or iterable')


def test_ends_with_bad_arg_none_failure():
    try:
        assert_that('fred').ends_with(None)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given suffix arg must not be none')


def test_ends_with_bad_arg_type_failure():
    try:
        assert_that('fred').ends_with(123)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given suffix arg must be a string')


def test_ends_with_bad_arg_empty_failure():
    try:
        assert_that('fred').ends_with('')
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given suffix arg must not be empty')


def test_matches():
    assert_that('fred').matches(r'\w')
    assert_that('fred').matches(r'\w{2}')
    assert_that('fred').matches(r'\w+')
    assert_that('fred').matches(r'^\w{4}$')
    assert_that('fred').matches(r'^.*?$')
    assert_that('123-456-7890').matches(r'\d{3}-\d{3}-\d{4}')


def test_matches_failure():
    try:
        assert_that('fred').matches(r'\d+')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <fred> to match pattern <\\d+>, but did not.')


def test_matches_bad_value_type_failure():
    try:
        assert_that(123).matches(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_matches_bad_arg_type_failure():
    try:
        assert_that('fred').matches(123)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given pattern arg must be a string')


def test_matches_bad_arg_empty_failure():
    try:
        assert_that('fred').matches('')
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given pattern arg must not be empty')


def test_does_not_match():
    assert_that('fred').does_not_match(r'\d+')
    assert_that('fred').does_not_match(r'\w{5}')
    assert_that('123-456-7890').does_not_match(r'^\d+$')


def test_does_not_match_failure():
    try:
        assert_that('fred').does_not_match(r'\w+')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <fred> to not match pattern <\\w+>, but did.')


def test_does_not_match_bad_value_type_failure():
    try:
        assert_that(123).does_not_match(12)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_does_not_match_bad_arg_type_failure():
    try:
        assert_that('fred').does_not_match(123)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given pattern arg must be a string')


def test_does_not_match_bad_arg_empty_failure():
    try:
        assert_that('fred').does_not_match('')
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('given pattern arg must not be empty')


def test_is_alpha():
    assert_that('foo').is_alpha()


def test_is_alpha_digit_failure():
    try:
        assert_that('foo123').is_alpha()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo123> to contain only alphabetic chars, but did not.')


def test_is_alpha_space_failure():
    try:
        assert_that('foo bar').is_alpha()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo bar> to contain only alphabetic chars, but did not.')


def test_is_alpha_punctuation_failure():
    try:
        assert_that('foo,bar').is_alpha()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo,bar> to contain only alphabetic chars, but did not.')


def test_is_alpha_bad_value_type_failure():
    try:
        assert_that(123).is_alpha()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_is_alpha_empty_value_failure():
    try:
        assert_that('').is_alpha()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val is empty')


def test_is_digit():
    assert_that('123').is_digit()


def test_is_digit_alpha_failure():
    try:
        assert_that('foo123').is_digit()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo123> to contain only digits, but did not.')


def test_is_digit_space_failure():
    try:
        assert_that('1 000 000').is_digit()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <1 000 000> to contain only digits, but did not.')


def test_is_digit_punctuation_failure():
    try:
        assert_that('-123').is_digit()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <-123> to contain only digits, but did not.')


def test_is_digit_bad_value_type_failure():
    try:
        assert_that(123).is_digit()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_is_digit_empty_value_failure():
    try:
        assert_that('').is_digit()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val is empty')


def test_is_lower():
    assert_that('foo').is_lower()
    assert_that('foo 123').is_lower()
    assert_that('123 456').is_lower()


def test_is_lower_failure():
    try:
        assert_that('FOO').is_lower()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <FOO> to contain only lowercase chars, but did not.')


def test_is_lower_bad_value_type_failure():
    try:
        assert_that(123).is_lower()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_is_lower_empty_value_failure():
    try:
        assert_that('').is_lower()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val is empty')


def test_is_upper():
    assert_that('FOO').is_upper()
    assert_that('FOO 123').is_upper()
    assert_that('123 456').is_upper()


def test_is_upper_failure():
    try:
        assert_that('foo').is_upper()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to contain only uppercase chars, but did not.')


def test_is_upper_bad_value_type_failure():
    try:
        assert_that(123).is_upper()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a string')


def test_is_upper_empty_value_failure():
    try:
        assert_that('').is_upper()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val is empty')


def test_is_unicode():
    assert_that(unicode('unicorn')).is_unicode()
    assert_that(unicode('unicorn 123')).is_unicode()
    assert_that(unicode('unicorn')).is_unicode()


def test_is_unicode_failure():
    try:
        assert_that(123).is_unicode()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <123> to be unicode, but was <int>.')


def test_chaining():
    assert_that('foo').is_type_of(str).is_length(3).contains('f').does_not_contain('x')
    assert_that('fred').starts_with('f').ends_with('d').matches(r'^f.*?d$').does_not_match(r'\d')
