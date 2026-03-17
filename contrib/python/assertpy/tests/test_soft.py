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

from assertpy import assert_that, soft_assertions, fail


def test_success():
    with soft_assertions():
        assert_that('foo').is_length(3)
        assert_that('foo').is_not_empty()
        assert_that('foo').is_true()
        assert_that('foo').is_alpha()
        assert_that('123').is_digit()
        assert_that('foo').is_lower()
        assert_that('FOO').is_upper()
        assert_that('foo').is_equal_to('foo')
        assert_that('foo').is_not_equal_to('bar')
        assert_that('foo').is_equal_to_ignoring_case('FOO')
        assert_that({'a': 1}).has_a(1)


def test_failure():
    try:
        with soft_assertions():
            assert_that('foo').is_length(4)
            assert_that('foo').is_empty()
            assert_that('foo').is_false()
            assert_that('foo').is_digit()
            assert_that('123').is_alpha()
            assert_that('foo').is_upper()
            assert_that('FOO').is_lower()
            assert_that('foo').is_equal_to('bar')
            assert_that('foo').is_not_equal_to('foo')
            assert_that('foo').is_equal_to_ignoring_case('BAR')
            assert_that({'a': 1}).has_a(2)
            assert_that({'a': 1}).has_foo(1)
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Expected <foo> to be of length <4>, but was <3>.')
        assert_that(out).contains('Expected <foo> to be empty string, but was not.')
        assert_that(out).contains('Expected <False>, but was not.')
        assert_that(out).contains('Expected <foo> to contain only digits, but did not.')
        assert_that(out).contains('Expected <123> to contain only alphabetic chars, but did not.')
        assert_that(out).contains('Expected <foo> to contain only uppercase chars, but did not.')
        assert_that(out).contains('Expected <FOO> to contain only lowercase chars, but did not.')
        assert_that(out).contains('Expected <foo> to be equal to <bar>, but was not.')
        assert_that(out).contains('Expected <foo> to be not equal to <foo>, but was.')
        assert_that(out).contains('Expected <foo> to be case-insensitive equal to <BAR>, but was not.')
        assert_that(out).contains('Expected <1> to be equal to <2> on key <a>, but was not.')
        assert_that(out).contains('Expected key <foo>, but val has no key <foo>.')


def test_failure_chain():
    try:
        with soft_assertions():
            assert_that('foo').is_length(4).is_empty().is_false().is_digit().is_upper() \
                .is_equal_to('bar').is_not_equal_to('foo').is_equal_to_ignoring_case('BAR')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Expected <foo> to be of length <4>, but was <3>.')
        assert_that(out).contains('Expected <foo> to be empty string, but was not.')
        assert_that(out).contains('Expected <False>, but was not.')
        assert_that(out).contains('Expected <foo> to contain only digits, but did not.')
        assert_that(out).contains('Expected <foo> to contain only uppercase chars, but did not.')
        assert_that(out).contains('Expected <foo> to be equal to <bar>, but was not.')
        assert_that(out).contains('Expected <foo> to be not equal to <foo>, but was.')
        assert_that(out).contains('Expected <foo> to be case-insensitive equal to <BAR>, but was not.')


def test_expected_exception_success():
    with soft_assertions():
        assert_that(func_err).raises(RuntimeError).when_called_with('foo').is_equal_to('err')


def test_expected_exception_failure():
    try:
        with soft_assertions():
            assert_that(func_err).raises(RuntimeError).when_called_with('foo').is_equal_to('bar')
            assert_that(func_ok).raises(RuntimeError).when_called_with('baz')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Expected <err> to be equal to <bar>, but was not.')
        assert_that(out).contains("Expected <func_ok> to raise <RuntimeError> when called with ('baz').")


def func_ok(arg):
    pass


def func_err(arg):
    raise RuntimeError('err')


def test_fail():
    try:
        with soft_assertions():
            fail()
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).is_equal_to('Fail!')


def test_fail_with_msg():
    try:
        with soft_assertions():
            fail('foobar')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).is_equal_to('Fail: foobar!')


def test_fail_with_soft_failing_asserts():
    try:
        with soft_assertions():
            assert_that('foo').is_length(4)
            assert_that('foo').is_empty()
            fail('foobar')
            assert_that('foo').is_not_equal_to('foo')
            assert_that('foo').is_equal_to_ignoring_case('BAR')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).is_equal_to('Fail: foobar!')
        assert_that(out).does_not_contain('Expected <foo> to be of length <4>, but was <3>.')
        assert_that(out).does_not_contain('Expected <foo> to be empty string, but was not.')
        assert_that(out).does_not_contain('Expected <foo> to be not equal to <foo>, but was.')
        assert_that(out).does_not_contain('Expected <foo> to be case-insensitive equal to <BAR>, but was not.')


def test_double_fail():
    try:
        with soft_assertions():
            fail()
            fail('foobar')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).is_equal_to('Fail!')


def test_nested():
    try:
        with soft_assertions():
            assert_that('a').is_equal_to('A')
            with soft_assertions():
                assert_that('b').is_equal_to('B')
                with soft_assertions():
                    assert_that('c').is_equal_to('C')
                assert_that('b').is_equal_to('B2')
            assert_that('a').is_equal_to('A2')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('1. Expected <a> to be equal to <A>, but was not.')
        assert_that(out).contains('2. Expected <b> to be equal to <B>, but was not.')
        assert_that(out).contains('3. Expected <c> to be equal to <C>, but was not.')
        assert_that(out).contains('4. Expected <b> to be equal to <B2>, but was not.')
        assert_that(out).contains('5. Expected <a> to be equal to <A2>, but was not.')


def test_recursive_nesting():
    def recurs(i):
        if i <= 0:
            return
        with soft_assertions():
            recurs(i-1)
            assert_that(i).is_equal_to(7)
    try:
        recurs(10)
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('1. Expected <1> to be equal to <7>, but was not.')
        assert_that(out).contains('2. Expected <2> to be equal to <7>, but was not.')
        assert_that(out).contains('3. Expected <3> to be equal to <7>, but was not.')
        assert_that(out).contains('4. Expected <4> to be equal to <7>, but was not.')
        assert_that(out).contains('5. Expected <5> to be equal to <7>, but was not.')
        assert_that(out).contains('6. Expected <6> to be equal to <7>, but was not.')
