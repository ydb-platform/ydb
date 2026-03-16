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

from assertpy import assert_that, fail, soft_fail, soft_assertions


def test_soft_fail_without_context():
    try:
        soft_fail()
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).is_equal_to('Fail!')
        assert_that(out).does_not_contain('should have raised error')


def test_soft_fail_with_msg_without_context():
    try:
        soft_fail('some msg')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).is_equal_to('Fail: some msg!')
        assert_that(out).does_not_contain('should have raised error')


def test_soft_fail():
    try:
        with soft_assertions():
            soft_fail()
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Fail!')
        assert_that(out).does_not_contain('should have raised error')


def test_soft_fail_with_msg():
    try:
        with soft_assertions():
            soft_fail('foobar')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Fail: foobar!')
        assert_that(out).does_not_contain('should have raised error')


def test_soft_fail_with_soft_failing_asserts():
    try:
        with soft_assertions():
            assert_that('foo').is_length(4)
            assert_that('foo').is_empty()
            soft_fail('foobar')
            assert_that('foo').is_not_equal_to('foo')
            assert_that('foo').is_equal_to_ignoring_case('BAR')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Expected <foo> to be of length <4>, but was <3>.')
        assert_that(out).contains('Expected <foo> to be empty string, but was not.')
        assert_that(out).contains('Fail: foobar!')
        assert_that(out).contains('Expected <foo> to be not equal to <foo>, but was.')
        assert_that(out).contains('Expected <foo> to be case-insensitive equal to <BAR>, but was not.')
        assert_that(out).does_not_contain('should have raised error')


def test_double_soft_fail():
    try:
        with soft_assertions():
            soft_fail()
            soft_fail('foobar')
        fail('should have raised error')
    except AssertionError as e:
        out = str(e)
        assert_that(out).contains('Fail!')
        assert_that(out).contains('Fail: foobar!')
        assert_that(out).does_not_contain('should have raised error')
