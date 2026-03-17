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


def test_is_in():
    assert_that(1).is_in(1)
    assert_that(1).is_in(1, 2, 3)
    assert_that('foo').is_in('foo', 'bar', 'baz')
    assert_that([1, 2, 3]).is_in([1, 2, 3], [2, 3, 4], [3, 4, 5])


def test_is_in_failure():
    try:
        assert_that(4).is_in(1, 2, 3)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <4> to be in <1, 2, 3>, but was not.')


def test_is_in_missing_arg_failure():
    try:
        assert_that(1).is_in()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')


def test_is_not_in():
    assert_that(4).is_not_in(1)
    assert_that(4).is_not_in(1, 2, 3)
    assert_that('fred').is_not_in('foo', 'bar', 'baz')
    assert_that([4, 4, 4]).is_not_in([1, 2, 3], [2, 3, 4], [3, 4, 5])


def test_is_not_in_failure():
    try:
        assert_that(1).is_not_in(1, 2, 3)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <1> to not be in <1, 2, 3>, but was.')


def test_is_not_in_missing_arg_failure():
    try:
        assert_that(1).is_not_in()
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('one or more args must be given')
