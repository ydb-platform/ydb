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


def test_is_true():
    assert_that(True).is_true()
    assert_that(1 == 1).is_true()
    assert_that(1).is_true()
    assert_that('a').is_true()
    assert_that([1]).is_true()
    assert_that({'a': 1}).is_true()


def test_is_true_failure():
    try:
        assert_that(False).is_true()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <True>, but was not.')


def test_is_false():
    assert_that(False).is_false()
    assert_that(1 == 2).is_false()
    assert_that(0).is_false()
    assert_that([]).is_false()
    assert_that({}).is_false()
    assert_that(()).is_false()


def test_is_false_failure():
    try:
        assert_that(True).is_false()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <False>, but was not.')
