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


def test_constructor():
    try:
        assert_that(1, 'extra msg').is_equal_to(2)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('[extra msg] Expected <1> to be equal to <2>, but was not.')


def test_described_as():
    try:
        assert_that(1).described_as('extra msg').is_equal_to(2)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('[extra msg] Expected <1> to be equal to <2>, but was not.')


def test_described_as_double():
    try:
        assert_that(1).described_as('extra msg').described_as('other msg').is_equal_to(2)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('[other msg] Expected <1> to be equal to <2>, but was not.')


def test_described_as_chained():
    try:
        assert_that(1).described_as('extra msg').is_equal_to(1).described_as('other msg').is_equal_to(1).described_as('last msg').is_equal_to(2)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('[last msg] Expected <1> to be equal to <2>, but was not.')
