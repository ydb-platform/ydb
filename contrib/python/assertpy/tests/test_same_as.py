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
from assertpy import assert_that, fail


def test_is_same_as():
    for obj in [object(), 1, 'foo', True, None, 123.456]:
        assert_that(obj).is_same_as(obj)


def test_is_same_as_failure():
    try:
        obj = object()
        other = object()
        assert_that(obj).is_same_as(other)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).matches('Expected <.+> to be identical to <.+>, but was not.')


def test_is_not_same_as():
    obj = object()
    other = object()
    assert_that(obj).is_not_same_as(other)
    assert_that(obj).is_not_same_as(1)
    assert_that(obj).is_not_same_as(True)
    assert_that(1).is_not_same_as(2)

    assert_that({'a': 1}).is_not_same_as({'a': 1})
    assert_that([1, 2, 3]).is_not_same_as([1, 2, 3])

    if sys.version_info[0] == 3 and sys.version_info[1] >= 7:
        assert_that((1, 2, 3)).is_same_as((1, 2, 3))  # tuples are identical in py 3.7
    else:
        assert_that((1, 2, 3)).is_not_same_as((1, 2, 3))


def test_is_not_same_as_failure():
    for obj in [object(), 1, 'foo', True, None, 123.456]:
        try:
            assert_that(obj).is_not_same_as(obj)
            fail('should have raised error')
        except AssertionError as ex:
            assert_that(str(ex)).matches('Expected <.+> to be not identical to <.+>, but was.')
