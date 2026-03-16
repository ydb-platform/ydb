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


class CustomList(object):

    def __init__(self, s):
        self._s = s
        self._idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            result = self._s[self._idx]
        except IndexError:
            raise StopIteration
        self._idx += 1
        return result

    def __getitem__(self, idx):
        return self._s[idx]


def test_custom_list():
    l = CustomList('foobar')
    assert_that([CustomList('foo'), CustomList('bar')]).extracting(0, -1).is_equal_to([('f', 'o'), ('b', 'r')])


def test_check_iterable():
    l = CustomList('foobar')
    ab = assert_that(None)
    ab._check_iterable(l)
    ab._check_iterable(l, check_getitem=True)
    ab._check_iterable(l, check_getitem=False)


def test_check_iterable_not_iterable():
    try:
        ab = assert_that(None)
        ab._check_iterable(123, name='my-int')
        fail('should have raised error')
    except TypeError as e:
        assert_that(str(e)).contains('my-int <int> is not iterable')


def test_check_iterable_no_getitem():
    try:
        ab = assert_that(None)
        ab._check_iterable(set([1]), name='my-set')
        fail('should have raised error')
    except TypeError as e:
        assert_that(str(e)).contains('my-set <set> does not have [] accessor')
