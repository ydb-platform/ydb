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

import os
import sys
import shutil
import datetime
import collections

import pytest
from assertpy import assert_that, fail

if sys.version_info[0] < 3:
    def test_snapshot_v2():
        try:
            assert_that(None).snapshot()
            fail('should have raised error')
        except NotImplementedError as ex:
            assert_that(str(ex)).is_equal_to('snapshot testing requires Python 3')


if sys.version_info[0] == 3:
    @pytest.mark.parametrize('count', [1, 2])
    def test_snapshot_v3(count):
        # test runs twice
        if count == 1:
            # on first pass, delete old snapshots...so they are re-created and saved
            if os.path.exists('__snapshots'):
                shutil.rmtree('__snapshots')
        if count == 2:
            # on second pass, snapshots are loaded and checked
            assert_that('__snapshots').exists().is_directory()

        assert_that(None).snapshot()

        assert_that(True).snapshot()
        assert_that(False).snapshot()

        assert_that(123).snapshot()
        assert_that(-456).snapshot()

        assert_that(123.456).snapshot()
        assert_that(-987.654).snapshot()

        assert_that('').snapshot()
        assert_that('foo').snapshot()

        assert_that([1, 2, 3]).snapshot()

        assert_that(['a', 'b', 'c']).snapshot()

        assert_that([[1, 2, 3], ['a', 'b', 'c']]).snapshot()

        assert_that(set(['a', 'b', 'c', 'a'])).snapshot()

        assert_that({'a': 1, 'b': 2, 'c': 3}).snapshot()

        assert_that({'a': {'x': 1}, 'b': {'y': 2}, 'c': {'z': 3}}).snapshot()

        assert_that({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]}).snapshot()

        assert_that({'a': set([1, 2]), 'b': set([3, 4]), 'c': set([5, 6])}).snapshot()

        assert_that({'a': {'b': {'c': {'x': {'y': {'z': 1}}}}}}).snapshot()

        assert_that(collections.OrderedDict([('a', 1), ('c', 3), ('b', 2)])).snapshot()

        assert_that(datetime.datetime(2000, 11, 22, 3, 44, 55)).snapshot()

        assert_that(1 + 2j).snapshot()

        # tuples are always converted to lists...can this be fixed?
        # assert_that((1, 2, 3)).snapshot()
        # assert_that({'a': (1,2), 'b': (3,4), 'c': (5,6)}).snapshot()

        assert_that({'custom': 'id'}).snapshot(id='mycustomid')

        assert_that({'custom': 'path'}).snapshot(path='mycustompath')

        foo = Foo()
        foo2 = Foo({
            'a': 1,
            'b': [1, 2, 3],
            'c': {'x': 1, 'y': 2, 'z': 3},
            'd': set([-1, 2, -3]),
            'e': datetime.datetime(2000, 11, 22, 3, 44, 55),
            'f': -1 - 2j
        })
        bar = Bar()

        assert_that(foo.x).is_equal_to(0)
        assert_that(foo.y).is_equal_to(1)

        assert_that(foo2.x['a']).is_equal_to(1)
        assert_that(foo2.x['b']).is_equal_to([1, 2, 3])
        assert_that(foo2.y).is_equal_to(1)

        assert_that(bar.x).is_equal_to(0)
        assert_that(bar.y).is_equal_to(1)

        assert_that(foo).snapshot()
        assert_that(foo2).snapshot()

        try:
            assert_that(bar).snapshot()
            if count == 2:
                fail('should have raised error')
        except AssertionError as ex:
            assert_that(str(ex)).contains('Expected ').contains(' to be equal to ').contains('test_snapshots.Bar').contains(', but was not.')

        assert_that({
            'none': None,
            'truthy': True,
            'falsy': False,
            'int': 123,
            'intneg': -456,
            'float': 123.456,
            'floatneg': -987.654,
            'empty': '',
            'str': 'foo',
            'list': [1, 2, 3],
            'liststr': ['a', 'b', 'c'],
            'listmix': [1, 'a', [2, 4, 6], set([1, 2, 3]), 3+6j],
            'set': set([1, 2, 3]),
            'dict': {'a': 1, 'b': 2, 'c': 3},
            'time': datetime.datetime(2000, 11, 22, 3, 44, 55),
            'complex': 1 + 2j,
            'foo': foo,
            'foo2': foo2
        }).snapshot()

        assert_that({'__type__': 'foo', '__data__': 'bar'}).snapshot()

    # def test_snapshot_not_serializable():
    #     try:
    #         assert_that(range(5)).snapshot()
    #         fail('should have raised error')
    #     except TypeError as ex:
    #         assert_that(str(ex)).ends_with('is not JSON serializable')

    def test_snapshot_custom_id_int():
        try:
            assert_that('foo').snapshot(id=123)
            fail('should have raised error')
        except ValueError as ex:
            assert_that(str(ex)).starts_with('failed to create snapshot filename')

    def test_snapshot_custom_path_none():
        try:
            assert_that('foo').snapshot(path=None)
            fail('should have raised error')
        except ValueError as ex:
            assert_that(str(ex)).starts_with('failed to create snapshot filename')

    class Foo(object):
        def __init__(self, x=0):
            self.x = x
            self.y = 1

        def __eq__(self, other):
            if isinstance(self, other.__class__):
                return self.__dict__ == other.__dict__
            return NotImplemented

    class Bar(Foo):
        def __eq__(self, other):
            return NotImplemented
