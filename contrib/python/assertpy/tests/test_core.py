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

from assertpy import assert_that


def test_fmt_items_empty():
    ab = assert_that(None)
    assert_that(ab._fmt_items([])).is_equal_to('<>')


def test_fmt_items_single():
    ab = assert_that(None)
    assert_that(ab._fmt_items([1])).is_equal_to('<1>')
    assert_that(ab._fmt_items(['foo'])).is_equal_to('<foo>')


def test_fmt_items_multiple():
    ab = assert_that(None)
    assert_that(ab._fmt_items([1, 2, 3])).is_equal_to('<1, 2, 3>')
    assert_that(ab._fmt_items(['a', 'b', 'c'])).is_equal_to("<'a', 'b', 'c'>")


def test_fmt_args_kwargs_empty():
    ab = assert_that(None)
    assert_that(ab._fmt_args_kwargs()).is_equal_to('')


def test_fmt_args_kwargs_single_arg():
    ab = assert_that(None)
    assert_that(ab._fmt_args_kwargs(1)).is_equal_to('1')
    assert_that(ab._fmt_args_kwargs('foo')).is_equal_to("'foo'")


def test_fmt_args_kwargs_multiple_args():
    ab = assert_that(None)
    assert_that(ab._fmt_args_kwargs(1, 2, 3)).is_equal_to('1, 2, 3')
    assert_that(ab._fmt_args_kwargs('a', 'b', 'c')).is_equal_to("'a', 'b', 'c'")


def test_fmt_args_kwargs_single_kwarg():
    ab = assert_that(None)
    assert_that(ab._fmt_args_kwargs(a=1)).is_equal_to("'a': 1")
    assert_that(ab._fmt_args_kwargs(f='foo')).is_equal_to("'f': 'foo'")


def test_fmt_args_kwargs_multiple_kwargs():
    ab = assert_that(None)
    assert_that(ab._fmt_args_kwargs(a=1, b=2, c=3)).is_equal_to("'a': 1, 'b': 2, 'c': 3")
    assert_that(ab._fmt_args_kwargs(a='a', b='b', c='c')).is_equal_to("'a': 'a', 'b': 'b', 'c': 'c'")


def test_fmt_args_kwargs_multiple_both():
    ab = assert_that(None)
    assert_that(ab._fmt_args_kwargs(1, 2, 3, a=4, b=5, c=6)).is_equal_to("1, 2, 3, 'a': 4, 'b': 5, 'c': 6")
    assert_that(ab._fmt_args_kwargs('a', 'b', 'c', d='g', e='h', f='i')).is_equal_to("'a', 'b', 'c', 'd': 'g', 'e': 'h', 'f': 'i'")


def test_check_dict_like_empty_dict():
    ab = assert_that(None)
    assert_that(ab._check_dict_like({}))


def test_check_dict_like_not_iterable():
    ab = assert_that(None)
    assert_that(ab._check_dict_like).raises(TypeError).when_called_with(123)\
        .is_equal_to('val <int> is not dict-like: not iterable')


def test_check_dict_like_missing_keys():
    ab = assert_that(None)
    assert_that(ab._check_dict_like).raises(TypeError).when_called_with('foo')\
        .is_equal_to('val <str> is not dict-like: missing keys()')


def test_check_dict_like_bool():
    ab = assert_that(None)
    assert_that(ab._check_dict_like({}, return_as_bool=True)).is_true()
    assert_that(ab._check_dict_like(123, return_as_bool=True)).is_false()
    assert_that(ab._check_dict_like('foo', return_as_bool=True)).is_false()
