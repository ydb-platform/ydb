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


class Foo(object):
    pass


class Bar(Foo):
    pass


def test_is_type_of():
    assert_that('foo').is_type_of(str)
    assert_that(123).is_type_of(int)
    assert_that(0.456).is_type_of(float)
    assert_that(['a', 'b']).is_type_of(list)
    assert_that(('a', 'b')).is_type_of(tuple)
    assert_that({'a': 1, 'b': 2}).is_type_of(dict)
    assert_that(set(['a', 'b'])).is_type_of(set)
    assert_that(None).is_type_of(type(None))
    assert_that(Foo()).is_type_of(Foo)
    assert_that(Bar()).is_type_of(Bar)


def test_is_type_of_failure():
    try:
        assert_that('foo').is_type_of(int)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo:str> to be of type <int>, but was not.')


def test_is_type_of_bad_arg_failure():
    try:
        assert_that('foo').is_type_of('bad')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a type')


def test_is_type_of_subclass_failure():
    try:
        assert_that(Bar()).is_type_of(Foo)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).starts_with('Expected <')
        assert_that(str(ex)).ends_with(':Bar> to be of type <Foo>, but was not.')


def test_is_instance_of():
    assert_that('foo').is_instance_of(str)
    assert_that(123).is_instance_of(int)
    assert_that(0.456).is_instance_of(float)
    assert_that(['a', 'b']).is_instance_of(list)
    assert_that(('a', 'b')).is_instance_of(tuple)
    assert_that({'a': 1, 'b': 2}).is_instance_of(dict)
    assert_that(set(['a', 'b'])).is_instance_of(set)
    assert_that(None).is_instance_of(type(None))
    assert_that(Foo()).is_instance_of(Foo)
    assert_that(Bar()).is_instance_of(Bar)
    assert_that(Bar()).is_instance_of(Foo)


def test_is_instance_of_failure():
    try:
        assert_that('foo').is_instance_of(int)
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo:str> to be instance of class <int>, but was not.')


def test_is_instance_of_bad_arg_failure():
    try:
        assert_that('foo').is_instance_of('bad')
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('given arg must be a class')
