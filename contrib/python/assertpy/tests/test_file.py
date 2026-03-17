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
import os
import tempfile
import pytest
from assertpy import assert_that, contents_of, fail


@pytest.fixture()
def tmpfile():
    tmp = tempfile.NamedTemporaryFile()
    tmp.write('foobar'.encode('utf-8'))
    tmp.seek(0)
    yield tmp
    tmp.close()


def test_contents_of_path(tmpfile):
    contents = contents_of(tmpfile.name)
    assert_that(contents).is_equal_to('foobar').starts_with('foo').ends_with('bar')


def test_contents_of_path_ascii(tmpfile):
    contents = contents_of(tmpfile.name, 'ascii')
    assert_that(contents).is_equal_to('foobar').starts_with('foo').ends_with('bar')


def test_contents_of_return_type(tmpfile):
    if sys.version_info[0] == 3:
        contents = contents_of(tmpfile.name)
        assert_that(contents).is_type_of(str)
    else:
        contents = contents_of(tmpfile.name)
        assert_that(contents).is_type_of(unicode)


def test_contents_of_return_type_ascii(tmpfile):
    if sys.version_info[0] == 3:
        contents = contents_of(tmpfile.name, 'ascii')
        assert_that(contents).is_type_of(str)
    else:
        contents = contents_of(tmpfile.name, 'ascii')
        assert_that(contents).is_type_of(str)


def test_contents_of_file(tmpfile):
    contents = contents_of(tmpfile.file)
    assert_that(contents).is_equal_to('foobar').starts_with('foo').ends_with('bar')


def test_contents_of_file_ascii(tmpfile):
    contents = contents_of(tmpfile.file, 'ascii')
    assert_that(contents).is_equal_to('foobar').starts_with('foo').ends_with('bar')


def test_contains_of_bad_type_failure(tmpfile):
    try:
        contents_of(123)
        fail('should have raised error')
    except ValueError as ex:
        assert_that(str(ex)).is_equal_to('val must be file or path, but was type <int>')


def test_contains_of_missing_file_failure(tmpfile):
    try:
        contents_of('missing.txt')
        fail('should have raised error')
    except IOError as ex:
        assert_that(str(ex)).contains_ignoring_case('no such file')


def test_exists(tmpfile):
    assert_that(tmpfile.name).exists()
    assert_that(os.path.dirname(tmpfile.name)).exists()


def test_exists_failure(tmpfile):
    try:
        assert_that('missing.txt').exists()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <missing.txt> to exist, but was not found.')


def test_exists_bad_val_failure(tmpfile):
    try:
        assert_that(123).exists()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a path')


def test_does_not_exist():
    assert_that('missing.txt').does_not_exist()


def test_does_not_exist_failure(tmpfile):
    try:
        assert_that(tmpfile.name).does_not_exist()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <{}> to not exist, but was found.'.format(tmpfile.name))


def test_does_not_exist_bad_val_failure(tmpfile):
    try:
        assert_that(123).does_not_exist()
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).is_equal_to('val is not a path')


def test_is_file(tmpfile):
    assert_that(tmpfile.name).is_file()


def test_is_file_exists_failure():
    try:
        assert_that('missing.txt').is_file()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <missing.txt> to exist, but was not found.')


def test_is_file_directory_failure(tmpfile):
    try:
        dirname = os.path.dirname(tmpfile.name)
        assert_that(dirname).is_file()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).matches('Expected <.*> to be a file, but was not.')


def test_is_directory(tmpfile):
    dirname = os.path.dirname(tmpfile.name)
    assert_that(dirname).is_directory()


def test_is_directory_exists_failure():
    try:
        assert_that('missing_dir').is_directory()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <missing_dir> to exist, but was not found.')


def test_is_directory_file_failure(tmpfile):
    try:
        assert_that(tmpfile.name).is_directory()
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).matches('Expected <.*> to be a directory, but was not.')


def test_is_named(tmpfile):
    basename = os.path.basename(tmpfile.name)
    assert_that(tmpfile.name).is_named(basename)


def test_is_named_failure(tmpfile):
    try:
        assert_that(tmpfile.name).is_named('foo.txt')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).matches('Expected filename <.*> to be equal to <foo.txt>, but was not.')


def test_is_named_bad_arg_type_failure(tmpfile):
    try:
        assert_that(tmpfile.name).is_named(123)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).matches('given filename arg must be a path')


def test_is_child_of(tmpfile):
    dirname = os.path.dirname(tmpfile.name)
    assert_that(tmpfile.name).is_child_of(dirname)


def test_is_child_of_failure(tmpfile):
    try:
        assert_that(tmpfile.name).is_child_of('foo_dir')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).matches('Expected file <.*> to be a child of <.*/foo_dir>, but was not.')


def test_is_child_of_bad_arg_type_failure(tmpfile):
    try:
        assert_that(tmpfile.name).is_child_of(123)
        fail('should have raised error')
    except TypeError as ex:
        assert_that(str(ex)).matches('given parent directory arg must be a path')
