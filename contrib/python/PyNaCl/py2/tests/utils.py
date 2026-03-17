# Copyright 2013-2018 Donald Stufft and individual contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import os

from six import byte2int, int2byte


def assert_equal(x, y):
    assert x == y
    assert not (x != y)


def assert_not_equal(x, y):
    assert x != y
    assert not (x == y)


def read_crypto_test_vectors(fname, maxels=0, delimiter=None):
    assert delimiter is not None and isinstance(delimiter, bytes)
    vectors = []
    path = os.path.join(__import__('yatest').common.source_path(os.path.dirname(__file__)), "data", fname)
    with open(path, "rb") as fp:
        for line in fp:
            line = line.rstrip()
            if line and line[0] != b'#'[0]:
                splt = [x for x in line.split(delimiter)]
                if maxels:
                    splt = splt[:maxels]
                vectors.append(tuple(splt))
    return vectors


def read_kv_test_vectors(fname, delimiter=None, newrecord=None):
    assert delimiter is not None and isinstance(delimiter, bytes)
    assert newrecord is not None and isinstance(newrecord, bytes)
    vectors = []
    path = os.path.join(__import__('yatest').common.source_path(os.path.dirname(__file__)), "data", fname)
    vector = {}
    with open(path, 'rb') as fp:
        for line in fp:
            line = line.rstrip()
            if line and line[0] != b'#'[0]:
                [k, v] = line.split(delimiter, 1)
                k, v = k.strip(), v.strip()
                if k == newrecord and k.decode('utf-8') in vector:
                    vectors.append(vector)
                    vector = {}
                vector[k.decode('utf-8')] = v
        vectors.append(vector)
    return vectors


def flip_byte(original, byte_offset):
    return (original[:byte_offset] +
            int2byte(0x01 ^ byte2int(original[byte_offset:byte_offset + 1])) +
            original[byte_offset + 1:])
