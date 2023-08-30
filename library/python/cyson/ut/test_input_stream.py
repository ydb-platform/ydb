# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division

import atexit
import io
import os
import tempfile

import pytest
import six

from cyson import Reader, InputStream, dumps


def prepare_file(string):
    filepath = tempfile.mktemp()

    with open(filepath, 'wb') as sink:
        sink.write(string)

    atexit.register(os.remove, filepath)

    return filepath


def prepare_bytesio(string, klass):
    obj = klass()
    obj.write(b'?:!;*')
    obj.write(string)
    obj.seek(5)

    return obj


def slice_string(string):
    index = 0
    length = len(string)

    while index < length:
        yield string[index:index + 2]
        index += 2


# <method name>, <input constructor>
CASES = (
    ('from_string', lambda x: x),
    ('from_iter', slice_string),
    ('from_file', lambda x: prepare_bytesio(x, io.BytesIO)),
    ('from_file', lambda x: open(prepare_file(x), 'rb')),
    ('from_fd', lambda x: os.open(prepare_file(x), os.O_RDONLY)),
)

if six.PY2:
    import StringIO
    import cStringIO

    CASES += (
        ('from_file', lambda x: prepare_bytesio(x, StringIO.StringIO)),
        ('from_file', lambda x: prepare_bytesio(x, cStringIO.StringIO)),
    )


DATA = {u'a': [1, u'word', 3], b'b': b'xyz', u'c': None}
ETALON = {b'a': [1, b'word', 3], b'b': b'xyz', b'c': None}


@pytest.fixture(scope='module')
def serialized_data():
    return dumps(DATA, format='binary')


def test_serizlized_data(serialized_data):
    assert type(serialized_data) is bytes


@pytest.mark.parametrize('method_name,make_input', CASES)
def test_input_streams(method_name, make_input, serialized_data):
    method = getattr(InputStream, method_name)
    input_stream = method(make_input(serialized_data))

    assert Reader(input_stream).node() == ETALON
