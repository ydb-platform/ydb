# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Refs leaks tests
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   MIT License
# :Copyright: © 2017, 2018, 2020, 2022, 2025 Lele Gaifax
#

# NB: this is a simplistic test that uses sys.gettotalrefcount(), available
# when the interpreter is built --with-pydebug, that tries to assert that
# repeated calls to dumps() and loads() does not leak object references.
# Since it's not an exact science, it should be taken with a grain of salt.

import datetime
import io
import sys
import uuid

import pytest

import rapidjson as rj


class Foo:
    def __init__(self, foo):
        self.foo = foo


def hook(d):
    if 'foo' in d:
        return Foo(d['foo'])
    return d


def default(obj):
    return {'foo': obj.foo}


# Empirical tolerance used to test the refcount growing
THRESHOLD = 7


plain_string = 'plain-string'
bigint = 123456789
pi = 3.1415926
right_now = datetime.datetime.now()
date = right_now.date()
time = right_now.time()
nil = uuid.UUID(int=0)
foo = Foo('foo')
array = [dict(index=j, array=[dict(index=i,
                                   plain_string=plain_string,
                                   bigint=bigint,
                                   pi=pi,
                                   right_now=right_now,
                                   date=date,
                                   time=time,
                                   nil=nil,
                                   foos=[foo, foo, foo, foo, foo])
                              for i in range(10)])
         for j in range(10)]

NO_OPTION = {}
DATETIMES = {'datetime_mode': rj.DM_ISO8601}
UUIDS = {'uuid_mode': rj.UM_CANONICAL}
FOOS_DUMP = {'default': default}
FOOS_LOAD = {'object_hook': hook}
ARRAY_DUMP = {'datetime_mode': rj.DM_ISO8601,
              'uuid_mode': rj.UM_CANONICAL,
              'default': default}
ARRAY_LOAD = {'datetime_mode': rj.DM_ISO8601,
              'uuid_mode': rj.UM_CANONICAL,
              'object_hook': hook}

@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
@pytest.mark.parametrize('value,dumps_options,loads_options', [
    ( plain_string, NO_OPTION, NO_OPTION ),
    ( bigint, NO_OPTION, NO_OPTION ),
    ( pi, NO_OPTION, NO_OPTION ),
    ( rj.RawJSON(' "foo" '), NO_OPTION, NO_OPTION),
    ( right_now, DATETIMES, DATETIMES ),
    ( date, DATETIMES, DATETIMES ),
    ( time, DATETIMES, DATETIMES ),
    ( nil, UUIDS, UUIDS ),
    ( foo, FOOS_DUMP, FOOS_LOAD ),
    ( array, ARRAY_DUMP, ARRAY_LOAD ),
])
def test_leaks(value, dumps_options, loads_options):
    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        asjson = rj.dumps(value, **dumps_options)
        aspython = rj.loads(asjson, **loads_options)
        del asjson
        del aspython
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD

    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        stream = io.BytesIO()
        none = rj.dump(value, stream, **dumps_options)
        stream.seek(0)
        aspython = rj.load(stream, **loads_options)
        del none
        del aspython
        del stream
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
def test_dump_iterator_leaks():
    rc0 = sys.gettotalrefcount()
    value = iter(range(10000))
    asjson = rj.dumps(value)
    del asjson
    del value
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
def test_decoder_call_leaks():
    class MyDecoder(rj.Decoder):
        def start_object(self):
            return {}
        def end_object(self, obj):
            return obj
        def end_array(self, array):
            return array
        def string(self, string):
            return string

    decoder = MyDecoder()
    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        value = decoder('["foo", {"foo": "bar"}]')
        del value
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


@pytest.mark.parametrize('value', ['Foo', rj.RawJSON('Foo')])
@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
def test_encoder_call_leaks(value):
    class MyEncoder(rj.Encoder):
        def __init__(self, value):
            self.value = value

        def default(self, obj):
            return self.value

    class Foo:
        pass

    encoder = MyEncoder(value)
    foo = Foo()
    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        value = encoder(foo)
        del value
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
def test_rawjson_constructor():
    raw_json = rj.RawJSON('["foo", "bar"]')
    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        value = '"foobar"'
        raw_json.__init__(value)
        del value
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
def test_rawjson_new():
    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        raw_json = rj.RawJSON('["foo", "bar"]')
        del raw_json
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


@pytest.mark.skipif(not hasattr(sys, 'gettotalrefcount'), reason='Non-debug Python')
def test_invalid_json_load_leaks():
    # See issue #148
    value = '{"a":{"b":}}'
    rc0 = sys.gettotalrefcount()
    for _ in range(1000):
        try:
            rj.loads(value)
        except rj.JSONDecodeError:
            pass
    del _
    rc1 = sys.gettotalrefcount()
    assert (rc1 - rc0) < THRESHOLD


def test_endarray_leak():
    # See issue #160
    value = '{"v": [1, 2]}'

    class Decoder1(rj.Decoder):
        pass

    class Decoder2(rj.Decoder):
        def end_array(self, seq):
            return list(seq)

    j1 = rj.loads(value)
    # Uhm, with Py 3.14 the refcount is 2...
    assert 2 <= sys.getrefcount(j1['v']) <= 3

    j2 = Decoder1()(value)
    assert 2 <= sys.getrefcount(j2['v']) <= 3

    j3 = Decoder2()(value)
    assert 2 <= sys.getrefcount(j3['v']) <= 3
