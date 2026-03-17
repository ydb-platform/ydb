# -*- coding: utf-8 -*-
import datetime
import pytest
import uuid

from schematics.common import *
from schematics.models import Model
from schematics.transforms import Converter, to_native, to_primitive
from schematics.types import *
from schematics.types.compound import *
from schematics.types.serializable import serializable


class BaseModel(Model):
    pass

class N(BaseModel):
    floatfield = FloatType()
    uuidfield = UUIDType()

class M(BaseModel):
    intfield = IntType()
    stringfield = StringType()
    dtfield = DateTimeType()
    utcfield = UTCDateTimeType()
    modelfield = ModelType(N)

field = ListType(ModelType(M)) # standalone field

primitives = { 'intfield': 3,
               'stringfield': 'foobar',
               'dtfield': '2015-11-26T09:00:00.000000',
               'utcfield': '2015-11-26T07:00:00.000000Z',
               'modelfield': {
                   'floatfield': 1.0,
                   'uuidfield': '54020382-291e-4192-b370-4850493ac5bc' }}

natives = { 'intfield': 3,
            'stringfield': 'foobar',
            'dtfield': datetime.datetime(2015, 11, 26, 9),
            'utcfield': datetime.datetime(2015, 11, 26, 7),
            'modelfield': { 
                'floatfield': 1.0,
                'uuidfield': uuid.UUID('54020382-291e-4192-b370-4850493ac5bc') }}


def test_to_native():

    m = M(primitives)
    output = m.to_native()
    assert type(output) is dict
    assert output == natives

    assert to_native(M, natives) == natives


def test_to_primitive():

    m = M(primitives)
    output = m.to_primitive()
    assert type(output) is dict
    assert output == primitives

    assert to_primitive(M, natives) == primitives


def test_standalone_field():

    converted = field.convert([primitives])
    assert converted == [natives]
    assert field.to_native(converted) == [natives]
    assert field.to_primitive(converted) == [primitives]


class Foo(object):
    def __init__(self, x, y):
        self.x, self.y = x, y
    def __eq__(self, other):
        return type(self) == type(other) \
                 and self.x == other.x and self.y == other.y

class FooType(BaseType):
    def to_native(self, value, context):
        if isinstance(value, Foo):
            return value
        return Foo(value['x'], value['y'])
    def to_primitive(self, value, context):
        return dict(x=value.x, y=value.y)


def test_custom_exporter():

    class X(Model):
        id = UUIDType()
        dt = UTCDateTimeType()
        foo = FooType()

    x = X({ 'id': '54020382-291e-4192-b370-4850493ac5bc',
            'dt': '2015-11-26T07:00',
            'foo': {'x': 1, 'y': 2} })

    assert x.to_native() == {
        'id': uuid.UUID('54020382-291e-4192-b370-4850493ac5bc'),
        'dt': datetime.datetime(2015, 11, 26, 7),
        'foo': Foo(1, 2) }

    class MyExportConverter(Converter):

        keep_as_native = (UTCDateTimeType, UUIDType)

        def __call__(self, field, value, context):
            if field.typeclass in self.keep_as_native:
                format = NATIVE
            else:
                format = PRIMITIVE
            return field.export(value, format, context)

        def post(self, model_class, data, context):
            data['n'] = 42
            return data

    exporter = MyExportConverter()

    assert x.export(field_converter=exporter) == {
        'id': uuid.UUID('54020382-291e-4192-b370-4850493ac5bc'),
        'dt': datetime.datetime(2015, 11, 26, 7),
        'foo': {'x': 1, 'y': 2},
        'n': 42
    }


def test_converter_function():

    class X(Model):
        id = UUIDType()
        dt = UTCDateTimeType()
        foo = FooType()

    x = X({ 'id': '54020382-291e-4192-b370-4850493ac5bc',
            'dt': '2015-11-26T07:00',
            'foo': {'x': 1, 'y': 2} })

    exporter = lambda field, value, context: field.export(value, PRIMITIVE, context)

    assert x.export(field_converter=exporter) == x.to_primitive()


def test_export_order():

    class O(Model):
        class Options:
            export_order = True
        f = StringType()
        a = StringType()
        e = StringType()
        b = StringType()
        d = StringType()
        c = StringType()

    assert list(O().to_primitive().keys()) == ['f', 'a', 'e', 'b', 'd', 'c']

    class O(Model):
        class Options:
            export_order = True
        b = StringType()
        f = StringType()
        c = StringType()
        e = StringType()
        d = StringType()
        a = StringType()

    assert list(O().to_primitive().keys()) == ['b', 'f', 'c', 'e', 'd', 'a']

