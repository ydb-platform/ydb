# -*- coding: utf-8 -*-
import pytest

from schematics.models import Model
from schematics.types import StringType, IntType, calculated, serializable
from schematics.schema import Schema, Field
from schematics.transforms import convert, to_primitive
from schematics.exceptions import DataError
from schematics.validate import validate
from schematics.contrib.machine import Machine


@pytest.fixture
def player_schema():

    def get_full_name(data, *a, **kw):
        if not data:
            return
        return '{first_name} {last_name}'.format(**data)

    def set_full_name(data, value, *a, **kw):
        if not value:
            return
        data['first_name'], _, data['last_name'] = value.partition(' ')

    schema = Schema('Player',
        Field('id', IntType()),
        Field('first_name', StringType(required=True)),
        Field('last_name', StringType(required=True)),
        Field('full_name', calculated(
            type=StringType(),
            fget=get_full_name,
            fset=set_full_name))
    )

    return schema


@pytest.fixture
def player_data():
    return {'id': '42', 'full_name': 'Arthur Dent', 'towel': True}


def test_functional_schema_required(player_schema):
    with pytest.raises(DataError):
        validate(player_schema, {}, partial=False)


def test_functional_schema(player_schema, player_data):
    schema = player_schema
    input_data = player_data

    data = input_data  # state = 'RAW'

    expected = {'id': 42, 'full_name': 'Arthur Dent'}
    data = convert(schema, data, partial=True)
    assert data == expected  # state = 'CONVERTED'

    expected = {'id': 42, 'first_name': 'Arthur', 'last_name': 'Dent',
                'full_name': 'Arthur Dent'}
    data = validate(schema, data, convert=False, partial=False)
    assert data == expected  # state = 'VALIDATED'

    expected = {'id': 42, 'first_name': 'Arthur', 'last_name': 'Dent',
                'full_name': 'Arthur Dent'}
    data = to_primitive(schema, data)
    assert data == expected  # state = 'SERIALIZED'


def test_state_machine_equivalence(player_schema, player_data):
    schema = player_schema
    input_data = player_data

    data = input_data.copy()
    machine = Machine(input_data, schema)
    assert machine.state == 'raw'

    data = convert(schema, data, partial=True)
    machine.convert()
    assert machine.state == 'converted'
    assert data == machine.data

    data = validate(schema, data, convert=False, partial=False)
    machine.validate()
    assert machine.state == 'validated'
    assert data == machine.data

    data = to_primitive(schema, data)
    machine.serialize()
    assert machine.state == 'serialized'
    assert data == machine.data


def test_object_model_equivalence():
    # functional
    def get_full_name(data, *a, **kw):
        if not data:
            return
        return '{first_name} {last_name}'.format(**data)

    def set_full_name(data, value, *a, **kw):
        if not value:
            return
        data['first_name'], _, data['last_name'] = value.partition(' ')

    schema = Schema('Player',
        Field('id', IntType()),
        Field('first_name', StringType(required=True)),
        Field('last_name', StringType(required=True)),
        Field('full_name', calculated(
            type=StringType(),
            fget=get_full_name,
            fset=set_full_name))
    )

    # object
    class Player(Model):
        id = IntType()
        first_name = StringType(required=True)
        last_name = StringType(required=True)

        @serializable(type=StringType())
        def full_name(self):
            return get_full_name(self)

        @full_name.setter
        def full_name(self, value):
            set_full_name(self, value)

    input_data = {'id': '42', 'full_name': 'Arthur Dent', 'towel': True}

    data = input_data.copy()
    player = Player(input_data, strict=False, validate=False, init=False)

    data = convert(schema, data, partial=True)
    assert data == player._data

    data = validate(schema, data, convert=False, partial=False)
    player.validate()
    assert data == player._data

    data = to_primitive(schema, data)
    player_dict = player.serialize()
    assert data == player_dict
