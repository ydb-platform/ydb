# -*- coding: utf-8 -*-

import pytest

from schematics.common import *
from schematics.models import Model
from schematics.types import *
from schematics.types.compound import *
from schematics.types.serializable import serializable
from schematics.undefined import Undefined


params = [(None, None), (None, ALL), (ALL, ALL), (DEFAULT, DEFAULT), (NOT_NONE, NOT_NONE), (NONEMPTY, NONEMPTY)]

@pytest.fixture(params=params)
def models(request):

    m_level, n_level = request.param

    class N(Model):
        subfield1 = StringType()
        subfield2 = StringType()
        subfield3 = StringType()
        subfield4 = StringType()

    class M(Model):
        field0 = StringType(export_level=DROP)
        field1 = StringType()
        field2 = StringType()
        field3 = StringType()
        field4 = StringType()
        field5 = StringType(export_level=NOT_NONE)
        field6 = StringType(export_level=ALL)
        listfield = ListType(StringType())
        modelfield1 = ModelType(N)
        modelfield2 = ModelType(N)

    if m_level:
        M._options.export_level = m_level

    if n_level:
        N._options.export_level = n_level

    return M, N


input = {
    'field0': 'foo',
    'field1': 'foo',
    'field2': '',
    'field3': None,
    'field5': None,
    'listfield': [],
    'modelfield1': {
        'subfield1': 'foo',
        'subfield2': '',
        'subfield3': None,
        },
    'modelfield2': {}
    }


def test_export_level(models):

    M, N = models
    m_level = M._options.export_level
    n_level = N._options.export_level

    output = M(input, init=False).to_primitive()

    if m_level == NONEMPTY and n_level == NONEMPTY:
        assert output == {
            'field1': 'foo',
            'field2': '',
            'field6': None,
            'modelfield1': {
                'subfield1': 'foo',
                'subfield2': '',
                },
            }
    elif m_level == NOT_NONE and n_level == NOT_NONE:
        assert output == {
            'field1': 'foo',
            'field2': '',
            'field6': None,
            'listfield': [],
            'modelfield1': {
                'subfield1': 'foo',
                'subfield2': '',
                },
            'modelfield2': {},
            }
    elif m_level == DEFAULT and n_level == DEFAULT:
        assert output == {
            'field1': 'foo',
            'field2': '',
            'field3': None,
            'field6': None,
            'listfield': [],
            'modelfield1': {
                'subfield1': 'foo',
                'subfield2': '',
                'subfield3': None,
                },
            'modelfield2': {},
            }
    elif m_level == ALL and n_level == ALL:
        assert output == {
            'field1': 'foo',
            'field2': '',
            'field3': None,
            'field4': None,
            'field6': None,
            'listfield': [],
            'modelfield1': {
                'subfield1': 'foo',
                'subfield2': '',
                'subfield3': None,
                'subfield4': None,
                },
            'modelfield2': {
                'subfield1': None,
                'subfield2': None,
                'subfield3': None,
                'subfield4': None,
                },
            }
    elif m_level == DEFAULT and n_level == ALL:
        assert output == {
            'field1': 'foo',
            'field2': '',
            'field3': None,
            'field6': None,
            'listfield': [],
            'modelfield1': {
                'subfield1': 'foo',
                'subfield2': '',
                'subfield3': None,
                'subfield4': None,
                },
            'modelfield2': {
                'subfield1': None,
                'subfield2': None,
                'subfield3': None,
                'subfield4': None,
                },
            }
    else:
        raise Exception("Assertions missing for testcase m_level={0}, n_level={1}".format(m_level, n_level))


def test_export_level_override(models):

    M, N = models
    m_level = M._options.export_level
    n_level = N._options.export_level

    m = M(input, init=False)

    assert m.to_primitive(export_level=DROP) == {}

    assert m.to_primitive(export_level=NONEMPTY) == {
        'field0': 'foo',
        'field1': 'foo',
        'field2': '',
        'modelfield1': {
            'subfield1': 'foo',
            'subfield2': '',
            },
        }

    assert m.to_primitive(export_level=NOT_NONE) == {
        'field0': 'foo',
        'field1': 'foo',
        'field2': '',
        'listfield': [],
        'modelfield1': {
            'subfield1': 'foo',
            'subfield2': '',
            },
        'modelfield2': {},
        }

    assert m.to_primitive(export_level=DEFAULT) == {
        'field0': 'foo',
        'field1': 'foo',
        'field2': '',
        'field3': None,
        'field5': None,
        'listfield': [],
        'modelfield1': {
            'subfield1': 'foo',
            'subfield2': '',
            'subfield3': None,
            },
        'modelfield2': {},
        }

    assert m.to_primitive(export_level=ALL) == {
        'field0': 'foo',
        'field1': 'foo',
        'field2': '',
        'field3': None,
        'field4': None,
        'field5': None,
        'field6': None,
        'listfield': [],
        'modelfield1': {
            'subfield1': 'foo',
            'subfield2': '',
            'subfield3': None,
            'subfield4': None,
            },
        'modelfield2': {
            'subfield1': None,
            'subfield2': None,
            'subfield3': None,
            'subfield4': None,
            },
        }


def test_custom_converter():

    class M(Model):
        x = IntType()
        y = IntType()
        z = IntType()

    def converter(field, value, context):
        if field.name == 'z':
            return Undefined
        else:
            return field.export(value, PRIMITIVE, context)

    m = M(dict(x=1, y=None, z=3))

    assert m.export(field_converter=converter, export_level=DEFAULT) == {'x': 1, 'y': None}

