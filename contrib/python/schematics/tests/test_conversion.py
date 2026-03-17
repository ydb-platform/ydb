# -*- coding: utf-8 -*-

from copy import copy
import pytest

from schematics.models import Model
from schematics.transforms import convert
from schematics.types import *
from schematics.types.compound import *
from schematics.exceptions import *
from schematics.undefined import Undefined


def missing(d, fieldname, init_to_none):
    if init_to_none:
        return d[fieldname] is None
    else:
        return fieldname not in d


def autofail(value, context):
    if value != [42]:
        raise ValidationError("Error!", info=99)

class M(Model):
    intfield = IntType(max_value=2)
    reqfield = StringType(required=True)
    matrixfield = ListType(ListType(IntType(max_value=2)))
    listfield = ListType(IntType(), max_size=3, validators=[autofail])
    modelfield = ModelType('M')


def get_input_dict(variant):

    inputdict = {
        'intfield': '1',
        'reqfield': 'foo',
        'listfield': [],
        'modelfield': {
            'reqfield': 'bar',
            'listfield': [1, 2, 3, 4],
            'modelfield': {
                'intfield': '3',
                'matrixfield': [[0, 1, 0, 1], [1, 2, 3, 4], ['1', '0', '1', '0']],
                'listfield': None,
                'modelfield': {
                    'intfield': '0',
                    'reqfield': 'foo',
                    'listfield': None}}}}

    if variant == 'noerrors':
        del inputdict['listfield']
        del inputdict['modelfield']['modelfield']['intfield']
        del inputdict['modelfield']['modelfield']['matrixfield']
        inputdict['modelfield']['listfield'] = [42]
        inputdict['modelfield']['modelfield']['reqfield'] = 'xyz'

    return inputdict


def get_input_instance(input_init, variant):

    inputinstance = M(init=input_init)
    inputinstance.intfield = '1'
    inputinstance.reqfield = 'foo'
    inputinstance.listfield = []
    inputinstance.modelfield = M(init=input_init)
    inputinstance.modelfield.reqfield = 'bar'
    inputinstance.modelfield.listfield = [1, 2, 3, 4]
    inputinstance.modelfield.modelfield = M(init=input_init)
    inputinstance.modelfield.modelfield.intfield = '3'
    inputinstance.modelfield.modelfield.matrixfield = [[0, 1, 0, 1], [1, 2, 3, 4], ['1', '0', '1', '0']]
    inputinstance.modelfield.modelfield.listfield = None
    inputinstance.modelfield.modelfield.modelfield = M(init=input_init)
    inputinstance.modelfield.modelfield.modelfield.intfield = '0'
    inputinstance.modelfield.modelfield.modelfield.reqfield = 'foo'
    inputinstance.modelfield.modelfield.modelfield.listfield = None

    if variant == 'noerrors':
        del inputinstance.listfield
        del inputinstance.modelfield.listfield
        del inputinstance.modelfield.modelfield.intfield
        del inputinstance.modelfield.modelfield.matrixfield
        inputinstance.modelfield.listfield = [42]
        inputinstance.modelfield.modelfield.reqfield = 'xyz'

    return inputinstance


@pytest.fixture
def input(input_instance, input_init, variant):
    if input_instance:
        return get_input_instance(input_init, variant)
    else:
        return get_input_dict(variant)


@pytest.mark.parametrize('variant', (None,))
@pytest.mark.parametrize('input_instance, input_init, init',
                       [( False,          None,       True),
                        ( False,          None,       False),
                        ( True,           False,      True),
                        ( True,           False,      False),
                        ( True,           True,       True),
                        ( True,           True,       False)])
def test_conversion(input, input_instance, input_init, init):

    init_to_none = input_init or init

    orig_input = copy(input)

    m = M(input, init=init)

    assert type(m.intfield) is int
    assert type(m.modelfield.modelfield.intfield) is int
    assert type(m.modelfield.modelfield.matrixfield[2][3]) is int
    assert type(m.listfield) is list
    assert type(m.modelfield) is M
    assert type(m.modelfield.modelfield) is M
    assert type(m.modelfield.modelfield.modelfield) is M
    assert type(m.modelfield.listfield) is list
    assert type(m.modelfield.modelfield.matrixfield) is list
    assert type(m.modelfield.modelfield.matrixfield[2]) is list

    assert m._data['listfield'] == []
    assert missing(m.modelfield._data, 'intfield', init_to_none)
    assert m.modelfield.modelfield._data['listfield'] is None
    assert missing(m.modelfield.modelfield._data, 'reqfield', init_to_none)

    assert input == orig_input

    if input_instance:
        assert m.modelfield is not input.modelfield
        assert m._data['modelfield'] is not input._data['modelfield']
        assert m.modelfield.listfield is not input.modelfield.listfield
    else:
        assert m.modelfield.listfield is not input['modelfield']['listfield']


@pytest.mark.parametrize('variant', (None,))
@pytest.mark.parametrize('input_instance, input_init, init',
                       [( False,          None,       True),
                        ( False,          None,       False),
                        ( True,           False,      True),
                        ( True,           False,      False),
                        ( True,           True,       True),
                        ( True,           True,       False)])
def test_conversion_to_dictl(input, input_instance, input_init, init):

    init_to_none = input_init or init

    orig_input = copy(input)

    m = convert(M, input, init_values=init, partial=True)

    assert type(m) is dict
    assert type(m['intfield']) is int
    assert type(m['modelfield']['modelfield']['intfield']) is int
    assert type(m['modelfield']['modelfield']['matrixfield'][2][3]) is int
    assert type(m['listfield']) is list
    assert type(m['modelfield']) is dict
    assert type(m['modelfield']['modelfield']) is dict
    assert type(m['modelfield']['modelfield']['modelfield']) is dict
    assert type(m['modelfield']['listfield']) is list
    assert type(m['modelfield']['modelfield']['matrixfield']) is list
    assert type(m['modelfield']['modelfield']['matrixfield'][2]) is list

    assert m['listfield'] == []
    assert missing(m['modelfield'], 'intfield', init_to_none)
    assert m['modelfield']['modelfield']['listfield'] is None
    assert missing(m['modelfield']['modelfield'], 'reqfield', init_to_none)

    assert input == orig_input

    if input_instance:
        assert m['modelfield'] is not input['modelfield']

    assert m['modelfield']['listfield'] is not input['modelfield']['listfield']


@pytest.mark.parametrize('variant', (None, 'noerrors'))
@pytest.mark.parametrize('partial', (True, False))
@pytest.mark.parametrize('import_, two_pass, input_instance, input_init, init',
                       [( True,    False,    False,          None,       True),
                        ( True,    False,    False,          None,       False),
                        ( True,    False,    True,           False,      True),
                        ( True,    False,    True,           False,      False),
                        ( True,    False,    True,           True,       True),
                        ( True,    False,    True,           True,       False),
                        ( True,    True,     False,          None,       True),
                        ( True,    True,     False,          None,       False),
                        ( True,    True,     True,           False,      True),
                        ( True,    True,     True,           False,      False),
                        ( True,    True,     True,           True,       True),
                        ( True,    True,     True,           True,       False),
                        ( False,   None,     True,           False,      True),
                        ( False,   None,     True,           False,      False),
                        ( False,   None,     True,           True,       True),
                        ( False,   None,     True,           True,       False)])
def test_conversion_with_validation(input, import_, two_pass, input_instance, input_init, init,
                                    partial, variant):

    init_to_none = input_init or init

    if variant == 'noerrors':

        orig_input = copy(input)

        if input_instance:
            assert input.modelfield is orig_input.modelfield

        if import_:
            if two_pass:
                m = M(input, init=init)
                m.validate(partial=partial)
            else:
                m = M(input, init=init, partial=partial, validate=True)
        else:
            input.validate(init_values=init, partial=partial)
            m = input

        assert input == orig_input

        if input_instance:
            if import_:
                assert m.modelfield is not input.modelfield
                assert m._data['modelfield'] is not input._data['modelfield']
                assert m.modelfield.listfield is not input.modelfield.listfield
            else:
                assert m.modelfield is input.modelfield
                assert m._data['modelfield'] is input._data['modelfield']
                assert m.modelfield.listfield is input.modelfield.listfield

        return

    if init_to_none:
        partial_data = {
            'intfield': 1,
            'reqfield': u'foo',
            'matrixfield': None,
            'modelfield': {
                'intfield': None,
                'reqfield': u'bar',
                'matrixfield': None,
                'modelfield': {
                    'reqfield': None,
                    'listfield': None,
                    'modelfield': M({
                        'intfield': 0,
                        'reqfield': u'foo',
                        'listfield': None})}}}
    else:
        partial_data = {
            'intfield': 1,
            'reqfield': u'foo',
            'modelfield': {
                'reqfield': u'bar',
                'modelfield': {
                    'listfield': None,
                    'modelfield': M({
                        'intfield': 0,
                        'reqfield': u'foo',
                        'listfield': None}, init=False)}}}

    with pytest.raises(DataError) as excinfo:
        if import_:
            if two_pass:
                m = M(input, init=init)
                m.validate(partial=partial)
            else:
                # variant = none
                # partial = true
                M(input, init=init, partial=partial, validate=True)
        else:
            input.validate(init_values=init, partial=partial)

    errors = excinfo.value.errors

    err_list = errors['listfield']
    assert type(err_list) is ValidationError
    assert len(err_list) == 1

    err_list = errors['modelfield']['listfield']
    assert type(err_list) is ValidationError
    assert len(err_list) == 2

    err_list = errors['modelfield']['modelfield']['intfield']
    assert len(err_list) == 1

    if not partial:
        err_list = errors['modelfield']['modelfield']['reqfield']
        assert len(err_list) == 1
        if init_to_none:
            partial_data['modelfield']['modelfield'].pop('reqfield')

    err_dict = errors['modelfield']['modelfield']['matrixfield']
    sub_err_dict = err_dict[1]
    assert list((k, type(v)) for k, v in sub_err_dict.items()) \
        == [(2, ValidationError), (3, ValidationError)]
    assert len(err_dict) == 1
    assert len(errors['modelfield']['modelfield']) == 2 + (0 if partial else 1)
    assert len(errors['modelfield']) == 2
    assert len(errors) == 2

    assert excinfo.value.partial_data == partial_data
