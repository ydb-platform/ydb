# -*- coding: utf-8 -*-

from copy import deepcopy

import pytest

from schematics.models import Model
from schematics.types import *
from schematics.types.compound import *
from schematics.exceptions import *
from schematics.undefined import Undefined


@pytest.mark.parametrize('init', (True, False))
def test_import_data(init):

    class M(Model):
        a, b, c, d = IntType(), IntType(), IntType(), IntType()

    m = M({
        'a': 1,
        'b': None,
        'c': 3
    }, init=init)

    m.import_data({
        'a': None,
        'b': 2
    })

    if init:
        assert m._data == {'a': None, 'b': 2, 'c': 3, 'd': None}
    else:
        assert m._data == {'a': None, 'b': 2, 'c': 3}


@pytest.mark.parametrize('init', (True, False))
def test_import_data_with_error(init):

    class M(Model):
        a, b, c, d = IntType(), IntType(), IntType(required=True), IntType()

    m = M({
        'a': 1,
        'b': None,
        'c': 3
    }, init=init)

    with pytest.raises(DataError):
        m.import_data({
            'a': None,
            'b': 2,
            'c': None,
        })

    if init:
        assert m._data == {'a': 1, 'b': None, 'c': 3, 'd': None}
    else:
        assert m._data == {'a': 1, 'b': None, 'c': 3}


@pytest.mark.parametrize('preconvert_source, populate_source',
                       [( False,             None),
                        ( True,              True),
                        ( True,              False)])
@pytest.mark.parametrize('recursive, populate_target, init_to_none, populated_result',
                       [( False,     True,            True,         True),
                        ( False,     False,           False,        False),
                        ( True,      True,            True,         True),
                        ( True,      False,           True,         True),
                        ( True,      False,           False,        False)])
def test_complex_import_data(recursive, preconvert_source, populate_source, populate_target,
                             init_to_none, populated_result):

    class M(Model):
        intfield = IntType(max_value=2)
        matrixfield = ListType(ListType(IntType))
        dictfield = DictType(IntType)
        modelfield = ModelType('M')

    origdict = {
        'intfield': '1',
        'dictfield': dict(a=1, b=2),
        'modelfield': {
            'intfield': '2',
            'matrixfield': [[0, 0, 0], [1, 1, 1], [2, 2, 2]],
            'dictfield': dict(a=11, b=22),
            'modelfield': {
                'intfield': '3',
                'dictfield': dict(a=111, b=222)}}}

    m = M(origdict, init=populate_target)

    sourcedict = {
        'intfield': '101',
        'dictfield': dict(c=3),
        'modelfield': {
            'matrixfield': [[9]],
            'modelfield': {
                'intfield': '103',
                'dictfield': dict(c=33)}}}

    sourcedata = deepcopy(sourcedict)

    if preconvert_source:
        sourcedata = M(sourcedata, init=populate_source)

    m.import_data(sourcedata, recursive=recursive, init_values=init_to_none)

    assert id(m) != id(sourcedata)

    if preconvert_source and populate_source:
        assert m == M(sourcedict, init=True)
    elif recursive:
        assert m == M({
            'intfield': '101',
            'dictfield': dict(c=3),
            'modelfield': {
                'intfield': '2',
                'matrixfield': [[9]],
                'dictfield': dict(a=11, b=22),
                'modelfield': {
                    'intfield': '103',
                    'dictfield': dict(c=33)}}}, init=populated_result)
    else:
        assert m == M(sourcedict, init=populated_result)

