# -*- coding: utf-8 -*-
import pytest

from schematics.models import Model
from schematics.types import BaseType, IntType, StringType
from schematics.types.compound import ListType, DictType, ModelType
from schematics.exceptions import ModelConversionError, ModelValidationError

def test_nested_mapping():

    mapping = {
        'model_mapping': {
            'modelfield1': {
                'subfield': 'importfield1'
            },
            'modelfield2': {
                'subfield': 'importfield2'
            },
        }
    }

    class SubModel(Model):
        subfield = StringType()

    class MainModel(Model):
        modelfield1 = ModelType(SubModel)
        modelfield2 = ModelType(SubModel)

    m1 = MainModel({
        'modelfield1': {'importfield1':'qweasd'},
        'modelfield2': {'importfield2':'qweasd'},
        }, deserialize_mapping=mapping)

    assert m1.modelfield1.subfield == 'qweasd'
    assert m1.modelfield2.subfield == 'qweasd'


def test_nested_mapping_with_required():

    mapping = {
        'model_mapping': {
            'modelfield': {
                'subfield': 'importfield'
            }
        }
    }

    class SubModel(Model):
        subfield = StringType(required=True)

    class MainModel(Model):
        modelfield = ModelType(SubModel)

    m1 = MainModel({
        'modelfield': {'importfield':'qweasd'},
        }, partial=False, deserialize_mapping=mapping)


def test_submodel_required_field():

    class SubModel(Model):
        req_field = StringType(required=True)
        opt_field = StringType()
        submodelfield = ModelType('SubModel')

    class MainModel(Model):
        intfield = IntType()
        stringfield = StringType()
        modelfield = ModelType(SubModel)

    # By default, model instantiation assumes partial=True
    m1 = MainModel({
        'modelfield': {'opt_field':'qweasd'}})

    with pytest.raises(ModelConversionError):
        m1 = MainModel({
            'modelfield': {'opt_field':'qweasd'}}, partial=False)

    # Validation implies partial=False
    with pytest.raises(ModelValidationError):
        m1.validate()

    m1.validate(partial=True)

    m1 = MainModel({
        'modelfield': {'req_field':'qweasd'}}, partial=False)

    with pytest.raises(ModelConversionError):
        m1 = MainModel({
            'modelfield': {'req_field':'qweasd', 'submodelfield': {'opt_field':'qweasd'}}}, partial=False)

    m1 = MainModel({
        'modelfield': {'req_field':'qweasd', 'submodelfield': {'req_field':'qweasd'}}}, partial=False)


def test_strict_propagation():

    class SubModel(Model):
        subfield = StringType()
        submodelfield = ModelType('SubModel')

    class MainModel(Model):
        modelfield = ModelType(SubModel)

    with pytest.raises(ModelConversionError):
        m1 = MainModel({
            'modelfield': {'extrafield':'qweasd'},
            }, strict=True)

    m1 = MainModel({
        'modelfield': {'extrafield':'qweasd'},
        }, strict=False)

    assert m1.modelfield._data == {'subfield': None, 'submodelfield': None}

    with pytest.raises(ModelConversionError):
        m1 = MainModel({
            'modelfield': {'submodelfield': {'extrafield':'qweasd'}},
            }, strict=True)

    m1 = MainModel({
        'modelfield': {'submodelfield': {'extrafield':'qweasd'}},
        }, strict=False)

