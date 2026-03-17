# -*- coding: utf-8 -*-
import pytest

from schematics.models import Model
from schematics.types.base import StringType
from schematics.types.compound import ListType, ModelType
from schematics.types.serializable import serializable
from schematics.exceptions import DataError


def test_reason_why_we_must_bind_fields():
    class Person(Model):
        name = StringType()

    p1 = Person()
    p2 = Person()

    assert p1 == p2
    assert id(p1) != id(p2)

    assert p1.name == p2.name
    assert id(p1.name) == id(p2.name)

    p1.name = "Jóhann"
    p1.validate()
    p2.validate()

    assert p1 != p2
    assert id(p1) != id(p2)
    assert p1.name != p2.name
    assert id(p1.name) != id(p2.name)

    assert id(p1._fields["name"]) == id(p2._fields["name"])


def test_reason_why_we_must_bind_fields_model_field():
    class Location(Model):
        country_code = StringType(required=True)

    class Person(Model):
        location = ModelType(Location)

    p1 = Person()
    p2 = Person()

    assert p1 == p2
    assert id(p1) != id(p2)

    assert p1.location == p2.location
    assert id(p1.location) == id(p2.location)

    p1.location = {"country_code": "us"}
    assert p1.location.country_code == "us"

    p2.location = {}

    with pytest.raises(DataError):
        p2.validate()

    assert p1 != p2
    assert id(p1) != id(p2)
    assert p1.location != p2.location
    assert id(p1.location) != id(p2.location)

    assert id(p1._fields["location"]) == id(p2._fields["location"])


def test_field_binding():
    class Person(Model):
        name = StringType(required=True)

    class Course(Model):
        id = StringType(required=True, validators=[])
        attending = ListType(ModelType(Person))

    class School(Model):
        courses = ListType(ModelType(Course))

    valid_data = {
        'courses': [
            {'id': 'ENG103', 'attending': [
                {'name': u'Danny'},
                {'name': u'Sandy'}]},
            {'id': 'ENG203', 'attending': [
                {'name': u'Danny'},
                {'name': u'Sandy'}
            ]}
        ]
    }

    new_school = School(valid_data)
    new_school.validate()

    school = School(valid_data)
    school.validate()

    assert id(new_school) != id(school)

    assert id(new_school.courses[0].attending[0]) != id(school.courses[0].attending[0])


def test_serializable_doesnt_keep_global_state():
    class Location(Model):
        country_code = StringType()

        @serializable
        def country_name(self):
            return "United States" if self.country_code == "US" else "Unknown"

    location_US = Location({"country_code": "US"})
    location_IS = Location({"country_code": "IS"})

    assert id(location_US._serializables["country_name"]) == id(
        location_IS._serializables["country_name"])


def test_field_inheritance():

    class A(Model):
        field1 = StringType()
        field2 = StringType()

    class B(A):
        field2 = StringType(required=True)
        field3 = StringType()

    assert A.field1 is A._fields['field1'] is not B._fields['field1']
    assert A.field2 is A._fields['field2'] is not B._fields['field2']
    assert 'field3' not in A._fields

    assert B.field1 is B._fields['field1']
    assert B.field2 is B._fields['field2']
    assert B.field3 is B._fields['field3']

    assert A.field2.required is False and B.field2.required is True


def test_serializable_inheritance():

    class A(Model):
        @serializable
        def s(self):
            pass

    class B(A):
        pass

    assert A.s is A._serializables['s'] is not B._serializables['s']
    assert B.s is B._serializables['s']
    assert A.s.type is not B.s.type
    assert A.s.fget is B.s.fget

