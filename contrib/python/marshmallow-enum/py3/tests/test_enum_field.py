# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
import warnings
from collections import namedtuple
from enum import Enum

import pytest
import marshmallow
from marshmallow import Schema, ValidationError
from marshmallow.fields import List
from marshmallow_enum import EnumField

PY2 = sys.version_info.major == 2
MARSHMALLOW_VERSION_MAJOR = int(marshmallow.__version__.split('.')[0])


class EnumTester(Enum):
    one = 1
    two = 2
    three = 3


SomeObj = namedtuple('SingleEnum', ['enum'])


class TestEnumFieldByName(object):

    def setup(self):
        self.field = EnumField(EnumTester)

    def test_serialize_enum(self):
        assert self.field._serialize(EnumTester.one, None, object()) == 'one'

    def test_serialize_none(self):
        assert self.field._serialize(None, None, object()) is None

    def test_deserialize_enum(self):
        assert self.field._deserialize('one', None, {}) == EnumTester.one

    def test_deserialize_none(self):
        assert self.field._deserialize(None, None, {}) is None

    def test_deserialize_nonexistent_member(self):
        with pytest.raises(ValidationError):
            self.field._deserialize('fred', None, {})


class TestEnumFieldValue(object):

    def test_deserialize_enum(self):
        field = EnumField(EnumTester, by_value=True)

        assert field._deserialize(1, None, {}) == EnumTester.one

    def test_serialize_enum(self):
        field = EnumField(EnumTester, by_value=True)
        assert field._serialize(EnumTester.one, None, object()) == 1

    def test_serialize_none(self):
        field = EnumField(EnumTester, by_value=True)
        assert field._serialize(None, None, object()) is None

    def test_deserialize_nonexistent_member(self):
        field = EnumField(EnumTester, by_value=True)

        with pytest.raises(ValidationError):
            field._deserialize(4, None, {})


class TestEnumFieldAsSchemaMember(object):

    class EnumSchema(Schema):
        enum = EnumField(EnumTester)
        none = EnumField(EnumTester)

    def test_enum_field_load(self):
        serializer = self.EnumSchema()

        data = serializer.load({'enum': 'one'})
        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == EnumTester.one

    def test_enum_field_dump(self):
        serializer = self.EnumSchema()

        data = serializer.dump(SomeObj(EnumTester.one))
        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == 'one'


class TestEnumByValueAsSchemaMember(object):

    class EnumSchema(Schema):
        enum = EnumField(EnumTester, by_value=True)
        none = EnumField(EnumTester, by_value=True)

    def test_enum_field_load(self):
        serializer = self.EnumSchema()

        data = serializer.load({'enum': 1})
        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == EnumTester.one

    def test_enum_field_dump(self):
        serializer = self.EnumSchema()

        data = serializer.dump(SomeObj(EnumTester.one))
        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == 1


class TestEnumFieldInListField(object):

    class ListEnumSchema(Schema):
        enum = List(EnumField(EnumTester))

    def test_enum_list_load(self):
        serializer = self.ListEnumSchema()

        data = serializer.load({'enum': ['one', 'two']})
        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == [EnumTester.one, EnumTester.two]

    def test_enum_list_dump(self):
        serializer = self.ListEnumSchema()
        data = serializer.dump(SomeObj([EnumTester.one, EnumTester.two]))

        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == ['one', 'two']


class TestEnumFieldByValueInListField(object):

    class ListEnumSchema(Schema):
        enum = List(EnumField(EnumTester, by_value=True))

    def test_enum_list_load(self):
        serializer = self.ListEnumSchema()
        data = serializer.load({'enum': [1, 2]})

        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == [EnumTester.one, EnumTester.two]

    def test_enum_list_dump(self):
        serializer = self.ListEnumSchema()
        data = serializer.dump(SomeObj([EnumTester.one, EnumTester.two]))

        if MARSHMALLOW_VERSION_MAJOR < 3:
            data = data.data

        assert data['enum'] == [1, 2]


class TestCustomErrorMessage(object):

    def test_custom_error_in_deserialize_by_value(self):
        if MARSHMALLOW_VERSION_MAJOR < 3:
            err_tpl = "{input} must be one of {choices}"
        else:
            err_tpl = "{input} must be one of {values}"

        field = EnumField(EnumTester, by_value=True, error=err_tpl)
        with pytest.raises(ValidationError) as excinfo:
            field._deserialize(4, None, {})

        expected = "4 must be one of 1, 2, 3"
        assert expected in str(excinfo.value)

    def test_custom_error_in_deserialize_by_name(self):
        if MARSHMALLOW_VERSION_MAJOR < 3:
            err_tpl = "{input} must be one of {choices}"
        else:
            err_tpl = "{input} must be one of {names}"

        field = EnumField(EnumTester, error=err_tpl)
        with pytest.raises(ValidationError) as excinfo:
            field._deserialize('four', None, {})
        expected = 'four must be one of one, two, three'
        assert expected in str(excinfo)

    def test_uses_default_error_if_no_custom_provided(self):
        field = EnumField(EnumTester, by_value=True)
        with pytest.raises(ValidationError) as excinfo:
            field._deserialize(4, None, {})
        expected = 'Invalid enum value 4'
        assert expected in str(excinfo.value)


class TestRegressions(object):

    @pytest.mark.parametrize('bad_value', [object, object(), [], {}, 1, 3.4, False, ()])
    def test_by_name_must_be_string(self, bad_value):

        class SomeEnum(Enum):
            red = 0
            yellow = 1
            green = 2

        class SomeSchema(Schema):
            if MARSHMALLOW_VERSION_MAJOR < 3:
                class Meta:
                    strict = True

            colors = EnumField(
                SomeEnum,
                by_value=False,
            )

        with pytest.raises(ValidationError) as excinfo:
            SomeSchema().load({'colors': bad_value})

        assert 'must be string' in str(excinfo.value)

    @pytest.mark.skipif(PY2, reason='py2 strings are bytes')
    def test_by_name_cannot_be_bytes(self):

        class SomeEnum(Enum):
            a = 1

        class SomeSchema(Schema):
            if MARSHMALLOW_VERSION_MAJOR < 3:
                class Meta:
                    strict = True

            f = EnumField(SomeEnum, by_value=False)

        with pytest.raises(ValidationError) as excinfo:
            SomeSchema().load({'f': b'a'})

        assert 'must be string' in str(excinfo.value)


class TestLoadDumpConfigBehavior(object):

    def test_load_and_dump_by_default_to_by_value(self):
        f = EnumField(EnumTester)
        assert f.load_by == EnumField.NAME
        assert f.dump_by == EnumField.NAME

    def test_load_by_raises_if_not_proper_value(self):
        with pytest.raises(ValueError) as excinfo:
            EnumField(EnumTester, load_by='lol')

        assert 'Invalid selection' in str(excinfo.value)

    def test_dump_by_raises_if_not_proper_value(self):
        with pytest.raises(ValueError) as excinfo:
            EnumField(EnumTester, dump_by='lol')

        assert 'Invalid selection' in str(excinfo.value)

    def test_dumps_to_value_when_configured_to(self):

        class SomeSchema(Schema):
            f = EnumField(EnumTester, dump_by=EnumField.VALUE)

        expected = {'f': 1}
        actual = SomeSchema().dump({'f': EnumTester.one})
        if MARSHMALLOW_VERSION_MAJOR < 3:
            actual = actual.data

        assert expected == actual

    def test_loads_to_value_when_configured_to(self):

        class SomeSchema(Schema):
            f = EnumField(EnumTester, load_by=EnumField.VALUE)

        expected = {'f': EnumTester.one}
        actual = SomeSchema().load({'f': 1})
        if MARSHMALLOW_VERSION_MAJOR < 3:
            actual = actual.data

        assert expected == actual

    def test_load_by_value_dump_by_name(self):

        class SomeSchema(Schema):
            f = EnumField(EnumTester, load_by=EnumField.VALUE, dump_by=EnumField.NAME)

        schema = SomeSchema()

        expected = {'f': 'one'}

        if MARSHMALLOW_VERSION_MAJOR < 3:
            actual = schema.dump(schema.load({'f': 1}).data).data
        else:
            actual = schema.dump(schema.load({'f': 1}))

        assert actual == expected

    def test_load_by_name_dump_by_value(self):

        class SomeSchema(Schema):
            f = EnumField(EnumTester, load_by=EnumField.NAME, dump_by=EnumField.VALUE)

        schema = SomeSchema()
        expected = {'f': 1}

        if MARSHMALLOW_VERSION_MAJOR < 3:
            actual = schema.dump(schema.load({'f': 'one'}).data).data
        else:
            actual = schema.dump(schema.load({'f': 'one'}))

        assert actual == expected


@pytest.mark.parametrize('format', ['{name}', '{value}', '{choices}'])
def test_old_error_format_inputs_are_deprecated(format):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always', DeprecationWarning)
        EnumField(EnumTester, error=format)
        warnings.simplefilter('default', DeprecationWarning)

    assert len(w) == 1
    assert issubclass(w[0].category, DeprecationWarning)
    assert "input" in str(w[0].message)
    assert "values" in str(w[0].message)
    assert "names" in str(w[0].message)


class TestUnicodeEnumValues(object):
    class UnicodeEnumTester(Enum):
        a = 'aæaф'
        b = 'basd'
        c = 'c僀23'

    values = 'aæaф, basd, c僀23'

    def test_user_error(self):
        with pytest.raises(ValidationError) as exc_info:
            EnumField(self.UnicodeEnumTester, error='{values}').fail('error')

        assert exc_info.value.messages[0] == self.values

    def test_by_value_error(self):
        class MyEnumField(EnumField):
            default_error_messages = {
                'by_value': '{values}'
            }

        with pytest.raises(ValidationError) as exc_info:
            EnumField(self.UnicodeEnumTester, error='{values}').fail('by_value')

        assert exc_info.value.messages[0] == self.values
