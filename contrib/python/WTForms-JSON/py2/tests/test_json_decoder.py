from pytest import raises
from wtforms import (
    BooleanField,
    Field,
    FieldList,
    Form,
    FormField,
    IntegerField,
    SelectMultipleField,
    StringField,
)

from wtforms_json import flatten_json, InvalidData


class TestJsonDecoder(object):
    def test_raises_error_if_given_data_not_dict_like(self):
        class MyForm(Form):
            pass
        with raises(InvalidData):
            flatten_json(MyForm, [])

    def test_unknown_attribute(self):
        class MyForm(Form):
            a = BooleanField()

        flatten_json(MyForm, {'b': 123})

    def test_unknown_attribute_without_skip_unknown_keys(self):
        class MyForm(Form):
            a = BooleanField()

        with raises(InvalidData):
            flatten_json(MyForm, {'b': 123}, skip_unknown_keys=False)

    def test_sharing_class_property(self):
        """ When an unknown attribute has the same name as a property on
        the Form class, the attribute is mistakenly seen as known because
        getattr can retrieve the property, but when property.field_class
        is accessed an error is raised. This tests that the error is not
        raised when skip_unknown_keys is True.
        """
        class MyForm(Form):
            a = BooleanField()

        flatten_json(MyForm, {'data': 13})

    def test_sharing_class_property_without_skip_unknown_keys(self):
        """ When an unknown attribute has the same name as a property on
        the Form class, the attribute is mistakenly seen as known because
        getattr can retrieve the property, but when property.field_class
        is accessed an error is raised. This tests that the error IS
        raised when skip_unknown_keys is False.
        """
        class MyForm(Form):
            a = BooleanField()

        with raises(InvalidData):
            flatten_json(MyForm, {'data': 123}, skip_unknown_keys=False)

    def test_supports_dicts(self):
        class MyForm(Form):
            a = BooleanField()
            b = IntegerField()

        assert (
            flatten_json(MyForm, {'a': False, 'b': 123}) ==
            {'a': False, 'b': 123}
        )

    def test_supports_select_multiple_field_decoding(self):
        class MyForm(Form):
            a = SelectMultipleField()

        assert flatten_json(MyForm, {'a': [1, 2, 3]}) == {'a': [1, 2, 3]}

    def test_supports_field_list_decoding(self):
        class MyForm(Form):
            a = FieldList(StringField())

        assert flatten_json(MyForm, {'a': [1, 2, 3]}) == {
            'a-0': 1,
            'a-1': 2,
            'a-2': 3
        }

    def test_supports_nested_dicts_and_lists(self):
        class OtherForm(Form):
            b = BooleanField()

        class MyForm(Form):
            a = FieldList(FormField(OtherForm))
        data = {
            'a': [{'b': True}]
        }
        assert flatten_json(MyForm, data) == {'a-0-b': True}

    def test_flatten_dict(self):
        class DeeplyNestedForm(Form):
            c = StringField()

        class NestedForm(Form):
            b = FormField(DeeplyNestedForm)

        class MyForm(Form):
            a = FormField(NestedForm)

        assert flatten_json(MyForm, {'a': {'b': {'c': 'd'}}}) == {
            'a-b-c': 'd'
        }

    def test_only_flatten_on_form_field(self):
        class DictField(Field):
            def process_formdata(self, valuelist):
                if valuelist:
                    data = valuelist[0]
                    if isinstance(data, dict):
                        self.data = data
                    else:
                        raise 'Unsupported datatype'
                else:
                    self.data = {}

        class MyForm(Form):
            a = IntegerField()
            b = DictField()

        assert (
            flatten_json(MyForm, {'a': False, 'b': {'key': 'value'}}) ==
            {'a': False, 'b': {'key': 'value'}}
        )

    def test_flatten_formfield_inheritance(self):
        class NestedForm(Form):
            b = StringField()

        class SpecialField(FormField):
            def __init__(self, *args, **kwargs):
                super(SpecialField, self).__init__(NestedForm, *args, **kwargs)

        class MyForm(Form):
            a = SpecialField()

        assert flatten_json(MyForm, {'a': {'b': 'c'}}) == {
            'a-b': 'c'
        }

    def test_flatten_listfield_inheritance(self):
        class SpecialField(FieldList):
            def __init__(self, *args, **kwargs):
                super(SpecialField, self).__init__(
                    StringField(),
                    *args,
                    **kwargs
                )

        class MyForm(Form):
            a = SpecialField()

        assert flatten_json(MyForm, {'a': [1, 2, 3]}) == {
            'a-0': 1,
            'a-1': 2,
            'a-2': 3
        }

    def test_flatten_nested_listfield_and_formfield_inheritance(self):
        class NestedForm(Form):
            b = StringField()

        class SpecialNestedField(FormField):
            def __init__(self, *args, **kwargs):
                super(SpecialNestedField, self).__init__(
                    NestedForm,
                    *args,
                    **kwargs
                )

        class SpecialField(FieldList):
            def __init__(self, *args, **kwargs):
                super(SpecialField, self).__init__(
                    SpecialNestedField(),
                    *args,
                    **kwargs
                )

        class MyForm(Form):
            a = SpecialField()

        assert flatten_json(MyForm, {'a': [{'b': 1}, {'b': 2}, {'b': 3}]}) == {
            'a-0-b': 1,
            'a-1-b': 2,
            'a-2-b': 3
        }
