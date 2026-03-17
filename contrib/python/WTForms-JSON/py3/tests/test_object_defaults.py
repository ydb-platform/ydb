import pytest

from wtforms import Form, IntegerField, StringField
from wtforms.validators import Optional


class MyForm(Form):
    a = IntegerField(validators=[Optional()])
    b = StringField()
    c = StringField(default='c')


class MyCallableForm(Form):
    a = IntegerField(validators=[Optional()])
    b = StringField()
    c = StringField(default=lambda: 'c')


@pytest.mark.parametrize('cls', [MyForm, MyCallableForm])
def test_object_defaults(cls):
    class SomeClass(object):
        a = 1
        b = 'someone'

    form = cls.from_json(obj=SomeClass())
    assert form.data == {'a': 1, 'b': 'someone', 'c': 'c'}


@pytest.mark.parametrize('cls', [MyForm, MyCallableForm])
def test_formdata_defaults(cls):
    form = cls.from_json({'a': 1, 'b': 'something'})
    assert form.data == {'a': 1, 'b': 'something', 'c': 'c'}
