# -*- coding: utf-8 -*-

import pytest

from schematics.common import PY2
from schematics.models import Model, ModelOptions
from schematics.transforms import whitelist, blacklist
from schematics.types import StringType, IntType, ListType, ModelType
from schematics.exceptions import *


def test_dict_methods_in_model():
    """
    a regression test to ensure that an issue where attributes on
    dictionaries are not being misintrepreted as actual schematics
    fields.
    """

    class M(Model):
        items, values, get, keys = IntType(), IntType(), IntType(), IntType()

    m = M({"items": 1, "values": 1, "get": 1, "keys": 1})
    m.validate()


def test_dict_methods_in_model_atoms():
    """
    atoms should return the raw values, and not call any overriden methods.
    """
    class M(Model):
        get = IntType()
    m = M({"get": 1})
    atom = list(m.atoms())[0]
    assert atom.name == "get"
    assert atom.value == 1


def test_nested_model_override_mapping_methods():
    """
    overriding mapping methods on child models should not cause issues
    with validation on the parent.
    """

    class Nested(Model):
        items, values, get, keys = IntType(), IntType(), IntType(), IntType()

    class Root(Model):
        keys = ModelType(Nested)

    root = Root({"keys": {"items": 1, "values": 1, "get": 1, "keys": 1}})
    root.validate()
    for key in ["items", "values", "get", "keys"]:
        assert getattr(root.keys, key) == 1


def test_init_with_dict():

    class M(Model):
        a, b, c, d = IntType(), IntType(), IntType(), IntType(default=0)

    m = M({'a': 1, 'b': None})
    assert m._data == {'a': 1, 'b': None, 'c': None, 'd': 0}
    assert m.a == 1
    assert m.b == None
    assert m.c == None
    assert m.d == 0

    m = M({'a': 1, 'b': None}, apply_defaults=False)
    assert m._data == {'a': 1, 'b': None, 'c': None, 'd': None}

    m = M({'a': 1, 'b': None}, init_values=False)
    assert m._data == {'a': 1, 'b': None, 'd': 0}

    m = M({'a': 1, 'b': None}, init=False)
    assert m._data == {'a': 1, 'b': None}

    m = M({'a': 1, 'b': None}, init=False, apply_defaults=True)
    assert m._data == {'a': 1, 'b': None, 'd': 0}

    m = M({'a': 1, 'b': None}, init=False, init_values=True)
    assert m._data == {'a': 1, 'b': None, 'c': None, 'd': None}

    m = M({'a': 1, 'b': None}, init=False, apply_defaults=True, init_values=True)
    assert m._data == {'a': 1, 'b': None, 'c': None, 'd': 0}


def test_defaults():

    class M(Model):
        d0 = IntType(default=0)
        dN = ListType(IntType, default=None)

    m = M()
    assert m._data == {'d0': 0, 'dN': None}
    m.validate()
    assert m._data == {'d0': 0, 'dN': None}

    m = M(apply_defaults=False)
    assert m._data == {'d0': None, 'dN': None}
    m.validate()
    assert m._data == {'d0': None, 'dN': None}

    m = M(init_values=False)
    assert m._data == {'d0': 0, 'dN': None}
    m.validate()
    assert m._data == {'d0': 0, 'dN': None}

    m = M(init=False)
    assert m._data == {}
    m.validate()
    assert m._data == {}

    m = M(init=False, apply_defaults=True)
    assert m._data == {'d0': 0, 'dN': None}
    m.validate()
    assert m._data == {'d0': 0, 'dN': None}


def test_invalid_model_fail_validation():
    class Player(Model):
        name = StringType(required=True)

    p = Player()
    assert p.name is None

    with pytest.raises(DataError):
        p.validate()


def test_invalid_models_validate_partially():
    class User(Model):
        name = StringType(required=True)

    u = User()
    u.validate(partial=True)


def test_model_with_rogue_field_throws_exception():
    class User(Model):
        name = StringType()

    with pytest.raises(DataError):
        User({'foo': 'bar'})


def test_equality():
    class Player(Model):
        id = IntType()

    p1 = Player({"id": 4})
    p2 = Player({"id": 4})

    assert p1 == p2

    p3 = Player({"id": 5})

    assert p1 == p2
    assert p1 != p3


def test_dict_interface():
    class Player(Model):
        name = StringType()

    p = Player()
    p.name = u"Jóhann"

    assert "name" in p
    assert p['name'] == u"Jóhann"
    assert 'fake_key'not in p


def test_init_model_from_another_model():
    class User(Model):
        name = StringType(required=True)
        bio = StringType(required=True)

    u = User(dict(name="A", bio="Asshole"))

    u2 = User(u)
    assert u == u2


def test_raises_validation_error_on_non_partial_validate():
    class User(Model):
        name = StringType(required=True)
        bio = StringType(required=True)

    u = User(dict(name="Joe"))

    with pytest.raises(DataError) as exception:
        u.validate()
    assert exception.value.messages, {"bio": [u"This field is required."]}


def test_model_inheritance():
    class Parent(Model):
        name = StringType(required=True)

    class Child(Parent):
        bio = StringType()

    input_data = {'bio': u'Genius', 'name': u'Joey'}

    model = Child(input_data)
    model.validate()

    assert model.serialize() == input_data

    child = Child({"name": "Baby Jane", "bio": "Always behaves"})
    assert child.name == "Baby Jane"
    assert child.bio == "Always behaves"


def test_validation_uses_internal_state():
    class User(Model):
        name = StringType(required=True)
        age = IntType(required=True)

    u = User({'name': u'Henry VIII'})
    u.age = 99
    u.validate()

    assert u.name == u'Henry VIII'
    assert u.age == 99


def test_validation_fails_if_internal_state_is_invalid():
    class User(Model):
        status = StringType()
        name = StringType(required=True)
        age = IntType(required=True)

    u = User()
    with pytest.raises(DataError) as exception:
        u.validate()

    assert exception.value.messages, {
        "name": ["This field is required."],
        "age": ["This field is required."],
    }

    assert u.status is None
    with pytest.raises(UndefinedValueError):
        u.name == u.age


def test_returns_nice_conversion_errors():
    class User(Model):
        name = StringType(required=True)
        age = IntType(required=True)

    with pytest.raises(DataError) as exception:
        User({"name": "Jóhann", "age": "100 years"})

    errors = exception.value.messages

    assert errors == {
        "age": [u'Value \'100 years\' is not int.'],
    }


def test_returns_partial_data_with_conversion_errors():
    class User(Model):
        name = StringType(required=True)
        age = IntType(required=True)
        account_level = IntType()

    with pytest.raises(DataError) as exception:
        User({"name": "Jóhann", "age": "100 years", "account_level": "3"})

    partial_data = exception.value.partial_data

    assert partial_data == {
        "name": u"Jóhann",
        "account_level": 3,
    }


def test_field_default():
    class User(Model):
        name = StringType(default=u'Doggy')

    u = User()
    assert User.name.__class__ == StringType
    assert u.name == u'Doggy'


def test_attribute_default_to_none_if_no_value():
    class User(Model):
        name = StringType()

    u = User()
    assert u.name is None


def test_field_has_default_value():
    class Question(Model):
        question_id = StringType(required=True)

        type = StringType(default="text")

    q = Question(dict(question_id=1))

    assert q.type == "text"
    assert "type" in q
    assert q.get("type") == "text"


def test_default_value_when_updating_model():
    class Question(Model):
        question_id = StringType(required=True)

        type = StringType(default="text")

    q = Question(dict(question_id=1, type="not default"))
    assert q.type == "not default"

    q.validate(dict(question_id=2))
    assert q.type == "not default"


def test_explicit_values_override_defaults():
    class User(Model):
        name = StringType(default=u'Doggy')

    u = User({"name": "Voffi"})
    u.validate()
    assert u.name == u'Voffi'

    u = User()
    u.name = "Guffi"
    u.validate()

    assert u.name == "Guffi"


def test_good_options_args():
    mo = ModelOptions(roles=None)
    assert mo is not None

    assert mo.roles == {}


def test_options_custom_args():
    class Foo(Model):
        class Options:
            _foo = 'bar'

    f = Foo()
    assert f._options._foo == 'bar'


def test_options_custom_args_inheritance():
    class Foo(Model):
        class Options:
            _foo = 'bar'

    class Moo(Foo):
        class Options:
            _bar = 'baz'

    m = Moo()
    assert m._options._foo == 'bar'
    assert m._options._bar == 'baz'


def test_no_options_args():
    args = {}
    mo = ModelOptions(**args)
    assert mo is not None


def test_options_parsing_from_model():
    class Foo(Model):

        class Options:
            namespace = 'foo'
            roles = {}

    f = Foo()
    fo = f._options

    assert fo.__class__ == ModelOptions
    assert fo.namespace == 'foo'
    assert fo.roles == {}


def test_options_parsing_from_optionsclass():
    class FooOptions(ModelOptions):

        def __init__(self, **kwargs):
            kwargs['namespace'] = kwargs.get('namespace') or 'foo'
            kwargs['roles'] = kwargs.get('roles') or {}
            super(FooOptions, self).__init__(**kwargs)

    class Foo(Model):
        __optionsclass__ = FooOptions

    f = Foo()
    fo = f._options

    assert fo.__class__ == FooOptions
    assert fo.namespace == 'foo'
    assert fo.roles == {}


def test_subclassing_preservers_roles():
    class Parent(Model):
        id = StringType()
        name = StringType()

        class Options:
            roles = {'public': blacklist("id")}

    class GrandParent(Parent):
        age = IntType()

    gramps = GrandParent({
        "id": "1",
        "name": "Edward",
        "age": 87
    })

    options = gramps._options

    assert options.roles == {
        "public": blacklist("id"),
    }


def test_subclassing_overides_roles():
    class Parent(Model):
        id = StringType()
        gender = StringType()
        name = StringType()

        class Options:
            roles = {
                'public': blacklist("id", "gender"),
                'gender': blacklist("gender")
            }

    class GrandParent(Parent):
        age = IntType()
        family_secret = StringType()

        class Options:
            roles = {
                'grandchildren': whitelist("age"),
                'public': blacklist("id", "family_secret")
            }

    gramps = GrandParent({
        "id": "1",
        "name": "Edward",
        "gender": "Male",
        "age": 87,
        "family_secret": "Secretly Canadian"
    })

    options = gramps._options

    assert options.roles == {
        "grandchildren": whitelist("age"),
        "public": blacklist("id", "family_secret"),
        "gender": blacklist("gender"),
    }


def test_as_field_validate():
    class User(Model):
        name = StringType()

    class Card(Model):
        user = ModelType(User)

    c = Card({"user": {'name': u'Doggy'}})
    assert c.user.name == u'Doggy'

    with pytest.raises(ConversionError):
        c.user = [1]

    c.validate()
    assert c.user.name == u'Doggy', u'Validation should not remove or modify existing data'


def test_model_field_validate_structure():
    class User(Model):
        name = StringType()

    class Card(Model):
        user = ModelType(User)

    with pytest.raises(DataError):
        Card({'user': [1, 2]})


def test_model_field_validate_only_when_field_is_set():
    class M0(Model):
        bar = StringType()

        def validate_bar(self, data, value):
            if data['bar'] and 'bar' not in data['bar']:
                raise ValidationError('Illegal value')

    class M1(Model):
        foo = StringType(required=True)

        def validate_foo(self, data, value):
            if 'foo' not in data['foo']:
                raise ValidationError('Illegal value')

    m = M0({})
    m.validate()

    m = M0({'bar': 'foo'})
    with pytest.raises(DataError) as e:
        m.validate()
        assert isinstance(e['foo'][0], ErrorMessage)
        assert 'Illegal value' in e['foo'][0]

    m = M0({'bar': 'foobar'})
    m.validate()

    m = M1({})
    with pytest.raises(DataError) as e:
        m.validate()
        assert isinstance(e['foo'], ConversionError)
        assert 'This field is required' in e['foo']

    m = M1({'foo': 'bar'})
    with pytest.raises(DataError) as e:
        m.validate()
        assert isinstance(e['foo'][0], ErrorMessage)
        assert 'Illegal value' in e['foo'][0]

    m = M1({'foo': 'foobar'})
    m.validate()


def test_model_deserialize_from_with_list():
    class User(Model):
        username = StringType(deserialize_from=['name', 'user'])

    assert User({'name': 'Ryan'}).username == 'Ryan'
    assert User({'user': 'Mike'}).username == 'Mike'
    assert User({'username': 'Mark'}).username == 'Mark'
    assert User({
        "username": "Mark",
        "name": "Second-class",
        "user": "key"
    }).username == 'Mark'


def test_model_deserialize_from_with_string():
    class User(Model):
        username = StringType(deserialize_from='name')

    assert User({'name': 'Mike'}).username == 'Mike'
    assert User({'username': 'Mark'}).username == 'Mark'
    assert User({'username': 'Mark', "name": "Second-class field"}).username == 'Mark'


def test_model_import_with_deserialize_mapping():
    class User(Model):
        username = StringType()

    mapping = {
        "username": ['name', 'user'],
    }

    assert User({'name': 'Ryan'}, deserialize_mapping=mapping).username == 'Ryan'
    assert User({'user': 'Mike'}, deserialize_mapping=mapping).username == 'Mike'
    assert User({'username': 'Mark'}, deserialize_mapping=mapping).username == 'Mark'
    assert User({'username': 'Mark', "name": "Second-class", "user": "key"},
                deserialize_mapping=mapping).username == 'Mark'


def test_model_import_data_with_mapping():
    class User(Model):
        username = StringType()

    mapping = {
        "username": ['name', 'user'],
    }

    user = User()
    user.import_data({'name': 'Ryan'}, mapping=mapping)
    assert user.username == 'Ryan'


def test_nested_model_import_data_with_mappings():
    class Nested(Model):
        nested_attr = StringType()

    class Root(Model):
        root_attr = StringType()
        nxt_level = ModelType(Nested)

    mapping = {
        'root_attr': ['attr'],
        'nxt_level': ['next'],
        'model_mapping': {
            'nxt_level': {
                'nested_attr': ['attr'],
            },
        },
    }

    root = Root()
    root.import_data({
        "attr": "root value",
        "next": {
            "attr": "nested value",
        },
    }, mapping=mapping)

    assert root.root_attr == 'root value'
    assert root.nxt_level.nested_attr == 'nested value'

    root = Root({
        "attr": "root value",
        "next": {
            "attr": "nested value",
        },
    }, deserialize_mapping=mapping)

    assert root.root_attr == 'root value'
    assert root.nxt_level.nested_attr == 'nested value'


class SimpleModel(Model):
    field1 = StringType()
    field2 = StringType()


def test_keys():
    inst = SimpleModel({'field1': 'foo',
                        'field2': 'bar'})

    assert inst.keys() == ['field1', 'field2']
    del inst.field2
    assert inst.keys() == ['field1']


def test_values():
    inst = SimpleModel({'field1': 'foo',
                        'field2': 'bar'})

    assert inst.values() == ['foo', 'bar']
    del inst.field2
    assert inst.values() == ['foo']


def test_items():
    inst = SimpleModel({'field1': 'foo',
                        'field2': 'bar'})

    assert inst.items() == [('field1', 'foo'), ('field2', 'bar')]
    del inst.field2
    assert inst.items() == [('field1', 'foo')]


def test_iter():
    inst = SimpleModel({'field1': 'foo',
                        'field2': 'bar'})

    assert [x for x in inst] == ['field1', 'field2']
    del inst.field2
    assert [x for x in inst] == ['field1']


def test_membership():
    inst = SimpleModel({'field1': 'foo',
                        'field2': 'bar'})

    assert 'field1' in inst and 'field2' in inst
    del inst.field2
    assert 'field1' in inst and 'field2' not in inst


def test_get():

    inst = SimpleModel({'field1': 'foo'})
    assert inst.get('field1') == 'foo'
    assert inst.get('foo') is None
    assert inst.get('foo', 'bar') == 'bar'

    inst = SimpleModel({'field1': 'foo'}, init=False)
    assert inst.get('foo') is None


def test_getitem():

    inst = SimpleModel({'field1': 'foo'})
    assert inst['field1'] == 'foo'
    assert inst['field2'] is None
    with pytest.raises(KeyError):
        inst['foo']

    inst = SimpleModel({'field1': 'foo'}, init=False)
    assert inst['field1'] == 'foo'
    with pytest.raises(UndefinedValueError):
        inst['field2']
    with pytest.raises(UnknownFieldError):
        inst['foo']


def test_setitem():
    inst = SimpleModel()

    with pytest.raises(KeyError):
        inst['foo'] = 1

    inst['field1'] = 'foo'
    assert inst.field1 == 'foo'


def test_delitem():
    inst = SimpleModel({'field1': 'foo'})

    with pytest.raises(KeyError):
        del inst['foo']

    del inst['field1']
    with pytest.raises(AttributeError):
        inst.field1


def test_eq():
    inst = SimpleModel({'field1': 'foo'})
    assert inst != 'foo'


def test_repr():
    inst = SimpleModel({'field1': 'foo'})
    assert repr(inst) == str(inst) == '<SimpleModel instance>'

    class FooModel(SimpleModel):
        def _repr_info(self):
            return str.join(', ', (self[k] for k in self))

    inst = FooModel({'field1': 'foo', 'field2': 'bar'})
    assert repr(inst) == '<FooModel: foo, bar>'

    inst = FooModel({'field1': u'é', 'field2': u'Ä'})
    if PY2:
        assert repr(inst) == '<FooModel: \\xe9, \\xc4>'
    else:
        assert repr(inst) == '<FooModel: é, Ä>'


def test_mock_recursive_model():

    class M(Model):
        m = ListType(ModelType('M', required=True), required=True)

    M.get_mock_object()


def test_append_field_to_model():

    class M(Model):
        a = IntType()

    M._append_field('b', StringType())

    input_data = {'a': 1, 'b': 'b'}

    m = M(input_data)
    assert m.b == 'b'
    assert m.serialize() == input_data
