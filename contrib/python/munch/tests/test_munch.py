# pylint: disable=unnecessary-lambda
import json
import pickle
from collections import namedtuple

import pytest
from munch import DefaultFactoryMunch, AutoMunch, DefaultMunch, Munch, munchify, unmunchify


def test_base():
    b = Munch()
    b.hello = 'world'
    assert b.hello == 'world'
    b['hello'] += "!"
    assert b.hello == 'world!'
    b.foo = Munch(lol=True)
    assert b.foo.lol is True
    assert b.foo is b['foo']

    assert sorted(b.keys()) == ['foo', 'hello']

    b.update({'ponies': 'are pretty!'}, hello=42)
    assert b == Munch({'ponies': 'are pretty!', 'foo': Munch({'lol': True}), 'hello': 42})

    assert sorted([(k, b[k]) for k in b]) == [('foo', Munch({'lol': True})), ('hello', 42), ('ponies', 'are pretty!')]

    format_munch = Munch(knights='lolcats', ni='can haz')
    assert "The {knights} who say {ni}!".format(**format_munch) == 'The lolcats who say can haz!'


def test_contains():
    b = Munch(ponies='are pretty!')
    assert 'ponies' in b
    assert ('foo' in b) is False

    b['foo'] = 42
    assert 'foo' in b

    b.hello = 'hai'
    assert 'hello' in b

    b[None] = 123
    assert None in b

    b[False] = 456
    assert False in b


def test_getattr():
    b = Munch(bar='baz', lol={})

    with pytest.raises(AttributeError):
        b.foo  # pylint: disable=pointless-statement

    assert b.bar == 'baz'
    assert getattr(b, 'bar') == 'baz'
    assert b['bar'] == 'baz'
    assert b.lol is b['lol']
    assert b.lol is getattr(b, 'lol')


def test_setattr():
    b = Munch(foo='bar', this_is='useful when subclassing')
    assert hasattr(b.values, '__call__')

    b.values = 'uh oh'
    assert b.values == 'uh oh'

    with pytest.raises(KeyError):
        b['values']  # pylint: disable=pointless-statement


def test_pickle():
    b = DefaultMunch.fromDict({"a": "b"})
    assert pickle.loads(pickle.dumps(b)) == b

def test_automunch():
    b = AutoMunch()
    b.urmom = {'sez': {'what': 'what'}}
    assert b.urmom.sez.what == 'what'  # pylint: disable=no-member


def test_delattr():
    b = Munch(lol=42)
    del b.lol

    with pytest.raises(KeyError):
        b['lol']  # pylint: disable=pointless-statement

    with pytest.raises(AttributeError):
        b.lol  # pylint: disable=pointless-statement


def test_toDict():
    b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
    assert sorted(b.toDict().items()) == [('foo', {'lol': True}), ('hello', 42), ('ponies', 'are pretty!')]

def test_dict_property():
    b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
    assert sorted(b.__dict__.items()) == [('foo', {'lol': True}), ('hello', 42), ('ponies', 'are pretty!')]

def test_repr():
    b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
    assert repr(b).startswith("Munch({'")
    assert "'ponies': 'are pretty!'" in repr(b)
    assert "'hello': 42" in repr(b)
    assert "'foo': Munch({'lol': True})" in repr(b)
    assert "'hello': 42" in repr(b)

    with_spaces = Munch({1: 2, 'a b': 9, 'c': Munch({'simple': 5})})
    assert repr(with_spaces).startswith("Munch({")
    assert "'a b': 9" in repr(with_spaces)
    assert "1: 2" in repr(with_spaces)
    assert "'c': Munch({'simple': 5})" in repr(with_spaces)

    assert eval(repr(with_spaces)) == Munch({'a b': 9, 1: 2, 'c': Munch({'simple': 5})})  # pylint: disable=eval-used


def test_dir():
    m = Munch(a=1, b=2)
    assert dir(m) == ['a', 'b']


def test_fromDict():
    b = Munch.fromDict({'urmom': {'sez': {'what': 'what'}}})
    assert b.urmom.sez.what == 'what'


def test_copy():
    m = Munch(urmom=Munch(sez=Munch(what='what')))
    c = m.copy()
    assert c is not m
    assert c.urmom is not m.urmom
    assert c.urmom.sez is not m.urmom.sez
    assert c.urmom.sez.what == 'what'
    assert c == m


def test_munchify():
    b = munchify({'urmom': {'sez': {'what': 'what'}}})
    assert b.urmom.sez.what == 'what'

    b = munchify({'lol': ('cats', {'hah': 'i win again'}), 'hello': [{'french': 'salut', 'german': 'hallo'}]})
    assert b.hello[0].french == 'salut'
    assert b.lol[1].hah == 'i win again'

def test_munchify_with_namedtuple():
    nt = namedtuple('nt', ['prop_a', 'prop_b'])

    b = munchify({'top': nt('in named tuple', 3)})
    assert b.top.prop_a == 'in named tuple'
    assert b.top.prop_b == 3

    b = munchify({'top': {'middle': nt(prop_a={'leaf': 'should be munchified'},
                                       prop_b={'leaf': 'should be munchified'})}})
    assert b.top.middle.prop_a.leaf == 'should be munchified'
    assert b.top.middle.prop_b.leaf == 'should be munchified'


def test_unmunchify():
    b = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
    assert sorted(unmunchify(b).items()) == [('foo', {'lol': True}), ('hello', 42), ('ponies', 'are pretty!')]

    b = Munch(foo=['bar', Munch(lol=True)], hello=42, ponies=('are pretty!', Munch(lies='are trouble!')))
    assert sorted(unmunchify(b).items()) == [('foo', ['bar', {'lol': True}]),
                                             ('hello', 42),
                                             ('ponies', ('are pretty!', {'lies': 'are trouble!'}))]


def test_unmunchify_namedtuple():
    nt = namedtuple('nt', ['prop_a', 'prop_b'])
    b = Munch(foo=Munch(lol=True), hello=nt(prop_a=42, prop_b='yop'), ponies='are pretty!')
    assert sorted(unmunchify(b).items()) == [('foo', {'lol': True}),
                                             ('hello', nt(prop_a=42, prop_b='yop')),
                                             ('ponies', 'are pretty!')]


def test_toJSON_and_fromJSON():
    # pylint: disable=unidiomatic-typecheck
    obj = Munch(foo=Munch(lol=True), hello=42, ponies='are pretty!')
    obj_json = obj.toJSON()
    assert json.dumps(obj) == obj_json
    new_obj = Munch.fromJSON(obj_json)
    assert type(obj) == Munch
    assert new_obj == obj

    default_value = object()
    dm_obj = DefaultMunch.fromJSON(obj_json, default_value)
    assert type(dm_obj) == DefaultMunch
    assert dm_obj == obj
    assert dm_obj['not_exist'] is default_value
    assert dm_obj.not_exist is default_value


@pytest.mark.parametrize("attrname", dir(Munch))
def test_reserved_attributes(attrname):
    # Make sure that the default attributes on the Munch instance are
    # accessible.

    taken_munch = Munch(**{attrname: 'abc123'})

    # Make sure that the attribute is determined as in the filled collection...
    assert attrname in taken_munch

    # ...and that it is available using key access...
    assert taken_munch[attrname] == 'abc123'

    # ...but that it is not available using attribute access.
    attr = getattr(taken_munch, attrname)
    assert attr != 'abc123'

    empty_munch = Munch()

    # Make sure that the attribute is not seen contained in the empty
    # collection...
    assert attrname not in empty_munch

    # ...and that the attr is of the correct original type.
    attr = getattr(empty_munch, attrname)
    if attrname == '__doc__':
        assert isinstance(attr, str)
    elif attrname in ('__hash__', '__weakref__'):
        assert attr is None
    elif attrname == '__module__':
        assert attr == 'munch'
    elif attrname == '__dict__':
        assert attr == {}
    elif attrname == '__static_attributes__':
        # Python 3.13: added __static_attributes__ attribute, populated by the
        # compiler, containing a tuple of names of attributes of this class
        # which are accessed through self.X from any function in its body.
        assert isinstance(attr, tuple)
    elif attrname == '__firstlineno__':
        # Python 3.13: added __firstlineno__ attribute, populated by the
        # compiler, containing the line number of the first line of the class definition
        assert isinstance(attr, int)
    else:
        assert callable(attr)


def test_getattr_default():
    b = DefaultMunch(bar='baz', lol={})
    assert b.foo is None
    assert b['foo'] is None

    assert b.bar == 'baz'
    assert getattr(b, 'bar') == 'baz'
    assert b['bar'] == 'baz'
    assert b.lol is b['lol']
    assert b.lol is getattr(b, 'lol')

    undefined = object()
    b = DefaultMunch(undefined, bar='baz', lol={})
    assert b.foo is undefined
    assert b['foo'] is undefined


def test_setattr_default():
    b = DefaultMunch(foo='bar', this_is='useful when subclassing')
    assert hasattr(b.values, '__call__')

    b.values = 'uh oh'
    assert b.values == 'uh oh'
    assert b['values'] is None

    assert b.__default__ is None
    assert '__default__' not in b


def test_delattr_default():
    b = DefaultMunch(lol=42)
    del b.lol

    assert b.lol is None
    assert b['lol'] is None


def test_pickle_default():
    b = DefaultMunch.fromDict({"a": "b"})
    assert pickle.loads(pickle.dumps(b)) == b


def test_fromDict_default():
    undefined = object()
    b = DefaultMunch.fromDict({'urmom': {'sez': {'what': 'what'}}}, undefined)
    assert b.urmom.sez.what == 'what'
    assert b.urmom.sez.foo is undefined


def test_copy_default():
    undefined = object()
    m = DefaultMunch.fromDict({'urmom': {'sez': {'what': 'what'}}}, undefined)
    c = m.copy()
    assert c is not m
    assert c.urmom is not m.urmom
    assert c.urmom.sez is not m.urmom.sez
    assert c.urmom.sez.what == 'what'
    assert c == m
    assert c.urmom.sez.foo is undefined
    assert c.urmom.sez.__undefined__ is undefined


def test_munchify_default():
    undefined = object()
    b = munchify(
        {'urmom': {'sez': {'what': 'what'}}},
        lambda d: DefaultMunch(undefined, d))
    assert b.urmom.sez.what == 'what'
    assert b.urdad is undefined
    assert b.urmom.sez.ni is undefined


def test_repr_default():
    b = DefaultMunch(foo=DefaultMunch(lol=True), ponies='are pretty!')
    assert repr(b).startswith("DefaultMunch(None, {'")
    assert "'ponies': 'are pretty!'" in repr(b)


def test_getattr_default_factory():
    b = DefaultFactoryMunch(lambda: None, bar='baz', lol={})
    assert b.foo is None
    assert b['foo'] is None

    assert b.bar == 'baz'
    assert getattr(b, 'bar') == 'baz'
    assert b['bar'] == 'baz'
    assert b.lol is b['lol']
    assert b.lol is getattr(b, 'lol')

    undefined = object()
    default = lambda: undefined
    b = DefaultFactoryMunch(default, bar='baz', lol={})
    assert b.foo is undefined
    assert b['foo'] is undefined

    default = lambda: object()
    b = DefaultFactoryMunch(default, bar='baz', lol={})
    assert b.foo is not b.baz
    assert b.foo is b['foo']
    assert b.foobar is b.foobar

    b = DefaultFactoryMunch(list)
    assert b.foo == []
    b.foo.append('bar')
    assert b.foo == ['bar']
    assert b.default_factory is list


def test_setattr_default_factory():
    b = DefaultFactoryMunch(lambda: None, foo='bar', this_is='useful when subclassing')
    assert hasattr(b.values, '__call__')

    b.values = 'uh oh'
    assert b.values == 'uh oh'
    assert b['values'] is None

    assert b.default_factory() is None
    assert 'default_factory' not in b


def test_delattr_default_factory():
    b = DefaultFactoryMunch(lambda: None, lol=42)
    del b.lol

    assert b.lol is None
    assert b['lol'] is None


def test_fromDict_default_factory():
    obj = object()
    undefined = lambda: obj
    b = DefaultFactoryMunch.fromDict({'urmom': {'sez': {'what': 'what'}}}, undefined)
    assert b.urmom.sez.what == 'what'
    assert b.urmom.sez.foo is undefined()


def test_copy_default_factory():
    undefined = lambda: object()
    m = DefaultFactoryMunch.fromDict({'urmom': {'sez': {'what': 'what'}}}, undefined)
    c = m.copy()
    assert c is not m
    assert c.urmom is not m.urmom
    assert c.urmom.sez is not m.urmom.sez
    assert c.urmom.sez.what == 'what'
    assert c == m


def test_munchify_default_factory():
    undefined = lambda: object()
    b = munchify(
        {'urmom': {'sez': {'what': 'what'}}},
        lambda d: DefaultFactoryMunch(undefined, d))
    assert b.urmom.sez.what == 'what'
    assert b.urdad is not undefined()
    assert b.urmom.sez.ni is not b.urdad


def test_munchify_cycle():
    # dict1 -> dict2 -> dict1
    x = dict(id="x")
    y = dict(x=x, id="y")
    x['y'] = y

    m = munchify(x)
    assert m.id == "x"
    assert m.y.id == "y"
    assert m.y.x is m

    # dict -> list -> dict
    x = dict(id="x")
    y = ["y", x]
    x["y"] = y

    m = munchify(x)
    assert m.id == "x"
    assert m.y[0] == "y"
    assert m.y[1] is m

    # dict -> tuple -> dict
    x = dict(id="x")
    y = ("y", x)
    x["y"] = y

    m = munchify(x)
    assert m.id == "x"
    assert m.y[0] == "y"
    assert m.y[1] is m

    # dict1 -> list -> dict2 -> list
    z = dict(id="z")
    y = ["y", z]
    z["y"] = y
    x = dict(id="x", y=y)

    m = munchify(x)
    assert m.id == "x"
    assert m.y[0] == "y"
    assert m.y[1].id == "z"
    assert m.y[1].y is m.y

    # dict1 -> tuple -> dict2 -> tuple
    z = dict(id="z")
    y = ("y", z)
    z["y"] = y
    x = dict(id="x", y=y)

    m = munchify(x)
    assert m.id == "x"
    assert m.y[0] == "y"
    assert m.y[1].id == "z"
    assert m.y[1].y is m.y

def test_unmunchify_cycle():
    # munch -> munch -> munch
    x = Munch(id="x")
    y = Munch(x=x, id="y")
    x.y = y

    d = unmunchify(x)
    assert d["id"] == "x"
    assert d["y"]["id"] == "y"
    assert d["y"]["x"] is d

    # munch -> list -> munch
    x = Munch(id="x")
    y = ["y", x]
    x.y = y

    d = unmunchify(x)
    assert d["id"] == "x"
    assert d["y"][0] == "y"
    assert d["y"][1] is d

    # munch -> tuple -> munch
    x = Munch(id="x")
    y = ("y", x)
    x.y = y

    d = unmunchify(x)
    assert d["id"] == "x"
    assert d["y"][0] == "y"
    assert d["y"][1] is d

    # munch1 -> list -> munch2 -> list
    z = Munch(id="z")
    y = ["y", z]
    z.y = y
    x = Munch(id="x", y=y)

    d = unmunchify(x)
    assert d["id"] == "x"
    assert d["y"][0] == "y"
    assert d["y"][1]["id"] == "z"
    assert d["y"][1]["y"] is d["y"]

    # munch1 -> tuple -> munch2 -> tuple
    z = Munch(id="z")
    y = ("y", z)
    z.y = y
    x = Munch(id="x", y=y)

    d = unmunchify(x)
    assert d["id"] == "x"
    assert d["y"][0] == "y"
    assert d["y"][1]["id"] == "z"
    assert d["y"][1]["y"] is d["y"]


def test_repr_default_factory():
    b = DefaultFactoryMunch(list, foo=DefaultFactoryMunch(list, lol=True), ponies='are pretty!')
    assert repr(b).startswith("DefaultFactoryMunch(list, {'")
    assert "'ponies': 'are pretty!'" in repr(b)

    assert eval(repr(b)) == b  # pylint: disable=eval-used


def test_pickling_unpickling_nested():
    m = {'a': {'b': 'c'}}
    m = munchify(m)
    assert m == Munch({'a': Munch({'b': 'c'})})
    assert isinstance(m.a, Munch)
    result = pickle.loads(pickle.dumps(m))
    assert result == m
    assert isinstance(result.a, Munch)


def test_setitem_dunder_for_subclass():

    def test_class(cls, *args):
        class CustomMunch(cls):
            def __setitem__(self, k, v):
                super().__setitem__(k, [v] * 2)
        custom_munch = CustomMunch(*args, a='foo')
        assert custom_munch.a == ['foo', 'foo']
        regular_dict = {}
        regular_dict.update(custom_munch)
        assert regular_dict['a'] == ['foo', 'foo']
        assert repr(regular_dict) == "{'a': ['foo', 'foo']}"
        custom_munch.setdefault('bar', 'baz')
        assert custom_munch.bar == ['baz', 'baz']

    test_class(Munch)
    test_class(DefaultFactoryMunch, list)
    test_class(DefaultMunch, 42)


def test_getitem_dunder_for_subclass():
    class CustomMunch(Munch):
        def __getitem__(self, k):
            return 42

    custom_munch = CustomMunch(a='foo')
    custom_munch.update({'b': 1})
    assert custom_munch.a == 42
    assert custom_munch.get('b') == 42
    assert custom_munch.copy() == Munch(a=42, b=42)


@pytest.mark.usefixtures("yaml")
def test_get_default_value(munch_obj):
    assert munch_obj.get("fake_key", "default_value") == "default_value"
    assert isinstance(munch_obj.toJSON(), str)
    assert isinstance(munch_obj.toYAML(), str)
    munch_obj.copy()
    data = munch_obj.toDict()
    munch_cls = type(munch_obj)
    kwargs = {} if munch_cls != DefaultFactoryMunch else {"default_factory": munch_obj.default_factory}
    munch_cls.fromDict(data, **kwargs)


def test_munchify_tuple_list():
    data = ([{'A': 'B'}],)
    actual = munchify(data)
    expected = ([Munch(A='B')],)
    assert actual == expected


def test_munchify_tuple_list_more_elements():
    data = (1, 2, [{'A': 'B'}])
    actual = munchify(data)
    expected = (1, 2, [Munch({'A': 'B'})])
    assert actual == expected
