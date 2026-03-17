# encoding: UTF-8
"""
Common tests that apply to multiple Attr-derived classes.
"""
import copy
from collections import namedtuple
try:
    from collections.abc import Mapping, ItemsView, KeysView, ValuesView
except ImportError:
    from collections import Mapping, ItemsView, KeysView, ValuesView
from itertools import chain
import pickle
from sys import version_info

import pytest
import six

from attrdict.default import AttrDefault
from attrdict.dictionary import AttrDict
from attrdict.mapping import AttrMap
from attrdict.mixins import Attr


Options = namedtuple(
    'Options',
    ('cls', 'constructor', 'mutable', 'iter_methods', 'view_methods',
     'recursive')
)


class AttrImpl(Attr):
    """
    An implementation of Attr.
    """

    def __init__(self, items=None, sequence_type=tuple):
        if items is None:
            items = {}
        elif not isinstance(items, Mapping):
            items = dict(items)

        self._mapping = items
        self._sequence_type = sequence_type

    def _configuration(self):
        """
        The configuration for an attrmap instance.
        """
        return self._sequence_type

    def __getitem__(self, key):
        """
        Access a value associated with a key.
        """
        return self._mapping[key]

    def __len__(self):
        """
        Check the length of the mapping.
        """
        return len(self._mapping)

    def __iter__(self):
        """
        Iterated through the keys.
        """
        return iter(self._mapping)

    def __getstate__(self):
        """
        Serialize the object.
        """
        return (self._mapping, self._sequence_type)

    def __setstate__(self, state):
        """
        Deserialize the object.
        """
        mapping, sequence_type = state
        self._mapping = mapping
        self._sequence_type = sequence_type

    @classmethod
    def _constructor(cls, mapping, configuration):
        """
        A standardized constructor.
        """
        return cls(mapping, sequence_type=configuration)


def common(cls, constructor=None, mutable=False, iter_methods=False,
           view_methods=False, recursive=True):
    """
    Iterates over tests common to multiple Attr-derived classes

    cls: The class that is being tested.
    mutable: (optional, False) Whether the object is supposed to be
        mutable.
    iter_methods: (optional, False) Whether the class implements
        iter<keys,values,items> under Python 2.
    view_methods: (optional, False) Whether the class implements
        view<keys,values,items> under Python 2.
    recursive: (optional, True) Whether recursive assignment works.
    """
    tests = (
        item_access, iteration, containment, length, equality,
        item_creation, item_deletion, sequence_typing, addition,
        to_kwargs, pickling,
    )

    mutable_tests = (
        pop, popitem, clear, update, setdefault, copying, deepcopying,
    )

    if constructor is None:
        constructor = cls

    options = Options(cls, constructor, mutable, iter_methods, view_methods,
                      recursive)

    if mutable:
        tests = chain(tests, mutable_tests)

    for test in tests:
        test.description = test.__doc__.format(cls=cls.__name__)

        yield test, options


def item_access(options):
    """Access items in {cls}."""
    mapping = options.constructor(
        {
            'foo': 'bar',
            '_lorem': 'ipsum',
            six.u('👻'): 'boo',
            3: 'three',
            'get': 'not the function',
            'sub': {'alpha': 'bravo'},
            'bytes': b'bytes',
            'tuple': ({'a': 'b'}, 'c'),
            'list': [{'a': 'b'}, {'c': 'd'}],
        }
    )

    # key that can be an attribute
    assert mapping['foo'] == 'bar'
    assert mapping.foo == 'bar'
    assert mapping('foo') == 'bar'
    assert mapping.get('foo') == 'bar'

    # key that cannot be an attribute
    assert mapping[3] == 'three'
    pytest.raises(TypeError, getattr, mapping, 3)
    assert mapping(3) == 'three'
    assert mapping.get(3) == 'three'

    # key that cannot be an attribute (sadly)
    assert mapping[six.u('👻')] == 'boo'
    if six.PY2:
        pytest.raises(UnicodeEncodeError, getattr, mapping, six.u('👻'))
    else:
        pytest.raises(AttributeError, getattr, mapping, six.u('👻'))
    assert mapping(six.u('👻')) == 'boo'
    assert mapping.get(six.u('👻')) == 'boo'

    # key that represents a hidden attribute
    assert mapping['_lorem'] == 'ipsum'
    pytest.raises(AttributeError, lambda: mapping._lorem)
    assert mapping('_lorem') == 'ipsum'
    assert mapping.get('_lorem') == 'ipsum'

    # key that represents an attribute that already exists
    assert mapping['get'] == 'not the function'
    assert mapping.get != 'not the function'
    assert mapping('get') == 'not the function'
    assert mapping.get('get') == 'not the function'

    # does recursion work
    pytest.raises(AttributeError, lambda: mapping['sub'].alpha)
    assert mapping.sub.alpha == 'bravo'
    assert mapping('sub').alpha == 'bravo'
    pytest.raises(AttributeError, lambda: mapping.get('sub').alpha)

    # bytes
    assert mapping['bytes'] == b'bytes'
    assert mapping.bytes == b'bytes'
    assert mapping('bytes') == b'bytes'
    assert mapping.get('bytes') == b'bytes'

    # tuple
    assert mapping['tuple'] == ({'a': 'b'}, 'c')
    assert mapping.tuple == ({'a': 'b'}, 'c')
    assert mapping('tuple') == ({'a': 'b'}, 'c')
    assert mapping.get('tuple') == ({'a': 'b'}, 'c')

    pytest.raises(AttributeError, lambda: mapping['tuple'][0].a)
    assert mapping.tuple[0].a == 'b'
    assert mapping('tuple')[0].a == 'b'
    pytest.raises(AttributeError, lambda: mapping.get('tuple')[0].a)

    assert isinstance(mapping['tuple'], tuple)
    assert isinstance(mapping.tuple, tuple)
    assert isinstance(mapping('tuple'), tuple)
    assert isinstance(mapping.get('tuple'), tuple)

    assert isinstance(mapping['tuple'][0], dict)
    assert isinstance(mapping.tuple[0], options.cls)
    assert isinstance(mapping('tuple')[0], options.cls)
    assert isinstance(mapping.get('tuple')[0], dict)

    assert isinstance(mapping['tuple'][1], str)
    assert isinstance(mapping.tuple[1], str)
    assert isinstance(mapping('tuple')[1], str)
    assert isinstance(mapping.get('tuple')[1], str)

    # list
    assert mapping['list'] == [{'a': 'b'}, {'c': 'd'}]
    assert mapping.list == ({'a': 'b'}, {'c': 'd'})
    assert mapping('list') == ({'a': 'b'}, {'c': 'd'})
    assert mapping.get('list') == [{'a': 'b'}, {'c': 'd'}]

    pytest.raises(AttributeError, lambda: mapping['list'][0].a)
    assert mapping.list[0].a == 'b'
    assert mapping('list')[0].a == 'b'
    pytest.raises(AttributeError, lambda: mapping.get('list')[0].a)

    assert isinstance(mapping['list'], list)
    assert isinstance(mapping.list, tuple)
    assert isinstance(mapping('list'), tuple)
    assert isinstance(mapping.get('list'), list)

    assert isinstance(mapping['list'][0], dict)
    assert isinstance(mapping.list[0], options.cls)
    assert isinstance(mapping('list')[0], options.cls)
    assert isinstance(mapping.get('list')[0], dict)

    assert isinstance(mapping['list'][1], dict)
    assert isinstance(mapping.list[1], options.cls)
    assert isinstance(mapping('list')[1], options.cls)
    assert isinstance(mapping.get('list')[1], dict)

    # Nonexistent key
    pytest.raises(KeyError, lambda: mapping['fake'])
    pytest.raises(AttributeError, lambda: mapping.fake)
    pytest.raises(AttributeError, lambda: mapping('fake'))
    assert mapping.get('fake') == None
    assert mapping.get('fake', 'bake') == 'bake'


def iteration(options):
    "Iterate over keys/values/items in {cls}"
    raw = {'foo': 'bar', 'lorem': 'ipsum', 'alpha': 'bravo'}

    mapping = options.constructor(raw)

    expected_keys = frozenset(('foo', 'lorem', 'alpha'))
    expected_values = frozenset(('bar', 'ipsum', 'bravo'))
    expected_items = frozenset(
        (('foo', 'bar'), ('lorem', 'ipsum'), ('alpha', 'bravo'))
    )

    assert set(iter(mapping)) == expected_keys

    actual_keys = mapping.keys()
    actual_values = mapping.values()
    actual_items = mapping.items()

    if six.PY2:
        for collection in (actual_keys, actual_values, actual_items):
            assert isinstance(collection, list)

        assert frozenset(actual_keys) == expected_keys
        assert frozenset(actual_values) == expected_values
        assert frozenset(actual_items) == expected_items

        if options.iter_methods:
            actual_keys = mapping.iterkeys()
            actual_values = mapping.itervalues()
            actual_items = mapping.iteritems()

            for iterable in (actual_keys, actual_values, actual_items):
                assert not isinstance(iterable, list)

            assert frozenset(actual_keys) == expected_keys
            assert frozenset(actual_values) == expected_values
            assert frozenset(actual_items) == expected_items

        if options.view_methods:
            actual_keys = mapping.viewkeys()
            actual_values = mapping.viewvalues()
            actual_items = mapping.viewitems()

            # These views don't actually extend from collections.*View
            for iterable in (actual_keys, actual_values, actual_items):
                assert not isinstance(iterable, list)

            assert frozenset(actual_keys) == expected_keys
            assert frozenset(actual_values) == expected_values
            assert frozenset(actual_items) == expected_items

            # What happens if mapping isn't a dict
            from attrdict.mapping import AttrMap

            mapping = options.constructor(AttrMap(raw))

            actual_keys = mapping.viewkeys()
            actual_values = mapping.viewvalues()
            actual_items = mapping.viewitems()

            # These views don't actually extend from collections.*View
            for iterable in (actual_keys, actual_values, actual_items):
                assert not isinstance(iterable, list)

            assert frozenset(actual_keys) == expected_keys
            assert frozenset(actual_values) == expected_values
            assert frozenset(actual_items) == expected_items

    else:  # methods are actually views
        assert isinstance(actual_keys, KeysView)
        assert frozenset(actual_keys) == expected_keys

        assert isinstance(actual_values, ValuesView)
        assert frozenset(actual_values) == expected_values

        assert isinstance(actual_items, ItemsView)
        assert frozenset(actual_items) == expected_items

    # make sure empty iteration works
    assert tuple(options.constructor().items()) == ()


def containment(options):
    "Check whether {cls} contains keys"
    mapping = options.constructor(
        {'foo': 'bar', frozenset((1, 2, 3)): 'abc', 1: 2}
    )
    empty = options.constructor()

    assert 'foo' in mapping
    assert not 'foo' in empty

    assert frozenset((1, 2, 3)) in mapping
    assert not frozenset((1, 2, 3)) in empty

    assert 1 in mapping
    assert not 1 in empty

    assert not 'banana' in mapping
    assert not 'banana' in empty


def length(options):
    "Get the length of an {cls} instance"
    assert len(options.constructor()) == 0
    assert len(options.constructor({'foo': 'bar'})) == 1
    assert len(options.constructor({'foo': 'bar', 'baz': 'qux'})) == 2


def equality(options):
    "Equality checks for {cls}"
    empty = {}
    mapping_a = {'foo': 'bar'}
    mapping_b = {'lorem': 'ipsum'}

    constructor = options.constructor

    assert constructor(empty) == empty
    assert not constructor(empty) != empty
    assert not constructor(empty) == mapping_a
    assert constructor(empty) != mapping_a
    assert not constructor(empty) == mapping_b
    assert constructor(empty) != mapping_b

    assert not constructor(mapping_a) == empty
    assert constructor(mapping_a) != empty
    assert constructor(mapping_a) == mapping_a
    assert not constructor(mapping_a) != mapping_a
    assert not constructor(mapping_a) == mapping_b
    assert constructor(mapping_a) != mapping_b

    assert not constructor(mapping_b) == empty
    assert constructor(mapping_b) != empty
    assert not constructor(mapping_b) == mapping_a
    assert constructor(mapping_b) != mapping_a
    assert constructor(mapping_b) == mapping_b
    assert not constructor(mapping_b) != mapping_b

    assert constructor(empty) == constructor(empty)
    assert not constructor(empty) != constructor(empty)
    assert not constructor(empty) == constructor(mapping_a)
    assert constructor(empty) != constructor(mapping_a)
    assert not constructor(empty) == constructor(mapping_b)
    assert constructor(empty) != constructor(mapping_b)

    assert not constructor(mapping_a) == constructor(empty)
    assert constructor(mapping_a) != constructor(empty)
    assert constructor(mapping_a) == constructor(mapping_a)
    assert not constructor(mapping_a) != constructor(mapping_a)
    assert not constructor(mapping_a) == constructor(mapping_b)
    assert constructor(mapping_a) != constructor(mapping_b)

    assert not constructor(mapping_b) == constructor(empty)
    assert constructor(mapping_b) != constructor(empty)
    assert not constructor(mapping_b) == constructor(mapping_a)
    assert constructor(mapping_b) != constructor(mapping_a)
    assert constructor(mapping_b) == constructor(mapping_b)
    assert not constructor(mapping_b) != constructor(mapping_b)

    assert constructor((('foo', 'bar'),)) == {'foo': 'bar'}


def item_creation(options):
    "Add a key-value pair to an {cls}"

    if not options.mutable:
        # Assignment shouldn't add to the dict
        mapping = options.constructor()

        try:
            mapping.foo = 'bar'
        except TypeError:
            pass  # may fail if setattr modified
        else:
            pass  # may assign, but shouldn't assign to dict

        def item():
            """
            Attempt to add an item.
            """
            mapping['foo'] = 'bar'

        pytest.raises(TypeError, item)

        assert not 'foo' in mapping
    else:
        mapping = options.constructor()

        # key that can be an attribute
        mapping.foo = 'bar'

        assert mapping.foo == 'bar'
        assert mapping['foo'] == 'bar'
        assert mapping('foo') == 'bar'
        assert mapping.get('foo') == 'bar'

        mapping['baz'] = 'qux'

        assert mapping.baz == 'qux'
        assert mapping['baz'] == 'qux'
        assert mapping('baz') == 'qux'
        assert mapping.get('baz') == 'qux'

        # key that cannot be an attribute
        pytest.raises(TypeError, setattr, mapping, 1, 'one')

        assert 1 not in mapping

        mapping[2] = 'two'

        assert mapping[2] == 'two'
        assert mapping(2) == 'two'
        assert mapping.get(2) == 'two'

        # key that represents a hidden attribute
        def add_foo():
            "add _foo to mapping"
            mapping._foo = '_bar'

        pytest.raises(TypeError, add_foo)
        assert not '_foo' in mapping

        mapping['_baz'] = 'qux'

        def get_baz():
            "get the _foo attribute"
            return mapping._baz

        pytest.raises(AttributeError, get_baz)
        assert mapping['_baz'] == 'qux'
        assert mapping('_baz') == 'qux'
        assert mapping.get('_baz') == 'qux'

        # key that represents an attribute that already exists
        def add_get():
            "add get to mapping"
            mapping.get = 'attribute'

        pytest.raises(TypeError, add_get)
        assert not 'get' in mapping

        mapping['get'] = 'value'

        assert mapping.get != 'value'
        assert mapping['get'] == 'value'
        assert mapping('get') == 'value'
        assert mapping.get('get') == 'value'

        # rewrite a value
        mapping.foo = 'manchu'

        assert mapping.foo == 'manchu'
        assert mapping['foo'] == 'manchu'
        assert mapping('foo') == 'manchu'
        assert mapping.get('foo') == 'manchu'

        mapping['bar'] = 'bell'

        assert mapping.bar == 'bell'
        assert mapping['bar'] == 'bell'
        assert mapping('bar') == 'bell'
        assert mapping.get('bar') == 'bell'

        if options.recursive:
            recursed = options.constructor({'foo': {'bar': 'baz'}})

            recursed.foo.bar = 'qux'
            recursed.foo.alpha = 'bravo'

            assert recursed == {'foo': {'bar': 'qux', 'alpha': 'bravo'}}


def item_deletion(options):
    "Remove a key-value from to an {cls}"
    if not options.mutable:
        mapping = options.constructor({'foo': 'bar'})

        # could be a TypeError or an AttributeError
        try:
            del mapping.foo
        except TypeError:
            pass
        except AttributeError:
            pass
        else:
            raise AssertionError('deletion should fail')

        def item(mapping):
            """
            Attempt to del an item
            """
            del mapping['foo']

        pytest.raises(TypeError, item, mapping)

        assert mapping == {'foo': 'bar'}
        assert mapping.foo == 'bar'
        assert mapping['foo'] == 'bar'
    else:
        mapping = options.constructor(
            {'foo': 'bar', 'lorem': 'ipsum', '_hidden': True, 'get': 'value'}
        )

        del mapping.foo
        assert not 'foo' in mapping

        del mapping['lorem']
        assert not 'lorem' in mapping

        def del_hidden():
            "delete _hidden"
            del mapping._hidden

        try:
            del_hidden()
        except KeyError:
            pass
        except TypeError:
            pass
        else:
            assert not "Test raised the appropriate exception"
        # pytest.raises(TypeError, del_hidden)
        assert '_hidden' in mapping

        del mapping['_hidden']
        assert not 'hidden' in mapping

        def del_get():
            "delete get"
            del mapping.get

        pytest.raises(TypeError, del_get)
        assert 'get' in mapping
        assert mapping.get('get'), 'value'

        del mapping['get']
        assert not 'get' in mapping
        assert mapping.get('get', 'banana'), 'banana'


def sequence_typing(options):
    "Does {cls} respect sequence type?"
    data = {'list': [{'foo': 'bar'}], 'tuple': ({'foo': 'bar'},)}

    tuple_mapping = options.constructor(data)

    assert isinstance(tuple_mapping.list, tuple)
    assert tuple_mapping.list[0].foo == 'bar'

    assert isinstance(tuple_mapping.tuple, tuple)
    assert tuple_mapping.tuple[0].foo == 'bar'

    list_mapping = options.constructor(data, sequence_type=list)

    assert isinstance(list_mapping.list, list)
    assert list_mapping.list[0].foo == 'bar'

    assert isinstance(list_mapping.tuple, list)
    assert list_mapping.tuple[0].foo == 'bar'

    none_mapping = options.constructor(data, sequence_type=None)

    assert isinstance(none_mapping.list, list)
    pytest.raises(AttributeError, lambda: none_mapping.list[0].foo)

    assert isinstance(none_mapping.tuple, tuple)
    pytest.raises(AttributeError, lambda: none_mapping.tuple[0].foo)


def addition(options):
    "Adding {cls} to other mappings."
    left = {
        'foo': 'bar',
        'mismatch': False,
        'sub': {'alpha': 'beta', 'a': 'b'},
    }

    right = {
        'lorem': 'ipsum',
        'mismatch': True,
        'sub': {'alpha': 'bravo', 'c': 'd'},
    }

    merged = {
        'foo': 'bar',
        'lorem': 'ipsum',
        'mismatch': True,
        'sub': {'alpha': 'bravo', 'a': 'b', 'c': 'd'}
    }

    opposite = {
        'foo': 'bar',
        'lorem': 'ipsum',
        'mismatch': False,
        'sub': {'alpha': 'beta', 'a': 'b', 'c': 'd'}
    }

    constructor = options.constructor

    pytest.raises(TypeError, lambda: constructor() + 1)
    pytest.raises(TypeError, lambda: 1 + constructor())

    assert constructor() + constructor() == {}
    assert constructor() + {} == {}
    assert {} + constructor() == {}

    assert constructor(left) + constructor() == left
    assert constructor(left) + {} == left
    assert {} + constructor(left) == left

    assert constructor() + constructor(left) == left
    assert constructor() + left == left
    assert left + constructor() == left

    assert constructor(left) + constructor(right) == merged
    assert constructor(left) + right == merged
    assert left + constructor(right) == merged

    assert constructor(right) + constructor(left) == opposite
    assert constructor(right) + left == opposite
    assert right + constructor(left) == opposite

    # test sequence type changes
    data = {'sequence': [{'foo': 'bar'}]}

    assert isinstance((constructor(data) + {}).sequence, tuple)
    assert isinstance((constructor(data) + constructor()).sequence, tuple
                      )

    assert isinstance((constructor(data, list) + {}).sequence, list)
    # assert #     isinstance((constructor(data, list) + constructor()).sequence, tuple
    # )

    assert isinstance((constructor(data, list) + {}).sequence, list)
    assert isinstance(
        (constructor(data, list) + constructor({}, list)).sequence,
        list
    )


def to_kwargs(options):
    "**{cls}"

    def return_results(**kwargs):
        "Return result passed into a function"
        return kwargs

    expected = {'foo': 1, 'bar': 2}

    assert return_results(**options.constructor()) == {}
    assert return_results(**options.constructor(expected)) == expected


def check_pickle_roundtrip(source, options, **kwargs):
    """
    serialize then deserialize a mapping, ensuring the result and initial
    objects are equivalent.
    """
    source = options.constructor(source, **kwargs)
    data = pickle.dumps(source)
    loaded = pickle.loads(data)

    assert isinstance(loaded, options.cls)

    assert source == loaded

    return loaded


def pickling(options):
    "Pickle {cls}"

    empty = check_pickle_roundtrip(None, options)
    assert empty == {}

    mapping = check_pickle_roundtrip({'foo': 'bar'}, options)
    assert mapping == {'foo': 'bar'}

    # make sure sequence_type is preserved
    raw = {'list': [{'a': 'b'}], 'tuple': ({'a': 'b'},)}

    as_tuple = check_pickle_roundtrip(raw, options)
    assert isinstance(as_tuple['list'], list)
    assert isinstance(as_tuple['tuple'], tuple)
    assert isinstance(as_tuple.list, tuple)
    assert isinstance(as_tuple.tuple, tuple)

    as_list = check_pickle_roundtrip(raw, options, sequence_type=list)
    assert isinstance(as_list['list'], list)
    assert isinstance(as_list['tuple'], tuple)
    assert isinstance(as_list.list, list)
    assert isinstance(as_list.tuple, list)

    as_raw = check_pickle_roundtrip(raw, options, sequence_type=None)
    assert isinstance(as_raw['list'], list)
    assert isinstance(as_raw['tuple'], tuple)
    assert isinstance(as_raw.list, list)
    assert isinstance(as_raw.tuple, tuple)


def pop(options):
    "Popping from {cls}"

    mapping = options.constructor({'foo': 'bar', 'baz': 'qux'})

    pytest.raises(KeyError, lambda: mapping.pop('lorem'))
    assert mapping.pop('lorem', 'ipsum') == 'ipsum'
    assert mapping == {'foo': 'bar', 'baz': 'qux'}

    assert mapping.pop('baz') == 'qux'
    assert not 'baz' in mapping
    assert mapping == {'foo': 'bar'}

    assert mapping.pop('foo', 'qux') == 'bar'
    assert not 'foo' in mapping
    assert mapping == {}


def popitem(options):
    "Popping items from {cls}"
    expected = {'foo': 'bar', 'lorem': 'ipsum', 'alpha': 'beta'}
    actual = {}

    mapping = options.constructor(dict(expected))

    for _ in range(3):
        key, value = mapping.popitem()

        assert expected[key] == value
        actual[key] = value

    assert expected == actual

    pytest.raises(AttributeError, lambda: mapping.foo)
    pytest.raises(AttributeError, lambda: mapping.lorem)
    pytest.raises(AttributeError, lambda: mapping.alpha)
    pytest.raises(KeyError, mapping.popitem)


def clear(options):
    "clear the {cls}"

    mapping = options.constructor(
        {'foo': 'bar', 'lorem': 'ipsum', 'alpha': 'beta'}
    )

    mapping.clear()

    assert mapping == {}

    pytest.raises(AttributeError, lambda: mapping.foo)
    pytest.raises(AttributeError, lambda: mapping.lorem)
    pytest.raises(AttributeError, lambda: mapping.alpha)


def update(options):
    "update a {cls}"

    mapping = options.constructor({'foo': 'bar', 'alpha': 'bravo'})

    mapping.update(alpha='beta', lorem='ipsum')
    assert mapping == {'foo': 'bar', 'alpha': 'beta', 'lorem': 'ipsum'}

    mapping.update({'foo': 'baz', 1: 'one'})
    assert mapping == {'foo': 'baz', 'alpha': 'beta', 'lorem': 'ipsum', 1: 'one'}

    assert mapping.foo == 'baz'
    assert mapping.alpha == 'beta'
    assert mapping.lorem == 'ipsum'
    assert mapping(1) == 'one'


def setdefault(options):
    "{cls}.setdefault"

    mapping = options.constructor({'foo': 'bar'})

    assert mapping.setdefault('foo', 'baz') == 'bar'
    assert mapping.foo == 'bar'

    assert mapping.setdefault('lorem', 'ipsum') == 'ipsum'
    assert mapping.lorem == 'ipsum'

    assert mapping.setdefault('return_none') is None
    assert mapping.return_none is None

    assert mapping.setdefault(1, 'one') == 'one'
    assert mapping[1] == 'one'

    assert mapping.setdefault('_hidden', 'yes') == 'yes'
    pytest.raises(AttributeError, lambda: mapping._hidden)
    assert mapping['_hidden'] == 'yes'

    assert mapping.setdefault('get', 'value') == 'value'
    assert mapping.get != 'value'
    assert mapping['get'] == 'value'


def copying(options):
    "copying a {cls}"
    mapping_a = options.constructor({'foo': {'bar': 'baz'}})
    mapping_b = copy.copy(mapping_a)
    mapping_c = mapping_b

    mapping_b.foo.lorem = 'ipsum'

    assert mapping_a == mapping_b
    assert mapping_b == mapping_c

    mapping_c.alpha = 'bravo'


def deepcopying(options):
    "deepcopying a {cls}"
    mapping_a = options.constructor({'foo': {'bar': 'baz'}})
    mapping_b = copy.deepcopy(mapping_a)
    mapping_c = mapping_b

    mapping_b['foo']['lorem'] = 'ipsum'

    assert mapping_a != mapping_b
    assert mapping_b == mapping_c

    mapping_c.alpha = 'bravo'

    assert mapping_a != mapping_b
    assert mapping_b == mapping_c

    assert not 'lorem' in mapping_a.foo
    assert mapping_a.setdefault('alpha', 'beta') == 'beta'
    assert mapping_c.alpha == 'bravo'


@pytest.mark.parametrize(('test', 'options'), common(AttrImpl, mutable=False))
def test_attr(test, options):
    """
    Tests for an class that implements Attr.
    """
    test(options)


@pytest.mark.parametrize(('test', 'options'), common(AttrMap, mutable=True))
def test_attrmap(test, options):
    """
    Run AttrMap against the common tests.
    """
    test(options)


def attr_dict_constructor(items=None, sequence_type=tuple):
    """
    Build a new AttrDict.
    """
    if items is None:
        items = {}

    return AttrDict._constructor(items, sequence_type)


@pytest.mark.parametrize(
    ('test', 'options'),
    common(
        AttrDict, constructor=attr_dict_constructor,
        mutable=True, iter_methods=True,
        view_methods=((2, 7) <= version_info < (3,)), recursive=False))
def test_attrdict(test, options):
    """
    Run AttrDict against the common tests.
    """
    test(options)


def attr_default_constructor(items=None, sequence_type=tuple):
    """
    Build a new AttrDefault.
    """
    if items is None:
        items = {}

    return AttrDefault(None, items, sequence_type)


@pytest.mark.parametrize(('test', 'options'), common(AttrDefault, constructor=attr_default_constructor, mutable=True))
def test_attrdefault(test, options):
    """
    Run AttrDefault against the common tests.
    """
    test(options)
