# encoding: UTF-8
"""
Tests for the AttrDict class.
"""
from six import PY2


def test_init():
    """
    Create a new AttrDict.
    """
    from attrdict.dictionary import AttrDict

    # empty
    assert AttrDict() == {}
    assert AttrDict(()) == {}
    assert AttrDict({}) == {}

    # with items
    assert AttrDict({'foo': 'bar'}) == {'foo': 'bar'}
    assert AttrDict((('foo', 'bar'),)) == {'foo': 'bar'}
    assert AttrDict(foo='bar') == {'foo': 'bar'}

    # non-overlapping
    assert AttrDict({}, foo='bar') == {'foo': 'bar'}
    assert AttrDict((), foo='bar') == {'foo': 'bar'}

    assert AttrDict({'alpha': 'bravo'}, foo='bar') == \
           {'foo': 'bar', 'alpha': 'bravo'}

    assert AttrDict((('alpha', 'bravo'),), foo='bar') == \
           {'foo': 'bar', 'alpha': 'bravo'}

    # updating
    assert AttrDict({'alpha': 'bravo'}, foo='bar', alpha='beta') == \
           {'foo': 'bar', 'alpha': 'beta'}

    assert AttrDict((('alpha', 'bravo'), ('alpha', 'beta')), foo='bar') == \
           {'foo': 'bar', 'alpha': 'beta'}

    assert AttrDict((('alpha', 'bravo'), ('alpha', 'beta')), alpha='bravo') == \
           {'alpha': 'bravo'}


def test_copy():
    """
    Make a dict copy of an AttrDict.
    """
    from attrdict.dictionary import AttrDict

    mapping_a = AttrDict({'foo': {'bar': 'baz'}})
    mapping_b = mapping_a.copy()
    mapping_c = mapping_b

    mapping_b['foo']['lorem'] = 'ipsum'

    assert mapping_a == mapping_b
    assert mapping_b == mapping_c


def test_fromkeys():
    """
    make a new sequence from a set of keys.
    """
    from attrdict.dictionary import AttrDict

    # default value
    assert AttrDict.fromkeys(()) == {}
    assert AttrDict.fromkeys({'foo': 'bar', 'baz': 'qux'}) == \
           {'foo': None, 'baz': None}
    assert AttrDict.fromkeys(('foo', 'baz')) == \
           {'foo': None, 'baz': None}

    # custom value
    assert AttrDict.fromkeys((), 0) == {}
    assert AttrDict.fromkeys({'foo': 'bar', 'baz': 'qux'}, 0) == \
           {'foo': 0, 'baz': 0}
    assert AttrDict.fromkeys(('foo', 'baz'), 0) == \
           {'foo': 0, 'baz': 0}


def test_repr():
    """
    repr(AttrDict)
    """
    from attrdict.dictionary import AttrDict

    assert repr(AttrDict()) == "AttrDict({})"
    assert repr(AttrDict({'foo': 'bar'})) == "AttrDict({'foo': 'bar'})"
    assert repr(AttrDict({1: {'foo': 'bar'}}))
    "AttrDict({1: {'foo': 'bar'}})"
    assert repr(AttrDict({1: AttrDict({'foo': 'bar'})})) == "AttrDict({1: AttrDict({'foo': 'bar'})})"


if not PY2:
    def test_has_key():
        """
        The now-depricated has_keys method
        """
        from attrdict.dictionary import AttrDict

        assert not hasattr(AttrDict(), 'has_key')
