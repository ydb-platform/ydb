"""
Tests for the AttrDefault class.
"""
import pytest
from six import PY2


def test_method_missing():
    """
    default values for AttrDefault
    """
    from attrdict.default import AttrDefault

    default_none = AttrDefault()
    default_list = AttrDefault(list, sequence_type=None)
    default_double = AttrDefault(lambda value: value * 2, pass_key=True)

    pytest.raises(AttributeError, lambda: default_none.foo)
    pytest.raises(KeyError, lambda: default_none['foo'])
    assert default_none == {}

    assert default_list.foo == []
    assert default_list['bar'] == []
    assert default_list == {'foo': [], 'bar': []}

    assert default_double.foo == 'foofoo'
    assert default_double['bar'] == 'barbar'
    assert default_double == {'foo': 'foofoo', 'bar': 'barbar'}


def test_repr():
    """
    repr(AttrDefault)
    """
    from attrdict.default import AttrDefault

    assert repr(AttrDefault(None)) == "AttrDefault(None, False, {})"

    # list's repr changes between python 2 and python 3
    type_or_class = 'type' if PY2 else 'class'

    assert repr(AttrDefault(list)) == \
           type_or_class.join(("AttrDefault(<", " 'list'>, False, {})"))

    assert repr(AttrDefault(list, {'foo': 'bar'}, pass_key=True)) == \
           type_or_class.join(
               ("AttrDefault(<", " 'list'>, True, {'foo': 'bar'})")
           )
