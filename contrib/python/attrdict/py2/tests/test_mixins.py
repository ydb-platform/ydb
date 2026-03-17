"""
Tests for the AttrDefault class.
"""
import pytest


def test_invalid_attributes():
    """
    Tests how set/delattr handle invalid attributes.
    """
    from attrdict.mapping import AttrMap

    mapping = AttrMap()

    # mapping currently has allow_invalid_attributes set to False
    def assign():
        """
        Assign to an invalid attribute.
        """
        mapping._key = 'value'

    pytest.raises(TypeError, assign)
    pytest.raises(AttributeError, lambda: mapping._key)
    assert mapping == {}

    mapping._setattr('_allow_invalid_attributes', True)

    assign()
    assert mapping._key == 'value'
    assert mapping == {}

    # delete the attribute
    def delete():
        """
        Delete an invalid attribute.
        """
        del mapping._key

    delete()
    pytest.raises(AttributeError, lambda: mapping._key)
    assert mapping == {}

    # now with disallowing invalid
    assign()
    mapping._setattr('_allow_invalid_attributes', False)

    pytest.raises(TypeError, delete)
    assert mapping._key == 'value'
    assert mapping == {}

    # force delete
    mapping._delattr('_key')
    pytest.raises(AttributeError, lambda: mapping._key)
    assert mapping == {}


def test_constructor():
    """
    _constructor MUST be implemented.
    """
    from attrdict.mixins import Attr

    class AttrImpl(Attr):
        """
        An implementation of attr that doesn't implement _constructor.
        """
        pass

    pytest.raises(NotImplementedError, lambda: AttrImpl._constructor({}, ()))
