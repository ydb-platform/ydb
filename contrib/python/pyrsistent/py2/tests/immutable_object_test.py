import pytest
from pyrsistent import immutable

class Empty(immutable(verbose=True)):
    pass


class Single(immutable('x')):
    pass


class FrozenMember(immutable('x, y_')):
    pass


class DerivedWithNew(immutable(['x', 'y'])):
    def __new__(cls, x, y):
        return super(DerivedWithNew, cls).__new__(cls, x, y)


def test_instantiate_object_with_no_members():
    t = Empty()
    t2 = t.set()

    assert t is t2


def test_assign_non_existing_attribute():
    t = Empty()

    with pytest.raises(AttributeError):
        t.set(a=1)


def test_basic_instantiation():
    t = Single(17)

    assert t.x == 17
    assert str(t) == 'Single(x=17)'


def test_cannot_modify_member():
    t = Single(17)

    with pytest.raises(AttributeError):
        t.x = 18

def test_basic_replace():
    t = Single(17)
    t2 = t.set(x=18)

    assert t.x == 17
    assert t2.x == 18


def test_cannot_replace_frozen_member():
    t = FrozenMember(17, 18)

    with pytest.raises(AttributeError):
        t.set(y_=18)


def test_derived_class_with_new():
    d = DerivedWithNew(1, 2)
    d2 = d.set(x=3)

    assert d2.x == 3
