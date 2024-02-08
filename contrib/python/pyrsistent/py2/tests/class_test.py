from pyrsistent._compat import Hashable
import math
import pickle
import pytest
import sys
import uuid
from pyrsistent import (
    field, InvariantException, PClass, optional, CheckedPVector,
    pmap_field, pset_field, pvector_field)


class Point(PClass):
    x = field(type=int, mandatory=True, invariant=lambda x: (x >= 0, 'X negative'))
    y = field(type=int, serializer=lambda formatter, y: formatter(y))
    z = field(type=int, initial=0)


class Hierarchy(PClass):
    point = field(type=Point)


class TypedContainerObj(PClass):
    map = pmap_field(str, str)
    set = pset_field(str)
    vec = pvector_field(str)


class UniqueThing(PClass):
    id = field(type=uuid.UUID, factory=uuid.UUID)
    x = field(type=int)


def test_create_ignore_extra():
    p = Point.create({'x': 5, 'y': 10, 'z': 15, 'a': 0}, ignore_extra=True)
    assert p.x == 5
    assert p.y == 10
    assert p.z == 15
    assert isinstance(p, Point)


def test_create_ignore_extra_false():
    with pytest.raises(AttributeError):
        _ = Point.create({'x': 5, 'y': 10, 'z': 15, 'a': 0})


def test_create_ignore_extra_true():
    h = Hierarchy.create(
        {'point': {'x': 5, 'y': 10, 'z': 15, 'extra_field_0': 'extra_data_0'}, 'extra_field_1': 'extra_data_1'},
        ignore_extra=True)
    assert isinstance(h, Hierarchy)


def test_evolve_pclass_instance():
    p = Point(x=1, y=2)
    p2 = p.set(x=p.x+2)

    # Original remains
    assert p.x == 1
    assert p.y == 2

    # Evolved object updated
    assert p2.x == 3
    assert p2.y == 2

    p3 = p2.set('x', 4)
    assert p3.x == 4
    assert p3.y == 2


def test_direct_assignment_not_possible():
    p = Point(x=1, y=2)

    with pytest.raises(AttributeError):
        p.x = 1

    with pytest.raises(AttributeError):
        setattr(p, 'x', 1)


def test_direct_delete_not_possible():
    p = Point(x=1, y=2)
    with pytest.raises(AttributeError):
        del p.x

    with pytest.raises(AttributeError):
        delattr(p, 'x')


def test_cannot_construct_with_undeclared_fields():
    with pytest.raises(AttributeError):
        Point(x=1, p=5)


def test_cannot_construct_with_wrong_type():
    with pytest.raises(TypeError):
        Point(x='a')


def test_cannot_construct_without_mandatory_fields():
    try:
        Point(y=1)
        assert False
    except InvariantException as e:
        assert "[Point.x]" in str(e)


def test_field_invariant_must_hold():
    try:
        Point(x=-1)
        assert False
    except InvariantException as e:
        assert "X negative" in str(e)


def test_initial_value_set_when_not_present_in_arguments():
    p = Point(x=1, y=2)

    assert p.z == 0


class Line(PClass):
    p1 = field(type=Point)
    p2 = field(type=Point)


def test_can_create_nested_structures_from_dict_and_serialize_back_to_dict():
    source = dict(p1=dict(x=1, y=2, z=3), p2=dict(x=10, y=20, z=30))
    l = Line.create(source)

    assert l.p1.x == 1
    assert l.p1.y == 2
    assert l.p1.z == 3
    assert l.p2.x == 10
    assert l.p2.y == 20
    assert l.p2.z == 30

    assert l.serialize(format=lambda val: val) == source


def test_can_serialize_with_custom_serializer():
    p = Point(x=1, y=1, z=1)

    assert p.serialize(format=lambda v: v + 17) == {'x': 1, 'y': 18, 'z': 1}


def test_implements_proper_equality_based_on_equality_of_fields():
    p1 = Point(x=1, y=2)
    p2 = Point(x=3)
    p3 = Point(x=1, y=2)

    assert p1 == p3
    assert not p1 != p3
    assert p1 != p2
    assert not p1 == p2


def test_is_hashable():
    p1 = Point(x=1, y=2)
    p2 = Point(x=3, y=2)

    d = {p1: 'A point', p2: 'Another point'}

    p1_like = Point(x=1, y=2)
    p2_like = Point(x=3, y=2)

    assert isinstance(p1, Hashable)
    assert d[p1_like] == 'A point'
    assert d[p2_like] == 'Another point'
    assert Point(x=10) not in d


def test_supports_nested_transformation():
    l1 = Line(p1=Point(x=2, y=1), p2=Point(x=20, y=10))

    l2 = l1.transform(['p1', 'x'], 3)

    assert l1.p1.x == 2

    assert l2.p1.x == 3
    assert l2.p1.y == 1
    assert l2.p2.x == 20
    assert l2.p2.y == 10


def test_repr():
    class ARecord(PClass):
        a = field()
        b = field()

    assert repr(ARecord(a=1, b=2)) in ('ARecord(a=1, b=2)', 'ARecord(b=2, a=1)')


def test_global_invariant_check():
    class UnitCirclePoint(PClass):
        __invariant__ = lambda cp: (0.99 < math.sqrt(cp.x*cp.x + cp.y*cp.y) < 1.01,
                                    "Point not on unit circle")
        x = field(type=float)
        y = field(type=float)

    UnitCirclePoint(x=1.0, y=0.0)

    with pytest.raises(InvariantException):
        UnitCirclePoint(x=1.0, y=1.0)


def test_supports_pickling():
    p1 = Point(x=2, y=1)
    p2 = pickle.loads(pickle.dumps(p1, -1))

    assert p1 == p2
    assert isinstance(p2, Point)


def test_supports_pickling_with_typed_container_fields():
    obj = TypedContainerObj(map={'foo': 'bar'}, set=['hello', 'there'], vec=['a', 'b'])
    obj2 = pickle.loads(pickle.dumps(obj))
    assert obj == obj2


def test_can_remove_optional_member():
    p1 = Point(x=1, y=2)
    p2 = p1.remove('y')

    assert p2 == Point(x=1)


def test_cannot_remove_mandatory_member():
    p1 = Point(x=1, y=2)

    with pytest.raises(InvariantException):
        p1.remove('x')


def test_cannot_remove_non_existing_member():
    p1 = Point(x=1)

    with pytest.raises(AttributeError):
        p1.remove('y')


def test_evolver_without_evolution_returns_original_instance():
    p1 = Point(x=1)
    e = p1.evolver()

    assert e.persistent() is p1


def test_evolver_with_evolution_to_same_element_returns_original_instance():
    p1 = Point(x=1)
    e = p1.evolver()
    e.set('x', p1.x)

    assert e.persistent() is p1


def test_evolver_supports_chained_set_and_remove():
    p1 = Point(x=1, y=2)

    assert p1.evolver().set('x', 3).remove('y').persistent() == Point(x=3)


def test_evolver_supports_dot_notation_for_setting_and_getting_elements():
    e = Point(x=1, y=2).evolver()

    e.x = 3
    assert e.x == 3
    assert e.persistent() == Point(x=3, y=2)


class Numbers(CheckedPVector):
    __type__ = int


class LinkedList(PClass):
    value = field(type='__tests__.class_test.Numbers')
    next = field(type=optional('__tests__.class_test.LinkedList'))


def test_string_as_type_specifier():
    l = LinkedList(value=[1, 2], next=LinkedList(value=[3, 4], next=None))

    assert isinstance(l.value, Numbers)
    assert list(l.value) == [1, 2]
    assert l.next.next is None


def test_multiple_invariants_on_field():
    # If the invariant returns a list of tests the results of running those tests will be
    # a tuple containing result data of all failing tests.

    class MultiInvariantField(PClass):
        one = field(type=int, invariant=lambda x: ((False, 'one_one'),
                                                   (False, 'one_two'),
                                                   (True, 'one_three')))
        two = field(invariant=lambda x: (False, 'two_one'))

    try:
        MultiInvariantField(one=1, two=2)
        assert False
    except InvariantException as e:
        assert set(e.invariant_errors) == set([('one_one', 'one_two'), 'two_one'])


def test_multiple_global_invariants():
    class MultiInvariantGlobal(PClass):
        __invariant__ = lambda self: ((False, 'x'), (False, 'y'))
        one = field()

    try:
        MultiInvariantGlobal(one=1)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == (('x', 'y'),)


def test_inherited_global_invariants():
    class Distant(object):
        def __invariant__(self):
            return [(self.distant, "distant")]

    class Nearby(Distant):
        def __invariant__(self):
            return [(self.nearby, "nearby")]

    class MultipleInvariantGlobal(Nearby, PClass):
        distant = field()
        nearby = field()

    try:
        MultipleInvariantGlobal(distant=False, nearby=False)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == (("nearby",), ("distant",),)


def test_diamond_inherited_global_invariants():
    counter = []
    class Base(object):
        def __invariant__(self):
            counter.append(None)
            return [(False, "base")]

    class Left(Base):
        pass

    class Right(Base):
        pass

    class SingleInvariantGlobal(Left, Right, PClass):
        pass

    try:
        SingleInvariantGlobal()
        assert False
    except InvariantException as e:
        assert e.invariant_errors == (("base",),)
        assert counter == [None]

def test_supports_weakref():
    import weakref
    weakref.ref(Point(x=1, y=2))


def test_supports_weakref_with_multi_level_inheritance():
    import weakref

    class PPoint(Point):
        a = field()

    weakref.ref(PPoint(x=1, y=2))


def test_supports_lazy_initial_value_for_field():
    class MyClass(PClass):
        a = field(int, initial=lambda: 2)

    assert MyClass() == MyClass(a=2)


def test_type_checks_lazy_initial_value_for_field():
    class MyClass(PClass):
        a = field(int, initial=lambda: "a")

    with pytest.raises(TypeError):
        MyClass()


def test_invariant_checks_lazy_initial_value_for_field():
    class MyClass(PClass):
        a = field(int, invariant=lambda x: (x < 5, "Too large"), initial=lambda: 10)

    with pytest.raises(InvariantException):
        MyClass()


def test_invariant_checks_static_initial_value():
    class MyClass(PClass):
        a = field(int, invariant=lambda x: (x < 5, "Too large"), initial=10)

    with pytest.raises(InvariantException):
        MyClass()


def test_lazy_invariant_message():
    class MyClass(PClass):
        a = field(int, invariant=lambda x: (x < 5, lambda: "{x} is too large".format(x=x)))

    try:
        MyClass(a=5)
        assert False
    except InvariantException as e:
        assert '5 is too large' in e.invariant_errors

# Skipping this test for now but it describes a corner case with using Enums in
# python 3 as types and a workaround to make it work.
@pytest.mark.skipif(sys.version_info < (3, 4) or True, reason="requires python3.4")
def test_enum_key_type():
    import enum
    class Foo(enum.Enum):
        Bar = 1
        Baz = 2

    # This currently fails because the enum is iterable
    class MyClass1(PClass):
        f = pmap_field(key_type=Foo, value_type=int)

    MyClass1()

    # This is OK since it's wrapped in a tuple
    class MyClass2(PClass):
        f = pmap_field(key_type=(Foo,), value_type=int)

    MyClass2()


def test_pickle_with_one_way_factory():
    thing = UniqueThing(id='25544626-86da-4bce-b6b6-9186c0804d64')
    assert pickle.loads(pickle.dumps(thing)) == thing


def test_evolver_with_one_way_factory():
    thing = UniqueThing(id='cc65249a-56fe-4995-8719-ea02e124b234')
    ev = thing.evolver()
    ev.x = 5  # necessary to prevent persistent() returning the original
    assert ev.persistent() == UniqueThing(id=str(thing.id), x=5)


def test_set_doesnt_trigger_other_factories():
    thing = UniqueThing(id='b413b280-de76-4e28-a8e3-5470ca83ea2c')
    thing.set(x=5)


def test_set_does_trigger_factories():
    class SquaredPoint(PClass):
        x = field(factory=lambda x: x ** 2)
        y = field()

    sp = SquaredPoint(x=3, y=10)
    assert (sp.x, sp.y) == (9, 10)

    sp2 = sp.set(x=4)
    assert (sp2.x, sp2.y) == (16, 10)


def test_value_can_be_overridden_in_subclass_new():
    class X(PClass):
        y = pvector_field(int)

        def __new__(cls, **kwargs):
            items = kwargs.get('y', None)
            if items is None:
                kwargs['y'] = ()
            return super(X, cls).__new__(cls, **kwargs)

    a = X(y=[])
    b = a.set(y=None)
    assert a == b
