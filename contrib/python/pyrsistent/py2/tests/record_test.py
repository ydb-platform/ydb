import pickle
import datetime
import pytest
import six
import uuid
from pyrsistent import (
    PRecord, field, InvariantException, ny, pset, PSet, CheckedPVector,
    PTypeError, pset_field, pvector_field, pmap_field, pmap, PMap,
    pvector, PVector, v, m)


class ARecord(PRecord):
    x = field(type=(int, float))
    y = field()


class Hierarchy(PRecord):
    point1 = field(ARecord)
    point2 = field(ARecord)
    points = pvector_field(ARecord)


class RecordContainingContainers(PRecord):
    map = pmap_field(str, str)
    vec = pvector_field(str)
    set = pset_field(str)


class UniqueThing(PRecord):
    id = field(type=uuid.UUID, factory=uuid.UUID)


class Something(object):
    pass

class Another(object):
    pass

def test_create_ignore_extra_true():
    h = Hierarchy.create(
        {'point1': {'x': 1, 'y': 'foo', 'extra_field_0': 'extra_data_0'},
         'point2': {'x': 1, 'y': 'foo', 'extra_field_1': 'extra_data_1'},
         'extra_field_2': 'extra_data_2',
         }, ignore_extra=True
    )
    assert h


def test_create_ignore_extra_true_sequence_hierarchy():
    h = Hierarchy.create(
        {'point1': {'x': 1, 'y': 'foo', 'extra_field_0': 'extra_data_0'},
         'point2': {'x': 1, 'y': 'foo', 'extra_field_1': 'extra_data_1'},
         'points': [{'x': 1, 'y': 'foo', 'extra_field_2': 'extra_data_2'},
                    {'x': 1, 'y': 'foo', 'extra_field_3': 'extra_data_3'}],
         'extra_field____': 'extra_data_2',
         }, ignore_extra=True
    )
    assert h


def test_create():
    r = ARecord(x=1, y='foo')
    assert r.x == 1
    assert r.y == 'foo'
    assert isinstance(r, ARecord)


def test_create_ignore_extra():
    r = ARecord.create({'x': 1, 'y': 'foo', 'z': None}, ignore_extra=True)
    assert r.x == 1
    assert r.y == 'foo'
    assert isinstance(r, ARecord)


def test_create_ignore_extra_false():
    with pytest.raises(AttributeError):
        _ = ARecord.create({'x': 1, 'y': 'foo', 'z': None})


def test_correct_assignment():
    r = ARecord(x=1, y='foo')
    r2 = r.set('x', 2.0)
    r3 = r2.set('y', 'bar')

    assert r2 == {'x': 2.0, 'y': 'foo'}
    assert r3 == {'x': 2.0, 'y': 'bar'}
    assert isinstance(r3, ARecord)


def test_direct_assignment_not_possible():
    with pytest.raises(AttributeError):
        ARecord().x = 1


def test_cannot_assign_undeclared_fields():
    with pytest.raises(AttributeError):
        ARecord().set('z', 5)


def test_cannot_assign_wrong_type_to_fields():
    try:
        ARecord().set('x', 'foo')
        assert False
    except PTypeError as e:
        assert e.source_class == ARecord
        assert e.field == 'x'
        assert e.expected_types == set([int, float])
        assert e.actual_type is type('foo')


def test_cannot_construct_with_undeclared_fields():
    with pytest.raises(AttributeError):
        ARecord(z=5)


def test_cannot_construct_with_fields_of_wrong_type():
    with pytest.raises(TypeError):
        ARecord(x='foo')


def test_support_record_inheritance():
    class BRecord(ARecord):
        z = field()

    r = BRecord(x=1, y='foo', z='bar')

    assert isinstance(r, BRecord)
    assert isinstance(r, ARecord)
    assert r == {'x': 1, 'y': 'foo', 'z': 'bar'}


def test_single_type_spec():
    class A(PRecord):
        x = field(type=int)

    r = A(x=1)
    assert r.x == 1

    with pytest.raises(TypeError):
        r.set('x', 'foo')


def test_remove():
    r = ARecord(x=1, y='foo')
    r2 = r.remove('y')

    assert isinstance(r2, ARecord)
    assert r2 == {'x': 1}


def test_remove_non_existing_member():
    r = ARecord(x=1, y='foo')

    with pytest.raises(KeyError):
        r.remove('z')


def test_field_invariant_must_hold():
    class BRecord(PRecord):
        x = field(invariant=lambda x: (x > 1, 'x too small'))
        y = field(mandatory=True)

    try:
        BRecord(x=1)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('x too small',)
        assert e.missing_fields == ('BRecord.y',)


def test_global_invariant_must_hold():
    class BRecord(PRecord):
        __invariant__ = lambda r: (r.x <= r.y, 'y smaller than x')
        x = field()
        y = field()

    BRecord(x=1, y=2)

    try:
        BRecord(x=2, y=1)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('y smaller than x',)
        assert e.missing_fields == ()


def test_set_multiple_fields():
    a = ARecord(x=1, y='foo')
    b = a.set(x=2, y='bar')

    assert b == {'x': 2, 'y': 'bar'}


def test_initial_value():
    class BRecord(PRecord):
        x = field(initial=1)
        y = field(initial=2)

    a = BRecord()
    assert a.x == 1
    assert a.y == 2


def test_enum_field():
    try:
        from enum import Enum
    except ImportError:
        return  # Enum not supported in this environment

    class TestEnum(Enum):
        x = 1
        y = 2

    class RecordContainingEnum(PRecord):
        enum_field = field(type=TestEnum)

    r = RecordContainingEnum(enum_field=TestEnum.x)
    assert r.enum_field == TestEnum.x

def test_type_specification_must_be_a_type():
    with pytest.raises(TypeError):
        class BRecord(PRecord):
            x = field(type=1)


def test_initial_must_be_of_correct_type():
    with pytest.raises(TypeError):
        class BRecord(PRecord):
            x = field(type=int, initial='foo')


def test_invariant_must_be_callable():
    with pytest.raises(TypeError):
        class BRecord(PRecord):
            x = field(invariant='foo')  # type: ignore


def test_global_invariants_are_inherited():
    class BRecord(PRecord):
        __invariant__ = lambda r: (r.x % r.y == 0, 'modulo')
        x = field()
        y = field()

    class CRecord(BRecord):
        __invariant__ = lambda r: (r.x > r.y, 'size')

    try:
        CRecord(x=5, y=3)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('modulo',)


def test_global_invariants_must_be_callable():
    with pytest.raises(TypeError):
        class CRecord(PRecord):
            __invariant__ = 1


def test_repr():
    r = ARecord(x=1, y=2)
    assert repr(r) == 'ARecord(x=1, y=2)' or repr(r) == 'ARecord(y=2, x=1)'


def test_factory():
    class BRecord(PRecord):
        x = field(type=int, factory=int)

    assert BRecord(x=2.5) == {'x': 2}


def test_factory_must_be_callable():
    with pytest.raises(TypeError):
        class BRecord(PRecord):
            x = field(type=int, factory=1)  # type: ignore


def test_nested_record_construction():
    class BRecord(PRecord):
        x = field(int, factory=int)

    class CRecord(PRecord):
        a = field()
        b = field(type=BRecord)

    r = CRecord.create({'a': 'foo', 'b': {'x': '5'}})
    assert isinstance(r, CRecord)
    assert isinstance(r.b, BRecord)
    assert r == {'a': 'foo', 'b': {'x': 5}}


def test_pickling():
    x = ARecord(x=2.0, y='bar')
    y = pickle.loads(pickle.dumps(x, -1))

    assert x == y
    assert isinstance(y, ARecord)

def test_supports_pickling_with_typed_container_fields():
    obj = RecordContainingContainers(
        map={'foo': 'bar'}, set=['hello', 'there'], vec=['a', 'b'])
    obj2 = pickle.loads(pickle.dumps(obj))
    assert obj == obj2

def test_all_invariant_errors_reported():
    class BRecord(PRecord):
        x = field(factory=int, invariant=lambda x: (x >= 0, 'x negative'))
        y = field(mandatory=True)

    class CRecord(PRecord):
        a = field(invariant=lambda x: (x != 0, 'a zero'))
        b = field(type=BRecord)

    try:
        CRecord.create({'a': 0, 'b': {'x': -5}})
        assert False
    except InvariantException as e:
        assert set(e.invariant_errors) == set(['x negative', 'a zero'])
        assert e.missing_fields == ('BRecord.y',)


def test_precord_factory_method_is_idempotent():
    class BRecord(PRecord):
        x = field()
        y = field()

    r = BRecord(x=1, y=2)
    assert BRecord.create(r) is r


def test_serialize():
    class BRecord(PRecord):
        d = field(type=datetime.date,
                  factory=lambda d: datetime.datetime.strptime(d, "%d%m%Y").date(),
                  serializer=lambda format, d: d.strftime('%Y-%m-%d') if format == 'ISO' else d.strftime('%d%m%Y'))

    assert BRecord(d='14012015').serialize('ISO') == {'d': '2015-01-14'}
    assert BRecord(d='14012015').serialize('other') == {'d': '14012015'}


def test_nested_serialize():
    class BRecord(PRecord):
        d = field(serializer=lambda format, d: format)

    class CRecord(PRecord):
        b = field()

    serialized = CRecord(b=BRecord(d='foo')).serialize('bar')

    assert serialized == {'b': {'d': 'bar'}}
    assert isinstance(serialized, dict)


def test_serializer_must_be_callable():
    with pytest.raises(TypeError):
        class CRecord(PRecord):
            x = field(serializer=1)  # type: ignore


def test_transform_without_update_returns_same_precord():
    r = ARecord(x=2.0, y='bar')
    assert r.transform([ny], lambda x: x) is r


class Application(PRecord):
    name = field(type=(six.text_type,) + six.string_types)
    image = field(type=(six.text_type,) + six.string_types)


class ApplicationVector(CheckedPVector):
    __type__ = Application


class Node(PRecord):
    applications = field(type=ApplicationVector)


def test_nested_create_serialize():
    node = Node(applications=[Application(name='myapp', image='myimage'),
                              Application(name='b', image='c')])

    node2 = Node.create({'applications': [{'name': 'myapp', 'image': 'myimage'},
                                          {'name': 'b', 'image': 'c'}]})

    assert node == node2

    serialized = node.serialize()
    restored = Node.create(serialized)

    assert restored == node


def test_pset_field_initial_value():
    """
    ``pset_field`` results in initial value that is empty.
    """
    class Record(PRecord):
        value = pset_field(int)
    assert Record() == Record(value=[])

def test_pset_field_custom_initial():
    """
    A custom initial value can be passed in.
    """
    class Record(PRecord):
        value = pset_field(int, initial=(1, 2))
    assert Record() == Record(value=[1, 2])

def test_pset_field_factory():
    """
    ``pset_field`` has a factory that creates a ``PSet``.
    """
    class Record(PRecord):
        value = pset_field(int)
    record = Record(value=[1, 2])
    assert isinstance(record.value, PSet)

def test_pset_field_checked_set():
    """
    ``pset_field`` results in a set that enforces its type.
    """
    class Record(PRecord):
        value = pset_field(int)
    record = Record(value=[1, 2])
    with pytest.raises(TypeError):
        record.value.add("hello")  # type: ignore

def test_pset_field_checked_vector_multiple_types():
    """
    ``pset_field`` results in a vector that enforces its types.
    """
    class Record(PRecord):
        value = pset_field((int, str))
    record = Record(value=[1, 2, "hello"])
    with pytest.raises(TypeError):
        record.value.add(object())

def test_pset_field_type():
    """
    ``pset_field`` enforces its type.
    """
    class Record(PRecord):
        value = pset_field(int)
    record = Record()
    with pytest.raises(TypeError):
        record.set("value", None)

def test_pset_field_mandatory():
    """
    ``pset_field`` is a mandatory field.
    """
    class Record(PRecord):
        value = pset_field(int)
    record = Record(value=[1])
    with pytest.raises(InvariantException):
        record.remove("value")

def test_pset_field_default_non_optional():
    """
    By default ``pset_field`` is non-optional, i.e. does not allow
    ``None``.
    """
    class Record(PRecord):
        value = pset_field(int)
    with pytest.raises(TypeError):
        Record(value=None)

def test_pset_field_explicit_non_optional():
    """
    If ``optional`` argument is ``False`` then ``pset_field`` is
    non-optional, i.e. does not allow ``None``.
    """
    class Record(PRecord):
        value = pset_field(int, optional=False)
    with pytest.raises(TypeError):
        Record(value=None)

def test_pset_field_optional():
    """
    If ``optional`` argument is true, ``None`` is acceptable alternative
    to a set.
    """
    class Record(PRecord):
        value = pset_field(int, optional=True)
    assert ((Record(value=[1, 2]).value, Record(value=None).value) ==
            (pset([1, 2]), None))

def test_pset_field_name():
    """
    The created set class name is based on the type of items in the set.
    """
    class Record(PRecord):
        value = pset_field(Something)
        value2 = pset_field(int)
    assert ((Record().value.__class__.__name__,
             Record().value2.__class__.__name__) ==
            ("SomethingPSet", "IntPSet"))

def test_pset_multiple_types_field_name():
    """
    The created set class name is based on the multiple given types of
    items in the set.
    """
    class Record(PRecord):
        value = pset_field((Something, int))

    assert (Record().value.__class__.__name__ ==
            "SomethingIntPSet")

def test_pset_field_name_string_type():
    """
    The created set class name is based on the type of items specified by name
    """
    class Record(PRecord):
        value = pset_field("__tests__.record_test.Something")
    assert Record().value.__class__.__name__ == "SomethingPSet"


def test_pset_multiple_string_types_field_name():
    """
    The created set class name is based on the multiple given types of
    items in the set specified by name
    """
    class Record(PRecord):
        value = pset_field(("__tests__.record_test.Something", "__tests__.record_test.Another"))

    assert Record().value.__class__.__name__ == "SomethingAnotherPSet"

def test_pvector_field_initial_value():
    """
    ``pvector_field`` results in initial value that is empty.
    """
    class Record(PRecord):
        value = pvector_field(int)
    assert Record() == Record(value=[])

def test_pvector_field_custom_initial():
    """
    A custom initial value can be passed in.
    """
    class Record(PRecord):
        value = pvector_field(int, initial=(1, 2))
    assert Record() == Record(value=[1, 2])

def test_pvector_field_factory():
    """
    ``pvector_field`` has a factory that creates a ``PVector``.
    """
    class Record(PRecord):
        value = pvector_field(int)
    record = Record(value=[1, 2])
    assert isinstance(record.value, PVector)

def test_pvector_field_checked_vector():
    """
    ``pvector_field`` results in a vector that enforces its type.
    """
    class Record(PRecord):
        value = pvector_field(int)
    record = Record(value=[1, 2])
    with pytest.raises(TypeError):
        record.value.append("hello")  # type: ignore

def test_pvector_field_checked_vector_multiple_types():
    """
    ``pvector_field`` results in a vector that enforces its types.
    """
    class Record(PRecord):
        value = pvector_field((int, str))
    record = Record(value=[1, 2, "hello"])
    with pytest.raises(TypeError):
        record.value.append(object())

def test_pvector_field_type():
    """
    ``pvector_field`` enforces its type.
    """
    class Record(PRecord):
        value = pvector_field(int)
    record = Record()
    with pytest.raises(TypeError):
        record.set("value", None)

def test_pvector_field_mandatory():
    """
    ``pvector_field`` is a mandatory field.
    """
    class Record(PRecord):
        value = pvector_field(int)
    record = Record(value=[1])
    with pytest.raises(InvariantException):
        record.remove("value")

def test_pvector_field_default_non_optional():
    """
    By default ``pvector_field`` is non-optional, i.e. does not allow
    ``None``.
    """
    class Record(PRecord):
        value = pvector_field(int)
    with pytest.raises(TypeError):
        Record(value=None)

def test_pvector_field_explicit_non_optional():
    """
    If ``optional`` argument is ``False`` then ``pvector_field`` is
    non-optional, i.e. does not allow ``None``.
    """
    class Record(PRecord):
        value = pvector_field(int, optional=False)
    with pytest.raises(TypeError):
        Record(value=None)

def test_pvector_field_optional():
    """
    If ``optional`` argument is true, ``None`` is acceptable alternative
    to a sequence.
    """
    class Record(PRecord):
        value = pvector_field(int, optional=True)
    assert ((Record(value=[1, 2]).value, Record(value=None).value) ==
            (pvector([1, 2]), None))

def test_pvector_field_name():
    """
    The created set class name is based on the type of items in the set.
    """
    class Record(PRecord):
        value = pvector_field(Something)
        value2 = pvector_field(int)
    assert ((Record().value.__class__.__name__,
             Record().value2.__class__.__name__) ==
            ("SomethingPVector", "IntPVector"))

def test_pvector_multiple_types_field_name():
    """
    The created vector class name is based on the multiple given types of
    items in the vector.
    """
    class Record(PRecord):
        value = pvector_field((Something, int))

    assert (Record().value.__class__.__name__ ==
            "SomethingIntPVector")

def test_pvector_field_name_string_type():
    """
    The created set class name is based on the type of items in the set
    specified by name.
    """
    class Record(PRecord):
        value = pvector_field("__tests__.record_test.Something")
    assert Record().value.__class__.__name__ == "SomethingPVector"

def test_pvector_multiple_string_types_field_name():
    """
    The created vector class name is based on the multiple given types of
    items in the vector.
    """
    class Record(PRecord):
        value = pvector_field(("__tests__.record_test.Something", "__tests__.record_test.Another"))

    assert Record().value.__class__.__name__ == "SomethingAnotherPVector"

def test_pvector_field_create_from_nested_serialized_data():
    class Foo(PRecord):
        foo = field(type=str)

    class Bar(PRecord):
        bar = pvector_field(Foo)

    data = Bar(bar=v(Foo(foo="foo")))
    Bar.create(data.serialize()) == data

def test_pmap_field_initial_value():
    """
    ``pmap_field`` results in initial value that is empty.
    """
    class Record(PRecord):
        value = pmap_field(int, int)
    assert Record() == Record(value={})

def test_pmap_field_factory():
    """
    ``pmap_field`` has a factory that creates a ``PMap``.
    """
    class Record(PRecord):
        value = pmap_field(int, int)
    record = Record(value={1:  1234})
    assert isinstance(record.value, PMap)

def test_pmap_field_checked_map_key():
    """
    ``pmap_field`` results in a map that enforces its key type.
    """
    class Record(PRecord):
        value = pmap_field(int, type(None))
    record = Record(value={1: None})
    with pytest.raises(TypeError):
        record.value.set("hello", None)  # type: ignore

def test_pmap_field_checked_map_value():
    """
    ``pmap_field`` results in a map that enforces its value type.
    """
    class Record(PRecord):
        value = pmap_field(int, type(None))
    record = Record(value={1: None})
    with pytest.raises(TypeError):
        record.value.set(2, 4)  # type: ignore

def test_pmap_field_checked_map_key_multiple_types():
    """
    ``pmap_field`` results in a map that enforces its key types.
    """
    class Record(PRecord):
        value = pmap_field((int, str), type(None))
    record = Record(value={1: None, "hello": None})
    with pytest.raises(TypeError):
        record.value.set(object(), None)

def test_pmap_field_checked_map_value_multiple_types():
    """
    ``pmap_field`` results in a map that enforces its value types.
    """
    class Record(PRecord):
        value = pmap_field(int, (str, type(None)))
    record = Record(value={1: None, 3: "hello"})
    with pytest.raises(TypeError):
        record.value.set(2, 4)

def test_pmap_field_mandatory():
    """
    ``pmap_field`` is a mandatory field.
    """
    class Record(PRecord):
        value = pmap_field(int, int)
    record = Record()
    with pytest.raises(InvariantException):
        record.remove("value")

def test_pmap_field_default_non_optional():
    """
    By default ``pmap_field`` is non-optional, i.e. does not allow
    ``None``.
    """
    class Record(PRecord):
        value = pmap_field(int, int)
    # Ought to be TypeError, but pyrsistent doesn't quite allow that:
    with pytest.raises(AttributeError):
        Record(value=None)

def test_pmap_field_explicit_non_optional():
    """
    If ``optional`` argument is ``False`` then ``pmap_field`` is
    non-optional, i.e. does not allow ``None``.
    """
    class Record(PRecord):
        value = pmap_field(int, int, optional=False)
    # Ought to be TypeError, but pyrsistent doesn't quite allow that:
    with pytest.raises(AttributeError):
        Record(value=None)

def test_pmap_field_optional():
    """
    If ``optional`` argument is true, ``None`` is acceptable alternative
    to a set.
    """
    class Record(PRecord):
        value = pmap_field(int, int, optional=True)
    assert (Record(value={1: 2}).value, Record(value=None).value) == \
           (pmap({1: 2}), None)

def test_pmap_field_name():
    """
    The created map class name is based on the types of items in the map.
    """
    class Record(PRecord):
        value = pmap_field(Something, Another)
        value2 = pmap_field(int, float)
    assert ((Record().value.__class__.__name__,
             Record().value2.__class__.__name__) ==
            ("SomethingToAnotherPMap", "IntToFloatPMap"))

def test_pmap_field_name_multiple_types():
    """
    The created map class name is based on the types of items in the map,
    including when there are multiple supported types.
    """
    class Record(PRecord):
        value = pmap_field((Something, Another), int)
        value2 = pmap_field(str, (int, float))
    assert ((Record().value.__class__.__name__,
             Record().value2.__class__.__name__) ==
            ("SomethingAnotherToIntPMap", "StrToIntFloatPMap"))

def test_pmap_field_name_string_type():
    """
    The created map class name is based on the types of items in the map
    specified by name.
    """
    class Record(PRecord):
        value = pmap_field("__tests__.record_test.Something", "__tests__.record_test.Another")
    assert Record().value.__class__.__name__ == "SomethingToAnotherPMap"

def test_pmap_field_name_multiple_string_types():
    """
    The created map class name is based on the types of items in the map,
    including when there are multiple supported types.
    """
    class Record(PRecord):
        value = pmap_field(("__tests__.record_test.Something", "__tests__.record_test.Another"), int)
        value2 = pmap_field(str, ("__tests__.record_test.Something", "__tests__.record_test.Another"))
    assert ((Record().value.__class__.__name__,
             Record().value2.__class__.__name__) ==
            ("SomethingAnotherToIntPMap", "StrToSomethingAnotherPMap"))

def test_pmap_field_invariant():
    """
    The ``invariant`` parameter is passed through to ``field``.
    """
    class Record(PRecord):
        value = pmap_field(
            int, int,
            invariant=(
                lambda pmap: (len(pmap) == 1, "Exactly one item required.")
            )
        )
    with pytest.raises(InvariantException):
        Record(value={})
    with pytest.raises(InvariantException):
        Record(value={1: 2, 3: 4})
    assert Record(value={1: 2}).value == {1: 2}


def test_pmap_field_create_from_nested_serialized_data():
    class Foo(PRecord):
        foo = field(type=str)

    class Bar(PRecord):
        bar = pmap_field(str, Foo)

    data = Bar(bar=m(foo_key=Foo(foo="foo")))
    Bar.create(data.serialize()) == data


def test_supports_weakref():
    import weakref
    weakref.ref(ARecord(x=1, y=2))


def test_supports_lazy_initial_value_for_field():
    class MyRecord(PRecord):
        a = field(int, initial=lambda: 2)

    assert MyRecord() == MyRecord(a=2)


def test_pickle_with_one_way_factory():
    """
    A field factory isn't called when restoring from pickle.
    """
    thing = UniqueThing(id='25544626-86da-4bce-b6b6-9186c0804d64')
    assert thing == pickle.loads(pickle.dumps(thing))
