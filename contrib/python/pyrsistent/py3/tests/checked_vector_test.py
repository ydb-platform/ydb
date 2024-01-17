import datetime
import pickle
import pytest
from pyrsistent import CheckedPVector, InvariantException, optional, CheckedValueTypeError, PVector


class Naturals(CheckedPVector):
    __type__ = int
    __invariant__ = lambda value: (value >= 0, 'Negative value')

def test_instantiate():
    x = Naturals([1, 2, 3])

    assert list(x) == [1, 2, 3]
    assert isinstance(x, Naturals)
    assert isinstance(x, PVector)

def test_append():
    x = Naturals()
    x2 = x.append(1)

    assert list(x2) == [1]
    assert isinstance(x2, Naturals)

def test_extend():
    x = Naturals()
    x2 = x.extend([1])

    assert list(x2) == [1]
    assert isinstance(x2, Naturals)

def test_set():
    x = Naturals([1, 2])
    x2 = x.set(1, 3)

    assert list(x2) == [1, 3]
    assert isinstance(x2, Naturals)


def test_invalid_type():
    try:
        Naturals([1, 2.0])
        assert False
    except CheckedValueTypeError as e:
        assert e.expected_types == (int,)
        assert e.actual_type is float
        assert e.actual_value == 2.0
        assert e.source_class is Naturals

    x = Naturals([1, 2])
    with pytest.raises(TypeError):
        x.append(3.0)

    with pytest.raises(TypeError):
        x.extend([3, 4.0])

    with pytest.raises(TypeError):
        x.set(1, 2.0)

    with pytest.raises(TypeError):
        x.evolver()[1] = 2.0

def test_breaking_invariant():
    try:
        Naturals([1, -1])
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('Negative value',)

    x = Naturals([1, 2])
    try:
        x.append(-1)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('Negative value',)

    try:
        x.extend([-1])
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('Negative value',)

    try:
        x.set(1, -1)
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('Negative value',)

def test_create_base_case():
    x =  Naturals.create([1, 2, 3])

    assert isinstance(x, Naturals)
    assert x == Naturals([1, 2, 3])

def test_create_with_instance_of_checked_pvector_returns_the_argument():
    x =  Naturals([1, 2, 3])

    assert Naturals.create(x) is x

class OptionalNaturals(CheckedPVector):
    __type__ = optional(int)
    __invariant__ = lambda value: (value is None or value >= 0, 'Negative value')

def test_multiple_allowed_types():
    assert list(OptionalNaturals([1, None, 3])) == [1, None, 3]

class NaturalsVector(CheckedPVector):
    __type__ = optional(Naturals)

def test_create_of_nested_structure():
    assert NaturalsVector([Naturals([1, 2]), Naturals([3, 4]), None]) ==\
           NaturalsVector.create([[1, 2], [3, 4], None])

def test_serialize_default_case():
    v = CheckedPVector([1, 2, 3])
    assert v.serialize() == [1, 2, 3]

class Dates(CheckedPVector):
    __type__ = datetime.date

    @staticmethod
    def __serializer__(format, d):
        return d.strftime(format)

def test_serialize_custom_serializer():
    d = datetime.date
    v = Dates([d(2015, 2, 2), d(2015, 2, 3)])
    assert v.serialize(format='%Y-%m-%d') == ['2015-02-02', '2015-02-03']

def test_type_information_is_inherited():
    class MultiDates(Dates):
        __type__ = int

    MultiDates([datetime.date(2015, 2, 4), 5])

    with pytest.raises(TypeError):
        MultiDates([5.0])

def test_invariants_are_inherited():
    class LimitNaturals(Naturals):
        __invariant__ = lambda value: (value < 10, 'Too big')

    try:
        LimitNaturals([10, -1])
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('Too big', 'Negative value')

def test_invariant_must_be_callable():
    with pytest.raises(TypeError):
        class InvalidInvariant(CheckedPVector):
            __invariant__ = 1

def test_type_spec_must_be_type():
    with pytest.raises(TypeError):
        class InvalidType(CheckedPVector):
            __type__ = 1

def test_repr():
    x = Naturals([1, 2])

    assert str(x) == 'Naturals([1, 2])'

def test_evolver_returns_same_instance_when_no_updates():
    x = Naturals([1, 2])
    assert x.evolver().persistent() is x

def test_pickling():
    x = Naturals([1, 2])
    y = pickle.loads(pickle.dumps(x, -1))

    assert x == y
    assert isinstance(y, Naturals)

def test_multiple_optional_types():
    class Numbers(CheckedPVector):
        __type__ = optional(int, float)

    numbers = Numbers([1, 2.5, None])
    assert numbers.serialize() == [1, 2.5, None]

    with pytest.raises(TypeError):
        numbers.append('foo')


class NaturalsVectorStr(CheckedPVector):
    __type__ = '__tests__.checked_vector_test.Naturals'


def test_check_with_string_specification():
    naturals_list = [Naturals([1, 2]), Naturals([3, 4])]
    nv = NaturalsVectorStr(naturals_list)
    assert nv == naturals_list


def test_create_with_string_specification():
    naturals_list = [[1, 2], [3, 4]]
    nv = NaturalsVectorStr.create(naturals_list)
    assert nv == naturals_list


def test_supports_weakref():
    import weakref
    weakref.ref(Naturals([]))


def test_create_with_generator_iterator():
    # See issue #97
    class Numbers(CheckedPVector):
        __type__ = int

    n = Numbers(i for i in [1, 2, 3])
    assert n == Numbers([1, 2, 3])