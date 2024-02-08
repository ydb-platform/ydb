import pickle
import pytest
from pyrsistent import CheckedPSet, PSet, InvariantException, CheckedType, CheckedPVector, CheckedValueTypeError


class Naturals(CheckedPSet):
    __type__ = int
    __invariant__ = lambda value: (value >= 0, 'Negative value')

def test_instantiate():
    x = Naturals([1, 2, 3, 3])

    assert list(x) == [1, 2, 3]
    assert isinstance(x, Naturals)
    assert isinstance(x, PSet)
    assert isinstance(x, CheckedType)

def test_add():
    x = Naturals()
    x2 = x.add(1)

    assert list(x2) == [1]
    assert isinstance(x2, Naturals)

def test_invalid_type():
    with pytest.raises(CheckedValueTypeError):
        Naturals([1, 2.0])

def test_breaking_invariant():
    try:
        Naturals([1, -1])
        assert False
    except InvariantException as e:
        assert e.invariant_errors == ('Negative value',)

def test_repr():
    x = Naturals([1, 2])

    assert str(x) == 'Naturals([1, 2])'

def test_default_serialization():
    x = Naturals([1, 2])

    assert x.serialize() == set([1, 2])

class StringNaturals(Naturals):
    @staticmethod
    def __serializer__(format, value):
        return format.format(value)

def test_custom_serialization():
    x = StringNaturals([1, 2])

    assert x.serialize("{0}") == set(["1", "2"])

class NaturalsVector(CheckedPVector):
    __type__ = Naturals

def test_multi_level_serialization():
    x = NaturalsVector.create([[1, 2], [3, 4]])

    assert str(x) == "NaturalsVector([Naturals([1, 2]), Naturals([3, 4])])"

    sx = x.serialize()
    assert sx == [set([1, 2]), set([3, 4])]
    assert isinstance(sx[0], set)

def test_create():
    assert Naturals.create([1, 2]) == Naturals([1, 2])

def test_evolver_returns_same_instance_when_no_updates():
    x = Naturals([1, 2])
    assert x.evolver().persistent() is x

def test_pickling():
    x = Naturals([1, 2])
    y = pickle.loads(pickle.dumps(x, -1))

    assert x == y
    assert isinstance(y, Naturals)


def test_supports_weakref():
    import weakref
    weakref.ref(Naturals([1, 2]))