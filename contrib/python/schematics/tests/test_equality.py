from unittest import mock

from schematics.models import Model
from schematics.types import IntType, StringType, ListType, ModelType


class M(Model):
    intfield = IntType()
    listfield = ListType(ModelType('M'))

class N(M):
    pass


def test_equality_against_mock_any():

    class TestModel(Model):
        pass

    assert TestModel() == mock.ANY


def test_equality_against_same_model():

    m1 = M({'intfield': 1, 'listfield': [M({'intfield': 9})]})
    m2 = M({'intfield': 1, 'listfield': [M({'intfield': 9})]})
    assert m1 == m2

    # Self-recursive data
    m1.listfield.append(m1)
    m2.listfield.append(m2)
    assert m1 == m2

    m2.listfield[0].intfield = 5
    assert m1 != m2


def test_equality_against_derived_model():

    m = M({'intfield': 1})
    n = N({'intfield': 1})
    assert m != n

