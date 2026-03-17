# -*- coding: utf-8 -*-

import pytest

from schematics.common import *
from schematics.models import Model
from schematics.types import *
from schematics.types.compound import *
from schematics.types.serializable import serializable
from schematics.undefined import *
from schematics.util import *


def test_undefined():

    assert str(Undefined) == 'Undefined'
    assert repr(Undefined) == 'Undefined'

    assert Undefined == Undefined
    assert not Undefined != Undefined

    assert bool(Undefined) is False

    assert Undefined not in (None, True, False, 0, 1)

    with pytest.raises(TypeError):
        int(Undefined)

    with pytest.raises(TypeError):
        Undefined < 0
    with pytest.raises(TypeError):
        Undefined > 0
    with pytest.raises(TypeError):
        Undefined >= 0
    with pytest.raises(TypeError):
        Undefined >= 0

    with pytest.raises(TypeError):
        Undefined()

    with pytest.raises(TypeError):
        Undefined.x = 1
    with pytest.raises(AttributeError):
        Undefined.x

    assert UndefinedType() is Undefined

    with pytest.raises(TypeError):
        class DerivativeType(UndefinedType):
            pass
        DerivativeType()


def test_setdefault():

    class A(object):
        y = 2
        z = None

    class B(A):
        pass

    b = B()
    result = setdefault(b, 'i', 9)
    assert result == b.i == 9
    with pytest.raises(AttributeError):
        B.i

    b = B()
    b.i = 1
    result = setdefault(b, 'i', 9)
    assert result == b.i == 1

    b = B()
    b.i = None
    result = setdefault(b, 'i', 9)
    assert result is b.i is None

    b = B()
    b.i = None
    result = setdefault(b, 'i', 9, overwrite_none=True)
    assert result == b.i == 9
    assert B.y == 2

    b = B()
    result = setdefault(b, 'y', 9)
    assert result == b.y == 9
    assert B.y == 2

    b = B()
    result = setdefault(b, 'y', 9, overwrite_none=True)
    assert result == b.y == 9
    assert B.y == 2

    b = B()
    result = setdefault(b, 'y', 9, search_mro=True)
    assert result == b.y == 2

    b = B()
    result = setdefault(b, 'z', 9, search_mro=True)
    assert result is b.z is None

    b = B()
    result = setdefault(b, 'z', 9, search_mro=True, overwrite_none=True)
    assert result == b.z == 9
    assert B.z is None


def test_constant():

    C = Constant('C', 99)
    assert C == 99
    assert C.name == 'C'

    with pytest.raises(ValueError):
        C = Constant('C', 'foo')

