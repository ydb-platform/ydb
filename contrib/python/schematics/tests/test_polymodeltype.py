import pytest

from schematics.models import Model, DataError
from schematics.types import StringType
from schematics.types.compound import PolyModelType, ListType
from schematics.util import get_all_subclasses


class A(Model): # fallback model (doesn't define a claim method)
    stringA = StringType()

class Aaa(A):
    pass

class B(A):
    stringB = StringType()
    regexp_test = StringType(regex='^[0-9]$', required=False)
    @classmethod
    def _claim_polymorphic(cls, data):
        return data.get('stringB') == 'bbb'

class C(B):
    stringC = StringType()
    @classmethod
    def _claim_polymorphic(cls, data):
        return data.get('stringC') == 'ccc'

class X(Model):
    pass

def claim_func(field, data):
    if 'stringB' in data and field.name == 'cfn':
        return B
    if 'stringC' in data and field.name == 'cfn':
        return C
    else:
        return None


class Foo(Model):
    base   = PolyModelType(A)       # accepts any subclass for import and export
    strict = PolyModelType([A, B])  # accepts [A, B] for import and export
    nfb    = PolyModelType([B, C])  # no fallback since A not present
    cfn    = PolyModelType([B, C], claim_function=claim_func)


def test_get_all_subclasses():
    assert A.__subclasses__() == [Aaa, B]
    assert B.__subclasses__() == [C]
    assert C.__subclasses__() == []

    assert get_all_subclasses(A) == [Aaa, B, C]


def test_inheritance_based_polymorphic(): # base

    foo = Foo({'base': {'stringB': 'bbb'}})
    assert type(foo.base) is B

    foo = Foo({'base': {'stringC': 'ccc'}})
    assert type(foo.base) is C

    foo = Foo({'base': {}}) # unrecognizable input => fall back to A
    assert type(foo.base) is A

    foo = Foo({'base': Aaa()})
    assert type(foo.base) is Aaa
    foo.validate()
    foo.to_primitive()

def test_enumerated_polymorphic(): # strict

    foo = Foo({'strict': {'stringB': 'bbb'}})
    assert type(foo.strict) is B

    foo = Foo({'strict': {}}) # unrecognizable input => fall back to A
    assert type(foo.strict) is A

    foo = Foo({'strict': B()})
    assert type(foo.strict) is B
    foo.validate()
    foo.to_primitive()

    Foo.strict.allow_subclasses = True
    foo = Foo({'strict': Aaa()})
    Foo.strict.allow_subclasses = False

def test_external_claim_function(): # cfn

    foo = Foo({'cfn': {'stringB': 'bbb', 'stringC': 'ccc'}}, strict=False)
    assert type(foo.cfn) is B

def test_multiple_matches():

    with pytest.raises(Exception):
        foo = Foo({'base': {'stringB': 'bbb', 'stringC': 'ccc'}})

def test_no_fallback(): # nfb

    with pytest.raises(Exception):
        foo = Foo({'nfb': {}})

def test_refuse_unrelated_import():

    with pytest.raises(Exception):
        foo = Foo({'base': D()})

    with pytest.raises(Exception):
        foo = Foo({'strict': Aaa()})

def test_refuse_unrelated_export():

    with pytest.raises(Exception):
        foo = Foo()
        foo.base = X()
        foo.to_primitive()

    with pytest.raises(Exception):
        foo = Foo()
        foo.strict = Aaa()
        foo.to_primitive()


def test_specify_model_by_name():

    class M(Model):
        single = PolyModelType('M')
        multi = PolyModelType([A, 'M', C])
        nested = ListType(ListType(PolyModelType('M')))

    assert M.single.is_allowed_model(M())
    assert M.multi.is_allowed_model(M())
    assert M.nested.field.field.is_allowed_model(M())


def test_validate_recursively():
    # Should validate
    foo = Foo({'strict': {'stringB': 'bbb', 'regexp_test': '1'}})
    foo.validate()

    foo = Foo({'strict': {'stringB': 'bbb', 'regexp_test': 'a'}})
    with pytest.raises(DataError):
        foo.validate()