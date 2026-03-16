import pytest

from graphene_sqlalchemy.registry import Registry
from graphene_sqlalchemy.types import SQLAlchemyObjectType
from .models import Pet


def test_register_incorrect_objecttype():
    reg = Registry()

    class Spam:
        pass

    with pytest.raises(AssertionError) as excinfo:
        reg.register(Spam)

    assert "Only classes of type SQLAlchemyObjectType can be registered" in str(
        excinfo.value
    )


def test_register_objecttype():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    try:
        reg.register(PetType)
    except AssertionError:
        pytest.fail("expected no AssertionError")
