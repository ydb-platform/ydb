import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import graphene

from graphene_sqlalchemy.converter import convert_sqlalchemy_composite
from graphene_sqlalchemy.registry import reset_global_registry
from .models import Base, CompositeFullName

test_db_url = 'sqlite://'  # use in-memory database for tests


@pytest.fixture(autouse=True)
def reset_registry():
    reset_global_registry()

    # Prevent tests that implicitly depend on Reporter from raising
    # Tests that explicitly depend on this behavior should re-register a converter
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.Field(graphene.Int)


@pytest.fixture(scope="function")
def session_factory():
    engine = create_engine(test_db_url)
    Base.metadata.create_all(engine)

    yield sessionmaker(bind=engine)

    # SQLite in-memory db is deleted when its connection is closed.
    # https://www.sqlite.org/inmemorydb.html
    engine.dispose()


@pytest.fixture(scope="function")
def session(session_factory):
    return session_factory()
