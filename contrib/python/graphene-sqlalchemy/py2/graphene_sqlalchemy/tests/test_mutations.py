import graphene
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from graphene_sqlalchemy import SQLAlchemyObjectType
from graphene_sqlalchemy.mutations import create, delete, update, delete_all
from graphene_sqlalchemy.registry import reset_global_registry
from graphene_sqlalchemy.types import SQLAlchemyList
from .models import Base, Reporter


@pytest.yield_fixture(scope="function")
def session(db):
    reset_global_registry()
    connection = db.engine.connect()
    transaction = connection.begin()
    Base.metadata.create_all(connection)

    # options = dict(bind=connection, binds={})
    session_factory = sessionmaker(bind=connection)
    session = scoped_session(session_factory)

    yield session

    # Finalize test here
    transaction.rollback()
    connection.close()
    session.remove()


def setup_fixtures(session):
    reporter = Reporter(first_name="ABC", last_name="def")
    session.add(reporter)
    reporter2 = Reporter(first_name="CBA", last_name="fed")
    session.add(reporter2)
    session.commit()


def test_should_create_with_create_field(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    SQLAlchemyList.build_columns_dict(ReporterType)

    class Query(graphene.ObjectType):
        reporters = graphene.List(ReporterType)

        def resolve_reporters(self, *args, **kwargs):
            return session.query(Reporter)

    class Mutation(graphene.ObjectType):
        createReporter = create(ReporterType)

    query = """
        query ReporterQuery {
          reporters {
            firstName,
            lastName,
            email
          }
        }
    """
    expected = {
        "reporters": []
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected

    query = """
        mutation createReporter{
            createReporter (firstName: "ABC", lastName: "def") {
                firstName,
                lastName,
                email
            }
        }
    """
    expected = {
        "createReporter": {
            "firstName": "ABC",
            "lastName": "def",
            "email": None,
        }
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_delete_with_delete_field(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = graphene.List(ReporterType)

        def resolve_reporters(self, *args, **kwargs):
            return session.query(Reporter)

    class Mutation(graphene.ObjectType):
        deleteReporter = delete(ReporterType)

    query = """
            query ReporterQuery {
              reporters {
                firstName,
                lastName,
                email
              }
            }
        """
    expected = {
        "reporters": [
            {
                "firstName": "ABC",
                "lastName": "def",
                "email": None
            },
            {
                "firstName": "CBA",
                "lastName": "fed",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected

    query = """
        mutation deleteReporter {
            deleteReporter (id: 1) {
                firstName,
                lastName,
                email
            }
        }
    """
    expected = {
        "deleteReporter": {
            "firstName": "ABC",
            "lastName": "def",
            "email": None,
        }
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected

    query = """
            query ReporterQuery {
              reporters {
                firstName,
                lastName,
                email
              }
            }
        """
    expected = {
        "reporters": [
            {
                "firstName": "CBA",
                "lastName": "fed",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected


def test_should_delete_all_with_delete_all_field(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = graphene.List(ReporterType)

        def resolve_reporters(self, *args, **kwargs):
            return session.query(Reporter)

    class Mutation(graphene.ObjectType):
        deleteReporters = delete_all(ReporterType)

    query = """
            query ReporterQuery {
              reporters {
                firstName,
                lastName,
                email
              }
            }
        """
    expected = {
        "reporters": [
            {
                "firstName": "ABC",
                "lastName": "def",
                "email": None
            },
            {
                "firstName": "CBA",
                "lastName": "fed",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected

    query = """
        mutation deleteReporters {
            deleteReporters (firstName: "ABC") {
                firstName,
                lastName,
                email
            }
        }
    """
    expected = {
        "deleteReporters": [
            {
                "firstName": "ABC",
                "lastName": "def",
                "email": None,
            }
        ]
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected

    query = """
            query ReporterQuery {
              reporters {
                firstName,
                lastName,
                email
              }
            }
        """
    expected = {
        "reporters": [
            {
                "firstName": "CBA",
                "lastName": "fed",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected


def test_should_update_with_update_field(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = graphene.List(ReporterType)

        def resolve_reporters(self, *args, **kwargs):
            return session.query(Reporter)

    class Mutation(graphene.ObjectType):
        updateReporter = update(ReporterType)

    query = """
        mutation updateReporter {
            updateReporter (id: 1, lastName: "updated", email: "test@test.io") {
                firstName,
                lastName,
                email
            }
        }
    """
    expected = {
        "updateReporter": {
            "firstName": "ABC",
            "lastName": "updated",
            "email": "test@test.io",
        }
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected

    query = """
            query ReporterQuery {
              reporters {
                firstName,
                lastName,
                email
              }
            }
        """
    expected = {
        "reporters": [
            {
                "firstName": "ABC",
                "lastName": "updated",
                "email": "test@test.io",
            },
            {
                "firstName": "CBA",
                "lastName": "fed",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected
