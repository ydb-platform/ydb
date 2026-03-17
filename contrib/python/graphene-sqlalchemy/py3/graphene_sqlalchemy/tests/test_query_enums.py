import graphene

from graphene_sqlalchemy.types import SQLAlchemyObjectType
from .models import HairKind, Pet, Reporter
from .test_query import add_test_data, to_std_dicts


def test_query_pet_kinds(session):
    add_test_data(session)

    class PetType(SQLAlchemyObjectType):

        class Meta:
            model = Pet

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)
        reporters = graphene.List(ReporterType)
        pets = graphene.List(PetType, kind=graphene.Argument(
            PetType.enum_for_field('pet_kind')))

        def resolve_reporter(self, _info):
            return session.query(Reporter).first()

        def resolve_reporters(self, _info):
            return session.query(Reporter)

        def resolve_pets(self, _info, kind):
            query = session.query(Pet)
            if kind:
                query = query.filter_by(pet_kind=kind.value)
            return query

    query = """
        query ReporterQuery {
          reporter {
            firstName
            lastName
            email
            favoritePetKind
            pets {
              name
              petKind
            }
          }
          reporters {
            firstName
            favoritePetKind
          }
          pets(kind: DOG) {
            name
            petKind
          }
        }
    """
    expected = {
        'reporter': {
            'firstName': 'John',
            'lastName': 'Doe',
            'email': None,
            'favoritePetKind': 'CAT',
            'pets': [{
                'name': 'Garfield',
                'petKind': 'CAT'
            }]
        },
        'reporters': [{
            'firstName': 'John',
            'favoritePetKind': 'CAT',
        }, {
            'firstName': 'Jane',
            'favoritePetKind': 'DOG',
        }],
        'pets': [{
            'name': 'Lassie',
            'petKind': 'DOG'
        }]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected


def test_query_more_enums(session):
    add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(PetType)

        def resolve_pet(self, _info):
            return session.query(Pet).first()

    query = """
        query PetQuery {
          pet {
            name,
            petKind
            hairKind
          }
        }
    """
    expected = {"pet": {"name": "Garfield", "petKind": "CAT", "hairKind": "SHORT"}}
    schema = graphene.Schema(query=Query)
    result = schema.execute(query)
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


def test_enum_as_argument(session):
    add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(
            PetType,
            kind=graphene.Argument(PetType.enum_for_field('pet_kind')))

        def resolve_pet(self, info, kind=None):
            query = session.query(Pet)
            if kind:
                query = query.filter(Pet.pet_kind == kind.value)
            return query.first()

    query = """
        query PetQuery($kind: PetKind) {
          pet(kind: $kind) {
            name,
            petKind
            hairKind
          }
        }
    """

    schema = graphene.Schema(query=Query)
    result = schema.execute(query, variables={"kind": "CAT"})
    assert not result.errors
    expected = {"pet": {"name": "Garfield", "petKind": "CAT", "hairKind": "SHORT"}}
    assert result.data == expected
    result = schema.execute(query, variables={"kind": "DOG"})
    assert not result.errors
    expected = {"pet": {"name": "Lassie", "petKind": "DOG", "hairKind": "LONG"}}
    result = to_std_dicts(result.data)
    assert result == expected


def test_py_enum_as_argument(session):
    add_test_data(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(
            PetType,
            kind=graphene.Argument(PetType._meta.fields["hair_kind"].type.of_type),
        )

        def resolve_pet(self, _info, kind=None):
            query = session.query(Pet)
            if kind:
                # enum arguments are expected to be strings, not PyEnums
                query = query.filter(Pet.hair_kind == HairKind(kind))
            return query.first()

    query = """
        query PetQuery($kind: HairKind) {
          pet(kind: $kind) {
            name,
            petKind
            hairKind
          }
        }
    """

    schema = graphene.Schema(query=Query)
    result = schema.execute(query, variables={"kind": "SHORT"})
    assert not result.errors
    expected = {"pet": {"name": "Garfield", "petKind": "CAT", "hairKind": "SHORT"}}
    assert result.data == expected
    result = schema.execute(query, variables={"kind": "LONG"})
    assert not result.errors
    expected = {"pet": {"name": "Lassie", "petKind": "DOG", "hairKind": "LONG"}}
    result = to_std_dicts(result.data)
    assert result == expected
