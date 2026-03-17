import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

import graphene
from graphene.relay import Connection, Node

from graphene_sqlalchemy.fields import SQLAlchemyConnectionField
from graphene_sqlalchemy.registry import reset_global_registry
from graphene_sqlalchemy.types import SQLAlchemyObjectType, SQLAlchemyList
from graphene_sqlalchemy.utils import sort_argument_for_model, sort_enum_for_model
from .models import Article, Base, Editor, Hairkind, Pet, Reporter


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
    pet = Pet(name="Lassie", pet_kind="dog", hair_kind=Hairkind.LONG)
    session.add(pet)
    reporter = Reporter(first_name="ABA", last_name="X")
    session.add(reporter)
    reporter2 = Reporter(first_name="ABO", last_name="Y")
    session.add(reporter2)
    article = Article(headline="Hi!")
    article.reporter = reporter
    session.add(article)
    editor = Editor(name="John")
    session.add(editor)
    session.commit()


def test_should_query_well_with_graphene_types(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)
        reporters = graphene.List(ReporterType)

        def resolve_reporter(self, *args, **kwargs):
            return session.query(Reporter).first()

        def resolve_reporters(self, *args, **kwargs):
            return session.query(Reporter)

    query = """
        query ReporterQuery {
          reporter {
            firstName,
            lastName,
            email
          }
          reporters {
            firstName
          }
        }
    """
    expected = {
        "reporter": {"firstName": "ABA", "lastName": "X", "email": None},
        "reporters": [{"firstName": "ABA"}, {"firstName": "ABO"}],
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected


def test_should_query_enums(session):
    setup_fixtures(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(PetType)

        def resolve_pet(self, *args, **kwargs):
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
    expected = {"pet": {"name": "Lassie", "petKind": "dog", "hairKind": "LONG"}}
    schema = graphene.Schema(query=Query)
    result = schema.execute(query)
    assert not result.errors
    assert result.data == expected, result.data


def test_enum_parameter(session):
    setup_fixtures(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(PetType, kind=graphene.Argument(PetType._meta.fields['pet_kind'].type.of_type))

        def resolve_pet(self, info, kind=None, *args, **kwargs):
            query = session.query(Pet)
            if kind:
                query = query.filter(Pet.pet_kind == kind)
            return query.first()

    query = """
        query PetQuery($kind: pet_kind) {
          pet(kind: $kind) {
            name,
            petKind
            hairKind
          }
        }
    """
    expected = {"pet": {"name": "Lassie", "petKind": "dog", "hairKind": "LONG"}}
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, variables={"kind": "cat"})
    assert not result.errors
    assert result.data == {"pet": None}
    result = schema.execute(query, variables={"kind": "dog"})
    assert not result.errors
    assert result.data == expected, result.data


def test_py_enum_parameter(session):
    setup_fixtures(session)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    class Query(graphene.ObjectType):
        pet = graphene.Field(PetType, kind=graphene.Argument(PetType._meta.fields['hair_kind'].type.of_type))

        def resolve_pet(self, info, kind=None, *args, **kwargs):
            query = session.query(Pet)
            if kind:
                # XXX Why kind passed in as a str instead of a Hairkind instance?
                query = query.filter(Pet.hair_kind == Hairkind(kind))
            return query.first()

    query = """
        query PetQuery($kind: Hairkind) {
          pet(kind: $kind) {
            name,
            petKind
            hairKind
          }
        }
    """
    expected = {"pet": {"name": "Lassie", "petKind": "dog", "hairKind": "LONG"}}
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, variables={"kind": "SHORT"})
    assert not result.errors
    assert result.data == {"pet": None}
    result = schema.execute(query, variables={"kind": "LONG"})
    assert not result.errors
    assert result.data == expected, result.data


def test_should_filter_with_sqlalchemy_fields(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = SQLAlchemyList(ReporterType)

    query = """
         query ReporterQuery {
           reporters(firstName: "ABA") {
             firstName,
             lastName,
             email
           }
         }
     """
    expected = {
        "reporters": [{
            "firstName": "ABA",
            "lastName": "X",
            "email": None
        }]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_filter_with_custom_argument(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = SQLAlchemyList(ReporterType, contains_o=graphene.Boolean())

        def query_reporters(self, info, query, **kwargs):
            return query.filter(Reporter.first_name.contains("O") == kwargs["contains_o"])

    query = """
         query ReporterQuery {
           reporters(lastName: "Y", containsO: true) {
             firstName,
             lastName,
             email
           }
         }
     """
    expected = {
        "reporters": [{
            "firstName": "ABO",
            "lastName": "Y",
            "email": None
        }]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected

    query = """
         query ReporterQuery {
           reporters(containsO: false) {
             firstName,
             lastName,
             email
           }
         }
     """
    expected = {
        "reporters": [{
            "firstName": "ABA",
            "lastName": "X",
            "email": None
        }]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_filter_with_custom_operator(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = SQLAlchemyList(ReporterType, operator="like")

    query = """
         query ReporterQuery {
           reporters(firstName: "%BO%") {
             firstName,
             lastName,
             email
           }
         }
     """
    expected = {
        "reporters": [{
            "firstName": "ABO",
            "lastName": "Y",
            "email": None
        }]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_order_by(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = SQLAlchemyList(ReporterType, order_by="firstName")

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
        "reporters": [{
            "firstName": "ABA",
            "lastName": "X",
            "email": None
        },
            {
                "firstName": "ABO",
                "lastName": "Y",
                "email": None
            }]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_order_by_asc(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = SQLAlchemyList(ReporterType, order_by="first_name ASC")

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
                "firstName": "ABA",
                "lastName": "X",
                "email": None
            },
            {
                "firstName": "ABO",
                "lastName": "Y",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_order_by_desc(session):
    setup_fixtures(session)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporters = SQLAlchemyList(ReporterType, order_by="firstName desc")

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
                "firstName": "ABO",
                "lastName": "Y",
                "email": None
            },
            {
                "firstName": "ABA",
                "lastName": "X",
                "email": None
            }
        ]
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_node(session):
    setup_fixtures(session)

    class ReporterNode(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

        @classmethod
        def get_node(cls, info, id):
            return Reporter(id=2, first_name="Cookie Monster")

    class ArticleNode(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

        # @classmethod
        # def get_node(cls, id, info):
        #     return Article(id=1, headline='Article node')

    class ArticleConnection(Connection):
        class Meta:
            node = ArticleNode

    class Query(graphene.ObjectType):
        node = Node.Field()
        reporter = graphene.Field(ReporterNode)
        article = graphene.Field(ArticleNode)
        all_articles = SQLAlchemyConnectionField(ArticleConnection)

        def resolve_reporter(self, *args, **kwargs):
            return session.query(Reporter).first()

        def resolve_article(self, *args, **kwargs):
            return session.query(Article).first()

    query = """
        query ReporterQuery {
          reporter {
            id,
            firstName,
            articles {
              edges {
                node {
                  headline
                }
              }
            }
            lastName,
            email
          }
          allArticles {
            edges {
              node {
                headline
              }
            }
          }
          myArticle: node(id:"QXJ0aWNsZU5vZGU6MQ==") {
            id
            ... on ReporterNode {
                firstName
            }
            ... on ArticleNode {
                headline
            }
          }
        }
    """
    expected = {
        "reporter": {
            "id": "UmVwb3J0ZXJOb2RlOjE=",
            "firstName": "ABA",
            "lastName": "X",
            "email": None,
            "articles": {"edges": [{"node": {"headline": "Hi!"}}]},
        },
        "allArticles": {"edges": [{"node": {"headline": "Hi!"}}]},
        "myArticle": {"id": "QXJ0aWNsZU5vZGU6MQ==", "headline": "Hi!"},
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_custom_identifier(session):
    setup_fixtures(session)

    class EditorNode(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (Node,)

    class EditorConnection(Connection):
        class Meta:
            node = EditorNode

    class Query(graphene.ObjectType):
        node = Node.Field()
        all_editors = SQLAlchemyConnectionField(EditorConnection)

    query = """
        query EditorQuery {
          allEditors {
            edges {
                node {
                    id,
                    name
                }
            }
          },
          node(id: "RWRpdG9yTm9kZTox") {
            ...on EditorNode {
              name
            }
          }
        }
    """
    expected = {
        "allEditors": {"edges": [{"node": {"id": "RWRpdG9yTm9kZTox", "name": "John"}}]},
        "node": {"name": "John"},
    }

    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def test_should_mutate_well(session):
    setup_fixtures(session)

    class EditorNode(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (Node,)

    class ReporterNode(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

        @classmethod
        def get_node(cls, id, info):
            return Reporter(id=2, first_name="Cookie Monster")

    class ArticleNode(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    class CreateArticle(graphene.Mutation):
        class Arguments:
            headline = graphene.String()
            reporter_id = graphene.ID()

        ok = graphene.Boolean()
        article = graphene.Field(ArticleNode)

        def mutate(self, info, headline, reporter_id):
            new_article = Article(headline=headline, reporter_id=reporter_id)

            session.add(new_article)
            session.commit()
            ok = True

            return CreateArticle(article=new_article, ok=ok)

    class Query(graphene.ObjectType):
        node = Node.Field()

    class Mutation(graphene.ObjectType):
        create_article = CreateArticle.Field()

    query = """
        mutation ArticleCreator {
          createArticle(
            headline: "My Article"
            reporterId: "1"
          ) {
            ok
            article {
                headline
                reporter {
                    id
                    firstName
                }
            }
          }
        }
    """
    expected = {
        "createArticle": {
            "ok": True,
            "article": {
                "headline": "My Article",
                "reporter": {"id": "UmVwb3J0ZXJOb2RlOjE=", "firstName": "ABA"},
            },
        }
    }

    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected


def sort_setup(session):
    pets = [
        Pet(id=2, name="Lassie", pet_kind="dog", hair_kind=Hairkind.LONG),
        Pet(id=22, name="Alf", pet_kind="cat", hair_kind=Hairkind.LONG),
        Pet(id=3, name="Barf", pet_kind="dog", hair_kind=Hairkind.LONG),
    ]
    session.add_all(pets)
    session.commit()


def test_sort(session):
    sort_setup(session)

    class PetNode(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (Node,)

    class PetConnection(Connection):
        class Meta:
            node = PetNode

    class Query(graphene.ObjectType):
        defaultSort = SQLAlchemyConnectionField(PetConnection)
        nameSort = SQLAlchemyConnectionField(PetConnection)
        multipleSort = SQLAlchemyConnectionField(PetConnection)
        descSort = SQLAlchemyConnectionField(PetConnection)
        singleColumnSort = SQLAlchemyConnectionField(
            PetConnection, sort=graphene.Argument(sort_enum_for_model(Pet))
        )
        noDefaultSort = SQLAlchemyConnectionField(
            PetConnection, sort=sort_argument_for_model(Pet, False)
        )
        noSort = SQLAlchemyConnectionField(PetConnection, sort=None)

    query = """
        query sortTest {
            defaultSort{
                edges{
                    node{
                        id
                    }
                }
            }
            nameSort(sort: name_asc){
                edges{
                    node{
                        name
                    }
                }
            }
            multipleSort(sort: [pet_kind_asc, name_desc]){
                edges{
                    node{
                        name
                        petKind
                    }
                }
            }
            descSort(sort: [name_desc]){
                edges{
                    node{
                        name
                    }
                }
            }
            singleColumnSort(sort: name_desc){
                edges{
                    node{
                        name
                    }
                }
            }
            noDefaultSort(sort: name_asc){
                edges{
                    node{
                        name
                    }
                }
            }
        }
    """

    def makeNodes(nodeList):
        nodes = [{"node": item} for item in nodeList]
        return {"edges": nodes}

    expected = {
        "defaultSort": makeNodes(
            [{"id": "UGV0Tm9kZToy"}, {"id": "UGV0Tm9kZToz"}, {"id": "UGV0Tm9kZToyMg=="}]
        ),
        "nameSort": makeNodes([{"name": "Alf"}, {"name": "Barf"}, {"name": "Lassie"}]),
        "noDefaultSort": makeNodes(
            [{"name": "Alf"}, {"name": "Barf"}, {"name": "Lassie"}]
        ),
        "multipleSort": makeNodes(
            [
                {"name": "Alf", "petKind": "cat"},
                {"name": "Lassie", "petKind": "dog"},
                {"name": "Barf", "petKind": "dog"},
            ]
        ),
        "descSort": makeNodes([{"name": "Lassie"}, {"name": "Barf"}, {"name": "Alf"}]),
        "singleColumnSort": makeNodes(
            [{"name": "Lassie"}, {"name": "Barf"}, {"name": "Alf"}]
        ),
    }  # yapf: disable

    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data == expected

    queryError = """
        query sortTest {
            singleColumnSort(sort: [pet_kind_asc, name_desc]){
                edges{
                    node{
                        name
                    }
                }
            }
        }
    """
    result = schema.execute(queryError, context_value={"session": session})
    assert result.errors is not None

    queryNoSort = """
        query sortTest {
            noDefaultSort{
                edges{
                    node{
                        name
                    }
                }
            }
            noSort{
                edges{
                    node{
                        name
                    }
                }
            }
        }
    """

    expectedNoSort = {
        "noDefaultSort": makeNodes(
            [{"name": "Alf"}, {"name": "Barf"}, {"name": "Lassie"}]
        ),
        "noSort": makeNodes([{"name": "Alf"}, {"name": "Barf"}, {"name": "Lassie"}]),
    }  # yapf: disable

    result = schema.execute(queryNoSort, context_value={"session": session})
    assert not result.errors
    for key, value in result.data.items():
        assert set(node["node"]["name"] for node in value["edges"]) == set(
            node["node"]["name"] for node in expectedNoSort[key]["edges"]
        )
