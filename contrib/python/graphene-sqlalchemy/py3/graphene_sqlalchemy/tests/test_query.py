import graphene
from graphene.relay import Node

from graphene_sqlalchemy.converter import convert_sqlalchemy_composite
from graphene_sqlalchemy.fields import SQLAlchemyConnectionField
from graphene_sqlalchemy.types import ORMField, SQLAlchemyObjectType
from .models import Article, CompositeFullName, Editor, HairKind, Pet, Reporter
from .utils import to_std_dicts


def add_test_data(session):
    reporter = Reporter(
        first_name='John', last_name='Doe', favorite_pet_kind='cat')
    session.add(reporter)
    pet = Pet(name='Garfield', pet_kind='cat', hair_kind=HairKind.SHORT)
    session.add(pet)
    pet.reporters.append(reporter)
    article = Article(headline='Hi!')
    article.reporter = reporter
    session.add(article)
    reporter = Reporter(
        first_name='Jane', last_name='Roe', favorite_pet_kind='dog')
    session.add(reporter)
    pet = Pet(name='Lassie', pet_kind='dog', hair_kind=HairKind.LONG)
    pet.reporters.append(reporter)
    session.add(pet)
    editor = Editor(name="Jack")
    session.add(editor)
    session.commit()


def test_query_fields(session):
    add_test_data(session)

    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.String()

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)
        reporters = graphene.List(ReporterType)

        def resolve_reporter(self, _info):
            return session.query(Reporter).first()

        def resolve_reporters(self, _info):
            return session.query(Reporter)

    query = """
        query {
          reporter {
            firstName
            columnProp
            hybridProp
            compositeProp
          }
          reporters {
            firstName
          }
        }
    """
    expected = {
        "reporter": {
            "firstName": "John",
            "hybridProp": "John",
            "columnProp": 2,
            "compositeProp": "John Doe",
        },
        "reporters": [{"firstName": "John"}, {"firstName": "Jane"}],
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query)
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


def test_query_node(session):
    add_test_data(session)

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

    class Query(graphene.ObjectType):
        node = Node.Field()
        reporter = graphene.Field(ReporterNode)
        all_articles = SQLAlchemyConnectionField(ArticleNode.connection)

        def resolve_reporter(self, _info):
            return session.query(Reporter).first()

    query = """
        query {
          reporter {
            id
            firstName
            articles {
              edges {
                node {
                  headline
                }
              }
            }
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
            "firstName": "John",
            "articles": {"edges": [{"node": {"headline": "Hi!"}}]},
        },
        "allArticles": {"edges": [{"node": {"headline": "Hi!"}}]},
        "myArticle": {"id": "QXJ0aWNsZU5vZGU6MQ==", "headline": "Hi!"},
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


def test_orm_field(session):
    add_test_data(session)

    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return graphene.String()

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

        first_name_v2 = ORMField(model_attr='first_name')
        hybrid_prop_v2 = ORMField(model_attr='hybrid_prop')
        column_prop_v2 = ORMField(model_attr='column_prop')
        composite_prop = ORMField()
        favorite_article_v2 = ORMField(model_attr='favorite_article')
        articles_v2 = ORMField(model_attr='articles')

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    class Query(graphene.ObjectType):
        reporter = graphene.Field(ReporterType)

        def resolve_reporter(self, _info):
            return session.query(Reporter).first()

    query = """
        query {
          reporter {
            firstNameV2
            hybridPropV2
            columnPropV2
            compositeProp
            favoriteArticleV2 {
              headline
            }
            articlesV2(first: 1) {
              edges {
                node {
                  headline
                }
              }
            }
          }
        }
    """
    expected = {
        "reporter": {
            "firstNameV2": "John",
            "hybridPropV2": "John",
            "columnPropV2": 2,
            "compositeProp": "John Doe",
            "favoriteArticleV2": {"headline": "Hi!"},
            "articlesV2": {"edges": [{"node": {"headline": "Hi!"}}]},
        },
    }
    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


def test_custom_identifier(session):
    add_test_data(session)

    class EditorNode(SQLAlchemyObjectType):
        class Meta:
            model = Editor
            interfaces = (Node,)

    class Query(graphene.ObjectType):
        node = Node.Field()
        all_editors = SQLAlchemyConnectionField(EditorNode.connection)

    query = """
        query {
          allEditors {
            edges {
                node {
                    id
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
        "allEditors": {"edges": [{"node": {"id": "RWRpdG9yTm9kZTox", "name": "Jack"}}]},
        "node": {"name": "Jack"},
    }

    schema = graphene.Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected


def test_mutation(session):
    add_test_data(session)

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
        mutation {
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
                "reporter": {"id": "UmVwb3J0ZXJOb2RlOjE=", "firstName": "John"},
            },
        }
    }

    schema = graphene.Schema(query=Query, mutation=Mutation)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    result = to_std_dicts(result.data)
    assert result == expected
