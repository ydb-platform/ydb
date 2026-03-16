from unittest import mock

import pytest
import sqlalchemy.exc
import sqlalchemy.orm.exc

from graphene import (Boolean, Dynamic, Field, Float, GlobalID, Int, List,
                      Node, NonNull, ObjectType, Schema, String)
from graphene.relay import Connection

from graphene_sqlalchemy import utils
from graphene_sqlalchemy.converter import convert_sqlalchemy_composite
from graphene_sqlalchemy.fields import (SQLAlchemyConnectionField,
                      UnsortedSQLAlchemyConnectionField, createConnectionField,
                      registerConnectionFieldFactory,
                      unregisterConnectionFieldFactory)
from graphene_sqlalchemy.types import ORMField, SQLAlchemyObjectType, SQLAlchemyObjectTypeOptions
from .models import Article, CompositeFullName, Pet, Reporter


def test_should_raise_if_no_model():
    re_err = r"valid SQLAlchemy Model"
    with pytest.raises(Exception, match=re_err):
        class Character1(SQLAlchemyObjectType):
            pass


def test_should_raise_if_model_is_invalid():
    re_err = r"valid SQLAlchemy Model"
    with pytest.raises(Exception, match=re_err):
        class Character(SQLAlchemyObjectType):
            class Meta:
                model = 1


def test_sqlalchemy_node(session):
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    reporter_id_field = ReporterType._meta.fields["id"]
    assert isinstance(reporter_id_field, GlobalID)

    reporter = Reporter()
    session.add(reporter)
    session.commit()
    info = mock.Mock(context={'session': session})
    reporter_node = ReporterType.get_node(info, reporter.id)
    assert reporter == reporter_node


def test_connection():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    assert issubclass(ReporterType.connection, Connection)


def test_sqlalchemy_default_fields():
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return String()

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    assert sorted(list(ReporterType._meta.fields.keys())) == sorted([
        # Columns
        "column_prop",
        "id",
        "first_name",
        "last_name",
        "email",
        "favorite_pet_kind",
        # Composite
        "composite_prop",
        # Hybrid
        "hybrid_prop_with_doc",
        "hybrid_prop",
        "hybrid_prop_str",
        "hybrid_prop_int",
        "hybrid_prop_float",
        "hybrid_prop_bool",
        "hybrid_prop_list",
        # Relationship
        "pets",
        "articles",
        "favorite_article",
    ])

    # column
    first_name_field = ReporterType._meta.fields['first_name']
    assert first_name_field.type == String
    assert first_name_field.description == "First name"

    # column_property
    column_prop_field = ReporterType._meta.fields['column_prop']
    assert column_prop_field.type == Int
    # "doc" is ignored by column_property
    assert column_prop_field.description is None

    # composite
    full_name_field = ReporterType._meta.fields['composite_prop']
    assert full_name_field.type == String
    # "doc" is ignored by composite
    assert full_name_field.description is None

    # hybrid_property
    hybrid_prop = ReporterType._meta.fields['hybrid_prop']
    assert hybrid_prop.type == String
    # "doc" is ignored by hybrid_property
    assert hybrid_prop.description is None

    # hybrid_property_str
    hybrid_prop_str = ReporterType._meta.fields['hybrid_prop_str']
    assert hybrid_prop_str.type == String
    # "doc" is ignored by hybrid_property
    assert hybrid_prop_str.description is None

    # hybrid_property_int
    hybrid_prop_int = ReporterType._meta.fields['hybrid_prop_int']
    assert hybrid_prop_int.type == Int
    # "doc" is ignored by hybrid_property
    assert hybrid_prop_int.description is None

    # hybrid_property_float
    hybrid_prop_float = ReporterType._meta.fields['hybrid_prop_float']
    assert hybrid_prop_float.type == Float
    # "doc" is ignored by hybrid_property
    assert hybrid_prop_float.description is None

    # hybrid_property_bool
    hybrid_prop_bool = ReporterType._meta.fields['hybrid_prop_bool']
    assert hybrid_prop_bool.type == Boolean
    # "doc" is ignored by hybrid_property
    assert hybrid_prop_bool.description is None

    # hybrid_property_list
    hybrid_prop_list = ReporterType._meta.fields['hybrid_prop_list']
    assert hybrid_prop_list.type == List(Int)
    # "doc" is ignored by hybrid_property
    assert hybrid_prop_list.description is None

    # hybrid_prop_with_doc
    hybrid_prop_with_doc = ReporterType._meta.fields['hybrid_prop_with_doc']
    assert hybrid_prop_with_doc.type == String
    # docstring is picked up from hybrid_prop_with_doc
    assert hybrid_prop_with_doc.description == "Docstring test"

    # relationship
    favorite_article_field = ReporterType._meta.fields['favorite_article']
    assert isinstance(favorite_article_field, Dynamic)
    assert favorite_article_field.type().type == ArticleType
    assert favorite_article_field.type().description is None


def test_sqlalchemy_override_fields():
    @convert_sqlalchemy_composite.register(CompositeFullName)
    def convert_composite_class(composite, registry):
        return String()

    class ReporterMixin(object):
        # columns
        first_name = ORMField(required=True)
        last_name = ORMField(description='Overridden')

    class ReporterType(SQLAlchemyObjectType, ReporterMixin):
        class Meta:
            model = Reporter
            interfaces = (Node,)

        # columns
        email = ORMField(deprecation_reason='Overridden')
        email_v2 = ORMField(model_attr='email', type_=Int)

        # column_property
        column_prop = ORMField(type_=String)

        # composite
        composite_prop = ORMField()

        # hybrid_property
        hybrid_prop_with_doc = ORMField(description='Overridden')
        hybrid_prop = ORMField(description='Overridden')

        # relationships
        favorite_article = ORMField(description='Overridden')
        articles = ORMField(deprecation_reason='Overridden')
        pets = ORMField(description='Overridden')

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (Node,)
            use_connection = False

    assert sorted(list(ReporterType._meta.fields.keys())) == sorted([
        # Fields from ReporterMixin
        "first_name",
        "last_name",
        # Fields from ReporterType
        "email",
        "email_v2",
        "column_prop",
        "composite_prop",
        "hybrid_prop_with_doc",
        "hybrid_prop",
        "favorite_article",
        "articles",
        "pets",
        # Then the automatic SQLAlchemy fields
        "id",
        "favorite_pet_kind",
        "hybrid_prop_str",
        "hybrid_prop_int",
        "hybrid_prop_float",
        "hybrid_prop_bool",
        "hybrid_prop_list",
    ])

    first_name_field = ReporterType._meta.fields['first_name']
    assert isinstance(first_name_field.type, NonNull)
    assert first_name_field.type.of_type == String
    assert first_name_field.description == "First name"
    assert first_name_field.deprecation_reason is None

    last_name_field = ReporterType._meta.fields['last_name']
    assert last_name_field.type == String
    assert last_name_field.description == "Overridden"
    assert last_name_field.deprecation_reason is None

    email_field = ReporterType._meta.fields['email']
    assert email_field.type == String
    assert email_field.description == "Email"
    assert email_field.deprecation_reason == "Overridden"

    email_field_v2 = ReporterType._meta.fields['email_v2']
    assert email_field_v2.type == Int
    assert email_field_v2.description == "Email"
    assert email_field_v2.deprecation_reason is None

    hybrid_prop_field = ReporterType._meta.fields['hybrid_prop']
    assert hybrid_prop_field.type == String
    assert hybrid_prop_field.description == "Overridden"
    assert hybrid_prop_field.deprecation_reason is None

    hybrid_prop_with_doc_field = ReporterType._meta.fields['hybrid_prop_with_doc']
    assert hybrid_prop_with_doc_field.type == String
    assert hybrid_prop_with_doc_field.description == "Overridden"
    assert hybrid_prop_with_doc_field.deprecation_reason is None

    column_prop_field_v2 = ReporterType._meta.fields['column_prop']
    assert column_prop_field_v2.type == String
    assert column_prop_field_v2.description is None
    assert column_prop_field_v2.deprecation_reason is None

    composite_prop_field = ReporterType._meta.fields['composite_prop']
    assert composite_prop_field.type == String
    assert composite_prop_field.description is None
    assert composite_prop_field.deprecation_reason is None

    favorite_article_field = ReporterType._meta.fields['favorite_article']
    assert isinstance(favorite_article_field, Dynamic)
    assert favorite_article_field.type().type == ArticleType
    assert favorite_article_field.type().description == 'Overridden'

    articles_field = ReporterType._meta.fields['articles']
    assert isinstance(articles_field, Dynamic)
    assert isinstance(articles_field.type(), UnsortedSQLAlchemyConnectionField)
    assert articles_field.type().deprecation_reason == "Overridden"

    pets_field = ReporterType._meta.fields['pets']
    assert isinstance(pets_field, Dynamic)
    assert isinstance(pets_field.type().type, List)
    assert pets_field.type().type.of_type == PetType
    assert pets_field.type().description == 'Overridden'


def test_invalid_model_attr():
    err_msg = (
        "Cannot map ORMField to a model attribute.\n"
        "Field: 'ReporterType.first_name'"
    )
    with pytest.raises(ValueError, match=err_msg):
        class ReporterType(SQLAlchemyObjectType):
            class Meta:
                model = Reporter

            first_name = ORMField(model_attr='does_not_exist')


def test_only_fields():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            only_fields = ("id", "last_name")

        first_name = ORMField()  # Takes precedence
        last_name = ORMField()  # Noop

    assert list(ReporterType._meta.fields.keys()) == ["first_name", "last_name", "id"]


def test_exclude_fields():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            exclude_fields = ("id", "first_name")

        first_name = ORMField()  # Takes precedence
        last_name = ORMField()  # Noop

    assert sorted(list(ReporterType._meta.fields.keys())) == sorted([
        "first_name",
        "last_name",
        "column_prop",
        "email",
        "favorite_pet_kind",
        "composite_prop",
        "hybrid_prop_with_doc",
        "hybrid_prop",
        "hybrid_prop_str",
        "hybrid_prop_int",
        "hybrid_prop_float",
        "hybrid_prop_bool",
        "hybrid_prop_list",
        "pets",
        "articles",
        "favorite_article",
    ])


def test_only_and_exclude_fields():
    re_err = r"'only_fields' and 'exclude_fields' cannot be both set"
    with pytest.raises(Exception, match=re_err):
        class ReporterType(SQLAlchemyObjectType):
            class Meta:
                model = Reporter
                only_fields = ("id", "last_name")
                exclude_fields = ("id", "last_name")


def test_sqlalchemy_redefine_field():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

        first_name = Int()

    first_name_field = ReporterType._meta.fields["first_name"]
    assert isinstance(first_name_field, Field)
    assert first_name_field.type == Int


def test_resolvers(session):
    """Test that the correct resolver functions are called"""

    class ReporterMixin(object):
        def resolve_id(root, _info):
            return 'ID'

    class ReporterType(ReporterMixin, SQLAlchemyObjectType):
        class Meta:
            model = Reporter

        email = ORMField()
        email_v2 = ORMField(model_attr='email')
        favorite_pet_kind = Field(String)
        favorite_pet_kind_v2 = Field(String)

        def resolve_last_name(root, _info):
            return root.last_name.upper()

        def resolve_email_v2(root, _info):
            return root.email + '_V2'

        def resolve_favorite_pet_kind_v2(root, _info):
            return str(root.favorite_pet_kind) + '_V2'

    class Query(ObjectType):
        reporter = Field(ReporterType)

        def resolve_reporter(self, _info):
            return session.query(Reporter).first()

    reporter = Reporter(first_name='first_name', last_name='last_name', email='email', favorite_pet_kind='cat')
    session.add(reporter)
    session.commit()

    schema = Schema(query=Query)
    result = schema.execute("""
        query {
            reporter {
                id
                firstName
                lastName
                email
                emailV2
                favoritePetKind
                favoritePetKindV2
            }
        }
    """)

    assert not result.errors
    # Custom resolver on a base class
    assert result.data['reporter']['id'] == 'ID'
    # Default field + default resolver
    assert result.data['reporter']['firstName'] == 'first_name'
    # Default field + custom resolver
    assert result.data['reporter']['lastName'] == 'LAST_NAME'
    # ORMField + default resolver
    assert result.data['reporter']['email'] == 'email'
    # ORMField + custom resolver
    assert result.data['reporter']['emailV2'] == 'email_V2'
    # Field + default resolver
    assert result.data['reporter']['favoritePetKind'] == 'cat'
    # Field + custom resolver
    assert result.data['reporter']['favoritePetKindV2'] == 'cat_V2'


# Test Custom SQLAlchemyObjectType Implementation

def test_custom_objecttype_registered():
    class CustomSQLAlchemyObjectType(SQLAlchemyObjectType):
        class Meta:
            abstract = True

    class CustomReporterType(CustomSQLAlchemyObjectType):
        class Meta:
            model = Reporter

    assert issubclass(CustomReporterType, ObjectType)
    assert CustomReporterType._meta.model == Reporter
    assert len(CustomReporterType._meta.fields) == 17


# Test Custom SQLAlchemyObjectType with Custom Options
def test_objecttype_with_custom_options():
    class CustomOptions(SQLAlchemyObjectTypeOptions):
        custom_option = None

    class SQLAlchemyObjectTypeWithCustomOptions(SQLAlchemyObjectType):
        class Meta:
            abstract = True

        @classmethod
        def __init_subclass_with_meta__(cls, custom_option=None, **options):
            _meta = CustomOptions(cls)
            _meta.custom_option = custom_option
            super(SQLAlchemyObjectTypeWithCustomOptions, cls).__init_subclass_with_meta__(
                _meta=_meta, **options
            )

    class ReporterWithCustomOptions(SQLAlchemyObjectTypeWithCustomOptions):
        class Meta:
            model = Reporter
            custom_option = "custom_option"

    assert issubclass(ReporterWithCustomOptions, ObjectType)
    assert ReporterWithCustomOptions._meta.model == Reporter
    assert ReporterWithCustomOptions._meta.custom_option == "custom_option"


# Tests for connection_field_factory

class _TestSQLAlchemyConnectionField(SQLAlchemyConnectionField):
    pass


def test_default_connection_field_factory():
    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    assert isinstance(ReporterType._meta.fields['articles'].type(), UnsortedSQLAlchemyConnectionField)


def test_custom_connection_field_factory():
    def test_connection_field_factory(relationship, registry):
        model = relationship.mapper.entity
        _type = registry.get_type_for_model(model)
        return _TestSQLAlchemyConnectionField(_type._meta.connection)

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)
            connection_field_factory = test_connection_field_factory

    class ArticleType(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    assert isinstance(ReporterType._meta.fields['articles'].type(), _TestSQLAlchemyConnectionField)


def test_deprecated_registerConnectionFieldFactory():
    with pytest.warns(DeprecationWarning):
        registerConnectionFieldFactory(_TestSQLAlchemyConnectionField)

        class ReporterType(SQLAlchemyObjectType):
            class Meta:
                model = Reporter
                interfaces = (Node,)

        class ArticleType(SQLAlchemyObjectType):
            class Meta:
                model = Article
                interfaces = (Node,)

        assert isinstance(ReporterType._meta.fields['articles'].type(), _TestSQLAlchemyConnectionField)


def test_deprecated_unregisterConnectionFieldFactory():
    with pytest.warns(DeprecationWarning):
        registerConnectionFieldFactory(_TestSQLAlchemyConnectionField)
        unregisterConnectionFieldFactory()

        class ReporterType(SQLAlchemyObjectType):
            class Meta:
                model = Reporter
                interfaces = (Node,)

        class ArticleType(SQLAlchemyObjectType):
            class Meta:
                model = Article
                interfaces = (Node,)

        assert not isinstance(ReporterType._meta.fields['articles'].type(), _TestSQLAlchemyConnectionField)


def test_deprecated_createConnectionField():
    with pytest.warns(DeprecationWarning):
        createConnectionField(None)


@mock.patch(utils.__name__ + '.class_mapper')
def test_unique_errors_propagate(class_mapper_mock):
    # Define unique error to detect
    class UniqueError(Exception):
        pass

    # Mock class_mapper effect
    class_mapper_mock.side_effect = UniqueError

    # Make sure that errors are propagated from class_mapper when instantiating new classes
    error = None
    try:
        class ArticleOne(SQLAlchemyObjectType):
            class Meta(object):
                model = Article
    except UniqueError as e:
        error = e

    # Check that an error occured, and that it was the unique error we gave
    assert error is not None
    assert isinstance(error, UniqueError)


@mock.patch(utils.__name__ + '.class_mapper')
def test_argument_errors_propagate(class_mapper_mock):
    # Mock class_mapper effect
    class_mapper_mock.side_effect = sqlalchemy.exc.ArgumentError

    # Make sure that errors are propagated from class_mapper when instantiating new classes
    error = None
    try:
        class ArticleTwo(SQLAlchemyObjectType):
            class Meta(object):
                model = Article
    except sqlalchemy.exc.ArgumentError as e:
        error = e

    # Check that an error occured, and that it was the unique error we gave
    assert error is not None
    assert isinstance(error, sqlalchemy.exc.ArgumentError)


@mock.patch(utils.__name__ + '.class_mapper')
def test_unmapped_errors_reformat(class_mapper_mock):
    # Mock class_mapper effect
    class_mapper_mock.side_effect = sqlalchemy.orm.exc.UnmappedClassError(object)

    # Make sure that errors are propagated from class_mapper when instantiating new classes
    error = None
    try:
        class ArticleThree(SQLAlchemyObjectType):
            class Meta(object):
                model = Article
    except ValueError as e:
        error = e

    # Check that an error occured, and that it was the unique error we gave
    assert error is not None
    assert isinstance(error, ValueError)
    assert "You need to pass a valid SQLAlchemy Model" in str(error)
