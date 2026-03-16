from graphene import Argument, Field, List, Mutation
from graphene.types.objecttype import ObjectTypeOptions
from graphene.types.utils import yank_fields_from_attrs
from sqlalchemy.inspection import inspect as sqlalchemyinspect

from graphene_sqlalchemy.types import construct_fields
from .fields import default_connection_field_factory
from .registry import get_global_registry
from .utils import get_session, get_snake_or_camel_attr


class SQLAlchemyMutationOptions(ObjectTypeOptions):
    model = None  # type: Model


class SQLAlchemyCreate(Mutation):
    @classmethod
    def __init_subclass_with_meta__(cls, model=None, registry=None, only_fields=(), exclude_fields=None, connection_field_factory=default_connection_field_factory, **options):
        meta = SQLAlchemyMutationOptions(cls)
        meta.model = model

        model_inspect = sqlalchemyinspect(model)
        cls._model_inspect = model_inspect

        if not isinstance(exclude_fields, list):
            if exclude_fields:
                exclude_fields = list(exclude_fields)
            else:
                exclude_fields = []

        for primary_key_column in model_inspect.primary_key:
            if primary_key_column.autoincrement != "auto":
                exclude_fields.append(primary_key_column.name)

        for relationship in model_inspect.relationships:
            exclude_fields.append(relationship.key)

        if not registry:
            registry = get_global_registry()

        arguments = yank_fields_from_attrs(
            construct_fields(model, registry, only_fields, exclude_fields, connection_field_factory),
            _as=Argument,
        )

        super(SQLAlchemyCreate, cls).__init_subclass_with_meta__(_meta=meta, arguments=arguments, **options)

    @classmethod
    def mutate(cls, self, info, **kwargs):
        session = get_session(info.context)

        meta = cls._meta

        model = meta.model()
        session.add(model)

        for key, value in kwargs.items():
            setattr(model, key, value)

        session.commit()

        return model

    @classmethod
    def Field(cls, *args, **kwargs):
        return Field(
            cls._meta.output, args=cls._meta.arguments, resolver=cls._meta.resolver
        )


class SQLAlchemyUpdate(Mutation):
    @classmethod
    def __init_subclass_with_meta__(cls, model=None, registry=None, only_fields=(), exclude_fields=None, connection_field_factory=default_connection_field_factory, **options):
        meta = SQLAlchemyMutationOptions(cls)
        meta.model = model

        model_inspect = sqlalchemyinspect(model)
        cls._model_inspect = model_inspect

        if not isinstance(exclude_fields, list):
            if exclude_fields:
                exclude_fields = list(exclude_fields)
            else:
                exclude_fields = []

        for relationship in model_inspect.relationships:
            exclude_fields.append(relationship.key)

        if not registry:
            registry = get_global_registry()

        arguments = yank_fields_from_attrs(
            construct_fields(model, registry, only_fields, exclude_fields, connection_field_factory),
            _as=Argument
        )

        super(SQLAlchemyUpdate, cls).__init_subclass_with_meta__(_meta=meta, arguments=arguments, **options)

    @classmethod
    def mutate(cls, self, info, **kwargs):
        session = get_session(info.context)

        meta = cls._meta

        query = session.query(meta.model)
        for primary_key_column in cls._model_inspect.primary_key:
            query = query.filter(getattr(meta.model, primary_key_column.key) == kwargs[primary_key_column.name])
        model = query.one()

        for key, value in kwargs.items():
            setattr(model, key, value)

        session.commit()

        return model

    @classmethod
    def Field(cls, *args, **kwargs):
        return Field(
            cls._meta.output, args=cls._meta.arguments, resolver=cls._meta.resolver
        )


class SQLAlchemyDelete(Mutation):
    @classmethod
    def __init_subclass_with_meta__(cls, model=None, registry=None, only_fields=(),
                                    exclude_fields=None, connection_field_factory=default_connection_field_factory, **options):
        meta = SQLAlchemyMutationOptions(cls)
        meta.model = model

        model_inspect = sqlalchemyinspect(model)
        cls._model_inspect = model_inspect

        only_fields = []
        exclude_fields = ()
        for primary_key_column in model_inspect.primary_key:
            only_fields.append(primary_key_column.name)

        if not registry:
            registry = get_global_registry()

        arguments = yank_fields_from_attrs(
            construct_fields(model, registry, only_fields, exclude_fields, connection_field_factory),
            _as=Argument
        )

        super(SQLAlchemyDelete, cls).__init_subclass_with_meta__(_meta=meta, arguments=arguments, **options)

    @classmethod
    def mutate(cls, self, info, **kwargs):
        session = get_session(info.context)

        meta = cls._meta

        query = session.query(meta.model)

        for primary_key_column in cls._model_inspect.primary_key:
            query = query.filter(getattr(meta.model, primary_key_column.key) == kwargs[primary_key_column.name])
        model = query.one()
        session.delete(model)

        session.commit()

        return model

    @classmethod
    def Field(cls, *args, **kwargs):
        return Field(
            cls._meta.output, args=cls._meta.arguments, resolver=cls._meta.resolver
        )


class SQLAlchemyListDelete(Mutation):
    @classmethod
    def __init_subclass_with_meta__(cls, model=None, registry=None, only_fields=(),
                                    exclude_fields=None, connection_field_factory=default_connection_field_factory, **options):
        meta = SQLAlchemyMutationOptions(cls)
        meta.model = model

        model_inspect = sqlalchemyinspect(model)
        for column in model_inspect.columns:
            column.nullable = True

        cls._model_inspect = model_inspect

        if not isinstance(exclude_fields, list):
            if exclude_fields:
                exclude_fields = list(exclude_fields)
            else:
                exclude_fields = []

        for relationship in model_inspect.relationships:
            exclude_fields.append(relationship.key)

        if not registry:
            registry = get_global_registry()

        arguments = yank_fields_from_attrs(
            construct_fields(model, registry, only_fields, exclude_fields, connection_field_factory),
            _as=Argument
        )

        super(SQLAlchemyListDelete, cls).__init_subclass_with_meta__(_meta=meta, arguments=arguments, **options)

    @classmethod
    def mutate(cls, self, info, **kwargs):
        session = get_session(info.context)

        meta = cls._meta

        query = session.query(meta.model)
        for key, value in kwargs.items():
            query = query.filter(get_snake_or_camel_attr(meta.model, key) == value)

        models = query.all()
        for model in models:
            session.delete(model)

        session.commit()

        return models

    @classmethod
    def Field(cls, *args, **kwargs):
        return Field(
            cls._meta.output, args=cls._meta.arguments, resolver=cls._meta.resolver
        )


def create(of_type):
    class CreateModel(SQLAlchemyCreate):
        class Meta:
            model = of_type._meta.model

        Output = of_type

    return CreateModel.Field()


def update(of_type):
    class UpdateModel(SQLAlchemyUpdate):
        class Meta:
            model = of_type._meta.model

        Output = of_type

    return UpdateModel.Field()


def delete(of_type):
    class DeleteModel(SQLAlchemyDelete):
        class Meta:
            model = of_type._meta.model

        Output = of_type

    return DeleteModel.Field()


def delete_all(of_type):
    class DeleteListModel(SQLAlchemyListDelete):
        class Meta:
            model = of_type._meta.model

        Output = List(of_type)

    return DeleteListModel.Field()
