import warnings

import marshmallow as ma

from ..convert import ModelConverter
from .schema_meta import SchemaMeta
from .load_instance_mixin import LoadInstanceMixin


class ModelSchemaOpts(LoadInstanceMixin.Opts, ma.SchemaOpts):
    """Options class for `ModelSchema`.
    Adds the following options:

    - ``model``: The SQLAlchemy model to generate the `Schema` from (required).
    - ``load_instance``: Whether to load model instances.
    - ``sqla_session``: SQLAlchemy session to be used for deserialization.
        This is only needed when ``load_instance`` is `True`. You can also pass a session to the Schema's `load` method.
    - ``transient``: Whether to load model instances in a transient state (effectively ignoring the session).
        Only relevant when ``load_instance`` is `True`.
    - ``include_fk``: Whether to include foreign fields; defaults to `False`.
    - ``include_relationships``: Whether to include relationships; defaults to `True`.
    - ``model_converter``: `ModelConverter` class to use for converting the SQLAlchemy model to
        marshmallow fields.
    """

    def __init__(self, meta, *args, **kwargs):
        super().__init__(meta, *args, **kwargs)
        self.model = getattr(meta, "model", None)
        self.model_converter = getattr(meta, "model_converter", ModelConverter)
        self.include_fk = getattr(meta, "include_fk", False)
        self.include_relationships = getattr(meta, "include_relationships", True)
        # Default load_instance to True for backwards compatibility
        self.load_instance = getattr(meta, "load_instance", True)


class ModelSchemaMeta(SchemaMeta):
    @classmethod
    def get_fields(mcs, converter, opts, base_fields, dict_cls):
        if opts.model is not None:
            return converter.fields_for_model(
                opts.model,
                fields=opts.fields,
                exclude=opts.exclude,
                include_fk=opts.include_fk,
                include_relationships=opts.include_relationships,
                base_fields=base_fields,
                dict_cls=dict_cls,
            )
        return dict_cls()


class ModelSchema(LoadInstanceMixin.Schema, ma.Schema, metaclass=ModelSchemaMeta):
    """Base class for SQLAlchemy model-based Schemas.

    Example: ::

        from marshmallow_sqlalchemy import ModelSchema
        from mymodels import User, session

        class UserSchema(ModelSchema):
            class Meta:
                model = User

        schema = UserSchema()

        user = schema.load({'name': 'Bill'}, session=session)
        existing_user = schema.load({'name': 'Bill'}, instance=User.query.first())

    :param session: Optional SQLAlchemy session; may be overridden in `load.`
    :param instance: Optional existing instance to modify; may be overridden in `load`.

    .. deprecated:: 0.22.0
        Use `SQLAlchemyAutoSchema <marshmallow_sqlalchemy.SQLAlchemyAutoSchema>` instead.
    """

    OPTIONS_CLASS = ModelSchemaOpts

    def __init_subclass__(cls):
        warnings.warn(
            "marshmallow_sqlalchemy.ModelSchema is deprecated. "
            "Subclass marshmallow_sqlalchemy.SQLAlchemyAutoSchema and set "
            "`load_instance = True` and `include_relationships = True` on `class Meta` instead.",
            DeprecationWarning,
        )
        return super().__init_subclass__()
