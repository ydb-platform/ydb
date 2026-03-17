import warnings

import marshmallow as ma

from ..convert import ModelConverter
from .schema_meta import SchemaMeta


class TableSchemaOpts(ma.SchemaOpts):
    """Options class for `TableSchema`.
    Adds the following options:

    - ``table``: The SQLAlchemy table to generate the `Schema` from (required).
    - ``model_converter``: `ModelConverter` class to use for converting the SQLAlchemy table to
        marshmallow fields.
    - ``include_fk``: Whether to include foreign fields; defaults to `False`.
    """

    def __init__(self, meta, *args, **kwargs):
        super().__init__(meta, *args, **kwargs)
        self.table = getattr(meta, "table", None)
        self.model_converter = getattr(meta, "model_converter", ModelConverter)
        self.include_fk = getattr(meta, "include_fk", False)


class TableSchemaMeta(SchemaMeta):
    @classmethod
    def get_fields(mcs, converter, opts, base_fields, dict_cls):
        if opts.table is not None:
            return converter.fields_for_table(
                opts.table,
                fields=opts.fields,
                exclude=opts.exclude,
                include_fk=opts.include_fk,
                base_fields=base_fields,
                dict_cls=dict_cls,
            )
        return dict_cls()


class TableSchema(ma.Schema, metaclass=TableSchemaMeta):
    """Base class for SQLAlchemy model-based Schemas.

    Example: ::

        from marshmallow_sqlalchemy import TableSchema
        from mymodels import engine, users

        class UserSchema(TableSchema):
            class Meta:
                table = users

        schema = UserSchema()

        select = users.select().limit(1)
        user = engine.execute(select).fetchone()
        serialized = schema.dump(user)

    .. deprecated:: 0.22.0
        Use `SQLAlchemyAutoSchema <marshmallow_sqlalchemy.SQLAlchemyAutoSchema>` instead.
    """

    OPTIONS_CLASS = TableSchemaOpts

    def __init_subclass__(cls):
        warnings.warn(
            "marshmallow_sqlalchemy.TableSchema is deprecated. Subclass marshmallow_sqlalchemy.SQLAlchemyAutoSchema instead.",
            DeprecationWarning,
        )
        return super().__init_subclass__()
