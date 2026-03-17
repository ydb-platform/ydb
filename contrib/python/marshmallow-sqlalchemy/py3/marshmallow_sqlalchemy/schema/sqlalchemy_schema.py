from marshmallow.fields import Field
from marshmallow.schema import Schema, SchemaMeta, SchemaOpts
import sqlalchemy as sa
from sqlalchemy.ext.declarative import DeclarativeMeta

from ..convert import ModelConverter
from ..exceptions import IncorrectSchemaTypeError
from .load_instance_mixin import LoadInstanceMixin


# This isn't really a field; it's a placeholder for the metaclass.
# This should be considered private API.
class SQLAlchemyAutoField(Field):
    def __init__(self, *, column_name=None, model=None, table=None, field_kwargs):
        super().__init__()

        if model and table:
            raise ValueError("Cannot pass both `model` and `table` options.")

        self.column_name = column_name
        self.model = model
        self.table = table
        self.field_kwargs = field_kwargs

    def create_field(self, schema_opts, column_name, converter):
        model = self.model or schema_opts.model
        if model:
            return converter.field_for(model, column_name, **self.field_kwargs)
        else:
            table = self.table if self.table is not None else schema_opts.table
            column = getattr(table.columns, column_name)
            return converter.column2field(column, **self.field_kwargs)

    # This field should never be bound to a schema.
    # If this method is called, it's probably because the schema is not a SQLAlchemySchema.
    def _bind_to_schema(self, field_name, schema):
        raise IncorrectSchemaTypeError(
            f"Cannot bind SQLAlchemyAutoField. Make sure that {schema} is a SQLAlchemySchema or SQLAlchemyAutoSchema."
        )


class SQLAlchemySchemaOpts(LoadInstanceMixin.Opts, SchemaOpts):
    """Options class for `SQLAlchemySchema`.
    Adds the following options:

    - ``model``: The SQLAlchemy model to generate the `Schema` from (mutually exclusive with ``table``).
    - ``table``: The SQLAlchemy table to generate the `Schema` from (mutually exclusive with ``model``).
    - ``load_instance``: Whether to load model instances.
    - ``sqla_session``: SQLAlchemy session to be used for deserialization.
        This is only needed when ``load_instance`` is `True`. You can also pass a session to the Schema's `load` method.
    - ``transient``: Whether to load model instances in a transient state (effectively ignoring the session).
        Only relevant when ``load_instance`` is `True`.
    - ``model_converter``: `ModelConverter` class to use for converting the SQLAlchemy model to marshmallow fields.
    """

    def __init__(self, meta, *args, **kwargs):
        super().__init__(meta, *args, **kwargs)

        self.model = getattr(meta, "model", None)
        self.table = getattr(meta, "table", None)
        if self.model is not None and self.table is not None:
            raise ValueError("Cannot set both `model` and `table` options.")
        self.model_converter = getattr(meta, "model_converter", ModelConverter)


class SQLAlchemyAutoSchemaOpts(SQLAlchemySchemaOpts):
    """Options class for `SQLAlchemyAutoSchema`.
    Has the same options as `SQLAlchemySchemaOpts`, with the addition of:

    - ``include_fk``: Whether to include foreign fields; defaults to `False`.
    - ``include_relationships``: Whether to include relationships; defaults to `False`.
    """

    def __init__(self, meta, *args, **kwargs):
        super().__init__(meta, *args, **kwargs)
        self.include_fk = getattr(meta, "include_fk", False)
        self.include_relationships = getattr(meta, "include_relationships", False)
        if self.table is not None and self.include_relationships:
            raise ValueError("Cannot set `table` and `include_relationships = True`.")


class SQLAlchemySchemaMeta(SchemaMeta):
    @classmethod
    def get_declared_fields(mcs, klass, cls_fields, inherited_fields, dict_cls):
        opts = klass.opts
        Converter = opts.model_converter
        converter = Converter(schema_cls=klass)
        fields = super().get_declared_fields(
            klass, cls_fields, inherited_fields, dict_cls
        )
        fields.update(mcs.get_declared_sqla_fields(fields, converter, opts, dict_cls))
        fields.update(mcs.get_auto_fields(fields, converter, opts, dict_cls))
        return fields

    @classmethod
    def get_declared_sqla_fields(mcs, base_fields, converter, opts, dict_cls):
        return {}

    @classmethod
    def get_auto_fields(mcs, fields, converter, opts, dict_cls):
        return dict_cls(
            {
                field_name: field.create_field(
                    opts, field.column_name or field_name, converter
                )
                for field_name, field in fields.items()
                if isinstance(field, SQLAlchemyAutoField)
                and field_name not in opts.exclude
            }
        )


class SQLAlchemyAutoSchemaMeta(SQLAlchemySchemaMeta):
    @classmethod
    def get_declared_sqla_fields(cls, base_fields, converter, opts, dict_cls):
        fields = dict_cls()
        if opts.table is not None:
            fields.update(
                converter.fields_for_table(
                    opts.table,
                    fields=opts.fields,
                    exclude=opts.exclude,
                    include_fk=opts.include_fk,
                    base_fields=base_fields,
                    dict_cls=dict_cls,
                )
            )
        elif opts.model is not None:
            fields.update(
                converter.fields_for_model(
                    opts.model,
                    fields=opts.fields,
                    exclude=opts.exclude,
                    include_fk=opts.include_fk,
                    include_relationships=opts.include_relationships,
                    base_fields=base_fields,
                    dict_cls=dict_cls,
                )
            )
        return fields


class SQLAlchemySchema(
    LoadInstanceMixin.Schema, Schema, metaclass=SQLAlchemySchemaMeta
):
    """Schema for a SQLAlchemy model or table.
    Use together with `auto_field` to generate fields from columns.

    Example: ::

        from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field

        from mymodels import User

        class UserSchema(SQLAlchemySchema):
            class Meta:
                model = User

            id = auto_field()
            created_at = auto_field(dump_only=True)
            name = auto_field()
    """

    OPTIONS_CLASS = SQLAlchemySchemaOpts


class SQLAlchemyAutoSchema(SQLAlchemySchema, metaclass=SQLAlchemyAutoSchemaMeta):
    """Schema that automatically generates fields from the columns of
     a SQLAlchemy model or table.

    Example: ::

        from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field

        from mymodels import User

        class UserSchema(SQLAlchemyAutoSchema):
            class Meta:
                model = User
                # OR
                # table = User.__table__

            created_at = auto_field(dump_only=True)
    """

    OPTIONS_CLASS = SQLAlchemyAutoSchemaOpts


def auto_field(
    column_name: str = None,
    *,
    model: DeclarativeMeta = None,
    table: sa.Table = None,
    **kwargs,
):
    """Mark a field to autogenerate from a model or table.

    :param column_name: Name of the column to generate the field from.
        If ``None``, matches the field name. If ``attribute`` is unspecified,
        ``attribute`` will be set to the same value as ``column_name``.
    :param model: Model to generate the field from.
        If ``None``, uses ``model`` specified on ``class Meta``.
    :param table: Table to generate the field from.
        If ``None``, uses ``table`` specified on ``class Meta``.
    :param kwargs: Field argument overrides.
    """
    if column_name is not None:
        kwargs.setdefault("attribute", column_name)
    return SQLAlchemyAutoField(
        column_name=column_name, model=model, table=table, field_kwargs=kwargs
    )
