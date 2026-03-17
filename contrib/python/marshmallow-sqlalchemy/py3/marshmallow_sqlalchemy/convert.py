import inspect
import functools
import warnings
import setuptools
from distutils.version import LooseVersion

import uuid
import marshmallow as ma
from marshmallow import validate, fields
from sqlalchemy.dialects import postgresql, mysql, mssql
from sqlalchemy.orm import SynonymProperty
import sqlalchemy as sa

from .exceptions import ModelConversionError
from .fields import Related, RelatedList


_META_KWARGS_DEPRECATED = LooseVersion(ma.__version__) >= LooseVersion("3.10.0")


def _is_field(value):
    return isinstance(value, type) and issubclass(value, fields.Field)


def _has_default(column):
    return (
        column.default is not None
        or column.server_default is not None
        or _is_auto_increment(column)
    )


def _is_auto_increment(column):
    return column.table is not None and column is column.table._autoincrement_column


def _postgres_array_factory(converter, data_type):
    return functools.partial(
        fields.List, converter._get_field_class_for_data_type(data_type.item_type)
    )


def _set_meta_kwarg(field_kwargs, key, value):
    if _META_KWARGS_DEPRECATED:
        field_kwargs["metadata"][key] = value
    else:
        field_kwargs[key] = value


def _field_update_kwargs(field_class, field_kwargs, kwargs):
    if not kwargs:
        return field_kwargs

    possible_field_keywords = {
        key
        for cls in inspect.getmro(field_class)
        for key, param in inspect.signature(cls).parameters.items()
        if param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        or param.kind is inspect.Parameter.KEYWORD_ONLY
    }
    for k, v in kwargs.items():
        if k in possible_field_keywords:
            field_kwargs[k] = v
        else:
            _set_meta_kwarg(field_kwargs, k, v)
    return field_kwargs


class ModelConverter:
    """Class that converts a SQLAlchemy model into a dictionary of corresponding
    marshmallow `Fields <marshmallow.fields.Field>`.
    """

    SQLA_TYPE_MAPPING = {
        sa.Enum: fields.Field,
        sa.JSON: fields.Raw,
        postgresql.BIT: fields.Integer,
        postgresql.OID: fields.Integer,
        postgresql.UUID: fields.UUID,
        postgresql.MACADDR: fields.String,
        postgresql.INET: fields.String,
        postgresql.CIDR: fields.String,
        postgresql.JSON: fields.Raw,
        postgresql.JSONB: fields.Raw,
        postgresql.HSTORE: fields.Raw,
        postgresql.ARRAY: _postgres_array_factory,
        postgresql.MONEY: fields.Decimal,
        postgresql.DATE: fields.Date,
        postgresql.TIME: fields.Time,
        mysql.BIT: fields.Integer,
        mysql.YEAR: fields.Integer,
        mysql.SET: fields.List,
        mysql.ENUM: fields.Field,
        mysql.INTEGER: fields.Integer,
        mysql.DATETIME: fields.DateTime,
        mssql.BIT: fields.Integer,
    }
    DIRECTION_MAPPING = {"MANYTOONE": False, "MANYTOMANY": True, "ONETOMANY": True}

    def __init__(self, schema_cls=None):
        self.schema_cls = schema_cls

    @property
    def type_mapping(self):
        if self.schema_cls:
            return self.schema_cls.TYPE_MAPPING
        else:
            return ma.Schema.TYPE_MAPPING

    def fields_for_model(
        self,
        model,
        *,
        include_fk=False,
        include_relationships=False,
        fields=None,
        exclude=None,
        base_fields=None,
        dict_cls=dict,
    ):
        result = dict_cls()
        base_fields = base_fields or {}
        for prop in model.__mapper__.iterate_properties:
            key = self._get_field_name(prop)
            if self._should_exclude_field(prop, fields=fields, exclude=exclude):
                # Allow marshmallow to validate and exclude the field key.
                result[key] = None
                continue
            if isinstance(prop, SynonymProperty):
                continue
            if hasattr(prop, "columns"):
                if not include_fk:
                    # Only skip a column if there is no overriden column
                    # which does not have a Foreign Key.
                    for column in prop.columns:
                        if not column.foreign_keys:
                            break
                    else:
                        continue
            if not include_relationships and hasattr(prop, "direction"):
                continue
            field = base_fields.get(key) or self.property2field(prop)
            if field:
                result[key] = field
        return result

    def fields_for_table(
        self,
        table,
        *,
        include_fk=False,
        fields=None,
        exclude=None,
        base_fields=None,
        dict_cls=dict,
    ):
        result = dict_cls()
        base_fields = base_fields or {}
        for column in table.columns:
            key = self._get_field_name(column)
            if self._should_exclude_field(column, fields=fields, exclude=exclude):
                # Allow marshmallow to validate and exclude the field key.
                result[key] = None
                continue
            if not include_fk and column.foreign_keys:
                continue
            # Overridden fields are specified relative to key generated by
            # self._get_key_for_column(...), rather than keys in source model
            field = base_fields.get(key) or self.column2field(column)
            if field:
                result[key] = field
        return result

    def property2field(self, prop, *, instance=True, field_class=None, **kwargs):
        # handle synonyms
        # Attribute renamed "_proxied_object" in 1.4
        for attr in ("_proxied_property", "_proxied_object"):
            proxied_obj = getattr(prop, attr, None)
            if proxied_obj is not None:
                prop = proxied_obj
        field_class = field_class or self._get_field_class_for_property(prop)
        if not instance:
            return field_class
        field_kwargs = self._get_field_kwargs_for_property(prop)
        _field_update_kwargs(field_class, field_kwargs, kwargs)
        ret = field_class(**field_kwargs)
        if (
            hasattr(prop, "direction")
            and self.DIRECTION_MAPPING[prop.direction.name]
            and prop.uselist is True
        ):
            related_list_kwargs = _field_update_kwargs(
                RelatedList, self.get_base_kwargs(), kwargs
            )
            ret = RelatedList(ret, **related_list_kwargs)
        return ret

    def column2field(self, column, *, instance=True, **kwargs):
        field_class = self._get_field_class_for_column(column)
        if not instance:
            return field_class
        field_kwargs = self.get_base_kwargs()
        self._add_column_kwargs(field_kwargs, column)
        _field_update_kwargs(field_class, field_kwargs, kwargs)
        return field_class(**field_kwargs)

    def field_for(self, model, property_name, **kwargs):
        target_model = model
        prop_name = property_name
        attr = getattr(model, property_name)
        remote_with_local_multiplicity = False
        if hasattr(attr, "remote_attr"):
            target_model = attr.target_class
            prop_name = attr.value_attr
            remote_with_local_multiplicity = attr.local_attr.prop.uselist
        prop = target_model.__mapper__.get_property(prop_name)
        converted_prop = self.property2field(prop, **kwargs)
        if remote_with_local_multiplicity:
            related_list_kwargs = _field_update_kwargs(
                RelatedList, self.get_base_kwargs(), kwargs
            )
            return RelatedList(converted_prop, **related_list_kwargs)
        else:
            return converted_prop

    def _get_field_name(self, prop_or_column):
        return prop_or_column.key

    def _get_field_class_for_column(self, column):
        return self._get_field_class_for_data_type(column.type)

    def _get_field_class_for_data_type(self, data_type):
        field_cls = None
        types = inspect.getmro(type(data_type))
        # First search for a field class from self.SQLA_TYPE_MAPPING
        for col_type in types:
            if col_type in self.SQLA_TYPE_MAPPING:
                field_cls = self.SQLA_TYPE_MAPPING[col_type]
                if callable(field_cls) and not _is_field(field_cls):
                    field_cls = field_cls(self, data_type)
                break
        else:
            # Try to find a field class based on the column's python_type
            try:
                python_type = data_type.python_type
            except NotImplementedError:
                python_type = None

            if python_type in self.type_mapping:
                field_cls = self.type_mapping[python_type]
            else:
                if hasattr(data_type, "impl"):
                    return self._get_field_class_for_data_type(data_type.impl)
                raise ModelConversionError(
                    "Could not find field column of type {}.".format(types[0])
                )
        return field_cls

    def _get_field_class_for_property(self, prop):
        if hasattr(prop, "direction"):
            field_cls = Related
        else:
            column = prop.columns[0]
            field_cls = self._get_field_class_for_column(column)
        return field_cls

    def _merge_validators(self, defaults, new):
        new_classes = [validator.__class__ for validator in new]
        return [
            validator
            for validator in defaults
            if validator.__class__ not in new_classes
        ] + new

    def _get_field_kwargs_for_property(self, prop):
        kwargs = self.get_base_kwargs()
        if hasattr(prop, "columns"):
            column = prop.columns[0]
            self._add_column_kwargs(kwargs, column)
            prop = column
        if hasattr(prop, "direction"):  # Relationship property
            self._add_relationship_kwargs(kwargs, prop)
        if getattr(prop, "doc", None):  # Useful for documentation generation
            _set_meta_kwarg(kwargs, "description", prop.doc)
        info = getattr(prop, "info", dict())
        overrides = info.get("marshmallow")
        if overrides is not None:
            warnings.warn(
                'Passing `info={"marshmallow": ...}` is deprecated. '
                "Use `SQLAlchemySchema` and `auto_field` instead.",
                DeprecationWarning,
            )
            validate = overrides.pop("validate", [])
            kwargs["validate"] = self._merge_validators(
                kwargs["validate"], validate
            )  # Ensure we do not override the generated validators.
            kwargs.update(overrides)  # Override other kwargs.
        return kwargs

    def _add_column_kwargs(self, kwargs, column):
        """Add keyword arguments to kwargs (in-place) based on the passed in
        `Column <sqlalchemy.schema.Column>`.
        """
        if column.nullable:
            kwargs["allow_none"] = True
        kwargs["required"] = not column.nullable and not _has_default(column)

        if hasattr(column.type, "enums"):
            kwargs["validate"].append(validate.OneOf(choices=column.type.enums))

        # Add a length validator if a max length is set on the column
        # Skip UUID columns
        # (see https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/54)
        if hasattr(column.type, "length"):
            column_length = column.type.length
            if column_length is not None:
                try:
                    python_type = column.type.python_type
                except (AttributeError, NotImplementedError):
                    python_type = None
                if not python_type or not issubclass(python_type, uuid.UUID):
                    kwargs["validate"].append(validate.Length(max=column_length))

        if getattr(column.type, "asdecimal", False):
            kwargs["places"] = getattr(column.type, "scale", None)

    def _add_relationship_kwargs(self, kwargs, prop):
        """Add keyword arguments to kwargs (in-place) based on the passed in
        relationship `Property`.
        """
        nullable = True
        for pair in prop.local_remote_pairs:
            if not pair[0].nullable:
                if prop.uselist is True:
                    nullable = False
                break
        kwargs.update({"allow_none": nullable, "required": not nullable})

    def _should_exclude_field(self, column, fields=None, exclude=None):
        key = self._get_field_name(column)
        if fields and key not in fields:
            return True
        if exclude and key in exclude:
            return True
        return False

    def get_base_kwargs(self):
        kwargs = {"validate": []}
        if _META_KWARGS_DEPRECATED:
            kwargs["metadata"] = {}
        return kwargs


default_converter = ModelConverter()

fields_for_model = default_converter.fields_for_model
"""Generate a dict of field_name: `marshmallow.fields.Field` pairs for the given model.
Note: SynonymProperties are ignored. Use an explicit field if you want to include a synonym.

:param model: The SQLAlchemy model
:param bool include_fk: Whether to include foreign key fields in the output.
:param bool include_relationships: Whether to include relationships fields in the output.
:return: dict of field_name: Field instance pairs
"""

property2field = default_converter.property2field
"""Convert a SQLAlchemy `Property` to a field instance or class.

:param Property prop: SQLAlchemy Property.
:param bool instance: If `True`, return  `Field` instance, computing relevant kwargs
    from the given property. If `False`, return the `Field` class.
:param kwargs: Additional keyword arguments to pass to the field constructor.
:return: A `marshmallow.fields.Field` class or instance.
"""

column2field = default_converter.column2field
"""Convert a SQLAlchemy `Column <sqlalchemy.schema.Column>` to a field instance or class.

:param sqlalchemy.schema.Column column: SQLAlchemy Column.
:param bool instance: If `True`, return  `Field` instance, computing relevant kwargs
    from the given property. If `False`, return the `Field` class.
:return: A `marshmallow.fields.Field` class or instance.
"""

field_for = default_converter.field_for
"""Convert a property for a mapped SQLAlchemy class to a marshmallow `Field`.
Example: ::

    date_created = field_for(Author, 'date_created', dump_only=True)
    author = field_for(Book, 'author')

:param type model: A SQLAlchemy mapped class.
:param str property_name: The name of the property to convert.
:param kwargs: Extra keyword arguments to pass to `property2field`
:return: A `marshmallow.fields.Field` class or instance.
"""
