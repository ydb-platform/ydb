"""Tools for generating forms based on SQLAlchemy models."""

import inspect

from sqlalchemy import inspect as sainspect
from wtforms import fields as wtforms_fields
from wtforms import validators
from wtforms.form import Form

from .fields import QuerySelectField
from .fields import QuerySelectMultipleField

__all__ = (
    "model_fields",
    "model_form",
)


def converts(*args):
    def _inner(func):
        func._converter_for = frozenset(args)
        return func

    return _inner


class ModelConversionError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


class ModelConverterBase:
    def __init__(self, converters, use_mro=True):
        self.use_mro = use_mro

        if not converters:
            converters = {}

        for name in dir(self):
            obj = getattr(self, name)
            if hasattr(obj, "_converter_for"):
                for classname in obj._converter_for:
                    converters[classname] = obj

        self.converters = converters

    def get_converter(self, column):
        """Searches `self.converters` for a converter method with an argument
        that matches the column's type."""
        if self.use_mro:
            types = inspect.getmro(type(column.type))
        else:
            types = [type(column.type)]

        # Search by module + name
        for col_type in types:
            type_string = f"{col_type.__module__}.{col_type.__name__}"

            # remove the 'sqlalchemy.' prefix for sqlalchemy <0.7 compatibility
            if type_string.startswith("sqlalchemy."):
                type_string = type_string[11:]

            if type_string in self.converters:
                return self.converters[type_string]

        # Search by name
        for col_type in types:
            if col_type.__name__ in self.converters:
                return self.converters[col_type.__name__]

        raise ModelConversionError(
            f"Could not find field converter for column {column.name} ({types[0]!r})."
        )

    def convert(self, model, mapper, prop, field_args, db_session=None):
        if not hasattr(prop, "columns") and not hasattr(prop, "direction"):
            return
        elif not hasattr(prop, "direction") and len(prop.columns) != 1:
            raise TypeError(
                "Do not know how to convert multiple-column properties currently"
            )

        kwargs = {
            "validators": [],
            "filters": [],
            "default": None,
            "description": prop.doc,
        }

        if field_args:
            kwargs.update(field_args)

        if kwargs["validators"]:
            # Copy to prevent modifying nested mutable values of the original
            kwargs["validators"] = list(kwargs["validators"])

        converter = None
        column = None

        if not hasattr(prop, "direction"):
            column = prop.columns[0]
            # Support sqlalchemy.schema.ColumnDefault, so users can benefit
            # from  setting defaults for fields, e.g.:
            #   field = Column(DateTimeField, default=datetime.utcnow)

            default = getattr(column, "default", None)

            if default is not None:
                # Only actually change default if it has an attribute named
                # 'arg' that's callable.
                callable_default = getattr(default, "arg", None)

                if callable_default is not None:
                    # ColumnDefault(val).arg can be also a plain value
                    default = (
                        callable_default(None)
                        if callable(callable_default)
                        else callable_default
                    )

            kwargs["default"] = default

            if column.nullable:
                kwargs["validators"].append(validators.Optional())
            else:
                kwargs["validators"].append(validators.InputRequired())

            converter = self.get_converter(column)
        else:
            # We have a property with a direction.
            if db_session is None:
                raise ModelConversionError(
                    f"Cannot convert field {prop.key}, need DB session."
                )

            foreign_model = prop.mapper.class_

            nullable = True
            for pair in prop.local_remote_pairs:
                if not pair[0].nullable:
                    nullable = False

            kwargs.update(
                {
                    "allow_blank": nullable,
                    "query_factory": lambda: db_session.query(foreign_model).all(),
                }
            )

            converter = self.converters[prop.direction.name]

        return converter(
            model=model, mapper=mapper, prop=prop, column=column, field_args=kwargs
        )


class ModelConverter(ModelConverterBase):
    def __init__(self, extra_converters=None, use_mro=True):
        super().__init__(extra_converters, use_mro=use_mro)

    @classmethod
    def _string_common(cls, column, field_args, **extra):
        if isinstance(column.type.length, int) and column.type.length:
            field_args["validators"].append(validators.Length(max=column.type.length))

    @converts("String")  # includes Unicode
    def conv_String(self, field_args, **extra):
        self._string_common(field_args=field_args, **extra)
        return wtforms_fields.StringField(**field_args)

    @converts("Text", "LargeBinary", "Binary")  # includes UnicodeText
    def conv_Text(self, field_args, **extra):
        self._string_common(field_args=field_args, **extra)
        return wtforms_fields.TextAreaField(**field_args)

    @converts("Boolean", "dialects.mssql.base.BIT")
    def conv_Boolean(self, field_args, **extra):
        return wtforms_fields.BooleanField(**field_args)

    @converts("Date")
    def conv_Date(self, field_args, **extra):
        return wtforms_fields.DateField(**field_args)

    @converts("DateTime")
    def conv_DateTime(self, field_args, **extra):
        return wtforms_fields.DateTimeField(**field_args)

    @converts("Enum")
    def conv_Enum(self, column, field_args, **extra):
        field_args["choices"] = [(e, e) for e in column.type.enums]
        return wtforms_fields.SelectField(**field_args)

    @converts("Integer")  # includes BigInteger and SmallInteger
    def handle_integer_types(self, column, field_args, **extra):
        unsigned = getattr(column.type, "unsigned", False)
        if unsigned:
            field_args["validators"].append(validators.NumberRange(min=0))
        return wtforms_fields.IntegerField(**field_args)

    @converts("Numeric")  # includes DECIMAL, Float/FLOAT, REAL, and DOUBLE
    def handle_decimal_types(self, column, field_args, **extra):
        # override default decimal places limit, use database defaults instead
        field_args.setdefault("places", None)
        return wtforms_fields.DecimalField(**field_args)

    @converts("dialects.mysql.types.YEAR", "dialects.mysql.base.YEAR")
    def conv_MSYear(self, field_args, **extra):
        field_args["validators"].append(validators.NumberRange(min=1901, max=2155))
        return wtforms_fields.StringField(**field_args)

    @converts("dialects.postgresql.types.INET", "dialects.postgresql.base.INET")
    def conv_PGInet(self, field_args, **extra):
        field_args.setdefault("label", "IP Address")
        field_args["validators"].append(validators.IPAddress())
        return wtforms_fields.StringField(**field_args)

    @converts("dialects.postgresql.types.MACADDR", "dialects.postgresql.base.MACADDR")
    def conv_PGMacaddr(self, field_args, **extra):
        field_args.setdefault("label", "MAC Address")
        field_args["validators"].append(validators.MacAddress())
        return wtforms_fields.StringField(**field_args)

    @converts(
        "sql.sqltypes.UUID",
        "dialects.postgresql.types.UUID",
        "dialects.postgresql.base.UUID",
    )
    def conv_PGUuid(self, field_args, **extra):
        field_args.setdefault("label", "UUID")
        field_args["validators"].append(validators.UUID())
        return wtforms_fields.StringField(**field_args)

    @converts("MANYTOONE")
    def conv_ManyToOne(self, field_args, **extra):
        return QuerySelectField(**field_args)

    @converts("MANYTOMANY", "ONETOMANY")
    def conv_ManyToMany(self, field_args, **extra):
        return QuerySelectMultipleField(**field_args)


def model_fields(
    model,
    db_session=None,
    only=None,
    exclude=None,
    field_args=None,
    converter=None,
    exclude_pk=False,
    exclude_fk=False,
):
    """Generate a dictionary of fields for a given SQLAlchemy model.

    See `model_form` docstring for description of parameters.
    """
    mapper = sainspect(model)
    converter = converter or ModelConverter()
    field_args = field_args or {}
    properties = []

    for prop in mapper.attrs.values():
        if getattr(prop, "columns", None):
            if exclude_fk and prop.columns[0].foreign_keys:
                continue
            elif exclude_pk and prop.columns[0].primary_key:
                continue

        properties.append((prop.key, prop))

    # ((p.key, p) for p in mapper.iterate_properties)
    if only:
        properties = (x for x in properties if x[0] in only)
    elif exclude:
        properties = (x for x in properties if x[0] not in exclude)

    field_dict = {}
    for name, prop in properties:
        field = converter.convert(model, mapper, prop, field_args.get(name), db_session)
        if field is not None:
            field_dict[name] = field

    return field_dict


def model_form(
    model,
    db_session=None,
    base_class=Form,
    only=None,
    exclude=None,
    field_args=None,
    converter=None,
    exclude_pk=True,
    exclude_fk=True,
    type_name=None,
):
    """
    Create a wtforms Form for a given SQLAlchemy model class::

        from wtforms_sqlalchemy.orm import model_form
        from myapp.models import User
        UserForm = model_form(User)

    :param model:
        A SQLAlchemy mapped model class.
    :param db_session:
        An optional SQLAlchemy Session.
    :param base_class:
        Base form class to extend from. Must be a ``wtforms.Form`` subclass.
    :param only:
        An optional iterable with the property names that should be included in
        the form. Only these properties will have fields.
    :param exclude:
        An optional iterable with the property names that should be excluded
        from the form. All other properties will have fields.
    :param field_args:
        An optional dictionary of field names mapping to keyword arguments used
        to construct each field object.
    :param converter:
        A converter to generate the fields based on the model properties. If
        not set, ``ModelConverter`` is used.
    :param exclude_pk:
        An optional boolean to force primary key exclusion.
    :param exclude_fk:
        An optional boolean to force foreign keys exclusion.
    :param type_name:
        An optional string to set returned type name.
    """
    if not hasattr(model, "_sa_class_manager"):
        raise TypeError("model must be a sqlalchemy mapped model")

    type_name = type_name or str(model.__name__ + "Form")
    field_dict = model_fields(
        model,
        db_session,
        only,
        exclude,
        field_args,
        converter,
        exclude_pk=exclude_pk,
        exclude_fk=exclude_fk,
    )
    return type(type_name, (base_class,), field_dict)
