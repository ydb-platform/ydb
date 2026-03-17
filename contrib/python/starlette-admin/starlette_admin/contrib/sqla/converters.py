# Inspired by wtforms-sqlalchemy
import enum
import inspect
from typing import Any, Callable, Dict, Optional, Sequence, Type

from sqlalchemy import ARRAY, Boolean, Column, Float, String
from sqlalchemy.orm import (
    ColumnProperty,
    InstrumentedAttribute,
    Mapper,
    RelationshipProperty,
)
from sqlalchemy.sql.elements import ColumnElement, Label
from starlette_admin.contrib.sqla.exceptions import NotSupportedColumn
from starlette_admin.contrib.sqla.fields import FileField, ImageField
from starlette_admin.converters import BaseModelConverter, converts
from starlette_admin.fields import (
    ArrowField,
    BaseField,
    BooleanField,
    CollectionField,
    ColorField,
    CountryField,
    CurrencyField,
    DateField,
    DateTimeField,
    DecimalField,
    EmailField,
    EnumField,
    FloatField,
    HasMany,
    HasOne,
    IntegerField,
    JSONField,
    ListField,
    PasswordField,
    PhoneField,
    StringField,
    TextAreaField,
    TimeField,
    TimeZoneField,
    URLField,
)
from starlette_admin.helpers import slugify_class_name


class BaseSQLAModelConverter(BaseModelConverter):
    def get_converter(self, col_type: Any) -> Callable[..., BaseField]:
        converter = self.find_converter_for_col_type(type(col_type))
        if converter is not None:
            return converter
        raise NotSupportedColumn(  # pragma: no cover
            f"Column {col_type} can not be converted automatically. Find the appropriate field manually or provide "
            "your custom converter"
        )

    def convert(self, *args: Any, **kwargs: Any) -> BaseField:
        return self.get_converter(kwargs.get("type"))(*args, **kwargs)

    def find_converter_for_col_type(
        self,
        col_type: Any,
    ) -> Optional[Callable[..., BaseField]]:
        types = inspect.getmro(col_type)

        # Search by module + name
        for col_type in types:
            type_string = f"{col_type.__module__}.{col_type.__name__}"
            if type_string in self.converters:
                return self.converters[type_string]

        # Search by name
        for col_type in types:
            if col_type.__name__ in self.converters:
                return self.converters[col_type.__name__]

            # Support for custom types which inherit TypeDecorator
            if hasattr(col_type, "impl"):
                impl = (
                    col_type.impl
                    if callable(col_type.impl)
                    else col_type.impl.__class__
                )
                return self.find_converter_for_col_type(impl)
        return None  # pragma: no cover

    def convert_fields_list(
        self, *, fields: Sequence[Any], model: Type[Any], **kwargs: Any
    ) -> Sequence[BaseField]:
        mapper: Mapper = kwargs.get("mapper")  # type: ignore [assignment]
        converted_fields = []
        for field in fields:
            if isinstance(field, BaseField):
                converted_fields.append(field)
            else:
                if isinstance(field, InstrumentedAttribute):
                    attr = mapper.attrs.get(field.key)
                else:
                    attr = mapper.attrs.get(field)
                if attr is None:
                    raise ValueError(f"Can't find column with key {field}")
                if isinstance(attr, RelationshipProperty):
                    identity = slugify_class_name(attr.entity.class_.__name__)
                    if attr.direction.name == "MANYTOONE" or (
                        attr.direction.name == "ONETOMANY" and not attr.uselist
                    ):
                        converted_fields.append(HasOne(attr.key, identity=identity))
                    else:
                        converted_fields.append(
                            HasMany(
                                attr.key,
                                identity=identity,
                                collection_class=attr.collection_class or list,
                            )
                        )
                elif isinstance(attr, ColumnProperty):
                    # Handle inherited primary keys (i.e.: joined table polymorphic inheritance)
                    is_inherited_pk = mapper.inherits is not None and any(
                        col.primary_key for col in attr.columns
                    )
                    if is_inherited_pk:
                        column = attr.columns[0]
                        converted_fields.append(
                            self.convert(
                                name=attr.key, type=column.type, column=column
                            ),
                        )
                    else:
                        assert (
                            len(attr.columns) == 1
                        ), "Multiple-column properties are not supported"
                        column = attr.columns[0]
                        if not column.foreign_keys:
                            converted_field = self.convert(
                                name=attr.key, type=column.type, column=column
                            )
                            converted_fields.append(converted_field)
        return converted_fields


class ModelConverter(BaseSQLAModelConverter):
    @classmethod
    def _field_common(
        cls, *, name: str, column: ColumnElement, **kwargs: Any
    ) -> Dict[str, Any]:
        if isinstance(column, Label):
            return {
                "name": column.key,
                "exclude_from_edit": True,
                "exclude_from_create": True,
            }
        return {
            "name": name,
            "help_text": column.comment,
            "required": (
                not column.nullable
                and not isinstance(column.type, (Boolean,))
                and not column.default
                and not column.server_default
            ),
        }

    @classmethod
    def _string_common(cls, *, type: Any, **kwargs: Any) -> Dict[str, Any]:
        if (
            isinstance(type, String)
            and isinstance(type.length, int)
            and type.length > 0
        ):
            return {"maxlength": type.length}
        return {}

    @classmethod
    def _file_common(cls, *, type: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"multiple": getattr(type, "multiple", False)}

    @converts(
        "String",
        "sqlalchemy.sql.sqltypes.Uuid",
        "sqlalchemy.dialects.postgresql.base.UUID",
        "sqlalchemy.dialects.postgresql.base.MACADDR",
        "sqlalchemy.dialects.postgresql.types.MACADDR",
        "sqlalchemy.dialects.postgresql.base.INET",
        "sqlalchemy.dialects.postgresql.types.INET",
        "sqlalchemy_utils.types.locale.LocaleType",
        "sqlalchemy_utils.types.ip_address.IPAddressType",
        "sqlalchemy_utils.types.uuid.UUIDType",
    )  # includes Unicode
    def conv_string(self, *args: Any, **kwargs: Any) -> BaseField:
        return StringField(
            **self._field_common(*args, **kwargs),
            **self._string_common(*args, **kwargs),
        )

    @converts("Text", "LargeBinary", "Binary")  # includes UnicodeText
    def conv_text(self, *args: Any, **kwargs: Any) -> BaseField:
        return TextAreaField(
            **self._field_common(*args, **kwargs),
            **self._string_common(*args, **kwargs),
        )

    @converts("Boolean", "BIT")
    def conv_boolean(self, *args: Any, **kwargs: Any) -> BaseField:
        return BooleanField(
            **self._field_common(*args, **kwargs),
        )

    @converts("DateTime")
    def conv_datetime(self, *args: Any, **kwargs: Any) -> BaseField:
        return DateTimeField(
            **self._field_common(*args, **kwargs),
        )

    @converts("Date")
    def conv_date(self, *args: Any, **kwargs: Any) -> BaseField:
        return DateField(
            **self._field_common(*args, **kwargs),
        )

    @converts("Time")
    def conv_time(self, *args: Any, **kwargs: Any) -> BaseField:
        return TimeField(
            **self._field_common(*args, **kwargs),
        )

    @converts("Enum")
    def conv_enum(self, *args: Any, **kwargs: Any) -> BaseField:
        _type = kwargs["type"]
        assert hasattr(_type, "enum_class")
        return EnumField(**self._field_common(*args, **kwargs), enum=_type.enum_class)

    @converts("Integer")  # includes BigInteger and SmallInteger
    def conv_integer(self, *args: Any, **kwargs: Any) -> BaseField:
        unsigned = getattr(kwargs["type"], "unsigned", False)
        extra = self._field_common(*args, **kwargs)
        if unsigned:
            extra["min"] = 0
        return IntegerField(**extra)

    @converts("Numeric")  # includes DECIMAL, Float/FLOAT, REAL, and DOUBLE
    def conv_numeric(self, *args: Any, **kwargs: Any) -> BaseField:
        if isinstance(kwargs["type"], Float) and not kwargs["type"].asdecimal:
            return FloatField(
                **self._field_common(*args, **kwargs),
            )
        return DecimalField(
            **self._field_common(*args, **kwargs),
        )

    @converts(
        "sqlalchemy.dialects.mysql.types.YEAR", "sqlalchemy.dialects.mysql.base.YEAR"
    )
    def conv_mysql_year(self, *args: Any, **kwargs: Any) -> BaseField:
        return IntegerField(**self._field_common(*args, **kwargs), min=1901, max=2155)

    @converts("ARRAY")
    def conv_array(self, *args: Any, **kwargs: Any) -> BaseField:
        _type = kwargs["type"]
        if isinstance(_type, ARRAY) and (
            _type.dimensions is None or _type.dimensions == 1
        ):
            kwargs.update(
                {
                    "column": Column(kwargs["name"], _type.item_type),
                    "type": _type.item_type,
                }
            )
            return ListField(self.convert(*args, **kwargs))
        raise NotSupportedColumn("Column ARRAY with dimensions != 1 is not supported")

    @converts("JSON", "sqlalchemy_utils.types.json.JSONType")
    def conv_json(self, *args: Any, **kwargs: Any) -> BaseField:
        return JSONField(**self._field_common(*args, **kwargs))

    @converts("sqlalchemy_file.types.FileField")
    def conv_sqla_filefield(self, *args: Any, **kwargs: Any) -> BaseField:
        return FileField(
            **self._field_common(*args, **kwargs), **self._file_common(*args, **kwargs)
        )

    @converts("sqlalchemy_file.types.ImageField")
    def conv_sqla_imagefield(self, *args: Any, **kwargs: Any) -> BaseField:
        return ImageField(
            **self._field_common(*args, **kwargs), **self._file_common(*args, **kwargs)
        )

    @converts("sqlalchemy_utils.types.arrow.ArrowType")
    def conv_arrow(self, *args: Any, **kwargs: Any) -> BaseField:
        return ArrowField(
            **self._field_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.color.ColorType")
    def conv_color(self, *args: Any, **kwargs: Any) -> BaseField:
        return ColorField(
            **self._field_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.email.EmailType")
    def conv_email(self, *args: Any, **kwargs: Any) -> BaseField:
        return EmailField(
            **self._field_common(*args, **kwargs),
            **self._string_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.password.PasswordType")
    def conv_password(self, *args: Any, **kwargs: Any) -> BaseField:
        return PasswordField(
            **self._field_common(*args, **kwargs),
            **self._string_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.phone_number.PhoneNumberType")
    def conv_phonenumbers(self, *args: Any, **kwargs: Any) -> BaseField:
        return PhoneField(
            **self._field_common(*args, **kwargs),
            **self._string_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.scalar_list.ScalarListType")
    def conv_scalar_list(self, *args: Any, **kwargs: Any) -> BaseField:
        return ListField(
            StringField(
                **self._field_common(*args, **kwargs),
            )
        )

    @converts("sqlalchemy_utils.types.url.URLType")
    def conv_url(self, *args: Any, **kwargs: Any) -> BaseField:
        return URLField(
            **self._field_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.timezone.TimezoneType")
    def conv_timezone(self, *args: Any, **kwargs: Any) -> BaseField:
        return TimeZoneField(
            **self._field_common(*args, **kwargs),
            coerce=kwargs["type"].python_type,
        )

    @converts("sqlalchemy_utils.types.country.CountryType")
    def conv_country(self, *args: Any, **kwargs: Any) -> BaseField:
        return CountryField(
            **self._field_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.currency.CurrencyType")
    def conv_currency(self, *args: Any, **kwargs: Any) -> BaseField:
        return CurrencyField(
            **self._field_common(*args, **kwargs),
        )

    @converts("sqlalchemy_utils.types.choice.ChoiceType")
    def conv_choice(self, *args: Any, **kwargs: Any) -> BaseField:
        _type = kwargs["type"]
        choices = _type.choices
        if isinstance(choices, type) and issubclass(choices, enum.Enum):
            return EnumField(
                **self._field_common(*args, **kwargs),
                enum=choices,
                coerce=_type.python_type,
            )
        return EnumField(
            **self._field_common(*args, **kwargs),
            choices=choices,
            coerce=_type.python_type,
        )

    @converts("sqlalchemy_utils.types.pg_composite.CompositeType")
    def conv_composite_type(self, *args: Any, **kwargs: Any) -> BaseField:
        _type = kwargs["type"]
        fields = []
        field_common = self._field_common(*args, **kwargs)
        for col in _type.columns:
            kwargs.update({"name": col.name, "column": col, "type": col.type})
            fields.append(self.convert(*args, **kwargs))
        return CollectionField(
            field_common["name"], fields=fields, required=field_common["required"]
        )
