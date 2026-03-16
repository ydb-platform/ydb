# mypy: disable-error-code="return-value"

"""
The converters are from Flask-Admin project.
"""

from __future__ import annotations

import enum
import inspect
import sys
from typing import (
    Any,
    Callable,
    Sequence,
    TypeVar,
    Union,
    no_type_check,
)

import anyio
from sqlalchemy import Boolean, select
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.orm import (
    ColumnProperty,
    RelationshipProperty,
    sessionmaker,
)
from sqlalchemy.sql.elements import Label
from wtforms import (
    BooleanField,
    DecimalField,
    Field,
    Form,
    IntegerField,
    StringField,
    TextAreaField,
    TimeField,
    validators,
)
from wtforms.fields.core import UnboundField

from sqladmin._types import MODEL_PROPERTY
from sqladmin._validators import (
    ColorValidator,
    CurrencyValidator,
    PhoneNumberValidator,
    TimezoneValidator,
)
from sqladmin.ajax import QueryAjaxModelLoader
from sqladmin.exceptions import NoConverterFound
from sqladmin.fields import (
    AjaxSelectField,
    AjaxSelectMultipleField,
    DateField,
    DateTimeField,
    FileField,
    IntervalField,
    JSONField,
    QuerySelectField,
    QuerySelectMultipleField,
    Select2TagsField,
    SelectField,
)
from sqladmin.helpers import (
    choice_type_coerce_factory,
    get_direction,
    get_object_identifier,
    is_async_session_maker,
    is_relationship,
)

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol


class Validator(Protocol):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...  # pragma: no cover

    def __call__(self, form: Form, field: Field) -> None: ...  # pragma: no cover


class ConverterCallable(Protocol):
    def __call__(
        self,
        model: type,
        prop: MODEL_PROPERTY,
        kwargs: dict[str, Any],
    ) -> UnboundField: ...  # pragma: no cover


T_CC = TypeVar("T_CC", bound=ConverterCallable)

_WTFORMS_PRIVATE_ATTRS = {"data", "errors", "process", "validate", "populate_obj"}
WTFORMS_ATTRS = {key: key + "_" for key in _WTFORMS_PRIVATE_ATTRS}
WTFORMS_ATTRS_REVERSED = {v: k for k, v in WTFORMS_ATTRS.items()}


@no_type_check
def converts(*args: str) -> Callable[[T_CC], T_CC]:
    def _inner(func: T_CC) -> T_CC:
        func._converter_for = frozenset(args)
        return func

    return _inner


class ModelConverterBase:
    _converters: dict[str, ConverterCallable] = {}

    def __init__(self) -> None:
        super().__init__()
        self._register_converters()

    def _register_converters(self) -> None:
        converters = {}

        for name in dir(self):
            obj = getattr(self, name)
            if hasattr(obj, "_converter_for"):
                for classname in obj._converter_for:
                    converters[classname] = obj

        self._converters = converters

    async def _prepare_kwargs(
        self,
        prop: MODEL_PROPERTY,
        session_maker: sessionmaker,
        field_args: dict[str, Any],
        field_widget_args: dict[str, Any],
        form_include_pk: bool,
        label: str | None = None,
        loader: QueryAjaxModelLoader | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(prop, (RelationshipProperty, ColumnProperty)):
            return None

        kwargs: Union[dict, None]
        kwargs = field_args.copy()
        widget_args = field_widget_args.copy()
        widget_args.setdefault("class", "form-control")

        kwargs.setdefault("label", label)
        kwargs.setdefault("validators", [])
        kwargs.setdefault("filters", [])
        kwargs.setdefault("default", None)
        kwargs.setdefault("description", prop.doc)
        kwargs.setdefault("render_kw", widget_args)

        if isinstance(prop, ColumnProperty):
            kwargs = self._prepare_column(
                prop=prop, kwargs=kwargs, form_include_pk=form_include_pk
            )
        else:
            kwargs = await self._prepare_relationship(
                prop=prop, session_maker=session_maker, kwargs=kwargs, loader=loader
            )

        return kwargs

    def _prepare_column(
        self,
        prop: ColumnProperty,
        form_include_pk: bool,
        kwargs: dict,
    ) -> Union[dict, None]:
        if len(prop.columns) != 1:
            raise NotImplementedError("Multiple-column properties are not supported")

        column = prop.columns[0]

        if (column.primary_key or column.foreign_keys) and not form_include_pk:
            return None

        default = getattr(column, "default", None) or kwargs.get("default")

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
        optional_types = (Boolean,)

        if column.nullable:
            kwargs["validators"].append(validators.Optional())

        if (
            not column.nullable
            and not isinstance(column.type, optional_types)
            and not column.default
            and not column.server_default
        ):
            kwargs["validators"].append(validators.InputRequired())

        return kwargs

    async def _prepare_relationship(
        self,
        prop: RelationshipProperty,
        kwargs: dict,
        session_maker: sessionmaker,
        loader: QueryAjaxModelLoader | None = None,
    ) -> dict:
        nullable = True
        for pair in prop.local_remote_pairs or []:
            if not pair[0].nullable:
                nullable = False

        kwargs["allow_blank"] = nullable

        if not loader:
            kwargs.setdefault(
                "data", await self._prepare_select_options(prop, session_maker)
            )

        return kwargs

    async def _prepare_select_options(
        self,
        prop: RelationshipProperty,
        session_maker: sessionmaker,
    ) -> list[tuple[str, Any]]:
        target_model = prop.mapper.class_
        stmt = select(target_model)

        if is_async_session_maker(session_maker):
            async with session_maker() as session:
                objects = await session.execute(stmt)
                return [
                    (str(self._get_identifier_value(obj)), str(obj))
                    for obj in objects.scalars().unique().all()
                ]
        else:
            with session_maker() as session:
                objects = await anyio.to_thread.run_sync(session.execute, stmt)
                return [
                    (str(self._get_identifier_value(obj)), str(obj))
                    for obj in objects.scalars().unique().all()
                ]

    def get_converter(self, prop: MODEL_PROPERTY) -> ConverterCallable:
        if isinstance(prop, RelationshipProperty):
            direction = get_direction(prop)
            return self._converters[direction]

        column = prop.columns[0]
        types = inspect.getmro(type(column.type))

        # Search by module + name
        for col_type in types:
            type_string = f"{col_type.__module__}.{col_type.__name__}"

            if type_string in self._converters:
                return self._converters[type_string]

        # Search by name
        for col_type in types:
            if col_type.__name__ in self._converters:
                return self._converters[col_type.__name__]

            # Support for custom types like SQLModel which inherit TypeDecorator
            if hasattr(col_type, "impl"):
                if callable(col_type.impl):  # type: ignore
                    impl = col_type.impl  # type: ignore
                else:
                    impl = col_type.impl.__class__  # type: ignore

                if impl.__name__ in self._converters:
                    return self._converters[impl.__name__]

        raise NoConverterFound(  # pragma: nocover
            f"Could not find field converter for column {column.name} ({types[0]!r})."
        )

    async def convert(
        self,
        model: type,
        prop: MODEL_PROPERTY,
        session_maker: sessionmaker,
        field_args: dict[str, Any],
        field_widget_args: dict[str, Any],
        form_include_pk: bool,
        label: str | None = None,
        override: type[Field] | None = None,
        form_ajax_refs: dict[str, QueryAjaxModelLoader] | None = None,
    ) -> UnboundField:
        loader = (form_ajax_refs or {}).get(prop.key)
        kwargs = await self._prepare_kwargs(
            prop=prop,
            session_maker=session_maker,
            field_args=field_args,
            field_widget_args=field_widget_args,
            label=label,
            form_include_pk=form_include_pk,
            loader=loader,
        )

        if kwargs is None:
            return None

        if override is not None:
            if not issubclass(override, Field):
                raise TypeError("Expected Field, got %s" % type(override))

            return override(**kwargs)

        multiple = (
            is_relationship(prop)
            and prop.direction.name in ("ONETOMANY", "MANYTOMANY")
            and prop.uselist
        )

        if loader:
            field = AjaxSelectMultipleField if multiple else AjaxSelectField
            return field(loader, **kwargs)

        converter = self.get_converter(prop=prop)
        return converter(model=model, prop=prop, kwargs=kwargs)

    def _get_identifier_value(self, o: Any) -> str:
        return str(get_object_identifier(o))


class ModelConverter(ModelConverterBase):
    @staticmethod
    def _string_common(prop: ColumnProperty) -> list[Validator]:
        li = []
        column = prop.columns[0]
        if isinstance(column.type.length, int) and column.type.length:  # type: ignore[attr-defined]
            li.append(validators.Length(max=column.type.length))  # type: ignore[attr-defined]
        return li

    @converts("String", "CHAR")  # includes Unicode
    def conv_string(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        extra_validators = self._string_common(prop)
        kwargs.setdefault("validators", [])
        kwargs["validators"].extend(extra_validators)
        return StringField(**kwargs)

    @converts("Text", "LargeBinary", "Binary")  # includes UnicodeText
    def conv_text(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        extra_validators = self._string_common(prop)
        kwargs["validators"].extend(extra_validators)
        return TextAreaField(**kwargs)

    @converts("Boolean", "dialects.mssql.base.BIT")
    def conv_boolean(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        if not prop.columns[0].nullable:
            kwargs.setdefault("render_kw", {})
            kwargs["render_kw"]["class"] = "form-check-input"
            return BooleanField(**kwargs)

        kwargs["allow_blank"] = True
        kwargs["choices"] = [(True, "True"), (False, "False")]
        kwargs["coerce"] = lambda v: str(v) == "True"
        return SelectField(**kwargs)

    @converts("Date")
    def conv_date(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return DateField(**kwargs)

    @converts("Time")
    def conv_time(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return TimeField(**kwargs)

    @converts("DateTime")
    def conv_datetime(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return DateTimeField(**kwargs)

    @converts("Enum")
    def conv_enum(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        available_choices = [
            (e, e)
            for e in prop.columns[0].type.enums  # type: ignore[attr-defined]
        ]
        accepted_values = [choice[0] for choice in available_choices]

        if prop.columns[0].nullable:
            kwargs["allow_blank"] = True
            accepted_values.append(None)
            filters = kwargs.get("filters", [])
            filters.append(lambda x: x or None)
            kwargs["filters"] = filters

        kwargs["choices"] = available_choices
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(validators.AnyOf(accepted_values))
        kwargs["coerce"] = lambda v: v.name if isinstance(v, enum.Enum) else str(v)
        return SelectField(**kwargs)

    @converts("Integer")  # includes BigInteger and SmallInteger
    def conv_integer(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return IntegerField(**kwargs)

    @converts("Numeric")  # includes DECIMAL, Float/FLOAT, REAL, and DOUBLE
    def conv_decimal(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        # override default decimal places limit, use database defaults instead
        kwargs.setdefault("places", None)
        return DecimalField(**kwargs)

    @converts("JSON", "JSONB")
    def conv_json(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return JSONField(**kwargs)

    @converts("Interval")
    def conv_interval(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs["render_kw"]["placeholder"] = "Like: 1 day 1:25:33.652"
        return IntervalField(**kwargs)

    @converts(
        "sqlalchemy.dialects.postgresql.base.INET",
        "sqlalchemy.dialects.postgresql.types.INET",
        "sqlalchemy_utils.types.ip_address.IPAddressType",
    )
    def conv_ip_address(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(validators.IPAddress(ipv4=True, ipv6=True))
        return StringField(**kwargs)

    @converts(
        "sqlalchemy.dialects.postgresql.base.MACADDR",
        "sqlalchemy.dialects.postgresql.types.MACADDR",
    )
    def conv_mac_address(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(validators.MacAddress())
        return StringField(**kwargs)

    @converts(
        "sqlalchemy.dialects.postgresql.base.UUID",
        "sqlalchemy.sql.sqltypes.UUID",
        "sqlalchemy.sql.sqltypes.Uuid",
        "sqlalchemy_utils.types.uuid.UUIDType",
    )
    def conv_uuid(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(validators.UUID())
        return StringField(**kwargs)

    @converts(
        "sqlalchemy.dialects.postgresql.base.ARRAY",
        "sqlalchemy.sql.sqltypes.ARRAY",
    )
    def conv_array(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return Select2TagsField(**kwargs)

    @converts("sqlalchemy_utils.types.email.EmailType")
    def conv_email(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(validators.Email())
        return StringField(**kwargs)

    @converts("sqlalchemy_utils.types.url.URLType")
    def conv_url(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(validators.URL())
        return StringField(**kwargs)

    @converts("sqlalchemy_utils.types.currency.CurrencyType")
    def conv_currency(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(CurrencyValidator())
        return StringField(**kwargs)

    @converts("sqlalchemy_utils.types.timezone.TimezoneType")
    def conv_timezone(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(
            TimezoneValidator(coerce_function=prop.columns[0].type._coerce)  # type: ignore[attr-defined]
        )
        return StringField(**kwargs)

    @converts("sqlalchemy_utils.types.phone_number.PhoneNumberType")
    def conv_phone_number(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(PhoneNumberValidator())
        return StringField(**kwargs)

    @converts("sqlalchemy_utils.types.color.ColorType")
    def conv_color(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs.setdefault("validators", [])
        kwargs["validators"].append(ColorValidator())
        return StringField(**kwargs)

    @converts("sqlalchemy_utils.types.choice.ChoiceType")
    @no_type_check
    def convert_choice_type(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        available_choices = []
        column = prop.columns[0]

        if isinstance(column.type.choices, enum.EnumMeta):
            available_choices = [(f.value, f.name) for f in column.type.choices]
        else:
            available_choices = column.type.choices

        accepted_values = [
            choice[0] if isinstance(choice, tuple) else choice.value
            for choice in available_choices
        ]

        if column.nullable:
            kwargs["allow_blank"] = column.nullable
            accepted_values.append(None)
            filters = kwargs.get("filters", [])
            filters.append(lambda x: x or None)
            kwargs["filters"] = filters

        kwargs["choices"] = available_choices
        kwargs["validators"].append(validators.AnyOf(accepted_values))
        kwargs["coerce"] = choice_type_coerce_factory(column.type)
        return SelectField(**kwargs)

    @converts("fastapi_storages.integrations.sqlalchemy.FileType")
    def conv_file(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return FileField(**kwargs)

    @converts("fastapi_storages.integrations.sqlalchemy.ImageType")
    def conv_image(
        self,
        model: type,
        prop: ColumnProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return FileField(**kwargs)

    @converts("ONETOONE")
    def conv_one_to_one(
        self,
        model: type,
        prop: RelationshipProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        kwargs["allow_blank"] = True
        return QuerySelectField(**kwargs)

    @converts("MANYTOONE")
    def conv_many_to_one(
        self,
        model: type,
        prop: RelationshipProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return QuerySelectField(**kwargs)

    @converts("MANYTOMANY", "ONETOMANY")
    def conv_many_to_many(
        self,
        model: type,
        prop: RelationshipProperty,
        kwargs: dict[str, Any],
    ) -> UnboundField:
        return QuerySelectMultipleField(**kwargs)


async def get_model_form(
    model: type,
    session_maker: sessionmaker,
    only: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    column_labels: dict[str, str] | None = None,
    form_args: dict[str, dict[str, Any]] | None = None,
    form_widget_args: dict[str, dict[str, Any]] | None = None,
    form_class: type[Form] = Form,
    form_overrides: dict[str, type[Field]] | None = None,
    form_ajax_refs: dict[str, QueryAjaxModelLoader] | None = None,
    form_include_pk: bool = False,
    form_converter: type[ModelConverterBase] = ModelConverter,
) -> type[Form]:
    type_name = model.__name__ + "Form"
    converter = form_converter()
    mapper = sqlalchemy_inspect(model)
    form_args = form_args or {}
    form_widget_args = form_widget_args or {}
    column_labels = column_labels or {}
    form_overrides = form_overrides or {}
    form_ajax_refs = form_ajax_refs or {}

    attributes = []
    names = only or mapper.attrs.keys()
    for name in names:
        attr = mapper.attrs[name]
        if (exclude and name in exclude) or (
            isinstance(attr, ColumnProperty) and isinstance(attr.expression, Label)
        ):
            continue
        attributes.append((name, attr))

    field_dict = {}
    for name, attr in attributes:
        field_args = form_args.get(name, {})
        field_args["name"] = name

        field_widget_args = form_widget_args.get(name, {})
        label = column_labels.get(name, None)
        override = form_overrides.get(name, None)
        field = await converter.convert(
            model=model,
            prop=attr,
            session_maker=session_maker,
            field_args=field_args,
            field_widget_args=field_widget_args,
            label=label,
            override=override,
            form_include_pk=form_include_pk,
            form_ajax_refs=form_ajax_refs,
        )
        if field is not None:
            field_dict_key = WTFORMS_ATTRS.get(name, name)
            field_dict[field_dict_key] = field

    return type(type_name, (form_class,), field_dict)
