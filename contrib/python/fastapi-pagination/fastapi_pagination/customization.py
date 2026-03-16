from __future__ import annotations

__all__ = [
    "ClsNamespace",
    "CustomizedPage",
    "PageCls",
    "PageCustomizer",
    "PageTransformer",
    "UseAdditionalFields",
    "UseCursorEncoding",
    "UseExcludedFields",
    "UseFieldTypeAnnotations",
    "UseFieldsAliases",
    "UseIncludeTotal",
    "UseModelConfig",
    "UseModule",
    "UseName",
    "UseOptionalFields",
    "UseOptionalParams",
    "UseParams",
    "UseParamsFields",
    "UsePydanticV1",
    "UseQuotedCursor",
    "UseRequiredFields",
    "UseResponseHeaders",
    "UseStrCursor",
    "get_page_bases",
    "new_page_cls",
]
from abc import abstractmethod
from collections.abc import Callable, Sequence
from contextlib import suppress
from copy import copy
from dataclasses import dataclass
from types import new_class
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

from fastapi import Query
from fastapi.params import Param
from pydantic import BaseModel, ConfigDict, create_model
from typing_extensions import Self, Unpack

from .api import response
from .bases import AbstractPage, AbstractParams, BaseAbstractPage, BaseRawParams
from .cursor import CursorDecoder, CursorEncoder
from .errors import UnsupportedFeatureError
from .pydantic import (
    get_field_tp,
    get_model_fields,
    is_pydantic_field,
    make_field_optional,
    make_field_required,
)
from .pydantic.v2 import FieldV2, UndefinedV2, is_pydantic_v2_model
from .types import Cursor
from .typing_utils import create_annotated_tp
from .utils import get_caller

if TYPE_CHECKING:
    from .pydantic.v1 import BaseModelV1


ClsNamespace: TypeAlias = dict[str, Any]
PageCls: TypeAlias = "type[AbstractPage[Any]]"

TPage = TypeVar("TPage", bound=PageCls)


def get_page_bases(cls: TPage) -> tuple[type[Any], ...]:
    bases: tuple[type[Any], ...]

    if is_pydantic_v2_model(cls):
        params = cls.__pydantic_generic_metadata__["parameters"]
        bases = (cls,) if not params else (cls[params], Generic[params])  # type: ignore[invalid-argument-type]
    elif cls.__concrete__:
        bases = (cls,)
    else:
        params = tuple(cls.__parameters__)
        bases = (cls[params], Generic[params])

    return bases


def new_page_cls(cls: TPage, new_ns: ClsNamespace) -> TPage:
    new_cls = new_class(
        new_ns.get("__name__", cls.__name__),
        get_page_bases(cls),
        exec_body=lambda ns: ns.update(new_ns),
    )

    return cast(TPage, new_cls)


def new_params_cls(cls: type[AbstractParams], new_ns: ClsNamespace) -> type[AbstractParams]:
    new_cls = new_class(
        new_ns.get("__name__", cls.__name__),
        (cls,),
        exec_body=lambda ns: ns.update(new_ns),
    )

    return cast(type[AbstractParams], new_cls)


if TYPE_CHECKING:
    from typing import Annotated as CustomizedPage
else:

    class CustomizedPage:
        def __class_getitem__(cls, item: Any) -> Any:
            if not isinstance(item, tuple):
                item = (item,)

            page_cls, *customizers = item

            assert isinstance(page_cls, type), f"Expected type, got {page_cls!r}"
            assert issubclass(page_cls, BaseAbstractPage), f"Expected subclass of AbstractPage, got {page_cls!r}"

            if not customizers:
                return page_cls

            original_name = page_cls.__name__.removesuffix("Customized")
            cls_name = f"{original_name}Customized"

            for customizer in customizers:
                if not isinstance(customizer, (PageCustomizer, PageTransformer)):
                    raise TypeError(f"Expected PageCustomizer or PageTransformer, got {customizer!r}")

            for customizer in customizers:
                if isinstance(customizer, PageTransformer):
                    page_cls = customizer.transform_page_cls(page_cls)

            new_ns = {
                "__name__": cls_name,
                "__qualname__": cls_name,
                "__module__": get_caller(),
                "__params_type__": page_cls.__params_type__,
                "__model_aliases__": copy(page_cls.__model_aliases__),
                "__model_exclude__": copy(page_cls.__model_exclude__),
            }

            if is_pydantic_v2_model(page_cls):
                new_ns["model_config"] = {}
            else:

                class Config:
                    pass

                new_ns["Config"] = Config

            for customizer in customizers:
                if isinstance(customizer, PageCustomizer):
                    customizer.customize_page_ns(page_cls, new_ns)

            params_type = new_params_cls(cast(type[AbstractParams], new_ns["__params_type__"]), {"__page_type__": None})
            new_ns["__params_type__"] = params_type
            return new_page_cls(page_cls, new_ns)


@runtime_checkable
class PageCustomizer(Protocol):
    @abstractmethod
    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        pass


@runtime_checkable
class PageTransformer(Protocol):
    @abstractmethod
    def transform_page_cls(self, page_cls: PageCls) -> PageCls:
        pass


@dataclass
class UseName(PageCustomizer):
    name: str

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        ns["__name__"] = self.name
        ns["__qualname__"] = self.name


@dataclass
class UseModule(PageCustomizer):
    module: str

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        ns["__module__"] = self.module


@dataclass
class _UseOptionalRequiredFields(PageCustomizer):
    required: bool
    fields: Sequence[str] = (
        "total",
        "page",
        "size",
        "pages",
        "limit",
        "offset",
    )

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        fields = get_model_fields(page_cls)
        fields_to_update = {name: field for name, field in fields.items() if name in self.fields}

        if not fields_to_update:
            return

        # wtf is going on here? :(((
        customizer: PageCustomizer
        if self.required:
            fields_to_update = {k: make_field_required(v) for k, v in fields_to_update.items()}

            if is_pydantic_v2_model(page_cls):
                # to make field required in pydantic v2 we just need to update its type annotation
                customizer = UseFieldTypeAnnotations(**{k: get_field_tp(v) for k, v in fields_to_update.items()})
            else:
                customizer = UseAdditionalFields(**{k: (get_field_tp(v), ...) for k, v in fields_to_update.items()})
        else:  # noqa: PLR5501
            if is_pydantic_v2_model(page_cls):
                fields_to_update = {k: make_field_optional(v) or v for k, v in fields_to_update.items()}
                customizer = UseAdditionalFields(
                    **{k: (create_annotated_tp(get_field_tp(v), v), None) for k, v in fields_to_update.items()}
                )
            else:
                customizer = UseAdditionalFields(
                    **{k: (get_field_tp(v) | None, None) for k, v in fields_to_update.items()},
                )

        customizer.customize_page_ns(page_cls, ns)


@dataclass
class UseOptionalFields(_UseOptionalRequiredFields):
    required: bool = False


@dataclass
class UseRequiredFields(_UseOptionalRequiredFields):
    required: bool = True


@dataclass
class UseIncludeTotal(PageCustomizer):
    include_total: bool

    update_annotations: bool = True
    affected_fields: Sequence[str] = (
        "total",
        "page",
        "size",
        "pages",
        "limit",
        "offset",
    )

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        include_total = self.include_total

        if TYPE_CHECKING:
            from .bases import AbstractParams as ParamsCls
        else:
            ParamsCls = ns["__params_type__"]

        class CustomizedParams(ParamsCls):
            def to_raw_params(self) -> BaseRawParams:
                raw_params = super().to_raw_params()
                raw_params.include_total = include_total

                return raw_params

        ns["__params_type__"] = CustomizedParams

        if self.update_annotations:
            customizer = _UseOptionalRequiredFields(
                required=self.include_total,
                fields=self.affected_fields,
            )
            customizer.customize_page_ns(page_cls, ns)


@dataclass
class UseCursorEncoding(PageCustomizer):
    encoder: CursorEncoder | None = None
    decoder: CursorDecoder | None = None

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if not (self.encoder or self.decoder):
            return

        if TYPE_CHECKING:
            from .cursor import CursorParams
        else:
            CursorParams = ns["__params_type__"]

        src = self

        class CustomizedParams(CursorParams):
            def encode_cursor(self, cursor: Cursor | None) -> str | None:
                if src.encoder:
                    return src.encoder(self, cursor)

                return super().encode_cursor(cursor)

            def decode_cursor(self, cursor: str | None) -> Cursor | None:
                if src.decoder:
                    return src.decoder(self, cursor)

                return super().decode_cursor(cursor)

        ns["__params_type__"] = CustomizedParams


@dataclass
class UseQuotedCursor(PageCustomizer):
    quoted_cursor: bool = True

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if TYPE_CHECKING:
            from .cursor import CursorParams
        else:
            CursorParams = ns["__params_type__"]

        class CustomizedParams(CursorParams):
            quoted_cursor: ClassVar[bool] = self.quoted_cursor

        ns["__params_type__"] = CustomizedParams


@dataclass
class UseStrCursor(PageCustomizer):
    str_cursor: bool = True

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if TYPE_CHECKING:
            from .cursor import CursorParams
        else:
            CursorParams = ns["__params_type__"]

        class CustomizedParams(CursorParams):
            str_cursor: ClassVar[bool] = self.str_cursor

        ns["__params_type__"] = CustomizedParams


@dataclass
class UseParams(PageCustomizer):
    params: type[AbstractParams]

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if page_cls.__params_type__ is not ns["__params_type__"]:
            raise ValueError(
                "Params type was already customized, cannot customize it again. "
                "(IncludeTotal/UseParamsFields must go after UseParams)"
            )

        ns["__params_type__"] = self.params


def _update_params_fields(cls: type[AbstractParams], fields: ClsNamespace) -> ClsNamespace:
    if not issubclass(cls, BaseModel):
        raise TypeError(f"{cls.__name__} must be subclass of BaseModel")

    model_fields = get_model_fields(cls)
    incorrect = sorted(fields.keys() - model_fields.keys() - cls.__class_vars__)

    if incorrect:
        ending = "s" if len(incorrect) > 1 else ""
        raise ValueError(f"Unknown field{ending} {', '.join(incorrect)}")

    anns = get_type_hints(cls)

    def _wrap_val(name: str, v: Any) -> Any:
        if name in cls.__class_vars__:
            return v
        if not (isinstance(v, Param) or is_pydantic_field(v)):
            return Query(v)

        return v

    def _get_ann(name: str, v: Any) -> Any:
        if is_pydantic_v2_model(cls):
            return getattr(v, "annotation", None) or anns[name]

        return anns[name]

    return {name: (_get_ann(name, val), _wrap_val(name, val)) for name, val in fields.items()}


class UseParamsFields(PageCustomizer):
    def __init__(self, **kwargs: Any) -> None:
        self.fields = kwargs

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        params_cls = ns["__params_type__"]

        ns["__params_type__"] = create_model(
            params_cls.__name__,
            __base__=params_cls,
            **_update_params_fields(params_cls, self.fields),
        )


class UseOptionalParams(PageCustomizer):
    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        params_cls = ns["__params_type__"]

        fields = get_model_fields(params_cls)
        new_fields = {name: make_field_optional(field) for name, field in fields.items()}

        customizer = UseParamsFields(**new_fields)
        customizer.customize_page_ns(page_cls, ns)

        optional_customizer = UseOptionalFields()
        optional_customizer.customize_page_ns(page_cls, ns)


class UseModelConfig(PageCustomizer):
    def __init__(self, **kwargs: Unpack[ConfigDict]) -> None:
        self.config = kwargs

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if is_pydantic_v2_model(page_cls):
            ns["model_config"].update(self.config)
        else:
            for key, val in self.config.items():
                setattr(ns["Config"], key, val)


def _pydantic_v1_get_inited_fields(cls: Any, /, *fields: str) -> ClsNamespace:
    if not hasattr(cls, "fields"):
        cls.fields = {}

    for f in fields:
        cls.fields.setdefault(f, {})

    return cls.fields


class UseExcludedFields(PageCustomizer):
    def __init__(self, *fields: str) -> None:
        self.fields = fields

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if is_pydantic_v2_model(page_cls):
            ns["__model_exclude__"].update(self.fields)
        else:
            fields_config = _pydantic_v1_get_inited_fields(ns["Config"], *self.fields)

            for f in self.fields:
                fields_config[f]["exclude"] = True


class UseFieldsAliases(PageCustomizer):
    def __init__(self, **aliases: str) -> None:
        self.aliases = aliases

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if is_pydantic_v2_model(page_cls):
            ns["__model_aliases__"].update(self.aliases)
        else:
            fields_config = _pydantic_v1_get_inited_fields(ns["Config"], *self.aliases)

            for name, alias in self.aliases.items():
                assert name in page_cls.__fields__, f"Unknown field {name!r}"
                fields_config[name]["alias"] = alias


_RawFieldDef: TypeAlias = Any | tuple[Any, Any]


class UseAdditionalFields(PageCustomizer):
    def __init__(self, **fields: _RawFieldDef) -> None:
        self.fields = fields

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        anns = ns.setdefault("__annotations__", {})

        for name, field in self.fields.items():
            if isinstance(field, tuple):
                anns[name], ns[name] = field
            else:
                anns[name] = field


class UseFieldTypeAnnotations(PageCustomizer):
    def __init__(self, **anns: Any) -> None:
        self.anns = anns

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        anns = ns.setdefault("__annotations__", {})
        anns.update(self.anns)


@dataclass
class UseResponseHeaders(PageCustomizer):
    resolver: Callable[[AbstractPage[Any]], dict[str, str | Sequence[str]]]

    def customize_page_ns(self, page_cls: PageCls, ns: ClsNamespace) -> None:
        if not is_pydantic_v2_model(page_cls):  # pragma: no cover
            raise UnsupportedFeatureError("UseResponseHeaders is only supported in Pydantic v2")

        def model_post_init(_self: AbstractPage[Any], context: Any, /) -> None:
            page_cls.model_post_init(_self, context)

            rsp = response()

            for k, v in self.resolver(_self).items():
                match v:
                    case str():
                        rsp.headers[k] = v
                    case Sequence() as items:
                        with suppress(KeyError):
                            del rsp.headers[k]

                        for item in items:
                            rsp.headers.append(k, item)
                    case _:
                        raise TypeError(f"Header value must be str or list[str], got {v!r}")

        ns["model_post_init"] = model_post_init


def _convert_v2_field_to_v1(field_v2: FieldV2) -> tuple[Any, Any]:
    from .pydantic.v1 import (
        FieldInfoV1,
        UndefinedV1,
    )

    default = field_v2.default
    if default is UndefinedV2:
        default = UndefinedV1

    type_ = get_field_tp(field_v2)

    if get_origin(type_) is Annotated:
        type_, *_ = type_.__args__

    return type_, FieldInfoV1(
        default=default,
        default_factory=field_v2.default_factory,
        alias=field_v2.alias or field_v2.validation_alias,
        title=field_v2.title,
        description=field_v2.description,
    )


def _convert_v2_page_cls_to_v1(page_cls: type[AbstractPage], /) -> BaseModelV1:
    from .pydantic.v1 import (
        BaseModelV1,
        ConfiguredBaseModelV1,
        GenericModelV1,
        create_model_v1,
    )

    assert issubclass(page_cls, AbstractPage)

    _create = page_cls.create.__func__
    *_, generic_base = get_page_bases(page_cls)
    tp_params = generic_base.__parameters__

    class _PydanticV2ToV1(
        BaseAbstractPage,
        GenericModelV1,
        ConfiguredBaseModelV1,
        generic_base,
    ):
        __params_type__ = copy(page_cls.__params_type__)
        __model_aliases__ = copy(page_cls.__model_aliases__)
        __model_exclude__ = copy(page_cls.__model_exclude__)

        @classmethod
        def create(
            cls,
            items: Sequence[Any],
            params: AbstractParams,
            **kwargs: Any,
        ) -> Self:
            return _create(
                cls,
                items=items,
                params=params,
                **kwargs,
            )

    new_cls = create_model_v1(  # type: ignore[call-overload]
        page_cls.__name__,
        __base__=(_PydanticV2ToV1[tp_params], Generic[tp_params]),  # type: ignore[misc,index]
        **{k: _convert_v2_field_to_v1(cast(FieldV2, v)) for k, v in get_model_fields(page_cls).items()},
    )

    return cast(BaseModelV1, new_cls)


@dataclass
class UsePydanticV1(PageTransformer):
    def transform_page_cls(self, page_cls: PageCls) -> PageCls:
        if not is_pydantic_v2_model(page_cls):
            return page_cls

        return cast(PageCls, _convert_v2_page_cls_to_v1(page_cls))
