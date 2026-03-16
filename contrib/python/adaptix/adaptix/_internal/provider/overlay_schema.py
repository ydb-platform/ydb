from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from typing import Any, Callable, ClassVar, Generic, Optional, TypeVar

from ..datastructures import ClassMap
from ..type_tools import strip_alias
from ..utils import Omitted
from .essential import CannotProvide, Mediator
from .loc_stack_filtering import LocStack
from .located_request import LocatedRequest
from .methods_provider import MethodsProvider, method_handler
from .provider_wrapper import Chain


@dataclass(frozen=True)
class Schema:
    pass


Sc = TypeVar("Sc", bound=Schema)
Ov = TypeVar("Ov", bound="Overlay")

Merger = Callable[[Any, Any, Any], Any]


@dataclass(frozen=True)
class Overlay(Generic[Sc]):
    _schema_cls: ClassVar[type[Schema]]  # ClassVar cannot contain TypeVar
    _mergers: ClassVar[Optional[Mapping[str, Merger]]]

    def __init_subclass__(cls, *args, **kwargs):
        for base in cls.__orig_bases__:
            if strip_alias(base) == Overlay:
                cls._schema_cls = base.__args__[0]
                break
        else:
            raise ValueError

        cls._mergers = None

    def _default_merge(self, old: Any, new: Any) -> Any:
        return new

    def _is_omitted(self, value: Any) -> bool:
        return value is Omitted()

    @classmethod
    def _load_mergers(cls) -> Mapping[str, Merger]:
        if cls._mergers is None:
            cls._mergers = {
                field.name: getattr(cls, f"_merge_{field.name}", cls._default_merge)
                for field in fields(cls)
            }
        return cls._mergers

    def merge(self: Ov, new: Ov) -> Ov:
        merged = {}
        for field_id, merger in self._load_mergers().items():
            old_field_value = getattr(self, field_id)
            new_field_value = getattr(new, field_id)
            if self._is_omitted(old_field_value):
                merged[field_id] = new_field_value
            elif self._is_omitted(new_field_value):
                merged[field_id] = old_field_value
            else:
                merged[field_id] = merger(self, old_field_value, new_field_value)
        return self.__class__(**merged)

    def to_schema(self) -> Sc:
        omitted_fields = [
            field_id for field_id, field_value in vars(self).items()
            if self._is_omitted(field_value)
        ]
        if omitted_fields:
            raise ValueError(
                f"Cannot create schema because overlay contains omitted values at {omitted_fields}",
            )
        # noinspection PyArgumentList
        return self._schema_cls(**vars(self))  # type: ignore[return-value]


@dataclass(frozen=True)
class OverlayRequest(LocatedRequest[Ov], Generic[Ov]):
    overlay_cls: type[Ov]


def provide_schema(overlay: type[Overlay[Sc]], mediator: Mediator, loc_stack: LocStack) -> Sc:
    stacked_overlay = mediator.mandatory_provide(
        OverlayRequest(
            loc_stack=loc_stack,
            overlay_cls=overlay,
        ),
    )
    if isinstance(loc_stack.last.type, type):
        for parent in loc_stack.last.type.mro()[1:]:
            try:
                new_overlay = mediator.delegating_provide(
                    OverlayRequest(
                        loc_stack=loc_stack.replace_last_type(parent),
                        overlay_cls=overlay,
                    ),
                )
            except CannotProvide:
                pass
            else:
                stacked_overlay = new_overlay.merge(stacked_overlay)

    return stacked_overlay.to_schema()


class OverlayProvider(MethodsProvider):
    def __init__(self, overlays: Iterable[Overlay], chain: Optional[Chain]):
        self._chain = chain
        self._overlays = ClassMap(*overlays)

    @method_handler
    def _provide_overlay(self, mediator: Mediator, request: OverlayRequest):
        try:
            overlay = self._overlays[request.overlay_cls]
        except KeyError:
            raise CannotProvide

        if self._chain is None:
            return overlay

        try:
            next_overlay = mediator.provide_from_next()
        except CannotProvide:
            return overlay

        if self._chain == Chain.FIRST:
            return next_overlay.merge(overlay)
        return overlay.merge(next_overlay)
