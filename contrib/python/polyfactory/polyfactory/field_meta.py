from __future__ import annotations

import functools
from collections.abc import Hashable, Mapping
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

from typing_extensions import get_args, get_origin

from polyfactory.constants import TYPE_MAPPING
from polyfactory.utils.helpers import (
    get_annotation_metadata,
    is_dataclass_instance,
    unwrap_annotated,
    unwrap_new_type,
)
from polyfactory.utils.normalize_type import normalize_type
from polyfactory.utils.predicates import is_annotated
from polyfactory.utils.types import NoneType

if TYPE_CHECKING:
    import datetime
    from collections.abc import Sequence
    from decimal import Decimal
    from random import Random
    from re import Pattern

    from typing_extensions import NotRequired, Self


class Null:
    """Sentinel class for empty values"""


class UrlConstraints(TypedDict):
    max_length: NotRequired[int]
    allowed_schemes: NotRequired[list[str]]
    host_required: NotRequired[bool]
    default_host: NotRequired[str]
    default_port: NotRequired[int]
    default_path: NotRequired[str]


class Constraints(TypedDict):
    """Metadata regarding a type constraints, if any"""

    allow_inf_nan: NotRequired[bool]
    decimal_places: NotRequired[int]
    ge: NotRequired[int | float | Decimal]
    gt: NotRequired[int | float | Decimal]
    item_type: NotRequired[Any]
    le: NotRequired[int | float | Decimal]
    lower_case: NotRequired[bool]
    lt: NotRequired[int | float | Decimal]
    max_digits: NotRequired[int]
    max_length: NotRequired[int]
    min_length: NotRequired[int]
    multiple_of: NotRequired[int | float | Decimal]
    path_type: NotRequired[Literal["file", "dir", "new"]]
    pattern: NotRequired[str | Pattern]
    tz: NotRequired[datetime.tzinfo]
    unique_items: NotRequired[bool]
    upper_case: NotRequired[bool]
    url: NotRequired[UrlConstraints]
    uuid_version: NotRequired[Literal[1, 3, 4, 5]]


class FieldMeta:
    """Factory field metadata container. This class is used to store the data about a field of a factory's model."""

    __slots__ = ("__dict__", "annotation", "children", "constraints", "default", "name", "random")

    annotation: Any
    random: Random
    children: list[FieldMeta] | None
    default: Any
    name: str
    constraints: Constraints | None

    def __init__(
        self,
        *,
        name: str,
        annotation: type,
        default: Any = Null,
        children: list[FieldMeta] | None = None,
        constraints: Constraints | None = None,
        required: bool = True,
    ) -> None:
        """Create a factory field metadata instance."""
        self.annotation = annotation
        self.children = children
        self.default = default
        self.name = name
        self.constraints = constraints
        self.required = required

    def __repr__(self) -> str:
        """Return a string representation of the field meta."""
        return f"FieldMeta(name={self.name!r}, annotation={self.annotation!r}, default={self.default!r}, children={self.children!r}, constraints={self.constraints!r})"

    @functools.cached_property
    def type_args(self) -> tuple[Any, ...]:
        """Return the normalized type args of the annotation, if any.

        :returns: a tuple of types.
        """
        annotation = self.annotation
        if is_annotated(annotation):
            annotation = get_args(self.annotation)[0]
        return tuple(
            TYPE_MAPPING.get(arg, arg) if isinstance(arg, Hashable) else arg  # type: ignore[call-overload]
            for arg in get_args(annotation)
        )

    @classmethod
    def from_type(
        cls,
        annotation: Any,
        name: str = "",
        default: Any = Null,
        constraints: Constraints | None = None,
        children: list[FieldMeta] | None = None,
        required: bool = True,
    ) -> Self:
        """Builder method to create a FieldMeta from a type annotation.

        :param annotation: A type annotation.
        :param name: Field name
        :param default: Default value, if any.
        :param constraints: A dictionary of constraints, if any.

        :returns: A field meta instance.
        """
        annotation = normalize_type(annotation)
        annotated = is_annotated(annotation)
        if not constraints and annotated:
            metadata = cls.get_constraints_metadata(annotation)
            constraints = cls.parse_constraints(metadata)

        # annotations can take many forms: Optional, an Annotated type, or anything with __args__
        # in order to normalize the annotation, we need to unwrap the annotation.
        if not annotated and (origin := get_origin(annotation)) and origin in TYPE_MAPPING:
            container = TYPE_MAPPING[origin]
            annotation = container[get_args(annotation)]  # type: ignore[index]

        field = cls(
            annotation=annotation,
            name=name,
            default=default,
            children=children,
            constraints=constraints,
            required=required,
        )

        if field.type_args and not field.children:
            field.children = [
                cls.from_type(annotation=unwrap_new_type(arg)) for arg in field.type_args if arg is not NoneType
            ]
        return field

    @classmethod
    def parse_constraints(cls, metadata: Sequence[Any]) -> "Constraints":
        constraints = {}

        for value in metadata:
            if is_annotated(value):
                _, inner_metadata = unwrap_annotated(value)
                constraints.update(cast("dict[str, Any]", cls.parse_constraints(metadata=inner_metadata)))
            elif func := getattr(value, "func", None):
                if func is str.islower:
                    constraints["lower_case"] = True
                elif func is str.isupper:
                    constraints["upper_case"] = True
                elif func is str.isascii:
                    constraints["pattern"] = "[[:ascii:]]"
                elif func is str.isdigit:
                    constraints["pattern"] = "[[:digit:]]"
            elif is_dataclass_instance(value) and (value_dict := asdict(value)) and ("allowed_schemes" in value_dict):
                constraints["url"] = {k: v for k, v in value_dict.items() if v is not None}
            # This is to support `Constraints`, but we can't do a isinstance with `Constraints` since isinstance
            # checks with `TypedDict` is not supported.
            elif isinstance(value, Mapping):
                constraints.update(value)
            else:
                constraints.update(
                    {
                        k: v
                        for k, v in {
                            "allow_inf_nan": getattr(value, "allow_inf_nan", None),
                            "decimal_places": getattr(value, "decimal_places", None),
                            "ge": getattr(value, "ge", None),
                            "gt": getattr(value, "gt", None),
                            "item_type": getattr(value, "item_type", None),
                            "le": getattr(value, "le", None),
                            "lower_case": getattr(value, "to_lower", None),
                            "lt": getattr(value, "lt", None),
                            "max_digits": getattr(value, "max_digits", None),
                            "max_length": getattr(value, "max_length", getattr(value, "max_length", None)),
                            "min_length": getattr(value, "min_length", getattr(value, "min_items", None)),
                            "multiple_of": getattr(value, "multiple_of", None),
                            "path_type": getattr(value, "path_type", None),
                            "pattern": getattr(value, "regex", getattr(value, "pattern", None)),
                            "tz": getattr(value, "tz", None),
                            "unique_items": getattr(value, "unique_items", None),
                            "upper_case": getattr(value, "to_upper", None),
                            "uuid_version": getattr(value, "uuid_version", None),
                        }.items()
                        if v is not None
                    },
                )
        return cast("Constraints", constraints)

    @classmethod
    def get_constraints_metadata(cls, annotation: Any) -> Sequence[Any]:
        """Get the metadatas of the constraints from the given annotation.

        :param annotation: A type annotation.
        :param random: An instance of random.Random.

        :returns: A list of the metadata in the annotation.
        """

        return get_annotation_metadata(annotation)
