from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar, overload

import pydantic
from packaging import version
from pydantic import BaseModel as _BaseModel

if TYPE_CHECKING:
    from pathlib import Path

PYDANTIC_VERSION = version.parse(pydantic.VERSION if isinstance(pydantic.VERSION, str) else str(pydantic.VERSION))

PYDANTIC_V2: bool = version.parse("2.0b3") <= PYDANTIC_VERSION

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:  # pragma: no cover
    from yaml import SafeLoader

try:
    from tomllib import load as load_tomllib  # type: ignore[ignoreMissingImports]
except ImportError:
    from tomli import load as load_tomllib  # type: ignore[ignoreMissingImports]


def load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        return load_tomllib(f)


SafeLoaderTemp = copy.deepcopy(SafeLoader)
SafeLoaderTemp.yaml_constructors = copy.deepcopy(SafeLoader.yaml_constructors)
SafeLoaderTemp.add_constructor(
    "tag:yaml.org,2002:timestamp",
    SafeLoaderTemp.yaml_constructors["tag:yaml.org,2002:str"],
)
SafeLoader = SafeLoaderTemp

Model = TypeVar("Model", bound=_BaseModel)
T = TypeVar("T")


@overload
def model_validator(
    mode: Literal["before"],
) -> (
    Callable[[Callable[[type[Model], T], T]], Callable[[type[Model], T], T]]
    | Callable[[Callable[[Model, T], T]], Callable[[Model, T], T]]
): ...


@overload
def model_validator(
    mode: Literal["after"],
) -> (
    Callable[[Callable[[type[Model], T], T]], Callable[[type[Model], T], T]]
    | Callable[[Callable[[Model, T], T]], Callable[[Model, T], T]]
    | Callable[[Callable[[Model], Model]], Callable[[Model], Model]]
): ...


@overload
def model_validator() -> (
    Callable[[Callable[[type[Model], T], T]], Callable[[type[Model], T], T]]
    | Callable[[Callable[[Model, T], T]], Callable[[Model, T], T]]
    | Callable[[Callable[[Model], Model]], Callable[[Model], Model]]
): ...


def model_validator(  # pyright: ignore[reportInconsistentOverload]
    mode: Literal["before", "after"] = "after",
) -> (
    Callable[[Callable[[type[Model], T], T]], Callable[[type[Model], T], T]]
    | Callable[[Callable[[Model, T], T]], Callable[[Model, T], T]]
    | Callable[[Callable[[Model], Model]], Callable[[Model], Model]]
):
    """
    Decorator for model validators in Pydantic models.

    Uses `model_validator` in Pydantic v2 and `root_validator` in Pydantic v1.

    We support only `before` mode because `after` mode needs different validator
    implementation for v1 and v2.
    """

    @overload
    def inner(method: Callable[[type[Model], T], T]) -> Callable[[type[Model], T], T]: ...

    @overload
    def inner(method: Callable[[Model, T], T]) -> Callable[[Model, T], T]: ...

    @overload
    def inner(method: Callable[[Model], Model]) -> Callable[[Model], Model]: ...

    def inner(
        method: Callable[[type[Model], T], T] | Callable[[Model, T], T] | Callable[[Model], Model],
    ) -> Callable[[type[Model], T], T] | Callable[[Model, T], T] | Callable[[Model], Model]:
        if PYDANTIC_V2:
            from pydantic import model_validator as model_validator_v2  # noqa: PLC0415

            if method == "before":
                return model_validator_v2(mode=mode)(classmethod(method))  # type: ignore[reportReturnType]
            return model_validator_v2(mode=mode)(method)  # type: ignore[reportReturnType]
        from pydantic import root_validator  # noqa: PLC0415

        return root_validator(method, pre=mode == "before")  # pyright: ignore[reportCallIssue]

    return inner


def field_validator(
    field_name: str,
    *fields: str,
    mode: Literal["before", "after"] = "after",
) -> Callable[[Any], Callable[[BaseModel, Any], Any]]:
    def inner(method: Callable[[Model, Any], Any]) -> Callable[[Model, Any], Any]:
        if PYDANTIC_V2:
            from pydantic import field_validator as field_validator_v2  # noqa: PLC0415

            return field_validator_v2(field_name, *fields, mode=mode)(method)
        from pydantic import validator  # noqa: PLC0415

        return validator(field_name, *fields, pre=mode == "before")(method)  # pyright: ignore[reportReturnType]

    return inner


if PYDANTIC_V2:
    from pydantic import ConfigDict
else:
    ConfigDict = dict


class BaseModel(_BaseModel):
    if PYDANTIC_V2:
        model_config = ConfigDict(strict=False)  # pyright: ignore[reportAssignmentType]
