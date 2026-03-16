from __future__ import annotations

import inspect
import re
from typing import (
    Any,
    Callable,
    Literal,
    TypeVar,
    Union,
    overload,
)

import pytest

from polyfactory.exceptions import ParameterException
from polyfactory.factories.base import BaseFactory
from polyfactory.utils.predicates import is_safe_subclass

Scope = Union[
    Literal["session", "package", "module", "class", "function"],
    Callable[[str, pytest.Config], Literal["session", "package", "module", "class", "function"]],
]
T = TypeVar("T", bound=BaseFactory[Any])

split_pattern_1 = re.compile(r"([A-Z]+)([A-Z][a-z])")
split_pattern_2 = re.compile(r"([a-z\d])([A-Z])")


def _get_fixture_name(name: str) -> str:
    """From inflection.underscore.

    :param name: str: A name.

    :returns: Normalized fixture name.

    """
    name = re.sub(split_pattern_1, r"\1_\2", name)
    name = re.sub(split_pattern_2, r"\1_\2", name)
    name = name.replace("-", "_")
    return name.lower()


class FactoryFixture:
    """Decorator that creates a pytest fixture from a factory"""

    __slots__ = ("autouse", "name", "scope")

    def __init__(
        self,
        scope: Scope = "function",
        autouse: bool = False,
        name: str | None = None,
    ) -> None:
        """Create a factory fixture decorator

        :param scope: Fixture scope
        :param autouse: Autouse the fixture
        :param name: Fixture name
        """
        self.scope = scope
        self.autouse = autouse
        self.name = name

    def __call__(self, factory: type[T], depth: int = 1) -> type[T]:
        if not is_safe_subclass(factory, BaseFactory):
            msg = f"{factory.__name__} is not a BaseFactory subclass."
            raise ParameterException(msg)

        fixture_name = self.name or _get_fixture_name(factory.__name__)
        fixture_register = pytest.fixture(
            scope=self.scope,  # pyright: ignore[reportArgumentType]
            name=fixture_name,
            autouse=self.autouse,
        )

        def _factory_fixture() -> type[T]:
            """The wrapped factory"""
            return factory

        caller_globals = inspect.stack()[depth][0].f_globals
        caller_globals[fixture_name] = fixture_register(_factory_fixture)

        return factory


@overload
def register_fixture(
    factory: None = None,
    *,
    scope: Scope = "function",
    autouse: bool = False,
    name: str | None = None,
) -> FactoryFixture: ...


@overload
def register_fixture(
    factory: type[T],
    *,
    scope: Scope = "function",
    autouse: bool = False,
    name: str | None = None,
) -> type[T]: ...


def register_fixture(
    factory: type[T] | None = None,
    *,
    scope: Scope = "function",
    autouse: bool = False,
    name: str | None = None,
) -> type[T] | FactoryFixture:
    """A decorator that allows registering model factories as fixtures.

    :param factory: An optional factory class to decorate.
    :param scope: Pytest scope.
    :param autouse: Auto use fixture.
    :param name: Fixture name.

    :returns: A fixture factory instance.
    """
    factory_fixture = FactoryFixture(scope=scope, autouse=autouse, name=name)
    return factory_fixture(factory, depth=2) if factory else factory_fixture
