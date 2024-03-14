from __future__ import annotations

import argparse
import pickle
from dataclasses import dataclass
from importlib import import_module
from sys import version_info as _version_info
from types import ModuleType
from typing import Callable, Type

try:
    from functools import cached_property  # Python 3.8+
except ImportError:
    from functools import lru_cache as _lru_cache

    def cached_property(func):
        return property(_lru_cache()(func))


import pytest

from multidict import MultiMapping, MutableMultiMapping

C_EXT_MARK = pytest.mark.c_extension
PY_38_AND_BELOW = _version_info < (3, 9)


@dataclass(frozen=True)
class MultidictImplementation:
    """A facade for accessing importable multidict module variants.

    An instance essentially represents a c-extension or a pure-python module.
    The actual underlying module is accessed dynamically through a property and
    is cached.

    It also has a text tag depending on what variant it is, and a string
    representation suitable for use in Pytest's test IDs via parametrization.
    """

    is_pure_python: bool
    """A flag showing whether this is a pure-python module or a C-extension."""

    @cached_property
    def tag(self) -> str:
        """Return a text representation of the pure-python attribute."""
        return "pure-python" if self.is_pure_python else "c-extension"

    @cached_property
    def imported_module(self) -> ModuleType:
        """Return a loaded importable containing a multidict variant."""
        importable_module = "_multidict_py" if self.is_pure_python else "_multidict"
        return import_module(f"multidict.{importable_module}")

    def __str__(self):
        """Render the implementation facade instance as a string."""
        return f"{self.tag}-module"


@pytest.fixture(
    scope="session",
    params=(
        pytest.param(
            MultidictImplementation(is_pure_python=False),
            marks=C_EXT_MARK,
        ),
        MultidictImplementation(is_pure_python=True),
    ),
    ids=str,
)
def multidict_implementation(request: pytest.FixtureRequest) -> MultidictImplementation:
    """Return a multidict variant facade."""
    return request.param


@pytest.fixture(scope="session")
def multidict_module(
    multidict_implementation: MultidictImplementation,
) -> ModuleType:
    """Return a pre-imported module containing a multidict variant."""
    return multidict_implementation.imported_module


@pytest.fixture(
    scope="session",
    params=("MultiDict", "CIMultiDict"),
    ids=("case-sensitive", "case-insensitive"),
)
def any_multidict_class_name(request: pytest.FixtureRequest) -> str:
    """Return a class name of a mutable multidict implementation."""
    return request.param


@pytest.fixture(scope="session")
def any_multidict_class(
    any_multidict_class_name: str,
    multidict_module: ModuleType,
) -> Type[MutableMultiMapping[str]]:
    """Return a class object of a mutable multidict implementation."""
    return getattr(multidict_module, any_multidict_class_name)


@pytest.fixture(scope="session")
def case_sensitive_multidict_class(
    multidict_module: ModuleType,
) -> Type[MutableMultiMapping[str]]:
    """Return a case-sensitive mutable multidict class."""
    return multidict_module.MultiDict


@pytest.fixture(scope="session")
def case_insensitive_multidict_class(
    multidict_module: ModuleType,
) -> Type[MutableMultiMapping[str]]:
    """Return a case-insensitive mutable multidict class."""
    return multidict_module.CIMultiDict


@pytest.fixture(scope="session")
def case_insensitive_str_class(multidict_module: ModuleType) -> Type[str]:
    """Return a case-insensitive string class."""
    return multidict_module.istr


@pytest.fixture(scope="session")
def any_multidict_proxy_class_name(any_multidict_class_name: str) -> str:
    """Return a class name of an immutable multidict implementation."""
    return f"{any_multidict_class_name}Proxy"


@pytest.fixture(scope="session")
def any_multidict_proxy_class(
    any_multidict_proxy_class_name: str,
    multidict_module: ModuleType,
) -> Type[MultiMapping[str]]:
    """Return an immutable multidict implementation class object."""
    return getattr(multidict_module, any_multidict_proxy_class_name)


@pytest.fixture(scope="session")
def case_sensitive_multidict_proxy_class(
    multidict_module: ModuleType,
) -> Type[MutableMultiMapping[str]]:
    """Return a case-sensitive immutable multidict class."""
    return multidict_module.MultiDictProxy


@pytest.fixture(scope="session")
def case_insensitive_multidict_proxy_class(
    multidict_module: ModuleType,
) -> Type[MutableMultiMapping[str]]:
    """Return a case-insensitive immutable multidict class."""
    return multidict_module.CIMultiDictProxy


@pytest.fixture(scope="session")
def multidict_getversion_callable(multidict_module: ModuleType) -> Callable:
    """Return a ``getversion()`` function for current implementation."""
    return multidict_module.getversion


def pytest_addoption(
    parser: pytest.Parser,
    pluginmanager: pytest.PytestPluginManager,
) -> None:
    """Define a new ``--c-extensions`` flag.

    This lets the callers deselect tests executed against the C-extension
    version of the ``multidict`` implementation.
    """
    del pluginmanager

    parser.addoption(
        "--c-extensions",  # disabled with `--no-c-extensions`
        action="store_true" if PY_38_AND_BELOW else argparse.BooleanOptionalAction,
        default=True,
        dest="c_extensions",
        help="Test C-extensions (on by default)",
    )

    if PY_38_AND_BELOW:
        parser.addoption(
            "--no-c-extensions",
            action="store_false",
            dest="c_extensions",
            help="Skip testing C-extensions (on by default)",
        )


def pytest_collection_modifyitems(
    session: pytest.Session,
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Deselect tests against C-extensions when requested via CLI."""
    test_c_extensions = config.getoption("--c-extensions") is True

    if test_c_extensions:
        return

    selected_tests = []
    deselected_tests = []

    for item in items:
        c_ext = item.get_closest_marker(C_EXT_MARK.name) is not None

        target_items_list = deselected_tests if c_ext else selected_tests
        target_items_list.append(item)

    config.hook.pytest_deselected(items=deselected_tests)
    items[:] = selected_tests


def pytest_configure(config: pytest.Config) -> None:
    """Declare the C-extension marker in config."""
    config.addinivalue_line(
        "markers",
        f"{C_EXT_MARK.name}: tests running against the C-extension implementation.",
    )


def pytest_generate_tests(metafunc):
    if "pickle_protocol" in metafunc.fixturenames:
        metafunc.parametrize(
            "pickle_protocol", list(range(pickle.HIGHEST_PROTOCOL + 1)), scope="session"
        )
