import argparse
from dataclasses import dataclass
from functools import cached_property
from importlib import import_module
from types import ModuleType

import pytest

C_EXT_MARK = pytest.mark.c_extension


@dataclass(frozen=True)
class PropcacheImplementation:
    """A facade for accessing importable propcache module variants.

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
        """Return a loaded importable containing a propcache variant."""
        importable_module = "_helpers_py" if self.is_pure_python else "_helpers_c"
        return import_module(f"propcache.{importable_module}")

    def __str__(self) -> str:
        """Render the implementation facade instance as a string."""
        return f"{self.tag}-module"


@pytest.fixture(
    scope="session",
    params=(
        pytest.param(
            PropcacheImplementation(is_pure_python=False),
            marks=C_EXT_MARK,
        ),
        PropcacheImplementation(is_pure_python=True),
    ),
    ids=str,
)
def propcache_implementation(request: pytest.FixtureRequest) -> PropcacheImplementation:
    """Return a propcache variant facade."""
    return request.param  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def propcache_module(
    propcache_implementation: PropcacheImplementation,
) -> ModuleType:
    """Return a pre-imported module containing a propcache variant."""
    return propcache_implementation.imported_module


def pytest_addoption(
    parser: pytest.Parser,
    pluginmanager: pytest.PytestPluginManager,
) -> None:
    """Define a new ``--c-extensions`` flag.

    This lets the callers deselect tests executed against the C-extension
    version of the ``propcache`` implementation.
    """
    del pluginmanager
    parser.addoption(
        "--c-extensions",  # disabled with `--no-c-extensions`
        action=argparse.BooleanOptionalAction,
        default=True,
        dest="c_extensions",
        help="Test C-extensions (on by default)",
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

    selected_tests: list[pytest.Item] = []
    deselected_tests: list[pytest.Item] = []

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
