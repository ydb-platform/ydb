from typing import TYPE_CHECKING, Protocol

import pytest
from django.db import DEFAULT_DB_ALIAS

from django_test_migrations.constants import MIGRATION_TEST_MARKER

if TYPE_CHECKING:
    from django_test_migrations.migrator import Migrator


def pytest_load_initial_conftests(early_config: pytest.Config) -> None:
    """Register pytest's markers."""
    early_config.addinivalue_line(
        'markers',
        f"{MIGRATION_TEST_MARKER}: mark the test as a Django's migration test.",
    )


def pytest_collection_modifyitems(
    session: pytest.Session,
    items: list[pytest.Item],  # noqa: WPS110
) -> None:
    """
    Mark all tests using ``migrator_factory`` fixture with proper marks.

    Add ``MIGRATION_TEST_MARKER`` marker to all items using
    ``migrator_factory`` fixture.

    """
    for pytest_item in items:
        if 'migrator_factory' in getattr(pytest_item, 'fixturenames', []):
            pytest_item.add_marker(MIGRATION_TEST_MARKER)


class MigratorFactory(Protocol):
    """Protocol for `migrator_factory` fixture."""

    def __call__(self, database_name: str | None = None) -> 'Migrator':
        """It only has a `__call__` magic method."""


@pytest.fixture
def migrator_factory(
    request: pytest.FixtureRequest,
    transactional_db: None,
    django_db_use_migrations: bool,  # noqa: FBT001
) -> MigratorFactory:
    """
    Pytest fixture to create migrators inside the pytest tests.

    How? Here's an example.

    .. code:: python

        @pytest.mark.django_db
        def test_migration(migrator_factory):
            migrator = migrator_factory('custom_db_alias')
            old_state = migrator.apply_initial_migration(('main_app', None))
            new_state = migrator.apply_tested_migration(
                ('main_app', '0001_initial'),
            )

            assert isinstance(old_state, ProjectState)
            assert isinstance(new_state, ProjectState)

    Why do we import :class:`Migrator` inside the fixture function?
    Otherwise, coverage won't work correctly during our internal tests.
    Why? Because modules in Python are singletons.
    Once imported, they will be stored in memory and reused.

    That's why we cannot import ``Migrator`` on a module level.
    Because it won't be caught be coverage later on.
    """
    from django_test_migrations.migrator import Migrator  # noqa: PLC0415

    if not django_db_use_migrations:
        pytest.skip('--nomigrations was specified')

    def factory(database_name: str | None = None) -> Migrator:
        migrator = Migrator(database_name)
        request.addfinalizer(migrator.reset)
        return migrator

    return factory


@pytest.fixture
def migrator(migrator_factory: MigratorFactory) -> 'Migrator':
    """
    Useful alias for ``'default'`` database in ``django``.

    That's a predefined instance of a ``migrator_factory``.

    How to use it? Here's an example.

    .. code:: python

        @pytest.mark.django_db
        def test_migration(migrator):
            old_state = migrator.apply_initial_migration(('main_app', None))
            new_state = migrator.apply_tested_migration(
                ('main_app', '0001_initial'),
            )

            assert isinstance(old_state, ProjectState)
            assert isinstance(new_state, ProjectState)

    Just one step easier than ``migrator_factory`` fixture.
    """
    return migrator_factory(DEFAULT_DB_ALIAS)
