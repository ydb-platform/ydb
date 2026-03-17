"""All pytest-django fixtures"""

from __future__ import with_statement

import os
import warnings
from contextlib import contextmanager
from functools import partial

import pytest

from . import live_server_helper
from .django_compat import is_django_unittest
from .lazy_django import skip_if_no_django

__all__ = [
    "django_db_setup",
    "db",
    "transactional_db",
    "django_db_reset_sequences",
    "admin_user",
    "django_user_model",
    "django_username_field",
    "client",
    "admin_client",
    "rf",
    "settings",
    "live_server",
    "_live_server_helper",
    "django_assert_num_queries",
    "django_assert_max_num_queries",
]


@pytest.fixture(scope="session")
def django_db_modify_db_settings_tox_suffix():
    skip_if_no_django()

    tox_environment = os.getenv("TOX_PARALLEL_ENV")
    if tox_environment:
        # Put a suffix like _py27-django21 on tox workers
        _set_suffix_to_test_databases(suffix=tox_environment)


@pytest.fixture(scope="session")
def django_db_modify_db_settings_xdist_suffix(request):
    skip_if_no_django()

    xdist_suffix = getattr(request.config, "workerinput", {}).get("workerid")
    if xdist_suffix:
        # Put a suffix like _gw0, _gw1 etc on xdist processes
        _set_suffix_to_test_databases(suffix=xdist_suffix)


@pytest.fixture(scope="session")
def django_db_modify_db_settings_parallel_suffix(
    django_db_modify_db_settings_tox_suffix,
    django_db_modify_db_settings_xdist_suffix,
):
    skip_if_no_django()


@pytest.fixture(scope="session")
def django_db_modify_db_settings(django_db_modify_db_settings_parallel_suffix):
    skip_if_no_django()


@pytest.fixture(scope="session")
def django_db_use_migrations(request):
    return not request.config.getvalue("nomigrations")


@pytest.fixture(scope="session")
def django_db_keepdb(request):
    return request.config.getvalue("reuse_db")


@pytest.fixture(scope="session")
def django_db_createdb(request):
    return request.config.getvalue("create_db")


@pytest.fixture(scope="session")
def django_db_setup(
    request,
    django_test_environment,
    django_db_blocker,
    django_db_use_migrations,
    django_db_keepdb,
    django_db_createdb,
    django_db_modify_db_settings,
):
    """Top level fixture to ensure test databases are available"""
    from .compat import setup_databases, teardown_databases

    setup_databases_args = {}

    if not django_db_use_migrations:
        _disable_native_migrations()

    if django_db_keepdb and not django_db_createdb:
        setup_databases_args["keepdb"] = True

    with django_db_blocker.unblock():
        db_cfg = setup_databases(
            verbosity=request.config.option.verbose,
            interactive=False,
            **setup_databases_args
        )

    def teardown_database():
        with django_db_blocker.unblock():
            try:
                teardown_databases(db_cfg, verbosity=request.config.option.verbose)
            except Exception as exc:
                request.node.warn(
                    pytest.PytestWarning(
                        "Error when trying to teardown test databases: %r" % exc
                    )
                )

    if not django_db_keepdb:
        request.addfinalizer(teardown_database)


def _django_db_fixture_helper(
    request, django_db_blocker, transactional=False, reset_sequences=False
):
    if is_django_unittest(request):
        return

    if not transactional and "live_server" in request.fixturenames:
        # Do nothing, we get called with transactional=True, too.
        return

    django_db_blocker.unblock()
    request.addfinalizer(django_db_blocker.restore)

    if transactional:
        from django.test import TransactionTestCase as django_case

        if reset_sequences:

            class ResetSequenceTestCase(django_case):
                reset_sequences = True

            django_case = ResetSequenceTestCase
    else:
        from django.test import TestCase as django_case

    test_case = django_case(methodName="__init__")
    test_case._pre_setup()
    request.addfinalizer(test_case._post_teardown)


def _disable_native_migrations():
    from django.conf import settings
    from django.core.management.commands import migrate

    from .migrations import DisableMigrations

    settings.MIGRATION_MODULES = DisableMigrations()

    class MigrateSilentCommand(migrate.Command):
        def handle(self, *args, **kwargs):
            kwargs["verbosity"] = 0
            return super(MigrateSilentCommand, self).handle(*args, **kwargs)

    migrate.Command = MigrateSilentCommand


def _set_suffix_to_test_databases(suffix):
    from django.conf import settings

    for db_settings in settings.DATABASES.values():
        test_name = db_settings.get("TEST", {}).get("NAME")

        if not test_name:
            if db_settings["ENGINE"] == "django.db.backends.sqlite3":
                continue
            test_name = "test_{}".format(db_settings["NAME"])

        if test_name == ":memory:":
            continue

        db_settings.setdefault("TEST", {})
        db_settings["TEST"]["NAME"] = "{}_{}".format(test_name, suffix)


# ############### User visible fixtures ################


@pytest.fixture(scope="function")
def db(request, django_db_setup, django_db_blocker):
    """Require a django test database.

    This database will be setup with the default fixtures and will have
    the transaction management disabled. At the end of the test the outer
    transaction that wraps the test itself will be rolled back to undo any
    changes to the database (in case the backend supports transactions).
    This is more limited than the ``transactional_db`` resource but
    faster.

    If multiple database fixtures are requested, they take precedence
    over each other in the following order (the last one wins): ``db``,
    ``transactional_db``, ``django_db_reset_sequences``.
    """
    if "django_db_reset_sequences" in request.fixturenames:
        request.getfixturevalue("django_db_reset_sequences")
    if (
        "transactional_db" in request.fixturenames
        or "live_server" in request.fixturenames
    ):
        request.getfixturevalue("transactional_db")
    else:
        _django_db_fixture_helper(request, django_db_blocker, transactional=False)


@pytest.fixture(scope="function")
def transactional_db(request, django_db_setup, django_db_blocker):
    """Require a django test database with transaction support.

    This will re-initialise the django database for each test and is
    thus slower than the normal ``db`` fixture.

    If you want to use the database with transactions you must request
    this resource.

    If multiple database fixtures are requested, they take precedence
    over each other in the following order (the last one wins): ``db``,
    ``transactional_db``, ``django_db_reset_sequences``.
    """
    if "django_db_reset_sequences" in request.fixturenames:
        request.getfixturevalue("django_db_reset_sequences")
    _django_db_fixture_helper(request, django_db_blocker, transactional=True)


@pytest.fixture(scope="function")
def django_db_reset_sequences(request, django_db_setup, django_db_blocker):
    """Require a transactional test database with sequence reset support.

    This behaves like the ``transactional_db`` fixture, with the addition
    of enforcing a reset of all auto increment sequences.  If the enquiring
    test relies on such values (e.g. ids as primary keys), you should
    request this resource to ensure they are consistent across tests.

    If multiple database fixtures are requested, they take precedence
    over each other in the following order (the last one wins): ``db``,
    ``transactional_db``, ``django_db_reset_sequences``.
    """
    _django_db_fixture_helper(
        request, django_db_blocker, transactional=True, reset_sequences=True
    )


@pytest.fixture()
def client():
    """A Django test client instance."""
    skip_if_no_django()

    from django.test.client import Client

    return Client()


@pytest.fixture()
def django_user_model(db):
    """The class of Django's user model."""
    from django.contrib.auth import get_user_model

    return get_user_model()


@pytest.fixture()
def django_username_field(django_user_model):
    """The fieldname for the username used with Django's user model."""
    return django_user_model.USERNAME_FIELD


@pytest.fixture()
def admin_user(db, django_user_model, django_username_field):
    """A Django admin user.

    This uses an existing user with username "admin", or creates a new one with
    password "password".
    """
    UserModel = django_user_model
    username_field = django_username_field
    username = "admin@example.com" if username_field == "email" else "admin"

    try:
        user = UserModel._default_manager.get(**{username_field: username})
    except UserModel.DoesNotExist:
        extra_fields = {}
        if username_field not in ("username", "email"):
            extra_fields[username_field] = "admin"
        user = UserModel._default_manager.create_superuser(
            username, "admin@example.com", "password", **extra_fields
        )
    return user


@pytest.fixture()
def admin_client(db, admin_user):
    """A Django test client logged in as an admin user."""
    from django.test.client import Client

    client = Client()
    client.login(username=admin_user.username, password="password")
    return client


@pytest.fixture()
def rf():
    """RequestFactory instance"""
    skip_if_no_django()

    from django.test.client import RequestFactory

    return RequestFactory()


class SettingsWrapper(object):
    _to_restore = []

    def __delattr__(self, attr):
        from django.test import override_settings

        override = override_settings()
        override.enable()
        from django.conf import settings

        delattr(settings, attr)

        self._to_restore.append(override)

    def __setattr__(self, attr, value):
        from django.test import override_settings

        override = override_settings(**{attr: value})
        override.enable()
        self._to_restore.append(override)

    def __getattr__(self, item):
        from django.conf import settings

        return getattr(settings, item)

    def finalize(self):
        for override in reversed(self._to_restore):
            override.disable()

        del self._to_restore[:]


@pytest.yield_fixture()
def settings():
    """A Django settings object which restores changes after the testrun"""
    skip_if_no_django()

    wrapper = SettingsWrapper()
    yield wrapper
    wrapper.finalize()


@pytest.fixture(scope="session")
def live_server(request):
    """Run a live Django server in the background during tests

    The address the server is started from is taken from the
    --liveserver command line option or if this is not provided from
    the DJANGO_LIVE_TEST_SERVER_ADDRESS environment variable.  If
    neither is provided ``localhost:8081,8100-8200`` is used.  See the
    Django documentation for its full syntax.

    NOTE: If the live server needs database access to handle a request
          your test will have to request database access.  Furthermore
          when the tests want to see data added by the live-server (or
          the other way around) transactional database access will be
          needed as data inside a transaction is not shared between
          the live server and test code.

          Static assets will be automatically served when
          ``django.contrib.staticfiles`` is available in INSTALLED_APPS.
    """
    skip_if_no_django()

    import django

    addr = request.config.getvalue("liveserver") or os.getenv(
        "DJANGO_LIVE_TEST_SERVER_ADDRESS"
    )

    if addr and ":" in addr:
        if django.VERSION >= (1, 11):
            ports = addr.split(":")[1]
            if "-" in ports or "," in ports:
                warnings.warn(
                    "Specifying multiple live server ports is not supported "
                    "in Django 1.11. This will be an error in a future "
                    "pytest-django release."
                )

    if not addr:
        if django.VERSION < (1, 11):
            addr = "localhost:8081,8100-8200"
        else:
            addr = "localhost"

    server = live_server_helper.LiveServer(addr)
    request.addfinalizer(server.stop)
    return server


@pytest.fixture(autouse=True, scope="function")
def _live_server_helper(request):
    """Helper to make live_server work, internal to pytest-django.

    This helper will dynamically request the transactional_db fixture
    for a test which uses the live_server fixture.  This allows the
    server and test to access the database without having to mark
    this explicitly which is handy since it is usually required and
    matches the Django behaviour.

    The separate helper is required since live_server can not request
    transactional_db directly since it is session scoped instead of
    function-scoped.

    It will also override settings only for the duration of the test.
    """
    if "live_server" not in request.fixturenames:
        return

    request.getfixturevalue("transactional_db")

    live_server = request.getfixturevalue("live_server")
    live_server._live_server_modified_settings.enable()
    request.addfinalizer(live_server._live_server_modified_settings.disable)


@contextmanager
def _assert_num_queries(config, num, exact=True, connection=None, info=None):
    from django.test.utils import CaptureQueriesContext

    if connection is None:
        from django.db import connection

    verbose = config.getoption("verbose") > 0
    with CaptureQueriesContext(connection) as context:
        yield context
        num_performed = len(context)
        if exact:
            failed = num != num_performed
        else:
            failed = num_performed > num
        if failed:
            msg = "Expected to perform {} queries {}{}".format(
                num,
                "" if exact else "or less ",
                "but {} done".format(
                    num_performed == 1 and "1 was" or "%d were" % (num_performed,)
                ),
            )
            if info:
                msg += "\n{}".format(info)
            if verbose:
                sqls = (q["sql"] for q in context.captured_queries)
                msg += "\n\nQueries:\n========\n\n%s" % "\n\n".join(sqls)
            else:
                msg += " (add -v option to show queries)"
            pytest.fail(msg)


@pytest.fixture(scope="function")
def django_assert_num_queries(pytestconfig):
    return partial(_assert_num_queries, pytestconfig)


@pytest.fixture(scope="function")
def django_assert_max_num_queries(pytestconfig):
    return partial(_assert_num_queries, pytestconfig, exact=False)
