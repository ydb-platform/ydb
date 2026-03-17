"""A pytest plugin which helps testing Django applications

This plugin handles creating and destroying the test environment and
test database and provides some useful text fixtures.
"""

import contextlib
import inspect
from functools import reduce
import os
import sys
import types

import pytest

from .django_compat import is_django_unittest  # noqa
from .fixtures import django_assert_num_queries  # noqa
from .fixtures import django_assert_max_num_queries  # noqa
from .fixtures import django_db_setup  # noqa
from .fixtures import django_db_use_migrations  # noqa
from .fixtures import django_db_keepdb  # noqa
from .fixtures import django_db_createdb  # noqa
from .fixtures import django_db_modify_db_settings  # noqa
from .fixtures import django_db_modify_db_settings_parallel_suffix  # noqa
from .fixtures import django_db_modify_db_settings_tox_suffix  # noqa
from .fixtures import django_db_modify_db_settings_xdist_suffix  # noqa
from .fixtures import _live_server_helper  # noqa
from .fixtures import admin_client  # noqa
from .fixtures import admin_user  # noqa
from .fixtures import client  # noqa
from .fixtures import db  # noqa
from .fixtures import django_user_model  # noqa
from .fixtures import django_username_field  # noqa
from .fixtures import live_server  # noqa
from .fixtures import django_db_reset_sequences  # noqa
from .fixtures import rf  # noqa
from .fixtures import settings  # noqa
from .fixtures import transactional_db  # noqa

from .lazy_django import django_settings_is_configured, skip_if_no_django

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib


SETTINGS_MODULE_ENV = "DJANGO_SETTINGS_MODULE"
CONFIGURATION_ENV = "DJANGO_CONFIGURATION"
INVALID_TEMPLATE_VARS_ENV = "FAIL_INVALID_TEMPLATE_VARS"

PY2 = sys.version_info[0] == 2

# pytest 4.2 handles unittest setup/teardown itself via wrapping fixtures.
_pytest_version_info = tuple(int(x) for x in pytest.__version__.split(".", 2)[:2])
_handle_unittest_methods = _pytest_version_info < (4, 2)

_report_header = []


# ############### pytest hooks ################


def pytest_addoption(parser):
    group = parser.getgroup("django")
    group.addoption(
        "--reuse-db",
        action="store_true",
        dest="reuse_db",
        default=False,
        help="Re-use the testing database if it already exists, "
        "and do not remove it when the test finishes.",
    )
    group.addoption(
        "--create-db",
        action="store_true",
        dest="create_db",
        default=False,
        help="Re-create the database, even if it exists. This "
        "option can be used to override --reuse-db.",
    )
    group.addoption(
        "--ds",
        action="store",
        type=str,
        dest="ds",
        default=None,
        help="Set DJANGO_SETTINGS_MODULE.",
    )
    group.addoption(
        "--dc",
        action="store",
        type=str,
        dest="dc",
        default=None,
        help="Set DJANGO_CONFIGURATION.",
    )
    group.addoption(
        "--nomigrations",
        "--no-migrations",
        action="store_true",
        dest="nomigrations",
        default=False,
        help="Disable Django migrations on test setup",
    )
    group.addoption(
        "--migrations",
        action="store_false",
        dest="nomigrations",
        default=False,
        help="Enable Django migrations on test setup",
    )
    parser.addini(
        CONFIGURATION_ENV, "django-configurations class to use by pytest-django."
    )
    group.addoption(
        "--liveserver",
        default=None,
        help="Address and port for the live_server fixture.",
    )
    parser.addini(
        SETTINGS_MODULE_ENV, "Django settings module to use by pytest-django."
    )

    parser.addini(
        "django_find_project",
        "Automatically find and add a Django project to the " "Python path.",
        type="bool",
        default=True,
    )
    group.addoption(
        "--fail-on-template-vars",
        action="store_true",
        dest="itv",
        default=False,
        help="Fail for invalid variables in templates.",
    )
    parser.addini(
        INVALID_TEMPLATE_VARS_ENV,
        "Fail for invalid variables in templates.",
        type="bool",
        default=False,
    )


PROJECT_FOUND = (
    "pytest-django found a Django project in %s "
    "(it contains manage.py) and added it to the Python path.\n"
    'If this is wrong, add "django_find_project = false" to '
    "pytest.ini and explicitly manage your Python path."
)

PROJECT_NOT_FOUND = (
    "pytest-django could not find a Django project "
    "(no manage.py file could be found). You must "
    "explicitly add your Django project to the Python path "
    "to have it picked up."
)

PROJECT_SCAN_DISABLED = (
    "pytest-django did not search for Django "
    "projects since it is disabled in the configuration "
    '("django_find_project = false")'
)


@contextlib.contextmanager
def _handle_import_error(extra_message):
    try:
        yield
    except ImportError as e:
        django_msg = (e.args[0] + "\n\n") if e.args else ""
        msg = django_msg + extra_message
        raise ImportError(msg)


def _add_django_project_to_path(args):
    def is_django_project(path):
        try:
            return path.is_dir() and (path / "manage.py").exists()
        except OSError:
            return False

    def arg_to_path(arg):
        # Test classes or functions can be appended to paths separated by ::
        arg = arg.split("::", 1)[0]
        return pathlib.Path(arg)

    def find_django_path(args):
        args = map(str, args)
        args = [arg_to_path(x) for x in args if not x.startswith("-")]

        cwd = pathlib.Path.cwd()
        if not args:
            args.append(cwd)
        elif cwd not in args:
            args.append(cwd)

        for arg in args:
            if is_django_project(arg):
                return arg
            for parent in arg.parents:
                if is_django_project(parent):
                    return parent
        return None

    project_dir = find_django_path(args)
    if project_dir:
        sys.path.insert(0, str(project_dir.absolute()))
        return PROJECT_FOUND % project_dir
    return PROJECT_NOT_FOUND


def _setup_django():
    if "django" not in sys.modules:
        return

    import django.conf

    # Avoid force-loading Django when settings are not properly configured.
    if not django.conf.settings.configured:
        return

    import django.apps

    if not django.apps.apps.ready:
        django.setup()

    _blocking_manager.block()


def _get_boolean_value(x, name, default=None):
    if x is None:
        return default
    if x in (True, False):
        return x
    possible_values = {"true": True, "false": False, "1": True, "0": False}
    try:
        return possible_values[x.lower()]
    except KeyError:
        raise ValueError(
            "{} is not a valid value for {}. "
            "It must be one of {}.".format(x, name, ", ".join(possible_values.keys()))
        )


def pytest_load_initial_conftests(early_config, parser, args):
    # Register the marks
    early_config.addinivalue_line(
        "markers",
        "django_db(transaction=False): Mark the test as using "
        "the Django test database.  The *transaction* argument marks will "
        "allow you to use real transactions in the test like Django's "
        "TransactionTestCase.",
    )
    early_config.addinivalue_line(
        "markers",
        "urls(modstr): Use a different URLconf for this test, similar to "
        "the `urls` attribute of Django's `TestCase` objects.  *modstr* is "
        "a string specifying the module of a URL config, e.g. "
        '"my_app.test_urls".',
    )
    early_config.addinivalue_line(
        "markers",
        "ignore_template_errors(): ignore errors from invalid template "
        "variables (if --fail-on-template-vars is used).",
    )

    options = parser.parse_known_args(args)

    if options.version or options.help:
        return

    django_find_project = _get_boolean_value(
        early_config.getini("django_find_project"), "django_find_project"
    )

    if django_find_project:
        _django_project_scan_outcome = _add_django_project_to_path(args)
    else:
        _django_project_scan_outcome = PROJECT_SCAN_DISABLED

    if (
        options.itv
        or _get_boolean_value(
            os.environ.get(INVALID_TEMPLATE_VARS_ENV), INVALID_TEMPLATE_VARS_ENV
        )
        or early_config.getini(INVALID_TEMPLATE_VARS_ENV)
    ):
        os.environ[INVALID_TEMPLATE_VARS_ENV] = "true"

    def _get_option_with_source(option, envname):
        if option:
            return option, "option"
        if envname in os.environ:
            return os.environ[envname], "env"
        cfgval = early_config.getini(envname)
        if cfgval:
            return cfgval, "ini"
        return None, None

    ds, ds_source = _get_option_with_source(options.ds, SETTINGS_MODULE_ENV)
    dc, dc_source = _get_option_with_source(options.dc, CONFIGURATION_ENV)

    if ds:
        _report_header.append("settings: %s (from %s)" % (ds, ds_source))
        os.environ[SETTINGS_MODULE_ENV] = ds

        if dc:
            _report_header.append("configuration: %s (from %s)" % (dc, dc_source))
            os.environ[CONFIGURATION_ENV] = dc

            # Install the django-configurations importer
            import configurations.importer

            configurations.importer.install()

        # Forcefully load Django settings, throws ImportError or
        # ImproperlyConfigured if settings cannot be loaded.
        from django.conf import settings as dj_settings

        with _handle_import_error(_django_project_scan_outcome):
            dj_settings.DATABASES

    _setup_django()


def pytest_report_header():
    if _report_header:
        return ["django: " + ", ".join(_report_header)]


@pytest.mark.trylast
def pytest_configure():
    # Allow Django settings to be configured in a user pytest_configure call,
    # but make sure we call django.setup()
    _setup_django()


def _classmethod_is_defined_at_leaf(cls, method_name):
    super_method = None

    for base_cls in cls.__mro__[1:]:  # pragma: no branch
        super_method = base_cls.__dict__.get(method_name)
        if super_method is not None:
            break

    assert super_method is not None, (
        "%s could not be found in base classes" % method_name
    )

    method = getattr(cls, method_name)

    try:
        f = method.__func__
    except AttributeError:
        pytest.fail("%s.%s should be a classmethod" % (cls, method_name))
    if PY2 and not (
        inspect.ismethod(method)
        and inspect.isclass(method.__self__)
        and issubclass(cls, method.__self__)
    ):
        pytest.fail("%s.%s should be a classmethod" % (cls, method_name))
    return f is not super_method.__func__


_disabled_classmethods = {}


def _disable_class_methods(cls):
    if cls in _disabled_classmethods:
        return

    _disabled_classmethods[cls] = (
        # Get the classmethod object (not the resulting bound method),
        # otherwise inheritance will be broken when restoring.
        cls.__dict__.get("setUpClass"),
        _classmethod_is_defined_at_leaf(cls, "setUpClass"),
        cls.__dict__.get("tearDownClass"),
        _classmethod_is_defined_at_leaf(cls, "tearDownClass"),
    )

    cls.setUpClass = types.MethodType(lambda cls: None, cls)
    cls.tearDownClass = types.MethodType(lambda cls: None, cls)


def _restore_class_methods(cls):
    (
        setUpClass,
        restore_setUpClass,
        tearDownClass,
        restore_tearDownClass,
    ) = _disabled_classmethods.pop(cls)

    try:
        del cls.setUpClass
    except AttributeError:
        raise

    try:
        del cls.tearDownClass
    except AttributeError:
        pass

    if restore_setUpClass:
        cls.setUpClass = setUpClass

    if restore_tearDownClass:
        cls.tearDownClass = tearDownClass


def pytest_runtest_setup(item):
    if _handle_unittest_methods:
        if django_settings_is_configured() and is_django_unittest(item):
            _disable_class_methods(item.cls)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    # If Django is not configured we don't need to bother
    if not django_settings_is_configured():
        return

    from django.test import TestCase, TransactionTestCase

    def get_order_number(test):
        if hasattr(test, "cls") and test.cls:
            # Beware, TestCase is a subclass of TransactionTestCase
            if issubclass(test.cls, TestCase):
                return 0
            if issubclass(test.cls, TransactionTestCase):
                return 1

        marker_db = test.get_closest_marker('django_db')
        if marker_db:
            transaction = validate_django_db(marker_db)[0]
            if transaction is True:
                return 1
        else:
            transaction = None

        fixtures = getattr(test, 'fixturenames', [])
        if "transactional_db" in fixtures:
            return 1

        if transaction is False:
            return 0
        if "db" in fixtures:
            return 0

        return 2

    items[:] = sorted(items, key=get_order_number)


@pytest.fixture(autouse=True, scope="session")
def django_test_environment(request):
    """
    Ensure that Django is loaded and has its testing environment setup.

    XXX It is a little dodgy that this is an autouse fixture.  Perhaps
        an email fixture should be requested in order to be able to
        use the Django email machinery just like you need to request a
        db fixture for access to the Django database, etc.  But
        without duplicating a lot more of Django's test support code
        we need to follow this model.
    """
    if django_settings_is_configured():
        _setup_django()
        from django.conf import settings as dj_settings
        from django.test.utils import setup_test_environment, teardown_test_environment

        dj_settings.DEBUG = False
        setup_test_environment()
        request.addfinalizer(teardown_test_environment)


@pytest.fixture(scope="session")
def django_db_blocker():
    """Wrapper around Django's database access.

    This object can be used to re-enable database access.  This fixture is used
    internally in pytest-django to build the other fixtures and can be used for
    special database handling.

    The object is a context manager and provides the methods
    .unblock()/.block() and .restore() to temporarily enable database access.

    This is an advanced feature that is meant to be used to implement database
    fixtures.
    """
    if not django_settings_is_configured():
        return None

    return _blocking_manager


@pytest.fixture(autouse=True)
def _django_db_marker(request):
    """Implement the django_db marker, internal to pytest-django.

    This will dynamically request the ``db``, ``transactional_db`` or
    ``django_db_reset_sequences`` fixtures as required by the django_db marker.
    """
    marker = request.node.get_closest_marker("django_db")
    if marker:
        transaction, reset_sequences = validate_django_db(marker)
        if reset_sequences:
            request.getfixturevalue("django_db_reset_sequences")
        elif transaction:
            request.getfixturevalue("transactional_db")
        else:
            request.getfixturevalue("db")


@pytest.fixture(autouse=True, scope="class")
def _django_setup_unittest(request, django_db_blocker):
    """Setup a django unittest, internal to pytest-django."""
    if not django_settings_is_configured() or not is_django_unittest(request):
        yield
        return

    # Fix/patch pytest.
    # Before pytest 5.4: https://github.com/pytest-dev/pytest/issues/5991
    # After pytest 5.4: https://github.com/pytest-dev/pytest-django/issues/824
    from _pytest.monkeypatch import MonkeyPatch

    def non_debugging_runtest(self):
        self._testcase(result=self)

    mp_debug = MonkeyPatch()
    mp_debug.setattr("_pytest.unittest.TestCaseFunction.runtest", non_debugging_runtest)

    request.getfixturevalue("django_db_setup")

    cls = request.node.cls

    with django_db_blocker.unblock():
        if _handle_unittest_methods:
            _restore_class_methods(cls)
            cls.setUpClass()
            _disable_class_methods(cls)

            yield

            _restore_class_methods(cls)
            cls.tearDownClass()
        else:
            yield

    if mp_debug:
        mp_debug.undo()


@pytest.fixture(scope="function", autouse=True)
def _dj_autoclear_mailbox():
    if not django_settings_is_configured():
        return

    from django.core import mail

    del mail.outbox[:]


@pytest.fixture(scope="function")
def mailoutbox(django_mail_patch_dns, _dj_autoclear_mailbox):
    if not django_settings_is_configured():
        return

    from django.core import mail

    return mail.outbox


@pytest.fixture(scope="function")
def django_mail_patch_dns(monkeypatch, django_mail_dnsname):
    from django.core import mail

    monkeypatch.setattr(mail.message, "DNS_NAME", django_mail_dnsname)


@pytest.fixture(scope="function")
def django_mail_dnsname():
    return "fake-tests.example.com"


@pytest.fixture(autouse=True, scope="function")
def _django_set_urlconf(request):
    """Apply the @pytest.mark.urls marker, internal to pytest-django."""
    marker = request.node.get_closest_marker("urls")
    if marker:
        skip_if_no_django()
        import django.conf

        try:
            from django.urls import clear_url_caches, set_urlconf
        except ImportError:
            # Removed in Django 2.0
            from django.core.urlresolvers import clear_url_caches, set_urlconf

        urls = validate_urls(marker)
        original_urlconf = django.conf.settings.ROOT_URLCONF
        django.conf.settings.ROOT_URLCONF = urls
        clear_url_caches()
        set_urlconf(None)

        def restore():
            django.conf.settings.ROOT_URLCONF = original_urlconf
            # Copy the pattern from
            # https://github.com/django/django/blob/master/django/test/signals.py#L152
            clear_url_caches()
            set_urlconf(None)

        request.addfinalizer(restore)


@pytest.fixture(autouse=True, scope="session")
def _fail_for_invalid_template_variable():
    """Fixture that fails for invalid variables in templates.

    This fixture will fail each test that uses django template rendering
    should a template contain an invalid template variable.
    The fail message will include the name of the invalid variable and
    in most cases the template name.

    It does not raise an exception, but fails, as the stack trace doesn't
    offer any helpful information to debug.
    This behavior can be switched off using the marker:
    ``pytest.mark.ignore_template_errors``
    """

    class InvalidVarException(object):
        """Custom handler for invalid strings in templates."""

        def __init__(self):
            self.fail = True

        def __contains__(self, key):
            """There is a test for '%s' in TEMPLATE_STRING_IF_INVALID."""
            return key == "%s"

        @staticmethod
        def _get_origin():
            stack = inspect.stack()

            # Try to use topmost `self.origin` first (Django 1.9+, and with
            # TEMPLATE_DEBUG)..
            for f in stack[2:]:
                func = f[3]
                if func == "render":
                    frame = f[0]
                    try:
                        origin = frame.f_locals["self"].origin
                    except (AttributeError, KeyError):
                        continue
                    if origin is not None:
                        return origin

            from django.template import Template

            # finding the ``render`` needle in the stack
            frame = reduce(
                lambda x, y: y[3] == "render" and "base.py" in y[1] and y or x, stack
            )
            # assert 0, stack
            frame = frame[0]
            # finding only the frame locals in all frame members
            f_locals = reduce(
                lambda x, y: y[0] == "f_locals" and y or x, inspect.getmembers(frame)
            )[1]
            # ``django.template.base.Template``
            template = f_locals["self"]
            if isinstance(template, Template):
                return template.name

        def __mod__(self, var):
            """Handle TEMPLATE_STRING_IF_INVALID % var."""
            origin = self._get_origin()
            if origin:
                msg = "Undefined template variable '%s' in '%s'" % (var, origin)
            else:
                msg = "Undefined template variable '%s'" % var
            if self.fail:
                pytest.fail(msg)
            else:
                return msg

    if (
        os.environ.get(INVALID_TEMPLATE_VARS_ENV, "false") == "true"
        and django_settings_is_configured()
    ):
        from django.conf import settings as dj_settings

        if dj_settings.TEMPLATES:
            dj_settings.TEMPLATES[0]["OPTIONS"][
                "string_if_invalid"
            ] = InvalidVarException()
        else:
            dj_settings.TEMPLATE_STRING_IF_INVALID = InvalidVarException()


@pytest.fixture(autouse=True)
def _template_string_if_invalid_marker(request):
    """Apply the @pytest.mark.ignore_template_errors marker,
     internal to pytest-django."""
    marker = request.keywords.get("ignore_template_errors", None)
    if os.environ.get(INVALID_TEMPLATE_VARS_ENV, "false") == "true":
        if marker and django_settings_is_configured():
            from django.conf import settings as dj_settings

            if dj_settings.TEMPLATES:
                dj_settings.TEMPLATES[0]["OPTIONS"]["string_if_invalid"].fail = False
            else:
                dj_settings.TEMPLATE_STRING_IF_INVALID.fail = False


@pytest.fixture(autouse=True, scope="function")
def _django_clear_site_cache():
    """Clears ``django.contrib.sites.models.SITE_CACHE`` to avoid
    unexpected behavior with cached site objects.
    """

    if django_settings_is_configured():
        from django.conf import settings as dj_settings

        if "django.contrib.sites" in dj_settings.INSTALLED_APPS:
            from django.contrib.sites.models import Site

            Site.objects.clear_cache()


# ############### Helper Functions ################


class _DatabaseBlockerContextManager(object):
    def __init__(self, db_blocker):
        self._db_blocker = db_blocker

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self._db_blocker.restore()


class _DatabaseBlocker(object):
    """Manager for django.db.backends.base.base.BaseDatabaseWrapper.

    This is the object returned by django_db_blocker.
    """

    def __init__(self):
        self._history = []
        self._real_ensure_connection = None

    @property
    def _dj_db_wrapper(self):
        from django.db.backends.base.base import BaseDatabaseWrapper

        # The first time the _dj_db_wrapper is accessed, we will save a
        # reference to the real implementation.
        if self._real_ensure_connection is None:
            self._real_ensure_connection = BaseDatabaseWrapper.ensure_connection

        return BaseDatabaseWrapper

    def _save_active_wrapper(self):
        return self._history.append(self._dj_db_wrapper.ensure_connection)

    def _blocking_wrapper(*args, **kwargs):
        __tracebackhide__ = True
        __tracebackhide__  # Silence pyflakes
        raise RuntimeError(
            "Database access not allowed, "
            'use the "django_db" mark, or the '
            '"db" or "transactional_db" fixtures to enable it.'
        )

    def unblock(self):
        """Enable access to the Django database."""
        self._save_active_wrapper()
        self._dj_db_wrapper.ensure_connection = self._real_ensure_connection
        return _DatabaseBlockerContextManager(self)

    def block(self):
        """Disable access to the Django database."""
        self._save_active_wrapper()
        self._dj_db_wrapper.ensure_connection = self._blocking_wrapper
        return _DatabaseBlockerContextManager(self)

    def restore(self):
        self._dj_db_wrapper.ensure_connection = self._history.pop()


_blocking_manager = _DatabaseBlocker()


def validate_django_db(marker):
    """Validate the django_db marker.

    It checks the signature and creates the ``transaction`` and
    ``reset_sequences`` attributes on the marker which will have the
    correct values.

    A sequence reset is only allowed when combined with a transaction.
    """

    def apifun(transaction=False, reset_sequences=False):
        return transaction, reset_sequences

    return apifun(*marker.args, **marker.kwargs)


def validate_urls(marker):
    """Validate the urls marker.

    It checks the signature and creates the `urls` attribute on the
    marker which will have the correct value.
    """

    def apifun(urls):
        return urls

    return apifun(*marker.args, **marker.kwargs)
