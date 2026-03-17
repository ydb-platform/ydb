from django import db
from django.apps import apps
from django.utils.module_loading import autodiscover_modules

from uwsgiconf import uwsgi
from uwsgiconf.exceptions import RuntimeConfigurationError
from uwsgiconf.settings import FORCE_STUB
from .settings import MODULE_INIT, MODULE_INIT_DEFAULT


def check_for_stub():
    """Check for uWSGI stub. Disallow it to prevent
    stub module caching when embedded mode with pyuwsgi is used."""

    if not uwsgi.is_stub:
        # Native uwsgi module.
        return

    if FORCE_STUB:
        # Stub is used deliberately (e.g. in a test suite).
        return

    msg = (
        'Something from uwsgiconf.uwsgi has been imported before uWSGI start. '
        'Please move uWSGI related stuff including such imports '
        f'into {MODULE_INIT}.py modules of your apps.'
    )

    raise RuntimeConfigurationError(msg)


check_for_stub()


from uwsgiconf.runtime.platform import uwsgi


@uwsgi.postfork_hooks.add()
def db_close_connections():
    """Close db connections after fork()."""
    db.connections.close_all()


if apps.apps_ready:

    if MODULE_INIT != MODULE_INIT_DEFAULT:
        # Import uWSGI init modules from applications.
        autodiscover_modules(MODULE_INIT)
