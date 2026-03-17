from os import environ

CONFIGS_MODULE_ATTR = 'uwsgi_configuration'

ENV_CONF_ALIAS = 'UWSGICONF_CONF_ALIAS'
ENV_CONF_READY = 'UWSGICONF_READY'
ENV_FORCE_STUB = 'UWSGICONF_FORCE_STUB'
ENV_MAINTENANCE = 'UWSGICONF_MAINTENANCE'
ENV_MAINTENANCE_INPLACE = 'UWSGICONF_MAINTENANCE_INPLACE'


FORCE_STUB = int(environ.get(ENV_FORCE_STUB, 0))
"""Forces using stub instead of uwsgi real module."""


def get_maintenance_path() -> str:
    """Return the maintenance trigger filepath.
    Introduced as a function to support embedded mode.

    """
    return environ.get(ENV_MAINTENANCE) or ''


def get_maintenance_inplace() -> bool:
    """Return the maintenance flag if it is set.
    Introduced as a function to support embedded mode.

    """
    return int(environ.get(ENV_MAINTENANCE_INPLACE, 0)) != 0
