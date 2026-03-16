import warnings
from django.conf import settings
from django.utils.module_loading import import_string

from . import HealthcheckFailure

# Ignore typing on Python <3.5
try:
    from typing import List
except ImportError:
    pass


DEFAULT_ALIVE_CHECKS = [("django_alive.checks.check_database", {})]


def perform_healthchecks():
    # typing: () -> (bool, List[str])
    errors = []
    ALIVE_CHECKS = getattr(settings, "ALIVE_CHECKS", DEFAULT_ALIVE_CHECKS)
    if isinstance(ALIVE_CHECKS, dict):
        # Deprecated dict format
        warnings.warn(
            "ALIVE_CHECKS should be a list of tuples, not a dict. "
            "Please update your settings.",
            DeprecationWarning,
        )
        checks = ALIVE_CHECKS.items()
    else:
        checks = ALIVE_CHECKS
    for func, kwargs in checks:
        try:
            import_string(func)(**kwargs)
        except HealthcheckFailure as e:
            errors.append(str(e))
    return not errors, errors
