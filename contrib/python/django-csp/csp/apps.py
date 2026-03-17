from django.apps import AppConfig

from csp.checks import *  # noqa: F403 (here to register the checks)


class CspConfig(AppConfig):
    name = "csp"
