"""
Subclass the existing 'runserver' command and change the default options
to disable static file serving, allowing WhiteNoise to handle static files.

There is some unpleasant hackery here because we don't know which command class
to subclass until runtime as it depends on which INSTALLED_APPS we have, so we
have to determine this dynamically.
"""

from __future__ import annotations

from importlib import import_module

from django.apps import apps


def get_next_runserver_command():
    """
    Return the next highest priority "runserver" command class
    """
    for app_name in get_lower_priority_apps():
        module_path = f"{app_name}.management.commands.runserver"
        try:
            return import_module(module_path).Command
        except (ImportError, AttributeError):
            pass


def get_lower_priority_apps():
    """
    Yield all app module names below the current app in the INSTALLED_APPS list
    """
    self_app_name = ".".join(__name__.split(".")[:-3])
    reached_self = False
    for app_config in apps.get_app_configs():
        if app_config.name == self_app_name:
            reached_self = True
        elif reached_self:
            yield app_config.name
    yield "django.core"


RunserverCommand = get_next_runserver_command()


class Command(RunserverCommand):
    def add_arguments(self, parser):
        super().add_arguments(parser)
        if parser.get_default("use_static_handler") is True:
            parser.set_defaults(use_static_handler=False)
            parser.description += (
                "\n(Wrapped by 'whitenoise.runserver_nostatic' to always"
                " enable '--nostatic')"
            )
