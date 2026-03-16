'''Pull settings from environment variables.'''

import ast
import logging
import os

LOGGER = logging.getLogger(__name__)

def _ignore_setting(name):
    return name == 'SETTINGS_MODULE'

def get_settings_from_environment(environ):
    '''Deduce settings from environment variables'''
    settings = {}
    for name, value in environ.items():
        if not name.startswith('DJANGO_'):
            continue
        name = name.replace('DJANGO_', '', 1)
        if _ignore_setting(name):
            continue
        try:
            settings[name] = ast.literal_eval(value)
        except (SyntaxError, ValueError) as err:
            LOGGER.warn("Unable to parse setting %s=%s (%s)", name, value, err)
    return settings

SETTINGS = get_settings_from_environment(os.environ)
