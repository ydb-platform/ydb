'''Automatic configuration for Django project.'''

import collections
import copy
from django.core.exceptions import ImproperlyConfigured
from django.conf import global_settings
from django.conf.urls import include, url
try:
    from django.conf.urls import patterns
except ImportError:
    def patterns(_, *args):
        return list(args)
from django.utils.functional import Promise
from django.utils.module_loading import module_has_submodule
import imp
import importlib
import operator

import logging
LOGGER = logging.getLogger(__name__)

MAX_ITERATIONS = 1000

class OrderingRelationship(object):
    '''
    This class defines a relationship between an element in a setting
    that's a list and one or more other entries.

    It's intended to be used in an autoconfig.py file like so::

        RELATIONSHIPS = [
            OrderingRelationship(
                'INSTALLED_APPS',
                'my.app',
                before = [
                    'django.contrib.admin',
                ],
                after = [
                ],
            )
        ]
    '''

    def __init__(
        self,
        setting_name,
        setting_value,
        before=None,
        after=None,
        add_missing=True
    ):
        self.setting_name = setting_name
        self.setting_value = setting_value
        self.before = before or []
        self.after = after or []
        self.add_missing = add_missing

    def apply_changes(self, settings):
        changes = 0

        if self.add_missing:
            for item in [self.setting_value] + self.before + self.after:
                if item not in settings[self.setting_name]:
                    settings[self.setting_name] = list(settings[self.setting_name]) + [item]
                    LOGGER.debug("Added %r to %r.", item, self.setting_name)
                    changes += 1
        elif self.setting_value not in settings[self.setting_name]:
            return changes

        for test, related_items, list_name in (
            (operator.gt, self.before, 'before'),
            (operator.lt, self.after, 'after'),
        ):
            current_value = settings[self.setting_name]

            for item in related_items:
                if item not in current_value:
                    continue
                if test(
                    current_value.index(self.setting_value),
                    current_value.index(item),
                ):
                    if isinstance(current_value, tuple):
                        current_value = list(current_value)
                    location = current_value.index(item)
                    current_value.remove(self.setting_value)
                    current_value.insert(location, self.setting_value)
                    settings[self.setting_name] = current_value
                    LOGGER.debug("Moved %r %r %r.", self.setting_value, list_name, item)
                    changes += 1

        return changes

def merge_dictionaries(current, new, only_defaults=False, template_special_case=False):
    '''
    Merge two settings dictionaries, recording how many changes were needed.

    '''
    changes = 0
    for key, value in new.items():
        if key not in current:
            if hasattr(global_settings, key):
                current[key] = getattr(global_settings, key)
                LOGGER.debug("Set %r to global default %r.", key, current[key])
            else:
                current[key] = copy.copy(value)
                LOGGER.debug("Set %r to %r.", key, current[key])
                changes += 1
                continue
        elif only_defaults:
            continue
        current_value = current[key]
        if hasattr(current_value, 'items'):
            changes += merge_dictionaries(current_value, value)
        elif isinstance(current_value, (list, tuple)):
            for element in value:
                if element not in current_value:
                    if template_special_case and key == 'TEMPLATES':
                        existing_matches = [
                            template for template in current_value if template['BACKEND'] == element['BACKEND']
                        ]
                        if existing_matches:
                            changes += merge_dictionaries(existing_matches[0], element)
                        else:
                            current[key] = list(current_value) + [element]
                            LOGGER.debug("Added %r to %r.", element, key)
                            changes += 1
                    else:
                        current[key] = list(current_value) + [element]
                        LOGGER.debug("Added %r to %r.", element, key)
                        changes += 1
        elif isinstance(current_value, Promise) or isinstance(value, Promise):
            # If we don't know what to do with it, replace it.
            if current_value is not value:
                current[key] = value
                LOGGER.debug("Set %r to %r.", key, current[key])
                changes += 1
        else:
            # If we don't know what to do with it, replace it.
            if current_value != value:
                current[key] = value
                LOGGER.debug("Set %r to %r.", key, current[key])
                changes += 1
    return changes

def autoconfig_module_exists(app_name):
    names = app_name.split('.')
    path = None
    for name in names:
        _, path, _ = imp.find_module(name, path)
        path = [path]
    try:
        imp.find_module('autoconfig', path)
        return True
    except ImportError:
        return False

def configure_settings(settings, environment_settings=True):
    '''
    Given a settings object, run automatic configuration of all
    the apps in INSTALLED_APPS.
    '''
    changes = 1
    iterations = 0

    while changes:
        changes = 0
        app_names = ['django_autoconfig'] + list(settings['INSTALLED_APPS'])
        if environment_settings:
            app_names.append('django_autoconfig.environment_settings')
        for app_name in app_names:
            import django_autoconfig.contrib
            if autoconfig_module_exists(app_name):
                module = importlib.import_module("%s.autoconfig" % (app_name,))
            elif app_name in django_autoconfig.contrib.CONTRIB_CONFIGS:
                module = django_autoconfig.contrib.CONTRIB_CONFIGS[app_name]
            else:
                continue
            changes += merge_dictionaries(
                settings,
                getattr(module, 'SETTINGS', {}),
                template_special_case=True,
            )
            changes += merge_dictionaries(
                settings,
                getattr(module, 'DEFAULT_SETTINGS', {}),
                only_defaults=True,
            )
            for relationship in getattr(module, 'RELATIONSHIPS', []):
                changes += relationship.apply_changes(settings)

        if iterations >= MAX_ITERATIONS:
            raise ImproperlyConfigured(
                'Autoconfiguration could not reach a consistent state'
            )
        iterations += 1
    LOGGER.debug("Autoconfiguration took %d iterations.", iterations)


def configure_urls(apps, index_view=None, prefixes=None):
    '''
    Configure urls from a list of apps.
    '''
    prefixes = prefixes or {}
    urlpatterns = patterns('')

    if index_view:
        from django.views.generic.base import RedirectView
        urlpatterns += patterns('',
            url(r'^$', RedirectView.as_view(pattern_name=index_view, permanent=False)),
        )

    for app_name in apps:
        app_module = importlib.import_module(app_name)
        if module_has_submodule(app_module, 'urls'):
            module = importlib.import_module("%s.urls" % app_name)
            if not hasattr(module, 'urlpatterns'):
                # Resolver will break if the urls.py file is completely blank.
                continue
            app_prefix = prefixes.get(app_name, app_name.replace("_","-"))
            urlpatterns += patterns(
                '',
                url(
                    r'^%s/' % app_prefix if app_prefix else '',
                    include("%s.urls" % app_name),
                ),
            )


    return urlpatterns
