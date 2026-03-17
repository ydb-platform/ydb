# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function

from importlib import import_module

from django.apps import apps
from django.template.backends.django import DjangoTemplates

from library.python.django.utils import find_modules_in_path


class ArcadiaTemplates(DjangoTemplates):
    def __init__(self, params):
        options = params.get('OPTIONS', {})
        options.setdefault('loaders', [
            'library.python.django.template.loaders.resource.Loader',
            'library.python.django.template.loaders.app_resource.Loader',
        ])
        if 'OPTIONS' not in params:
            params['OPTIONS'] = options
        super(ArcadiaTemplates, self).__init__(params)

    def get_installed_libraries(self):
        """
        Return the built-in template tag libraries and those from installed
        applications. Libraries are stored in a dictionary where keys are the
        individual module names, not the full module paths. Example:
        django.templatetags.i18n is stored as i18n.
        """
        libraries = {}
        candidates = ['django.templatetags']
        candidates.extend(
            '%s.templatetags' % app_config.name
            for app_config in apps.get_app_configs())

        for candidate in candidates:
            try:
                pkg = import_module(candidate)
            except ImportError:
                # No templatetags package defined. This is safe to ignore.
                continue

            for name in self.get_package_libraries_from_resource(pkg):
                libraries[name[len(candidate) + 1:]] = name

        return libraries

    @staticmethod
    def get_package_libraries_from_resource(pkg):
        for mod in find_modules_in_path(pkg.__name__):
            module = import_module("%s.%s" % (pkg.__name__, mod))
            if hasattr(module, 'register'):
                yield "%s.%s" % (pkg.__name__, mod)
