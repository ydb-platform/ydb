"""
Wrapper for loading templates from "templates" directories in INSTALLED_APPS
packages.
"""
import os

from django.apps import apps
try:
    from functools import lru_cache
except ImportError:
    # Compatibility with django-1/python2
    from django.utils.lru_cache import lru_cache

from library.python.django.template.loaders.resource import Loader as ResourceLoader
from library.python.resource import resfs_files


class Loader(ResourceLoader):

    def get_dirs(self):
        return self.get_app_template_dirs('templates')

    @lru_cache()
    def get_app_template_dirs(self, dirname):
        """
        Return an iterable of paths of directories to load app templates from.

        dirname is the name of the subdirectory containing templates inside
        installed applications.
        """
        template_dirs = []
        for app_config in apps.get_app_configs():
            if not app_config.path:
                continue
            mod = app_config.module
            try:
                getfilename = mod.__loader__.get_filename
            except AttributeError:
                # Not a PY_SRCS module.
                continue
            # 1. Look for RESOURCE_FILES with PREFIX.
            # Get module arcpath, even with Y_PYTHON_SOURCE_ROOT.
            module_file = getfilename(mod.__name__)
            path = os.path.dirname(module_file) + '/' + dirname + '/'
            if not resfs_files(path):
                # 2. Fallback to unprefixed RESOURCE_FILES.
                path = mod.__name__.replace('.', '/') + '/' + dirname + '/'
            template_dirs.append(path)
        # Immutable return value because it will be cached and shared by callers.
        return tuple(template_dirs)
