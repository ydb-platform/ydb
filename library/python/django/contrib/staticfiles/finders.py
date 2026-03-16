# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function

import os
from collections import OrderedDict

from django.apps import apps
from django.contrib.staticfiles import utils
from django.contrib.staticfiles.finders import FileSystemFinder, AppDirectoriesFinder

from library.python.django.contrib.staticfiles.storage import ResourceFilesStorage, ResfsStorage
from library.python.resource import resfs_files


class ResfsFinder(FileSystemFinder):
    """Обнаруживатель файлов статики внутри исполнямых аркадийных файлов
    ("ресурсной файловой системы" - resfs).

    Используется для нахождения статики, определённой в STATICFILES_DIRS.

    Требуется, чтобы в STATICFILES_DIRS были указаны пути с префиксами.
    Например, если в ya.make

        RESOURCE_FILES(PREFIX billing/bcl/src/
            bcl/static/js/file.js
            bcl/static/css/file.css
        )

        то в settings.py проекта для работы статики потребуется:

        STATICFILES_DIRS = [
            ('billing/bcl/src', 'bcl/static/')
        ]

    """
    storage_class = ResfsStorage

    def __init__(self, *args, **kwargs):
        super(ResfsFinder, self).__init__(*args, **kwargs)

        # Rebuild storages using proper storage class.
        self.storages = OrderedDict()

        for prefix, root in self.locations:
            storage = self.storage_class(root)
            storage._prefix = prefix
            self.storages[root] = storage

    def list(self, ignore_patterns):
        # В django 4 добавилась проверка на существование директории.
        # Нам это мешает, поэтому оставляем реализацию из предыдущих версий.
        for prefix, root in self.locations:
            storage = self.storages[root]
            for path in utils.get_files(storage, ignore_patterns):
                yield path, storage


class ArcadiaAppFinder(AppDirectoriesFinder):
    storage_class = ResourceFilesStorage

    def __init__(self, app_names=None, *args, **kwargs):
        # The list of apps that are handled
        self.apps = []
        # Mapping of app names to storage instances
        self.storages = OrderedDict()
        app_configs = apps.get_app_configs()
        if app_names:
            app_names = set(app_names)
            app_configs = [ac for ac in app_configs if ac.name in app_names]

        static = '/' + self.source_dir + '/'
        for app_config in app_configs:
            mod = app_config.module
            try:
                getfilename = mod.__loader__.get_filename
            except AttributeError:
                # Not a PY_SRCS module.
                continue
            # 1. Look for RESOURCE_FILES with PREFIX.
            # Get module arcpath, even with Y_PYTHON_SOURCE_ROOT.
            module_file = getfilename(mod.__name__)
            path = os.path.dirname(module_file) + static
            if not resfs_files(path):
                # 2. Fallback to unprefixed RESOURCE_FILES.
                path = mod.__name__.replace('.', '/') + static
            app_storage = self.storage_class(path)
            self.storages[app_config.name] = app_storage
            if app_config.name not in self.apps:
                self.apps.append(app_config.name)
