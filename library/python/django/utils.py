# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function

import inspect
import os
import pkgutil
from contextlib import contextmanager


def find_modules_in_path(import_path):
    try:
        import importlib
        module = importlib.import_module(import_path)
        for imp, name, is_pkg in pkgutil.iter_modules(path=module.__path__):
            if name[0] not in '_.~':
                yield name
    except ImportError:
        pass


def get_module_dir(module):
    paths = list(getattr(module, '__path__', []))
    # Yandex - specific: actually in arcadia __path__ could be different, it could be path to module on FS,
    # or it could be path to executable (including executable) + module name, if code executed normally.
    # We need to find a path to module directory in both cases.
    if len(paths) == 1:
        path = os.path.normpath(paths[0])
        module_relative_path = module.__name__.split('.')
        splitted_path = path.split('/')
        while splitted_path:
            module_path = os.path.join('/'.join(splitted_path + module_relative_path))
            if os.path.exists(module_path):
                return module_path
            splitted_path = splitted_path[:-1]
    # End of Yandex-specific
    raise ValueError("Cannot determine directory containing %s" % repr(module))


def patch_settings_for_arcadia():
    """Адаптирует текущие настройки Django к работе после Аркадийной сборки.
     Должна вызываться из settings.py проекта.

    Позволяет:
        * использовать шаблоны, встроенные в бинарник;
        * хранить статику в бинарнике для последующей выгрузки и раздачи сервером;
        * использовать шаблоны в формах.

    STATICFILES_DIRS, если используется, должна содержать кортежи вида
    (префикс_от_корня_аркадии, имя_поддиректории). Например, если в ya.make

        RESOURCE_FILES(PREFIX billing/bcl/src/
            bcl/static/js/file.js
            bcl/static/css/file.css
        )

        то в settings.py проекта для работы статики потребуется:

        STATICFILES_DIRS = [
            ('billing/bcl/src', 'bcl/static/')
        ]

    """
    from django.conf import global_settings

    caller_frame = inspect.currentframe().f_back
    globals_ = caller_frame.f_globals

    @contextmanager
    def existing_or_default(name):
        """Контекстный менеджер, корректирующий значение изменяемого
        объекта (словарь, список и т.д.) настройки Django.

        """
        value = globals_.get(name, getattr(global_settings, name))
        yield value
        globals_[name] = value

    prefix_lib = 'library.python.django.'
    prefix_lib_staticfiles = prefix_lib + 'contrib.staticfiles.'
    prefix_lib_tpl = prefix_lib + 'template.'

    # Поиск статики внутри исполняемых аркадийных файлов.
    with existing_or_default('STATICFILES_FINDERS') as finders:
        finders[:] = [
            prefix_lib_staticfiles + 'finders.ResfsFinder',  # Статика общепроектная.
            prefix_lib_staticfiles + 'finders.ArcadiaAppFinder',  # Статика внутри подключаемых приложений.
        ]

    # Поиск шаблонов внутри исполняемых аркадийных файлов.
    with existing_or_default('TEMPLATES') as templates:
        template_engine = templates[0]
        template_engine['BACKEND'] = prefix_lib_tpl + 'backends.arcadia.ArcadiaTemplates'
        template_engine.pop('APP_DIRS', '')

    # Прочие настройки.
    globals_['FORM_RENDERER'] = prefix_lib_tpl + 'backends.forms_renderer.ArcadiaRenderer'
