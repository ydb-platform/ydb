# coding: utf-8
"""
install: add patch_all to pylint.__init__.py + (1.9) quiet=1 for OptionsManagerMixIn
"""
from __future__ import print_function

import importlib
import os
import pkgutil
import sys


def iter_plugins(directory):
    imported = {}
    py_package = 'pylint.' + os.path.basename(directory)
    for _, name, _is_pkg in pkgutil.iter_modules([directory]):
        if name in imported:
            continue
        try:
            module = importlib.import_module(py_package + '.' + name)
        except ValueError:
            continue
        except ImportError as exc:
            print("Problem importing module %s: %s" % (name, exc), file=sys.stderr)
        else:
            if hasattr(module, 'register'):
                yield name, module
                imported[name] = 1


def iter_extensions():
    import pylint.extensions

    for name, _module in iter_plugins(pylint.extensions.__path__[0]):
        yield 'pylint.extensions.' + name


def patch_list_extensions():
    try:
        import pylint.config.callback_actions
    except ImportError:
        return

    def wrapper(*args, **kwargs):
        for name in sorted(iter_extensions()):
            print(name)
        sys.exit(0)

    pylint.config.callback_actions._ListExtensionsAction.__call__ = wrapper


def patch_enable_all_extensions():
    try:
        import pylint.config.utils
    except ImportError:
        return

    def wrapper(run, value):
        assert value is None
        for name in iter_extensions():
            if name not in run._plugins:
                run._plugins.append(name)

    key = '--enable-all-extensions'
    params = pylint.config.utils.PREPROCESSABLE_OPTIONS[key]
    pylint.config.utils.PREPROCESSABLE_OPTIONS[key] = params[0], wrapper, params[2]


def patch_reporter():
    from pylint.reporters import CollectingReporter

    CollectingReporter._display = lambda *args, **kwargs: None


def patch_register_plugins():
    import pylint.utils

    def wrapper(linter, directory):
        for _name, module in iter_plugins(directory):
            module.register(linter)

    pylint.utils.register_plugins = wrapper


def patch_all():
    patch_register_plugins()
    patch_reporter()
    patch_enable_all_extensions()
    patch_list_extensions()
