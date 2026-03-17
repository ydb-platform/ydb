# coding: utf-8
"""
install: replace brain_* listdir in __init__.py with arcadia_hook.patch_all()
"""

import os
import sys

import astroid.interpreter._import.spec as astroid_spec
import astroid.modutils


def patch_all():
    for module in sys.extra_modules:
        if module.startswith('astroid.brain.brain_'):
            __import__(module)

    astroid_spec._SPEC_FINDERS = (ArcFinder,)
    astroid.modutils.file_info_from_modpath = file_info_from_modpath


KNOWN_MODULES = {}


def find_modules(srcs_fpath):
    import yatest.common
    if sys.version_info.major == 3:
        py_stdlib_relpath = 'contrib/tools/python3/Lib'
    else:
        py_stdlib_relpath = 'contrib/tools/python/src/Lib'
    py_stdlib_srcpath = yatest.common.source_path(py_stdlib_relpath)
    if not os.path.exists(py_stdlib_srcpath):
        raise RuntimeError('Directory {!r} not found; you should add in into DATA() section of ya.make with current PYTEST()'.format(py_stdlib_relpath))
    _find_modules(py_stdlib_srcpath)
    _find_modules(srcs_fpath)
    if sys.version_info >= (3, 13):
        KNOWN_MODULES["collections.abc"] = KNOWN_MODULES["_collections_abc"]


def _find_modules(srcdir):
    for root, dnames, fnames in os.walk(srcdir):
        rel_root = os.path.relpath(root, srcdir).lstrip('.')
        mod_prefix = rel_root.replace('/', '.')
        if '-' in mod_prefix:
            continue
        if mod_prefix:
            mod_prefix += '.'
        for fname in fnames:
            if '-' in fname or not fname.endswith('.py') or fname == '__init__.py':
                continue
            fpath = os.path.join(root, fname)
            KNOWN_MODULES[mod_prefix + fname[:-3]] = fpath
        for dname in dnames:
            dpath = os.path.join(root, dname, '__init__.py')
            if '-' in dname or not os.path.exists(dpath):
                continue
            KNOWN_MODULES[mod_prefix + dname] = dpath


class ArcFinder(astroid_spec.Finder):
    @staticmethod
    def find_module(modname, module_parts, processed, submodule_path):
        if not KNOWN_MODULES:
            raise RuntimeError('call for astroid.arcadia_patch.find_modules required')

        if processed:
            modname = '.'.join(list(processed) + [modname])

        location = KNOWN_MODULES.get(modname)
        if location is not None:
            return astroid_spec.ModuleSpec(modname, astroid_spec.ModuleType.PY_SOURCE, location=location)

        if modname in sys.builtin_module_names:
            if modname not in sys.modules:
                __import__(modname)
            return astroid_spec.ModuleSpec(modname, astroid_spec.ModuleType.C_BUILTIN, location='__builtin__')


def file_info_from_modpath(modpath, path=None, context_file=None):
    if context_file is not None:
        context = os.path.dirname(context_file)
    else:
        context = context_file
    if modpath[0] == 'xml':
        # handle _xmlplus
        try:
            return astroid.modutils._spec_from_modpath(['_xmlplus'] + modpath[1:], path, context)
        except ImportError:
            return astroid.modutils._spec_from_modpath(modpath, path, context)
    elif modpath == ['os', 'path']:
        return astroid.modutils._spec_from_modpath([os.path.__name__], path, context)._replace(name='os.path')
    return astroid.modutils._spec_from_modpath(modpath, path, context)
