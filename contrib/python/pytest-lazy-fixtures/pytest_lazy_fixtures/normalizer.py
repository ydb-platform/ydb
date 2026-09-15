from __future__ import annotations

import copy

import pytest

from .utils import get_fixturenames_closure_and_arg2fixturedefs, uniq


def normalize_metafunc_calls(metafunc, used_keys: set[str] | None = None):
    newcalls = []
    for callspec in metafunc._calls:
        calls = _normalize_call(callspec, metafunc, used_keys)
        newcalls.extend(calls)
    metafunc._calls = newcalls


def _copy_metafunc(metafunc):
    copied = copy.copy(metafunc)
    copied.fixturenames = copy.copy(metafunc.fixturenames)
    copied._calls = []
    copied._arg2fixturedefs = copy.copy(metafunc._arg2fixturedefs)
    return copied


def _normalize_call(callspec, metafunc, used_keys: set[str] | None):
    fm = metafunc.config.pluginmanager.get_plugin("funcmanage")

    used_keys = used_keys or set()
    params = callspec.params.copy() if pytest.version_tuple >= (8, 0, 0) else {**callspec.params, **callspec.funcargs}
    valtype_keys = params.keys() - used_keys

    for arg in valtype_keys:
        value = params[arg]
        fixturenames_closure, arg2fixturedefs = get_fixturenames_closure_and_arg2fixturedefs(
            fm, metafunc.definition.parent, value
        )

        if fixturenames_closure and arg2fixturedefs:
            extra_fixturenames = [fname for fname in uniq(fixturenames_closure) if fname not in params]

            newmetafunc = _copy_metafunc(metafunc)
            # Only for deadfixtures call, to not break logic
            if getattr(metafunc.config.option, "deadfixtures", False):
                newmetafunc.definition._fixtureinfo.name2fixturedefs.update(arg2fixturedefs)
            newmetafunc.fixturenames = extra_fixturenames
            newmetafunc._arg2fixturedefs.update(arg2fixturedefs)
            newmetafunc._calls = [callspec]
            fm.pytest_generate_tests(newmetafunc)
            normalize_metafunc_calls(newmetafunc, used_keys | {arg})
            return newmetafunc._calls

        used_keys.add(arg)
    return [callspec]
