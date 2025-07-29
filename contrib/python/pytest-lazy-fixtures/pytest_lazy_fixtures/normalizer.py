from __future__ import annotations

import copy
from typing import Any, Iterable, Iterator

import pytest

from .lazy_fixture import LazyFixtureWrapper
from .lazy_fixture_callable import LazyFixtureCallableWrapper


def _get_fixturenames_closure_and_arg2fixturedefs(fm, metafunc, value) -> tuple[list[str], dict[str, Any]]:
    if isinstance(value, LazyFixtureCallableWrapper):
        extra_fixturenames_args, arg2fixturedefs_args = _get_fixturenames_closure_and_arg2fixturedefs(
            fm,
            metafunc,
            value.args,
        )
        extra_fixturenames_kwargs, arg2fixturedefs_kwargs = _get_fixturenames_closure_and_arg2fixturedefs(
            fm,
            metafunc,
            value.kwargs,
        )
        return [*extra_fixturenames_args, *extra_fixturenames_kwargs], {
            **arg2fixturedefs_args,
            **arg2fixturedefs_kwargs,
        }
    if isinstance(value, LazyFixtureWrapper):
        if pytest.version_tuple >= (8, 0, 0):
            fixturenames_closure, arg2fixturedefs = fm.getfixtureclosure(metafunc.definition.parent, [value.name], {})
        else:  # pragma: no cover
            # TODO: add tox
            _, fixturenames_closure, arg2fixturedefs = fm.getfixtureclosure([value.name], metafunc.definition.parent)

        return fixturenames_closure, arg2fixturedefs
    extra_fixturenames, arg2fixturedefs = [], {}
    # we need to check exact type
    if type(value) is dict:
        value = list(value.values())
    # we need to check exact type
    if type(value) in {list, tuple, set}:
        for val in value:
            ef, arg2f = _get_fixturenames_closure_and_arg2fixturedefs(fm, metafunc, val)
            extra_fixturenames.extend(ef)
            arg2fixturedefs.update(arg2f)
    return extra_fixturenames, arg2fixturedefs


def normalize_metafunc_calls(metafunc, used_keys=None):
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


def _uniq(values: Iterable[str]) -> Iterator[str]:
    seen = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            yield value


def _normalize_call(callspec, metafunc, used_keys):
    fm = metafunc.config.pluginmanager.get_plugin("funcmanage")

    used_keys = used_keys or set()
    params = callspec.params.copy() if pytest.version_tuple >= (8, 0, 0) else {**callspec.params, **callspec.funcargs}
    valtype_keys = params.keys() - used_keys

    for arg in valtype_keys:
        value = params[arg]
        fixturenames_closure, arg2fixturedefs = _get_fixturenames_closure_and_arg2fixturedefs(fm, metafunc, value)

        if fixturenames_closure and arg2fixturedefs:
            extra_fixturenames = [fname for fname in _uniq(fixturenames_closure) if fname not in params]

            newmetafunc = _copy_metafunc(metafunc)
            newmetafunc.fixturenames = extra_fixturenames
            newmetafunc._arg2fixturedefs.update(arg2fixturedefs)
            newmetafunc._calls = [callspec]
            fm.pytest_generate_tests(newmetafunc)
            normalize_metafunc_calls(newmetafunc, used_keys | {arg})
            return newmetafunc._calls

        used_keys.add(arg)
    return [callspec]
