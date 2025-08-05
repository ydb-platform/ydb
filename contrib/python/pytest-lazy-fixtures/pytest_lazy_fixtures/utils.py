from __future__ import annotations

from typing import Any, Iterable, Iterator

import pytest

from .lazy_fixture import LazyFixtureWrapper
from .lazy_fixture_callable import LazyFixtureCallableWrapper


def uniq(values: Iterable[str]) -> Iterator[str]:
    seen = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            yield value


def _getfixtureclosure(fm, parent_node, value_name):
    if pytest.version_tuple >= (8, 0, 0):
        fixturenames_closure, arg2fixturedefs = fm.getfixtureclosure(parent_node, [value_name], {})
    else:  # pragma: no cover
        # TODO: add tox
        _, fixturenames_closure, arg2fixturedefs = fm.getfixtureclosure([value_name], parent_node)
    return fixturenames_closure, arg2fixturedefs


def get_fixturenames_closure_and_arg2fixturedefs(fm, parent_node, value) -> tuple[list[str], dict[str, Any]]:
    if isinstance(value, LazyFixtureCallableWrapper):
        extra_fixturenames_args, arg2fixturedefs_args = get_fixturenames_closure_and_arg2fixturedefs(
            fm,
            parent_node,
            value.args,
        )
        extra_fixturenames_kwargs, arg2fixturedefs_kwargs = get_fixturenames_closure_and_arg2fixturedefs(
            fm,
            parent_node,
            value.kwargs,
        )
        extra_fixturenames_func, arg2fixturedefs_func = [], {}
        if value._func is None:
            extra_fixturenames_func, arg2fixturedefs_func = _getfixtureclosure(fm, parent_node, value.name)
        return [*extra_fixturenames_args, *extra_fixturenames_kwargs, *extra_fixturenames_func], {
            **arg2fixturedefs_args,
            **arg2fixturedefs_kwargs,
            **arg2fixturedefs_func,
        }
    if isinstance(value, LazyFixtureWrapper):
        return _getfixtureclosure(fm, parent_node, value.name)
    extra_fixturenames, arg2fixturedefs = [], {}
    # we need to check exact type
    if type(value) is dict:
        value = list(value.values()) + list(value.keys())
    # we need to check exact type
    if type(value) in {list, tuple, set}:
        for val in value:
            ef, arg2f = get_fixturenames_closure_and_arg2fixturedefs(fm, parent_node, val)
            extra_fixturenames.extend(ef)
            arg2fixturedefs.update(arg2f)
    return extra_fixturenames, arg2fixturedefs
