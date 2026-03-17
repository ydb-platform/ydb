from __future__ import annotations

import itertools
import json
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any


try:
    from importlib.resources import files
except ImportError:
    from importlib_resources import files  # type: ignore


Key = str | tuple
Value = dict[Key, Any] | list[dict[Key, Any]]

_registry: dict[Key, Value] = {}


def merge_dicts(left: dict[Key, Any], right: dict[Key, Any]) -> dict[Key, Any]:
    merged = {}
    for key in frozenset(right) & frozenset(left):
        left_value, right_value = left[key], right[key]
        if isinstance(left_value, dict) and isinstance(right_value, dict):
            merged[key] = merge_dicts(left_value, right_value)
        else:
            merged[key] = right_value

    for key, value in itertools.chain(left.items(), right.items()):
        if key not in merged:
            merged[key] = value
    return merged


def has(name: Key) -> bool:
    return name in _registry


def get(name: Key) -> Value:
    if has(name):
        return _registry[name]

    data: Value | None = None
    directory = files(__package__) / f"{name}_registry"
    for entry in directory.iterdir():
        if not entry.name.endswith(".json"):
            continue
        with entry.open(encoding="utf-8") as fp:
            chunk = json.load(fp)
            if entry.stem.endswith("v2"):
                chunk = parse_v2(chunk)
            if data is None:
                data = chunk
            elif isinstance(data, list):
                data.extend(chunk)
            elif isinstance(data, dict):
                data = merge_dicts(data, chunk)
    if data is None:
        raise ValueError(f"Failed to load registry {name}")
    return save(name, data)


def parse_v2(data: dict[str, Any]) -> list[dict[str, Any]]:
    entries = data["entries"]

    def expand(entry: dict[str, Any], src: str, dst: str) -> list[dict[str, Any]]:
        values = entry.pop(src)
        entry.setdefault("primary", False)
        return [{**entry, dst: value} for value in values]

    return list(
        itertools.chain.from_iterable(
            expand(entry, src=data["expand_from"], dst=data["expand_into"]) for entry in entries
        )
    )


def save(name: Key, data: Value) -> Value:
    _registry[name] = data
    return data


def build_index(
    base_name: str,
    index_name: str,
    key: str | tuple[str, ...],
    accumulate: bool = False,
    **predicate: Any,
) -> None:
    def make_key(entry: dict[Key, Any]) -> tuple | str:
        return tuple(entry[k] for k in key) if isinstance(key, tuple) else entry[key]

    def match(entry: dict[Key, Any]) -> bool:
        return all(entry[k] == v for k, v in predicate.items())

    base = get(base_name)
    assert isinstance(base, list)
    if accumulate:
        data = defaultdict(list)
        for entry in base:
            if not match(entry):
                continue
            index_key = make_key(entry)
            if index_key and all(index_key):
                data[index_key].append(entry)
        save(index_name, dict(data))
    else:
        entries = {}
        for entry in base:
            if not match(entry):
                continue
            entries[make_key(entry)] = entry
        save(index_name, entries)


def manipulate(name: Key, func: Callable) -> None:
    registry = get(name)
    if isinstance(registry, dict):
        for key, value in registry.items():
            registry[key] = func(key, value)
    elif isinstance(registry, list):
        registry = [func(item) for item in registry]
    save(name, registry)
