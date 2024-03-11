from typing import Type

import pytest

from multidict import MultiMapping


def test_guard_items(
    case_sensitive_multidict_class: Type[MultiMapping[str]],
) -> None:
    md = case_sensitive_multidict_class({"a": "b"})
    it = iter(md.items())
    md["a"] = "c"
    with pytest.raises(RuntimeError):
        next(it)


def test_guard_keys(
    case_sensitive_multidict_class: Type[MultiMapping[str]],
) -> None:
    md = case_sensitive_multidict_class({"a": "b"})
    it = iter(md.keys())
    md["a"] = "c"
    with pytest.raises(RuntimeError):
        next(it)


def test_guard_values(
    case_sensitive_multidict_class: Type[MultiMapping[str]],
) -> None:
    md = case_sensitive_multidict_class({"a": "b"})
    it = iter(md.values())
    md["a"] = "c"
    with pytest.raises(RuntimeError):
        next(it)
