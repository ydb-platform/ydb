from typing import List

import pytest

from typeguard import TypeCheckWarning, check_type, config, typechecked, warn_on_error


def test_check_type(recwarn):
    with pytest.warns(TypeCheckWarning) as warning:
        check_type(1, str, typecheck_fail_callback=warn_on_error)

    assert len(warning.list) == 1
    assert warning.list[0].filename == __file__
    assert warning.list[0].lineno == test_check_type.__code__.co_firstlineno + 2


def test_typechecked(monkeypatch, recwarn):
    @typechecked
    def foo() -> List[int]:
        return ["aa"]  # type: ignore[list-item]

    monkeypatch.setattr(config, "typecheck_fail_callback", warn_on_error)
    with pytest.warns(TypeCheckWarning) as warning:
        foo()

    assert len(warning.list) == 1
    assert warning.list[0].filename == __file__
    assert warning.list[0].lineno == test_typechecked.__code__.co_firstlineno + 3
