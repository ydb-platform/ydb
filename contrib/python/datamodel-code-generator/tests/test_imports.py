from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from datamodel_code_generator.imports import Import, Imports

if TYPE_CHECKING:
    from collections.abc import Sequence


@pytest.mark.parametrize(
    ("inputs", "value"),
    [
        ([(None, "foo")], "import foo"),
        ([(".", "foo")], "from . import foo"),
        ([("bar", "foo")], "from bar import foo"),
        ([("bar", "foo"), ("bar", "baz")], "from bar import baz, foo"),
        ([("bar", "foo"), ("rab", "oof")], "from bar import foo\nfrom rab import oof"),
        ([("bar", "foo"), ("bar", "foo")], "from bar import foo"),
        ([(None, "foo.baz")], "import foo.baz"),
    ],
)
def test_dump(inputs: Sequence[tuple[str | None, str]], value: str) -> None:
    """Test creating import lines."""

    imports = Imports()
    imports.append(Import(from_=from_, import_=import_) for from_, import_ in inputs)

    assert str(imports) == value
