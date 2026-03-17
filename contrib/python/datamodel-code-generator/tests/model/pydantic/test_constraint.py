from __future__ import annotations

import pytest

from datamodel_code_generator.model.pydantic.base_model import Constraints
from datamodel_code_generator.types import UnionIntFloat


@pytest.mark.parametrize(
    ("gt", "expected"),
    [
        (None, False),
        (4, True),
        (0, True),
        (0.0, True),
    ],
)
def test_constraint(gt: float | None, expected: bool) -> None:
    constraints = Constraints()
    constraints.gt = UnionIntFloat(gt) if gt is not None else None
    assert constraints.has_constraints == expected
