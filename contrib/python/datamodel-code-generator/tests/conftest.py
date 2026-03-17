from __future__ import annotations

import pytest

from datamodel_code_generator import MIN_VERSION


@pytest.fixture(scope="session")
def min_version() -> str:
    return f"3.{MIN_VERSION}"
