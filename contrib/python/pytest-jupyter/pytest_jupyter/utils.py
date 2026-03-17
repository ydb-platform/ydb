"""Utilities for pytest-jupyter."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from pathlib import Path


def mkdir(tmp_path: Path, *parts: str) -> Path:
    """Make a directory given extra path parts."""
    new_path = tmp_path.joinpath(*parts)
    if not new_path.exists():
        new_path.mkdir(parents=True)
    return new_path
