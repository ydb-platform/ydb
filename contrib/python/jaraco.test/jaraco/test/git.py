from __future__ import annotations

import shutil
import subprocess

import pytest


def _resolve_git() -> str | None:
    """Locate a functional git executable if available."""
    try:
        subprocess.run(
            ['git', '--version'],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return None
    return shutil.which('git')


@pytest.fixture
def ensure_git() -> None:
    """
    Require a working git executable for the test.

    >>> getfixture('ensure_git')
    """
    _resolve_git() or pytest.skip("'git' command unavailable")
