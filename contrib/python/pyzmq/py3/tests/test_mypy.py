"""
Test our typing with mypy
"""

import os
import sys
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen

import pytest

pytest.importorskip("mypy")

repo_root = Path(__file__).parents[1]


print(repo_root)
examples_dir = repo_root / "examples"
mypy_dir = repo_root / "mypy_tests"


def run_mypy(*mypy_args):
    """Run mypy for a path

    Captures output and reports it on errors
    """
    p = Popen(
        [sys.executable, "-m", "mypy"] + list(mypy_args), stdout=PIPE, stderr=STDOUT
    )
    o, _ = p.communicate()
    out = o.decode("utf8", "replace")
    print(out)
    assert p.returncode == 0, out


examples = [path.name for path in examples_dir.glob("*") if path.is_dir()]


@pytest.mark.parametrize("example", examples)
def test_mypy_example(example):
    example_dir = examples_dir / example
    run_mypy("--disallow-untyped-calls", str(example_dir))


mypy_tests = [p.name for p in mypy_dir.glob("*.py")]


@pytest.mark.parametrize("filename", mypy_tests)
def test_mypy(filename):
    run_mypy("--disallow-untyped-calls", str(mypy_dir / filename))
