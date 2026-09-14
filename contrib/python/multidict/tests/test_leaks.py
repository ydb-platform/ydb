import pathlib
import platform
import subprocess
import sys

import pytest

IS_PYPY = platform.python_implementation() == "PyPy"


@pytest.mark.parametrize(
    ("script"),
    (
        "multidict_extend_dict.py",
        "multidict_extend_multidict.py",
        "multidict_extend_tuple.py",
        "multidict_update_multidict.py",
        "multidict_pop.py",
    ),
)
@pytest.mark.leaks
@pytest.mark.skipif(IS_PYPY, reason="leak testing is not supported on PyPy")
def test_leak(script: str) -> None:
    """Run isolated leak test script and check for leaks."""
    leak_test_script = pathlib.Path(__file__).parent.joinpath("isolated", script)

    subprocess.run(
        [sys.executable, "-u", str(leak_test_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
