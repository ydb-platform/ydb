import json
import os
import platform
import subprocess

import pytest

POSITIVE_FILE = "positive.py"
NEGATIVE_FILE = "negative.py"

pytestmark = [
    pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="MyPy does not work with PyPy yet",
    )
]


def get_mypy_cmd(filename: str) -> list[str]:
    return ["mypy", "-O", "json", "--strict", filename]


def get_negative_mypy_output() -> str:
    """
    Get the output from running mypy on the negative examples file.
    """
    process = subprocess.run(
        get_mypy_cmd(NEGATIVE_FILE), stdout=subprocess.PIPE, check=False
    )
    output = process.stdout.decode()
    assert output
    return output


def get_expected_errors() -> dict[int, str]:
    """
    Extract the expected errors from comments in the negative examples file.
    """
    with open(NEGATIVE_FILE) as f:
        lines = f.readlines()

    expected = {}

    for idx, line in enumerate(lines):
        line = line.rstrip()
        if "# error" in line:
            expected[idx + 1] = line[line.index("# error") + 9 :]

    # Sanity check.  Should update if negative.py changes.
    assert len(expected) == 9
    return expected


def get_mypy_errors() -> dict[int, str]:
    """
    Extract the errors from running mypy on the negative examples file.
    """
    mypy_output = get_negative_mypy_output()

    got = {}
    for line in mypy_output.splitlines():
        error = json.loads(line)
        got[error["line"]] = f"{error['message']}  [{error['code']}]"

    return got


@pytest.fixture
def chdir_local() -> None:
    """
    Change to the local directory. This is so that mypy treats imports from
    typeguard as external imports instead of source code (which is handled
    differently by mypy).
    """
    os.chdir(os.path.dirname(__file__))


# Этот тест ожидает "mypy" в PATH
@pytest.mark.skip
@pytest.mark.usefixtures("chdir_local")
def test_positive() -> None:
    """
    Run mypy on the positive test file.  There should be no errors.
    """
    subprocess.check_call(get_mypy_cmd(POSITIVE_FILE))


# Этот тест ожидает "mypy" в PATH
@pytest.mark.skip
@pytest.mark.usefixtures("chdir_local")
def test_negative() -> None:
    """
    Run mypy on the negative test file. This should fail. The errors from mypy
    should match the comments in the file.
    """
    got_errors = get_mypy_errors()
    expected_errors = get_expected_errors()

    if set(got_errors) != set(expected_errors):
        raise RuntimeError(
            f"Expected error lines {set(expected_errors)} does not "
            + f"match mypy error lines {set(got_errors)}."
        )

    mismatches = [
        (idx, expected_errors[idx], got_errors[idx])
        for idx in expected_errors
        if expected_errors[idx] != got_errors[idx]
    ]
    for idx, expected, got in mismatches:
        print(f"Line {idx}", f"Expected: {expected}", f"Got:      {got}", sep="\n\t")

    if mismatches:
        raise RuntimeError("Error messages changed")
