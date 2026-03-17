### IMPORTS
### ============================================================================
## Future
from __future__ import annotations

## Standard Library
import subprocess
import sys

## Installed
import pytest

## Application
import pythonjsonlogger


### TESTS
### ============================================================================
def test_jsonlogger_deprecated():
    with pytest.deprecated_call():
        import pythonjsonlogger.jsonlogger
    return


def test_jsonlogger_reserved_attrs_deprecated():
    with pytest.deprecated_call():
        # Note: We use json instead of jsonlogger as jsonlogger will also produce
        # a DeprecationWarning and we specifically want the one for RESERVED_ATTRS
        pythonjsonlogger.json.RESERVED_ATTRS
    return


@pytest.mark.parametrize(
    "command",
    [
        "from pythonjsonlogger import jsonlogger",
        "import pythonjsonlogger.jsonlogger",
        "from pythonjsonlogger.jsonlogger import JsonFormatter",
        "from pythonjsonlogger.jsonlogger import RESERVED_ATTRS",
    ],
)
def test_import(command: str):
    import os
    env = os.environ.copy()
    env["Y_PYTHON_ENTRY_POINT"] = ":main"
    output = subprocess.check_output([sys.executable, "-c", f"{command};print('OK')"], env=env)
    assert output.strip() == b"OK"
    return
