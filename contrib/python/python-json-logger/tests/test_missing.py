### IMPORTS
### ============================================================================
## Future
from __future__ import annotations

## Standard Library

## Installed
import pytest

## Application
import pythonjsonlogger
from pythonjsonlogger.utils import package_is_available
from pythonjsonlogger.exception import MissingPackageError

### CONSTANTS
### ============================================================================
MISSING_PACKAGE_NAME = "package_name_is_definintely_not_available"
MISSING_PACKAGE_EXTRA = "package_extra_that_is_unique"


### TESTS
### ============================================================================
def test_package_is_available():
    assert package_is_available("json")
    return


def test_package_not_available():
    assert not package_is_available(MISSING_PACKAGE_NAME)
    return


def test_package_not_available_throw():
    with pytest.raises(MissingPackageError) as e:
        package_is_available(MISSING_PACKAGE_NAME, throw_error=True)
    assert MISSING_PACKAGE_NAME in e.value.msg
    assert MISSING_PACKAGE_EXTRA not in e.value.msg
    return


def test_package_not_available_throw_extras():
    with pytest.raises(MissingPackageError) as e:
        package_is_available(
            MISSING_PACKAGE_NAME, throw_error=True, extras_name=MISSING_PACKAGE_EXTRA
        )
    assert MISSING_PACKAGE_NAME in e.value.msg
    assert MISSING_PACKAGE_EXTRA in e.value.msg
    return


## Python JSON Logger Specific
## -----------------------------------------------------------------------------
if not pythonjsonlogger.ORJSON_AVAILABLE:

    def test_orjson_import_error():
        with pytest.raises(MissingPackageError, match="orjson"):
            import pythonjsonlogger.orjson
        return


if not pythonjsonlogger.MSGSPEC_AVAILABLE:

    def test_msgspec_import_error():
        with pytest.raises(MissingPackageError, match="msgspec"):
            import pythonjsonlogger.msgspec
        return
