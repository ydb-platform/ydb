import importlib
import re
import sys
import warnings

import pytest

import josepy


@pytest.mark.skipif(sys.version_info[:2] != (3, 8), reason="requires Python 3.8")
def test_warns() -> None:
    with pytest.warns(DeprecationWarning, match=re.escape(r"Python 3.8 support")):
        importlib.reload(josepy)


@pytest.mark.skipif(sys.version_info[:2] == (3, 8), reason="requires Python != 3.8")
def test_does_not_warn() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        importlib.reload(josepy)


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
