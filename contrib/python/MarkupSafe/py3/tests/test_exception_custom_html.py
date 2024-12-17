from __future__ import annotations

import pytest

from markupsafe import escape


class CustomHtmlThatRaises:
    def __html__(self) -> str:
        raise ValueError(123)


def test_exception_custom_html() -> None:
    """Checks whether exceptions in custom __html__ implementations are
    propagated correctly.

    There was a bug in the native implementation at some point:
    https://github.com/pallets/markupsafe/issues/108
    """
    obj = CustomHtmlThatRaises()

    with pytest.raises(ValueError):
        escape(obj)
