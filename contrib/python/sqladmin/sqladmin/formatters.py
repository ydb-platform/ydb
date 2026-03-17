from typing import Any

from markupsafe import Markup


def empty_formatter(value: Any) -> str:
    """Return empty string for `None` value"""
    return ""


def bool_formatter(value: bool) -> Markup:
    """Return check icon if value is `True` or X otherwise."""
    icon_class = "fa-check text-success" if value else "fa-times text-danger"
    return Markup("<i class='fa {}'></i>").format(icon_class)


BASE_FORMATTERS = {
    type(None): empty_formatter,
    bool: bool_formatter,
}
