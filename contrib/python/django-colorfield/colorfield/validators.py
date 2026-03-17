import re

from django.core.validators import RegexValidator
from django.utils.translation import gettext_lazy as _

COLOR_HEX_RE = re.compile("^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$")
color_hex_validator = RegexValidator(
    COLOR_HEX_RE,
    _("Enter a valid hex color, eg. #000000"),
    "invalid",
)


COLOR_HEXA_RE = re.compile("#([A-Fa-f0-9]{8}|[A-Fa-f0-9]{4})$")
color_hexa_validator = RegexValidator(
    COLOR_HEXA_RE,
    _("Enter a valid hexa color, eg. #00000000"),
    "invalid",
)

COLOR_RGB_RE = re.compile(
    # prefix and opening parenthesis
    r"^rgb\("
    # first number: red channel
    r"(\d{1,3})"
    # comma and optional space
    r",\s?"
    # second number: green channel
    r"(\d{1,3})"
    # comma and optional space
    r",\s?"
    # third number: blue channel
    r"(\d{1,3})"
    # closing parenthesis
    r"\)$"
)
color_rgb_validator = RegexValidator(
    COLOR_RGB_RE,
    _("Enter a valid rgb color, eg. rgb(128, 128, 128)"),
    "invalid",
)
COLOR_RGBA_RE = re.compile(
    # prefix and opening parenthesis
    r"^rgba\("
    # first number: red channel
    r"(\d{1,3})"
    # comma and optional space
    r",\s?"
    # second number: green channel
    r"(\d{1,3})"
    # comma and optional space
    r",\s?"
    # third number: blue channel
    r"(\d{1,3})"
    # comma and optional space
    r",\s?"
    # alpha channel: decimal number between 0 and 1
    r"(0(\.\d{1,2})?|1(\.0)?)"
    # closing parenthesis
    r"\)$"
)
color_rgba_validator = RegexValidator(
    COLOR_RGBA_RE,
    _("Enter a valid rgba color, eg. rgba(128, 128, 128, 0.5)"),
    "invalid",
)
