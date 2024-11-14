from __future__ import annotations

from .prettytable import PrettyTable

try:
    from colorama import init

    init()
except ImportError:
    pass


RESET_CODE = "\x1b[0m"


class Theme:
    def __init__(
        self,
        default_color: str = "",
        vertical_char: str = "|",
        vertical_color: str = "",
        horizontal_char: str = "-",
        horizontal_color: str = "",
        junction_char: str = "+",
        junction_color: str = "",
    ) -> None:
        self.default_color = Theme.format_code(default_color)
        self.vertical_char = vertical_char
        self.vertical_color = Theme.format_code(vertical_color)
        self.horizontal_char = horizontal_char
        self.horizontal_color = Theme.format_code(horizontal_color)
        self.junction_char = junction_char
        self.junction_color = Theme.format_code(junction_color)

    @staticmethod
    def format_code(s: str) -> str:
        """Takes string and intelligently puts it into an ANSI escape sequence"""
        if s.strip() == "":
            return ""
        elif s.startswith("\x1b["):
            return s
        else:
            return f"\x1b[{s}m"


class Themes:
    DEFAULT = Theme()
    DYSLEXIA_FRIENDLY = Theme(
        default_color="38;5;223",
        vertical_color="38;5;22",
        horizontal_color="38;5;22",
        junction_color="38;5;58",
    )
    EARTH = Theme(
        default_color="33",
        vertical_color="38;5;94",
        horizontal_color="38;5;22",
        junction_color="38;5;130",
    )
    GLARE_REDUCTION = Theme(
        default_color="38;5;252",
        vertical_color="38;5;240",
        horizontal_color="38;5;240",
        junction_color="38;5;246",
    )
    HIGH_CONTRAST = Theme(
        default_color="97",
        vertical_color="91",
        horizontal_color="94",
        junction_color="93",
    )
    LAVENDER = Theme(
        default_color="38;5;183",
        vertical_color="35",
        horizontal_color="38;5;147",
        junction_color="38;5;219",
    )
    OCEAN = Theme(
        default_color="96",
        vertical_color="34",
        horizontal_color="34",
        junction_color="36",
    )
    OCEAN_DEEP = Theme(
        default_color="96",
        vertical_color="34",
        horizontal_color="36",
        junction_color="94",
    )
    PASTEL = Theme(
        default_color="38;5;223",
        vertical_color="38;5;152",
        horizontal_color="38;5;187",
        junction_color="38;5;157",
    )


class ColorTable(PrettyTable):
    def __init__(self, field_names=None, **kwargs) -> None:
        super().__init__(field_names=field_names, **kwargs)
        # TODO: Validate option

        self.theme = kwargs.get("theme") or Themes.DEFAULT

    @property
    def theme(self) -> Theme:
        return self._theme

    @theme.setter
    def theme(self, value: Theme) -> None:
        self._theme = value
        self.update_theme()

    def update_theme(self) -> None:
        theme = self._theme

        self._vertical_char = (
            theme.vertical_color
            + theme.vertical_char
            + RESET_CODE
            + theme.default_color
        )

        self._horizontal_char = (
            theme.horizontal_color
            + theme.horizontal_char
            + RESET_CODE
            + theme.default_color
        )

        self._junction_char = (
            theme.junction_color
            + theme.junction_char
            + RESET_CODE
            + theme.default_color
        )

    def get_string(self, **kwargs) -> str:
        return super().get_string(**kwargs) + RESET_CODE
