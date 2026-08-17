from __future__ import annotations

import pytest
from test_prettytable import CITY_DATA, CITY_DATA_HEADER

from prettytable.colortable import RESET_CODE, ColorTable, Theme, Themes

TYPE_CHECKING = False
if TYPE_CHECKING:
    from prettytable import PrettyTable


@pytest.fixture
def row_colortable() -> PrettyTable:
    table = ColorTable()
    table.field_names = CITY_DATA_HEADER
    for row in CITY_DATA:
        table.add_row(row)
    return table


@pytest.fixture
def color_theme() -> Theme:
    return Theme(
        default_color="31",
        vertical_color="32",
        horizontal_color="33",
        junction_color="34",
    )


class TestColorTable:
    def test_themeless(
        self, row_prettytable: PrettyTable, row_colortable: ColorTable
    ) -> None:
        # Not worth the logic customizing the reset code
        # For now we'll just get rid of it
        assert (
            row_colortable.get_string().replace(RESET_CODE, "")
            == row_prettytable.get_string()
        )

    def test_theme_setter(self, color_theme: Theme) -> None:
        table1 = ColorTable(theme=color_theme)

        table2 = ColorTable()
        table2.theme = color_theme

        assert table1.theme == table2.theme

        dict1 = table1.__dict__
        dict2 = table2.__dict__

        # So we don't compare functions
        for func in ("_sort_key", "_row_filter"):
            del dict1[func]
            del dict2[func]

        assert dict1 == dict2


class TestFormatCode:
    def test_basic(self) -> None:
        assert Theme.format_code("31") == "\x1b[31m"

    def test_prefix(self) -> None:
        assert Theme.format_code("\x1b[35m") == "\x1b[35m"

    def test_escapes(self) -> None:
        assert Theme.format_code("\033[41m") == "\x1b[41m"
        assert Theme.format_code("\u001b[41m") == "\x1b[41m"

    def test_empty(self) -> None:
        assert Theme.format_code("") == ""

    def test_stripped(self) -> None:
        assert Theme.format_code("\t\t     \t") == ""

    def test_multiple(self) -> None:
        assert Theme.format_code("30;42") == "\x1b[30;42m"
        assert Theme.format_code("\x1b[30;42m") == "\x1b[30;42m"


class TestColorTableRendering:
    """Tests for the rendering of the color table

    Methods
    -------
    test_color_table_rendering
        Tests the color table rendering using the default alignment (`'c'`)
    """

    @pytest.mark.parametrize(
        ["with_title", "with_header"],
        [
            (False, True),  # the default
            (True, True),  # titled
            (True, False),  # titled, no header
            (True, True),  # both title and header
        ],
    )
    def test_color_table_rendering(self, with_title: bool, with_header: bool) -> None:
        """Tests the color table rendering using the default alignment (`'c'`)"""
        chars = {
            "+": "\x1b[36m+\x1b[0m\x1b[96m",
            "-": "\x1b[34m-\x1b[0m\x1b[96m",
            "|": "\x1b[34m|\x1b[0m\x1b[96m",
            " ": " ",
        }

        plus = chars.get("+")
        minus = chars.get("-")
        pipe = chars.get("|")
        space = chars.get(" ")
        assert isinstance(plus, str)
        assert isinstance(minus, str)
        assert isinstance(pipe, str)
        assert isinstance(space, str)

        # +-----------------------+
        # |        Efforts        |
        # +---+---+---+---+---+---+
        # | A | B | C | D | E | f |
        # +---+---+---+---+---+---+
        # | 1 | 2 | 3 | 4 | 5 | 6 |
        # +---+---+---+---+---+---+

        header = (
            plus + minus * 23 + plus,
            pipe + space * 8 + "Efforts" + space * 8 + pipe,
            (plus + minus * 3) * 6 + plus,
        )

        body = (
            "".join(pipe + space + char + space for char in "ABCDEF") + pipe,
            (plus + minus * 3) * 6 + plus,
            "".join(pipe + space + char + space for char in "123456") + pipe,
            (plus + minus * 3) * 6 + plus,
        )

        if with_title:
            header_str = str("\n".join(header))
        else:
            header_str = str(header[2])
        if with_header:
            body_str = str("\n".join(body))
        else:
            body_str = str("\n".join(body[2:]))

        table = ColorTable(
            ("A", "B", "C", "D", "E", "F"),
            theme=Themes.OCEAN,
        )

        if with_title:
            table.title = "Efforts"
        table.header = with_header
        table.add_row([1, 2, 3, 4, 5, 6])

        expected = f"{header_str}\n{body_str}\x1b[0m"
        result = str(table)

        assert expected == result

    def test_all_themes(self) -> None:
        """Tests rendering with all available themes"""
        table = ColorTable(
            ("A", "B", "C", "D", "E", "F"),
        )
        table.title = "Theme Test"
        table.add_row([1, 2, 3, 4, 5, 6])

        for theme in vars(Themes).values():
            if isinstance(theme, Theme):
                table.theme = theme
                result = str(table)
                assert result  # Simple check to ensure rendering doesn't fail
