from __future__ import annotations

import datetime as dt
import io
import random
import sqlite3
from math import e, pi, sqrt
from typing import Any

import pytest
from pytest_lazy_fixtures import lf

import prettytable
from prettytable import (
    ALL,
    DEFAULT,
    DOUBLE_BORDER,
    FRAME,
    HEADER,
    MARKDOWN,
    MSWORD_FRIENDLY,
    NONE,
    ORGMODE,
    PLAIN_COLUMNS,
    RANDOM,
    SINGLE_BORDER,
    PrettyTable,
    from_csv,
    from_db_cursor,
    from_html,
    from_html_one,
    from_json,
)


def test_version() -> None:
    assert isinstance(prettytable.__version__, str)
    assert prettytable.__version__[0].isdigit()
    assert prettytable.__version__.count(".") >= 2
    assert prettytable.__version__[-1].isdigit()


def helper_table(*, rows: int = 3) -> PrettyTable:
    table = PrettyTable(["", "Field 1", "Field 2", "Field 3"])
    v = 1
    for row in range(rows):
        # Some have spaces, some not, to help test padding columns of different widths
        table.add_row([v, f"value {v}", f"value{v+1}", f"value{v+2}"])
        v += 3
    return table


@pytest.fixture
def row_prettytable() -> PrettyTable:
    # Row by row...
    table = PrettyTable()
    table.field_names = ["City name", "Area", "Population", "Annual Rainfall"]
    table.add_row(["Adelaide", 1295, 1158259, 600.5])
    table.add_row(["Brisbane", 5905, 1857594, 1146.4])
    table.add_row(["Darwin", 112, 120900, 1714.7])
    table.add_row(["Hobart", 1357, 205556, 619.5])
    table.add_row(["Sydney", 2058, 4336374, 1214.8])
    table.add_row(["Melbourne", 1566, 3806092, 646.9])
    table.add_row(["Perth", 5386, 1554769, 869.4])
    return table


@pytest.fixture
def col_prettytable() -> PrettyTable:
    # Column by column...
    table = PrettyTable()
    table.add_column(
        "City name",
        ["Adelaide", "Brisbane", "Darwin", "Hobart", "Sydney", "Melbourne", "Perth"],
    )
    table.add_column("Area", [1295, 5905, 112, 1357, 2058, 1566, 5386])
    table.add_column(
        "Population", [1158259, 1857594, 120900, 205556, 4336374, 3806092, 1554769]
    )
    table.add_column(
        "Annual Rainfall", [600.5, 1146.4, 1714.7, 619.5, 1214.8, 646.9, 869.4]
    )
    return table


@pytest.fixture
def mix_prettytable() -> PrettyTable:
    # A mix of both!
    table = PrettyTable()
    table.field_names = ["City name", "Area"]
    table.add_row(["Adelaide", 1295])
    table.add_row(["Brisbane", 5905])
    table.add_row(["Darwin", 112])
    table.add_row(["Hobart", 1357])
    table.add_row(["Sydney", 2058])
    table.add_row(["Melbourne", 1566])
    table.add_row(["Perth", 5386])
    table.add_column(
        "Population", [1158259, 1857594, 120900, 205556, 4336374, 3806092, 1554769]
    )
    table.add_column(
        "Annual Rainfall", [600.5, 1146.4, 1714.7, 619.5, 1214.8, 646.9, 869.4]
    )
    return table


class TestNoneOption:
    def test_none_char_valid_option(self) -> None:
        PrettyTable(["Field 1", "Field 2", "Field 3"], none_format="")

    def test_none_char_invalid_option(self) -> None:
        with pytest.raises(TypeError) as exc:
            PrettyTable(["Field 1", "Field 2", "Field 3"], none_format=2)
        assert "must be a string" in str(exc.value)

    def test_no_value_replace_none(self) -> None:
        table = PrettyTable(["Field 1", "Field 2", "Field 3"])
        table.add_row(["value 1", None, "value 2"])
        assert (
            table.get_string().strip()
            == """
+---------+---------+---------+
| Field 1 | Field 2 | Field 3 |
+---------+---------+---------+
| value 1 |   None  | value 2 |
+---------+---------+---------+
""".strip()
        )

    def test_no_value_replace_none_with_default_field_names(self) -> None:
        table = PrettyTable()
        table.add_row(["value 1", "None", "value 2"])
        assert (
            table.get_string().strip()
            == """
+---------+---------+---------+
| Field 1 | Field 2 | Field 3 |
+---------+---------+---------+
| value 1 |   None  | value 2 |
+---------+---------+---------+
""".strip()
        )

    def test_replace_none_all(self) -> None:
        table = PrettyTable(["Field 1", "Field 2", "Field 3"], none_format="N/A")
        table.add_row(["value 1", None, "None"])
        assert (
            table.get_string().strip()
            == """
+---------+---------+---------+
| Field 1 | Field 2 | Field 3 |
+---------+---------+---------+
| value 1 |   N/A   |   N/A   |
+---------+---------+---------+
""".strip()
        )

    def test_replace_none_by_col(self) -> None:
        table = PrettyTable(["Field 1", "Field 2", "Field 3"])
        table.none_format["Field 2"] = "N/A"
        table.none_format["Field 3"] = ""
        table.add_row(["value 1", None, None])
        assert (
            table.get_string().strip()
            == """
+---------+---------+---------+
| Field 1 | Field 2 | Field 3 |
+---------+---------+---------+
| value 1 |   N/A   |         |
+---------+---------+---------+
""".strip()
        )

    def test_replace_none_recompute_width(self) -> None:
        table = PrettyTable()
        table.add_row([None])
        table.none_format = "0123456789"
        assert (
            table.get_string().strip()
            == """
+------------+
|  Field 1   |
+------------+
| 0123456789 |
+------------+
""".strip()
        )

    def test_replace_none_maintain_width_on_recompute(self) -> None:
        table = PrettyTable()
        table.add_row(["Hello"])
        table.none_format = "0123456789"
        assert (
            table.get_string().strip()
            == """
+---------+
| Field 1 |
+---------+
|  Hello  |
+---------+
""".strip()
        )

    def test_replace_none_recompute_width_multi_column(self) -> None:
        table = PrettyTable()
        table.add_row(["Hello", None, "World"])
        table.none_format = "0123456789"
        assert (
            table.get_string().strip()
            == """
+---------+------------+---------+
| Field 1 |  Field 2   | Field 3 |
+---------+------------+---------+
|  Hello  | 0123456789 |  World  |
+---------+------------+---------+
""".strip()
        )


class TestBuildEquivalence:
    """Make sure that building a table row-by-row and column-by-column yield the same
    results"""

    @pytest.mark.parametrize(
        ["left_hand", "right_hand"],
        [
            (
                lf("row_prettytable"),
                lf("col_prettytable"),
            ),
            (
                lf("row_prettytable"),
                lf("mix_prettytable"),
            ),
        ],
    )
    def test_equivalence_ascii(
        self, left_hand: PrettyTable, right_hand: PrettyTable
    ) -> None:
        assert left_hand.get_string() == right_hand.get_string()

    @pytest.mark.parametrize(
        ["left_hand", "right_hand"],
        [
            (
                lf("row_prettytable"),
                lf("col_prettytable"),
            ),
            (
                lf("row_prettytable"),
                lf("mix_prettytable"),
            ),
        ],
    )
    def test_equivalence_html(
        self, left_hand: PrettyTable, right_hand: PrettyTable
    ) -> None:
        assert left_hand.get_html_string() == right_hand.get_html_string()

    @pytest.mark.parametrize(
        ["left_hand", "right_hand"],
        [
            (
                lf("row_prettytable"),
                lf("col_prettytable"),
            ),
            (
                lf("row_prettytable"),
                lf("mix_prettytable"),
            ),
        ],
    )
    def test_equivalence_latex(
        self, left_hand: PrettyTable, right_hand: PrettyTable
    ) -> None:
        assert left_hand.get_latex_string() == right_hand.get_latex_string()


class TestDeleteColumn:
    def test_delete_column(self) -> None:
        table = PrettyTable()
        table.add_column("City name", ["Adelaide", "Brisbane", "Darwin"])
        table.add_column("Area", [1295, 5905, 112])
        table.add_column("Population", [1158259, 1857594, 120900])
        table.del_column("Area")

        without_row = PrettyTable()
        without_row.add_column("City name", ["Adelaide", "Brisbane", "Darwin"])
        without_row.add_column("Population", [1158259, 1857594, 120900])

        assert table.get_string() == without_row.get_string()

    def test_delete_illegal_column_raises_error(self) -> None:
        table = PrettyTable()
        table.add_column("City name", ["Adelaide", "Brisbane", "Darwin"])

        with pytest.raises(ValueError):
            table.del_column("City not-a-name")


@pytest.fixture(scope="function")
def field_name_less_table() -> PrettyTable:
    table = PrettyTable()
    table.add_row(["Adelaide", 1295, 1158259, 600.5])
    table.add_row(["Brisbane", 5905, 1857594, 1146.4])
    table.add_row(["Darwin", 112, 120900, 1714.7])
    table.add_row(["Hobart", 1357, 205556, 619.5])
    table.add_row(["Sydney", 2058, 4336374, 1214.8])
    table.add_row(["Melbourne", 1566, 3806092, 646.9])
    table.add_row(["Perth", 5386, 1554769, 869.4])
    return table


class TestFieldNameLessTable:
    """Make sure that building and stringing a table with no fieldnames works fine"""

    def test_can_string_ascii(self, field_name_less_table: prettytable) -> None:
        output = field_name_less_table.get_string()
        assert "|  Field 1  | Field 2 | Field 3 | Field 4 |" in output
        assert "|  Adelaide |   1295  | 1158259 |  600.5  |" in output

    def test_can_string_html(self, field_name_less_table: prettytable) -> None:
        output = field_name_less_table.get_html_string()
        assert "<th>Field 1</th>" in output
        assert "<td>Adelaide</td>" in output

    def test_can_string_latex(self, field_name_less_table: prettytable) -> None:
        output = field_name_less_table.get_latex_string()
        assert "Field 1 & Field 2 & Field 3 & Field 4 \\\\" in output
        assert "Adelaide & 1295 & 1158259 & 600.5 \\\\" in output

    def test_add_field_names_later(self, field_name_less_table: prettytable) -> None:
        field_name_less_table.field_names = [
            "City name",
            "Area",
            "Population",
            "Annual Rainfall",
        ]
        assert (
            "City name | Area | Population | Annual Rainfall"
            in field_name_less_table.get_string()
        )


@pytest.fixture(scope="function")
def aligned_before_table() -> PrettyTable:
    table = PrettyTable()
    table.align = "r"
    table.field_names = ["City name", "Area", "Population", "Annual Rainfall"]
    table.add_row(["Adelaide", 1295, 1158259, 600.5])
    table.add_row(["Brisbane", 5905, 1857594, 1146.4])
    table.add_row(["Darwin", 112, 120900, 1714.7])
    table.add_row(["Hobart", 1357, 205556, 619.5])
    table.add_row(["Sydney", 2058, 4336374, 1214.8])
    table.add_row(["Melbourne", 1566, 3806092, 646.9])
    table.add_row(["Perth", 5386, 1554769, 869.4])
    return table


@pytest.fixture(scope="function")
def aligned_after_table() -> PrettyTable:
    table = PrettyTable()
    table.field_names = ["City name", "Area", "Population", "Annual Rainfall"]
    table.add_row(["Adelaide", 1295, 1158259, 600.5])
    table.add_row(["Brisbane", 5905, 1857594, 1146.4])
    table.add_row(["Darwin", 112, 120900, 1714.7])
    table.add_row(["Hobart", 1357, 205556, 619.5])
    table.add_row(["Sydney", 2058, 4336374, 1214.8])
    table.add_row(["Melbourne", 1566, 3806092, 646.9])
    table.add_row(["Perth", 5386, 1554769, 869.4])
    table.align = "r"
    return table


class TestAlignment:
    """Make sure alignment works regardless of when it was set"""

    def test_aligned_ascii(
        self, aligned_before_table: prettytable, aligned_after_table: prettytable
    ) -> None:
        before = aligned_before_table.get_string()
        after = aligned_after_table.get_string()
        assert before == after

    def test_aligned_html(
        self, aligned_before_table: prettytable, aligned_after_table: prettytable
    ) -> None:
        before = aligned_before_table.get_html_string()
        after = aligned_after_table.get_html_string()
        assert before == after

    def test_aligned_latex(
        self, aligned_before_table: prettytable, aligned_after_table: prettytable
    ) -> None:
        before = aligned_before_table.get_latex_string()
        after = aligned_after_table.get_latex_string()
        assert before == after


@pytest.fixture(scope="function")
def city_data_prettytable() -> PrettyTable:
    """Just build the Australian capital city data example table."""
    table = PrettyTable(["City name", "Area", "Population", "Annual Rainfall"])
    table.add_row(["Adelaide", 1295, 1158259, 600.5])
    table.add_row(["Brisbane", 5905, 1857594, 1146.4])
    table.add_row(["Darwin", 112, 120900, 1714.7])
    table.add_row(["Hobart", 1357, 205556, 619.5])
    table.add_row(["Sydney", 2058, 4336374, 1214.8])
    table.add_row(["Melbourne", 1566, 3806092, 646.9])
    table.add_row(["Perth", 5386, 1554769, 869.4])
    return table


@pytest.fixture(scope="function")
def city_data_from_csv() -> PrettyTable:
    csv_string = """City name, Area, Population, Annual Rainfall
    Sydney, 2058, 4336374, 1214.8
    Melbourne, 1566, 3806092, 646.9
    Brisbane, 5905, 1857594, 1146.4
    Perth, 5386, 1554769, 869.4
    Adelaide, 1295, 1158259, 600.5
    Hobart, 1357, 205556, 619.5
    Darwin, 0112, 120900, 1714.7"""
    csv_fp = io.StringIO(csv_string)
    return from_csv(csv_fp)


class TestOptionOverride:
    """Make sure all options are properly overwritten by get_string."""

    def test_border(self, city_data_prettytable: prettytable) -> None:
        default = city_data_prettytable.get_string()
        override = city_data_prettytable.get_string(border=False)
        assert default != override

    def test_header(self, city_data_prettytable) -> None:
        default = city_data_prettytable.get_string()
        override = city_data_prettytable.get_string(header=False)
        assert default != override

    def test_hrules_all(self, city_data_prettytable) -> None:
        default = city_data_prettytable.get_string()
        override = city_data_prettytable.get_string(hrules=ALL)
        assert default != override

    def test_hrules_none(self, city_data_prettytable) -> None:
        default = city_data_prettytable.get_string()
        override = city_data_prettytable.get_string(hrules=NONE)
        assert default != override


class TestOptionAttribute:
    """Make sure all options which have an attribute interface work as they should.
    Also make sure option settings are copied correctly when a table is cloned by
    slicing."""

    def test_set_for_all_columns(self, city_data_prettytable) -> None:
        city_data_prettytable.field_names = sorted(city_data_prettytable.field_names)
        city_data_prettytable.align = "l"
        city_data_prettytable.max_width = 10
        city_data_prettytable.start = 2
        city_data_prettytable.end = 4
        city_data_prettytable.sortby = "Area"
        city_data_prettytable.reversesort = True
        city_data_prettytable.header = True
        city_data_prettytable.border = False
        city_data_prettytable.hrules = True
        city_data_prettytable.int_format = "4"
        city_data_prettytable.float_format = "2.2"
        city_data_prettytable.padding_width = 2
        city_data_prettytable.left_padding_width = 2
        city_data_prettytable.right_padding_width = 2
        city_data_prettytable.vertical_char = "!"
        city_data_prettytable.horizontal_char = "~"
        city_data_prettytable.junction_char = "*"
        city_data_prettytable.top_junction_char = "@"
        city_data_prettytable.bottom_junction_char = "#"
        city_data_prettytable.right_junction_char = "$"
        city_data_prettytable.left_junction_char = "%"
        city_data_prettytable.top_right_junction_char = "^"
        city_data_prettytable.top_left_junction_char = "&"
        city_data_prettytable.bottom_right_junction_char = "("
        city_data_prettytable.bottom_left_junction_char = ")"
        city_data_prettytable.format = True
        city_data_prettytable.attributes = {"class": "prettytable"}
        assert (
            city_data_prettytable.get_string() == city_data_prettytable[:].get_string()
        )

    def test_set_for_one_column(self, city_data_prettytable) -> None:
        city_data_prettytable.align["Rainfall"] = "l"
        city_data_prettytable.max_width["Name"] = 10
        city_data_prettytable.int_format["Population"] = "4"
        city_data_prettytable.float_format["Area"] = "2.2"
        assert (
            city_data_prettytable.get_string() == city_data_prettytable[:].get_string()
        )

    def test_preserve_internal_border(self) -> None:
        table = PrettyTable(preserve_internal_border=True)
        assert table.preserve_internal_border is True


@pytest.fixture(scope="module")
def db_cursor():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    yield cur
    cur.close()
    conn.close()


@pytest.fixture(scope="module")
def init_db(db_cursor):
    db_cursor.execute(
        "CREATE TABLE cities "
        "(name TEXT, area INTEGER, population INTEGER, rainfall REAL)"
    )
    db_cursor.execute('INSERT INTO cities VALUES ("Adelaide", 1295, 1158259, 600.5)')
    db_cursor.execute('INSERT INTO cities VALUES ("Brisbane", 5905, 1857594, 1146.4)')
    db_cursor.execute('INSERT INTO cities VALUES ("Darwin", 112, 120900, 1714.7)')
    db_cursor.execute('INSERT INTO cities VALUES ("Hobart", 1357, 205556, 619.5)')
    db_cursor.execute('INSERT INTO cities VALUES ("Sydney", 2058, 4336374, 1214.8)')
    db_cursor.execute('INSERT INTO cities VALUES ("Melbourne", 1566, 3806092, 646.9)')
    db_cursor.execute('INSERT INTO cities VALUES ("Perth", 5386, 1554769, 869.4)')
    yield
    db_cursor.execute("DROP TABLE cities")


class TestBasic:
    """Some very basic tests."""

    def test_table_rows(self, city_data_prettytable: PrettyTable) -> None:
        rows = city_data_prettytable.rows
        assert len(rows) == 7
        assert rows[0] == ["Adelaide", 1295, 1158259, 600.5]

    def _test_no_blank_lines(self, table: prettytable) -> None:
        string = table.get_string()
        lines = string.split("\n")
        assert "" not in lines

    def _test_all_length_equal(self, table: prettytable) -> None:
        string = table.get_string()
        lines = string.split("\n")
        lengths = [len(line) for line in lines]
        lengths = set(lengths)
        assert len(lengths) == 1

    def test_no_blank_lines(self, city_data_prettytable) -> None:
        """No table should ever have blank lines in it."""
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal(self, city_data_prettytable) -> None:
        """All lines in a table should be of the same length."""
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_with_title(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.title = "My table"
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_with_title(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.title = "My table"
        self._test_all_length_equal(city_data_prettytable)

    def test_all_lengths_equal_with_long_title(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length, even with a long title."""
        city_data_prettytable.title = "My table (75 characters wide) " + "=" * 45
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_without_border(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.border = False
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_without_border(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.border = False
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_without_header(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.header = False
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_without_header(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.header = False
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_with_hrules_none(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.hrules = NONE
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_with_hrules_none(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.hrules = NONE
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_with_hrules_all(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.hrules = ALL
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_with_hrules_all(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.hrules = ALL
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_with_style_msword(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.set_style(MSWORD_FRIENDLY)
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_with_style_msword(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.set_style(MSWORD_FRIENDLY)
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_with_int_format(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.int_format = "04"
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_with_int_format(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.int_format = "04"
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_with_float_format(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        city_data_prettytable.float_format = "6.2f"
        self._test_no_blank_lines(city_data_prettytable)

    def test_all_lengths_equal_with_float_format(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        city_data_prettytable.float_format = "6.2f"
        self._test_all_length_equal(city_data_prettytable)

    def test_no_blank_lines_from_csv(self, city_data_from_csv: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        self._test_no_blank_lines(city_data_from_csv)

    def test_all_lengths_equal_from_csv(self, city_data_from_csv: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        self._test_all_length_equal(city_data_from_csv)

    @pytest.mark.usefixtures("init_db")
    def test_no_blank_lines_from_db(self, db_cursor) -> None:
        """No table should ever have blank lines in it."""
        db_cursor.execute("SELECT * FROM cities")
        pt = from_db_cursor(db_cursor)
        self._test_no_blank_lines(pt)

    @pytest.mark.usefixtures("init_db")
    def test_all_lengths_equal_from_db(self, db_cursor) -> None:
        """No table should ever have blank lines in it."""
        db_cursor.execute("SELECT * FROM cities")
        pt = from_db_cursor(db_cursor)
        self._test_all_length_equal(pt)


class TestEmptyTable:
    """Make sure the print_empty option works"""

    def test_print_empty_true(self, city_data_prettytable: PrettyTable) -> None:
        table = PrettyTable()
        table.field_names = ["City name", "Area", "Population", "Annual Rainfall"]

        assert table.get_string(print_empty=True) != ""
        assert table.get_string(print_empty=True) != city_data_prettytable.get_string(
            print_empty=True
        )

    def test_print_empty_false(self, city_data_prettytable: PrettyTable) -> None:
        table = PrettyTable()
        table.field_names = ["City name", "Area", "Population", "Annual Rainfall"]

        assert table.get_string(print_empty=False) == ""
        assert table.get_string(print_empty=False) != city_data_prettytable.get_string(
            print_empty=False
        )

    def test_interaction_with_border(self) -> None:
        table = PrettyTable()
        table.field_names = ["City name", "Area", "Population", "Annual Rainfall"]

        assert table.get_string(border=False, print_empty=True) == ""


class TestSlicing:
    def test_slice_all(self, city_data_prettytable: PrettyTable) -> None:
        table = city_data_prettytable[:]
        assert city_data_prettytable.get_string() == table.get_string()

    def test_slice_first_two_rows(self, city_data_prettytable: PrettyTable) -> None:
        table = city_data_prettytable[0:2]
        string = table.get_string()
        assert len(string.split("\n")) == 6
        assert "Adelaide" in string
        assert "Brisbane" in string
        assert "Melbourne" not in string
        assert "Perth" not in string

    def test_slice_last_two_rows(self, city_data_prettytable: PrettyTable) -> None:
        table = city_data_prettytable[-2:]
        string = table.get_string()
        assert len(string.split("\n")) == 6
        assert "Adelaide" not in string
        assert "Brisbane" not in string
        assert "Melbourne" in string
        assert "Perth" in string


class TestSorting:
    def test_sort_by_different_per_columns(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        city_data_prettytable.sortby = city_data_prettytable.field_names[0]
        old = city_data_prettytable.get_string()
        for field in city_data_prettytable.field_names[1:]:
            city_data_prettytable.sortby = field
            new = city_data_prettytable.get_string()
            assert new != old

    def test_reverse_sort(self, city_data_prettytable: PrettyTable) -> None:
        for field in city_data_prettytable.field_names:
            city_data_prettytable.sortby = field
            city_data_prettytable.reversesort = False
            forward = city_data_prettytable.get_string()
            city_data_prettytable.reversesort = True
            backward = city_data_prettytable.get_string()
            forward_lines = forward.split("\n")[2:]  # Discard header lines
            backward_lines = backward.split("\n")[2:]
            backward_lines.reverse()
            assert forward_lines == backward_lines

    def test_sort_key(self, city_data_prettytable: PrettyTable) -> None:
        # Test sorting by length of city name
        def key(vals):
            vals[0] = len(vals[0])
            return vals

        city_data_prettytable.sortby = "City name"
        city_data_prettytable.sort_key = key
        assert (
            city_data_prettytable.get_string().strip()
            == """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|   Perth   | 5386 |  1554769   |      869.4      |
|   Darwin  | 112  |   120900   |      1714.7     |
|   Hobart  | 1357 |   205556   |      619.5      |
|   Sydney  | 2058 |  4336374   |      1214.8     |
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
| Melbourne | 1566 |  3806092   |      646.9      |
+-----------+------+------------+-----------------+
""".strip()
        )

    def test_sort_slice(self) -> None:
        """Make sure sorting and slicing interact in the expected way"""
        table = PrettyTable(["Foo"])
        for i in range(20, 0, -1):
            table.add_row([i])
        new_style = table.get_string(sortby="Foo", end=10)
        assert "10" in new_style
        assert "20" not in new_style
        oldstyle = table.get_string(sortby="Foo", end=10, oldsortslice=True)
        assert "10" not in oldstyle
        assert "20" in oldstyle


@pytest.fixture(scope="function")
def float_pt() -> PrettyTable:
    table = PrettyTable(["Constant", "Value"])
    table.add_row(["Pi", pi])
    table.add_row(["e", e])
    table.add_row(["sqrt(2)", sqrt(2)])
    return table


class TestFloatFormat:
    def test_no_decimals(self, float_pt: PrettyTable) -> None:
        float_pt.float_format = ".0f"
        float_pt.caching = False
        assert "." not in float_pt.get_string()

    def test_round_to_5dp(self, float_pt: PrettyTable) -> None:
        float_pt.float_format = ".5f"
        string = float_pt.get_string()
        assert "3.14159" in string
        assert "3.141592" not in string
        assert "2.71828" in string
        assert "2.718281" not in string
        assert "2.718282" not in string
        assert "1.41421" in string
        assert "1.414213" not in string

    def test_pad_with_2zeroes(self, float_pt: PrettyTable) -> None:
        float_pt.float_format = "06.2f"
        string = float_pt.get_string()
        assert "003.14" in string
        assert "002.72" in string
        assert "001.41" in string


class TestBreakLine:
    @pytest.mark.parametrize(
        ["rows", "hrule", "expected_result"],
        [
            (
                [["value 1", "value2\nsecond line"], ["value 3", "value4"]],
                ALL,
                """
+---------+-------------+
| Field 1 |   Field 2   |
+---------+-------------+
| value 1 |    value2   |
|         | second line |
+---------+-------------+
| value 3 |    value4   |
+---------+-------------+
""",
            ),
            (
                [
                    ["value 1", "value2\nsecond line"],
                    ["value 3\n\nother line", "value4\n\n\nvalue5"],
                ],
                ALL,
                """
+------------+-------------+
|  Field 1   |   Field 2   |
+------------+-------------+
|  value 1   |    value2   |
|            | second line |
+------------+-------------+
|  value 3   |    value4   |
|            |             |
| other line |             |
|            |    value5   |
+------------+-------------+
""",
            ),
            (
                [
                    ["value 1", "value2\nsecond line"],
                    ["value 3\n\nother line", "value4\n\n\nvalue5"],
                ],
                FRAME,
                """
+------------+-------------+
|  Field 1   |   Field 2   |
+------------+-------------+
|  value 1   |    value2   |
|            | second line |
|  value 3   |    value4   |
|            |             |
| other line |             |
|            |    value5   |
+------------+-------------+
""",
            ),
        ],
    )
    def test_break_line_ascii(
        self, rows: list[list[Any]], hrule: int, expected_result: str
    ) -> None:
        table = PrettyTable(["Field 1", "Field 2"])
        for row in rows:
            table.add_row(row)
        result = table.get_string(hrules=hrule)
        assert result.strip() == expected_result.strip()

    def test_break_line_html(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"])
        table.add_row(["value 1", "value2\nsecond line"])
        table.add_row(["value 3", "value4"])
        result = table.get_html_string(hrules=ALL)
        assert (
            result.strip()
            == """
<table>
    <thead>
        <tr>
            <th>Field 1</th>
            <th>Field 2</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>value 1</td>
            <td>value2<br>second line</td>
        </tr>
        <tr>
            <td>value 3</td>
            <td>value4</td>
        </tr>
    </tbody>
</table>
""".strip()
        )


class TestAnsiWidth:
    colored = "\033[31mC\033[32mO\033[31mL\033[32mO\033[31mR\033[32mE\033[31mD\033[0m"

    def test_color(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"])
        table.add_row([self.colored, self.colored])
        table.add_row(["nothing", "neither"])
        result = table.get_string()
        assert (
            result.strip()
            == f"""
+---------+---------+
| Field 1 | Field 2 |
+---------+---------+
| {self.colored} | {self.colored} |
| nothing | neither |
+---------+---------+
""".strip()
        )

    def test_reset(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"])
        table.add_row(["abc def\033(B", "\033[31mabc def\033[m"])
        table.add_row(["nothing", "neither"])
        result = table.get_string()
        assert (
            result.strip()
            == """
+---------+---------+
| Field 1 | Field 2 |
+---------+---------+
| abc def\033(B | \033[31mabc def\033[m |
| nothing | neither |
+---------+---------+
""".strip()
        )


class TestFromDB:
    @pytest.mark.usefixtures("init_db")
    def test_non_select_cursor(self, db_cursor) -> None:
        db_cursor.execute(
            'INSERT INTO cities VALUES ("Adelaide", 1295, 1158259, 600.5)'
        )
        assert from_db_cursor(db_cursor) is None


class TestJSONOutput:
    def test_json_output(self) -> None:
        t = helper_table()
        result = t.get_json_string()
        assert (
            result.strip()
            == """
[
    [
        "",
        "Field 1",
        "Field 2",
        "Field 3"
    ],
    {
        "": 1,
        "Field 1": "value 1",
        "Field 2": "value2",
        "Field 3": "value3"
    },
    {
        "": 4,
        "Field 1": "value 4",
        "Field 2": "value5",
        "Field 3": "value6"
    },
    {
        "": 7,
        "Field 1": "value 7",
        "Field 2": "value8",
        "Field 3": "value9"
    }
]""".strip()
        )
        options = {"fields": ["Field 1", "Field 3"]}
        result = t.get_json_string(**options)
        assert (
            result.strip()
            == """
[
    [
        "Field 1",
        "Field 3"
    ],
    {
        "Field 1": "value 1",
        "Field 3": "value3"
    },
    {
        "Field 1": "value 4",
        "Field 3": "value6"
    },
    {
        "Field 1": "value 7",
        "Field 3": "value9"
    }
]""".strip()
        )

    def test_json_output_options(self) -> None:
        t = helper_table()
        result = t.get_json_string(header=False, indent=None, separators=(",", ":"))
        assert (
            result
            == """[{"":1,"Field 1":"value 1","Field 2":"value2","Field 3":"value3"},"""
            """{"":4,"Field 1":"value 4","Field 2":"value5","Field 3":"value6"},"""
            """{"":7,"Field 1":"value 7","Field 2":"value8","Field 3":"value9"}]"""
        )


class TestHtmlOutput:
    def test_html_output(self) -> None:
        t = helper_table()
        result = t.get_html_string()
        assert (
            result.strip()
            == """
<table>
    <thead>
        <tr>
            <th></th>
            <th>Field 1</th>
            <th>Field 2</th>
            <th>Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>value 1</td>
            <td>value2</td>
            <td>value3</td>
        </tr>
        <tr>
            <td>4</td>
            <td>value 4</td>
            <td>value5</td>
            <td>value6</td>
        </tr>
        <tr>
            <td>7</td>
            <td>value 7</td>
            <td>value8</td>
            <td>value9</td>
        </tr>
    </tbody>
</table>
""".strip()
        )

    def test_html_output_formatted(self) -> None:
        t = helper_table()
        result = t.get_html_string(format=True)
        assert (
            result.strip()
            == """
<table frame="box" rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 2</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value2</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value3</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">4</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 4</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value5</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value6</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">7</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 7</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value8</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value9</td>
        </tr>
    </tbody>
</table>
""".strip()  # noqa: E501
        )

    def test_html_output_with_title(self) -> None:
        t = helper_table()
        t.title = "Title & Title"
        result = t.get_html_string(attributes={"bgcolor": "red", "a<b": "1<2"})
        assert (
            result.strip()
            == """
<table bgcolor="red" a&lt;b="1&lt;2">
    <caption>Title &amp; Title</caption>
    <thead>
        <tr>
            <th></th>
            <th>Field 1</th>
            <th>Field 2</th>
            <th>Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>value 1</td>
            <td>value2</td>
            <td>value3</td>
        </tr>
        <tr>
            <td>4</td>
            <td>value 4</td>
            <td>value5</td>
            <td>value6</td>
        </tr>
        <tr>
            <td>7</td>
            <td>value 7</td>
            <td>value8</td>
            <td>value9</td>
        </tr>
    </tbody>
</table>
""".strip()
        )

    def test_html_output_formatted_with_title(self) -> None:
        t = helper_table()
        t.title = "Title & Title"
        result = t.get_html_string(
            attributes={"bgcolor": "red", "a<b": "1<2"}, format=True
        )
        assert (
            result.strip()
            == """
<table frame="box" rules="cols" bgcolor="red" a&lt;b="1&lt;2">
    <caption>Title &amp; Title</caption>
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 2</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value2</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value3</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">4</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 4</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value5</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value6</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">7</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 7</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value8</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value9</td>
        </tr>
    </tbody>
</table>
""".strip()  # noqa: E501
        )

    def test_html_output_without_escaped_header(self) -> None:
        t = helper_table(rows=0)
        t.field_names = ["", "Field 1", "<em>Field 2</em>", "<a href='#'>Field 3</a>"]
        result = t.get_html_string(escape_header=False)
        assert (
            result.strip()
            == """
<table>
    <thead>
        <tr>
            <th></th>
            <th>Field 1</th>
            <th><em>Field 2</em></th>
            <th><a href='#'>Field 3</a></th>
        </tr>
    </thead>
    <tbody>
    </tbody>
</table>
""".strip()
        )

    def test_html_output_without_escaped_data(self) -> None:
        t = helper_table(rows=0)
        t.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = t.get_html_string(escape_data=False)
        assert (
            result.strip()
            == """
<table>
    <thead>
        <tr>
            <th></th>
            <th>Field 1</th>
            <th>Field 2</th>
            <th>Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td><b>value 1</b></td>
            <td><span style='text-decoration: underline;'>value2</span></td>
            <td><a href='#'>value3</a></td>
        </tr>
    </tbody>
</table>
""".strip()
        )

    def test_html_output_with_escaped_header(self) -> None:
        t = helper_table(rows=0)
        t.field_names = ["", "Field 1", "<em>Field 2</em>", "<a href='#'>Field 3</a>"]
        result = t.get_html_string(escape_header=True)
        assert (
            result.strip()
            == """
<table>
    <thead>
        <tr>
            <th></th>
            <th>Field 1</th>
            <th>&lt;em&gt;Field 2&lt;/em&gt;</th>
            <th>&lt;a href=&#x27;#&#x27;&gt;Field 3&lt;/a&gt;</th>
        </tr>
    </thead>
    <tbody>
    </tbody>
</table>
""".strip()
        )

    def test_html_output_with_escaped_data(self) -> None:
        t = helper_table(rows=0)
        t.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = t.get_html_string(escape_data=True)
        assert (
            result.strip()
            == """
<table>
    <thead>
        <tr>
            <th></th>
            <th>Field 1</th>
            <th>Field 2</th>
            <th>Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>&lt;b&gt;value 1&lt;/b&gt;</td>
            <td>&lt;span style=&#x27;text-decoration: underline;&#x27;&gt;value2&lt;/span&gt;</td>
            <td>&lt;a href=&#x27;#&#x27;&gt;value3&lt;/a&gt;</td>
        </tr>
    </tbody>
</table>
""".strip()  # noqa: E501
        )

    def test_html_output_formatted_without_escaped_header(self) -> None:
        t = helper_table(rows=0)
        t.field_names = ["", "Field 1", "<em>Field 2</em>", "<a href='#'>Field 3</a>"]
        result = t.get_html_string(escape_header=False, format=True)
        assert (
            result.strip()
            == """
<table frame="box" rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"><em>Field 2</em></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"><a href='#'>Field 3</a></th>
        </tr>
    </thead>
    <tbody>
    </tbody>
</table>
""".strip()  # noqa: E501
        )

    def test_html_output_formatted_without_escaped_data(self) -> None:
        t = helper_table(rows=0)
        t.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = t.get_html_string(escape_data=False, format=True)
        assert (
            result.strip()
            == """
<table frame="box" rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 2</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top"><b>value 1</b></td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top"><span style='text-decoration: underline;'>value2</span></td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top"><a href='#'>value3</a></td>
        </tr>
    </tbody>
</table>
""".strip()  # noqa: E501
        )

    def test_html_output_formatted_with_escaped_header(self) -> None:
        t = helper_table(rows=0)
        t.field_names = ["", "Field 1", "<em>Field 2</em>", "<a href='#'>Field 3</a>"]
        result = t.get_html_string(escape_header=True, format=True)
        assert (
            result.strip()
            == """
<table frame="box" rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">&lt;em&gt;Field 2&lt;/em&gt;</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">&lt;a href=&#x27;#&#x27;&gt;Field 3&lt;/a&gt;</th>
        </tr>
    </thead>
    <tbody>
    </tbody>
</table>
""".strip()  # noqa: E501
        )

    def test_html_output_formatted_with_escaped_data(self) -> None:
        t = helper_table(rows=0)
        t.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = t.get_html_string(escape_data=True, format=True)
        assert (
            result.strip()
            == """
<table frame="box" rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 2</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">&lt;b&gt;value 1&lt;/b&gt;</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">&lt;span style=&#x27;text-decoration: underline;&#x27;&gt;value2&lt;/span&gt;</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">&lt;a href=&#x27;#&#x27;&gt;value3&lt;/a&gt;</td>
        </tr>
    </tbody>
</table>
""".strip()  # noqa: E501
        )


class TestPositionalJunctions:
    """Verify different cases for positional-junction characters"""

    def test_default(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)

        assert (
            city_data_prettytable.get_string().strip()
            == """
╔═══════════╦══════╦════════════╦═════════════════╗
║ City name ║ Area ║ Population ║ Annual Rainfall ║
╠═══════════╬══════╬════════════╬═════════════════╣
║  Adelaide ║ 1295 ║  1158259   ║      600.5      ║
║  Brisbane ║ 5905 ║  1857594   ║      1146.4     ║
║   Darwin  ║ 112  ║   120900   ║      1714.7     ║
║   Hobart  ║ 1357 ║   205556   ║      619.5      ║
║   Sydney  ║ 2058 ║  4336374   ║      1214.8     ║
║ Melbourne ║ 1566 ║  3806092   ║      646.9      ║
║   Perth   ║ 5386 ║  1554769   ║      869.4      ║
╚═══════════╩══════╩════════════╩═════════════════╝""".strip()
        )

    def test_no_header(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)
        city_data_prettytable.header = False

        assert (
            city_data_prettytable.get_string().strip()
            == """
╔═══════════╦══════╦═════════╦════════╗
║  Adelaide ║ 1295 ║ 1158259 ║ 600.5  ║
║  Brisbane ║ 5905 ║ 1857594 ║ 1146.4 ║
║   Darwin  ║ 112  ║  120900 ║ 1714.7 ║
║   Hobart  ║ 1357 ║  205556 ║ 619.5  ║
║   Sydney  ║ 2058 ║ 4336374 ║ 1214.8 ║
║ Melbourne ║ 1566 ║ 3806092 ║ 646.9  ║
║   Perth   ║ 5386 ║ 1554769 ║ 869.4  ║
╚═══════════╩══════╩═════════╩════════╝""".strip()
        )

    def test_with_title(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)
        city_data_prettytable.title = "Title"

        assert (
            city_data_prettytable.get_string().strip()
            == """
╔═════════════════════════════════════════════════╗
║                      Title                      ║
╠═══════════╦══════╦════════════╦═════════════════╣
║ City name ║ Area ║ Population ║ Annual Rainfall ║
╠═══════════╬══════╬════════════╬═════════════════╣
║  Adelaide ║ 1295 ║  1158259   ║      600.5      ║
║  Brisbane ║ 5905 ║  1857594   ║      1146.4     ║
║   Darwin  ║ 112  ║   120900   ║      1714.7     ║
║   Hobart  ║ 1357 ║   205556   ║      619.5      ║
║   Sydney  ║ 2058 ║  4336374   ║      1214.8     ║
║ Melbourne ║ 1566 ║  3806092   ║      646.9      ║
║   Perth   ║ 5386 ║  1554769   ║      869.4      ║
╚═══════════╩══════╩════════════╩═════════════════╝""".strip()
        )

    def test_with_title_no_header(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)
        city_data_prettytable.title = "Title"
        city_data_prettytable.header = False
        assert (
            city_data_prettytable.get_string().strip()
            == """
╔═════════════════════════════════════╗
║                Title                ║
╠═══════════╦══════╦═════════╦════════╣
║  Adelaide ║ 1295 ║ 1158259 ║ 600.5  ║
║  Brisbane ║ 5905 ║ 1857594 ║ 1146.4 ║
║   Darwin  ║ 112  ║  120900 ║ 1714.7 ║
║   Hobart  ║ 1357 ║  205556 ║ 619.5  ║
║   Sydney  ║ 2058 ║ 4336374 ║ 1214.8 ║
║ Melbourne ║ 1566 ║ 3806092 ║ 646.9  ║
║   Perth   ║ 5386 ║ 1554769 ║ 869.4  ║
╚═══════════╩══════╩═════════╩════════╝""".strip()
        )

    def test_hrule_all(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)
        city_data_prettytable.title = "Title"
        city_data_prettytable.hrules = ALL
        assert (
            city_data_prettytable.get_string().strip()
            == """
╔═════════════════════════════════════════════════╗
║                      Title                      ║
╠═══════════╦══════╦════════════╦═════════════════╣
║ City name ║ Area ║ Population ║ Annual Rainfall ║
╠═══════════╬══════╬════════════╬═════════════════╣
║  Adelaide ║ 1295 ║  1158259   ║      600.5      ║
╠═══════════╬══════╬════════════╬═════════════════╣
║  Brisbane ║ 5905 ║  1857594   ║      1146.4     ║
╠═══════════╬══════╬════════════╬═════════════════╣
║   Darwin  ║ 112  ║   120900   ║      1714.7     ║
╠═══════════╬══════╬════════════╬═════════════════╣
║   Hobart  ║ 1357 ║   205556   ║      619.5      ║
╠═══════════╬══════╬════════════╬═════════════════╣
║   Sydney  ║ 2058 ║  4336374   ║      1214.8     ║
╠═══════════╬══════╬════════════╬═════════════════╣
║ Melbourne ║ 1566 ║  3806092   ║      646.9      ║
╠═══════════╬══════╬════════════╬═════════════════╣
║   Perth   ║ 5386 ║  1554769   ║      869.4      ║
╚═══════════╩══════╩════════════╩═════════════════╝""".strip()
        )

    def test_vrules_none(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)
        city_data_prettytable.vrules = NONE
        assert (
            city_data_prettytable.get_string().strip()
            == "═══════════════════════════════════════════════════\n"
            "  City name   Area   Population   Annual Rainfall  \n"
            "═══════════════════════════════════════════════════\n"
            "   Adelaide   1295    1158259          600.5       \n"
            "   Brisbane   5905    1857594          1146.4      \n"
            "    Darwin    112      120900          1714.7      \n"
            "    Hobart    1357     205556          619.5       \n"
            "    Sydney    2058    4336374          1214.8      \n"
            "  Melbourne   1566    3806092          646.9       \n"
            "    Perth     5386    1554769          869.4       \n"
            "═══════════════════════════════════════════════════".strip()
        )

    def test_vrules_frame_with_title(self, city_data_prettytable: PrettyTable) -> None:
        city_data_prettytable.set_style(DOUBLE_BORDER)
        city_data_prettytable.vrules = FRAME
        city_data_prettytable.title = "Title"
        assert (
            city_data_prettytable.get_string().strip()
            == """
╔═════════════════════════════════════════════════╗
║                      Title                      ║
╠═════════════════════════════════════════════════╣
║ City name   Area   Population   Annual Rainfall ║
╠═════════════════════════════════════════════════╣
║  Adelaide   1295    1158259          600.5      ║
║  Brisbane   5905    1857594          1146.4     ║
║   Darwin    112      120900          1714.7     ║
║   Hobart    1357     205556          619.5      ║
║   Sydney    2058    4336374          1214.8     ║
║ Melbourne   1566    3806092          646.9      ║
║   Perth     5386    1554769          869.4      ║
╚═════════════════════════════════════════════════╝""".strip()
        )


class TestStyle:
    @pytest.mark.parametrize(
        "style, expected",
        [
            pytest.param(
                DEFAULT,
                """
+---+---------+---------+---------+
|   | Field 1 | Field 2 | Field 3 |
+---+---------+---------+---------+
| 1 | value 1 |  value2 |  value3 |
| 4 | value 4 |  value5 |  value6 |
| 7 | value 7 |  value8 |  value9 |
+---+---------+---------+---------+
""",
                id="DEFAULT",
            ),
            pytest.param(
                MARKDOWN,  # TODO fix
                """
|     | Field 1 | Field 2 | Field 3 |
| :-: | :-----: | :-----: | :-----: |
|  1  | value 1 |  value2 |  value3 |
|  4  | value 4 |  value5 |  value6 |
|  7  | value 7 |  value8 |  value9 |
""",
                id="MARKDOWN",
            ),
            pytest.param(
                MSWORD_FRIENDLY,
                """
|   | Field 1 | Field 2 | Field 3 |
| 1 | value 1 |  value2 |  value3 |
| 4 | value 4 |  value5 |  value6 |
| 7 | value 7 |  value8 |  value9 |
""",
                id="MSWORD_FRIENDLY",
            ),
            pytest.param(
                ORGMODE,
                """
|---+---------+---------+---------|
|   | Field 1 | Field 2 | Field 3 |
|---+---------+---------+---------|
| 1 | value 1 |  value2 |  value3 |
| 4 | value 4 |  value5 |  value6 |
| 7 | value 7 |  value8 |  value9 |
|---+---------+---------+---------|
""",
                id="ORGMODE",
            ),
            pytest.param(
                PLAIN_COLUMNS,
                """
         Field 1        Field 2        Field 3        
1        value 1         value2         value3        
4        value 4         value5         value6        
7        value 7         value8         value9
""",  # noqa: W291
                id="PLAIN_COLUMNS",
            ),
            pytest.param(
                RANDOM,
                """
'^^^^^'^^^^^^^^^^^'^^^^^^^^^^'^^^^^^^^^^'
%    1%    value 1%    value2%    value3%
'^^^^^'^^^^^^^^^^^'^^^^^^^^^^'^^^^^^^^^^'
%    4%    value 4%    value5%    value6%
'^^^^^'^^^^^^^^^^^'^^^^^^^^^^'^^^^^^^^^^'
%    7%    value 7%    value8%    value9%
'^^^^^'^^^^^^^^^^^'^^^^^^^^^^'^^^^^^^^^^'
""",
                id="RANDOM",
            ),
            pytest.param(
                DOUBLE_BORDER,
                """
╔═══╦═════════╦═════════╦═════════╗
║   ║ Field 1 ║ Field 2 ║ Field 3 ║
╠═══╬═════════╬═════════╬═════════╣
║ 1 ║ value 1 ║  value2 ║  value3 ║
║ 4 ║ value 4 ║  value5 ║  value6 ║
║ 7 ║ value 7 ║  value8 ║  value9 ║
╚═══╩═════════╩═════════╩═════════╝
""",
            ),
            pytest.param(
                SINGLE_BORDER,
                """
┌───┬─────────┬─────────┬─────────┐
│   │ Field 1 │ Field 2 │ Field 3 │
├───┼─────────┼─────────┼─────────┤
│ 1 │ value 1 │  value2 │  value3 │
│ 4 │ value 4 │  value5 │  value6 │
│ 7 │ value 7 │  value8 │  value9 │
└───┴─────────┴─────────┴─────────┘
""",
            ),
        ],
    )
    def test_style(self, style, expected) -> None:
        # Arrange
        t = helper_table()
        random.seed(1234)

        # Act
        t.set_style(style)

        # Assert
        result = t.get_string()
        assert result.strip() == expected.strip()

    def test_style_invalid(self) -> None:
        # Arrange
        t = helper_table()

        # Act / Assert
        # This is an hrule style, not a table style
        with pytest.raises(ValueError):
            t.set_style(ALL)

    @pytest.mark.parametrize(
        "style, expected",
        [
            pytest.param(
                MARKDOWN,
                """
| l |  c  | r | Align left | Align centre | Align right |
| :-| :-: |-: | :----------| :----------: |-----------: |
| 1 |  2  | 3 | value 1    |    value2    |      value3 |
| 4 |  5  | 6 | value 4    |    value5    |      value6 |
| 7 |  8  | 9 | value 7    |    value8    |      value9 |
""",
                id="MARKDOWN",
            ),
        ],
    )
    def test_style_align(self, style, expected) -> None:
        # Arrange
        t = PrettyTable(["l", "c", "r", "Align left", "Align centre", "Align right"])
        v = 1
        for row in range(3):
            # Some have spaces, some not, to help test padding columns of
            # different widths
            t.add_row([v, v + 1, v + 2, f"value {v}", f"value{v + 1}", f"value{v + 2}"])
            v += 3

        # Act
        t.set_style(style)
        t.align["l"] = t.align["Align left"] = "l"
        t.align["c"] = t.align["Align centre"] = "c"
        t.align["r"] = t.align["Align right"] = "r"

        # Assert
        result = t.get_string()
        assert result.strip() == expected.strip()


class TestCsvOutput:
    def test_csv_output(self) -> None:
        t = helper_table()
        assert t.get_csv_string(delimiter="\t", header=False) == (
            "1\tvalue 1\tvalue2\tvalue3\r\n"
            "4\tvalue 4\tvalue5\tvalue6\r\n"
            "7\tvalue 7\tvalue8\tvalue9\r\n"
        )
        assert t.get_csv_string() == (
            ",Field 1,Field 2,Field 3\r\n"
            "1,value 1,value2,value3\r\n"
            "4,value 4,value5,value6\r\n"
            "7,value 7,value8,value9\r\n"
        )
        options = {"fields": ["Field 1", "Field 3"]}
        assert t.get_csv_string(**options) == (
            "Field 1,Field 3\r\n"
            "value 1,value3\r\n"
            "value 4,value6\r\n"
            "value 7,value9\r\n"
        )


class TestLatexOutput:
    def test_latex_output(self) -> None:
        t = helper_table()
        assert t.get_latex_string() == (
            "\\begin{tabular}{cccc}\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\end{tabular}"
        )
        options = {"fields": ["Field 1", "Field 3"]}
        assert t.get_latex_string(**options) == (
            "\\begin{tabular}{cc}\r\n"
            "Field 1 & Field 3 \\\\\r\n"
            "value 1 & value3 \\\\\r\n"
            "value 4 & value6 \\\\\r\n"
            "value 7 & value9 \\\\\r\n"
            "\\end{tabular}"
        )

    def test_latex_output_formatted(self) -> None:
        t = helper_table()
        assert t.get_latex_string(format=True) == (
            "\\begin{tabular}{|c|c|c|c|}\r\n"
            "\\hline\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\hline\r\n"
            "\\end{tabular}"
        )

        options = {"fields": ["Field 1", "Field 3"]}
        assert t.get_latex_string(format=True, **options) == (
            "\\begin{tabular}{|c|c|}\r\n"
            "\\hline\r\n"
            "Field 1 & Field 3 \\\\\r\n"
            "value 1 & value3 \\\\\r\n"
            "value 4 & value6 \\\\\r\n"
            "value 7 & value9 \\\\\r\n"
            "\\hline\r\n"
            "\\end{tabular}"
        )

        options = {"vrules": FRAME}
        assert t.get_latex_string(format=True, **options) == (
            "\\begin{tabular}{|cccc|}\r\n"
            "\\hline\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\hline\r\n"
            "\\end{tabular}"
        )

        options = {"hrules": ALL}
        assert t.get_latex_string(format=True, **options) == (
            "\\begin{tabular}{|c|c|c|c|}\r\n"
            "\\hline\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "\\hline\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "\\hline\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "\\hline\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\hline\r\n"
            "\\end{tabular}"
        )

    def test_latex_output_header(self) -> None:
        t = helper_table()
        assert t.get_latex_string(format=True, hrules=HEADER) == (
            "\\begin{tabular}{|c|c|c|c|}\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "\\hline\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\end{tabular}"
        )


class TestJSONConstructor:
    def test_json_and_back(self, city_data_prettytable: PrettyTable) -> None:
        json_string = city_data_prettytable.get_json_string()
        new_table = from_json(json_string)
        assert new_table.get_string() == city_data_prettytable.get_string()


class TestHtmlConstructor:
    def test_html_and_back(self, city_data_prettytable: PrettyTable) -> None:
        html_string = city_data_prettytable.get_html_string()
        new_table = from_html(html_string)[0]
        assert new_table.get_string() == city_data_prettytable.get_string()

    def test_html_one_and_back(self, city_data_prettytable: PrettyTable) -> None:
        html_string = city_data_prettytable.get_html_string()
        new_table = from_html_one(html_string)
        assert new_table.get_string() == city_data_prettytable.get_string()

    def test_html_one_fail_on_many(self, city_data_prettytable: PrettyTable) -> None:
        html_string = city_data_prettytable.get_html_string()
        html_string += city_data_prettytable.get_html_string()
        with pytest.raises(ValueError):
            from_html_one(html_string)


@pytest.fixture
def japanese_pretty_table() -> PrettyTable:
    table = PrettyTable(["Kanji", "Hiragana", "English"])
    table.add_row(["神戸", "こうべ", "Kobe"])
    table.add_row(["京都", "きょうと", "Kyoto"])
    table.add_row(["長崎", "ながさき", "Nagasaki"])
    table.add_row(["名古屋", "なごや", "Nagoya"])
    table.add_row(["大阪", "おおさか", "Osaka"])
    table.add_row(["札幌", "さっぽろ", "Sapporo"])
    table.add_row(["東京", "とうきょう", "Tokyo"])
    table.add_row(["横浜", "よこはま", "Yokohama"])
    return table


@pytest.fixture
def emoji_pretty_table() -> PrettyTable:
    thunder1 = [
        '\033[38;5;226m _`/""\033[38;5;250m.-.    \033[0m',
        "\033[38;5;226m  ,\\_\033[38;5;250m(   ).  \033[0m",
        "\033[38;5;226m   /\033[38;5;250m(___(__) \033[0m",
        "\033[38;5;228;5m    ⚡\033[38;5;111;25mʻ ʻ\033[38;5;228;5m"
        "⚡\033[38;5;111;25mʻ ʻ \033[0m",
        "\033[38;5;111m    ʻ ʻ ʻ ʻ  \033[0m",
    ]
    thunder2 = [
        "\033[38;5;240;1m     .-.     \033[0m",
        "\033[38;5;240;1m    (   ).   \033[0m",
        "\033[38;5;240;1m   (___(__)  \033[0m",
        "\033[38;5;21;1m  ‚ʻ\033[38;5;228;5m⚡\033[38;5;21;25mʻ‚\033[38;5;228;5m"
        "⚡\033[38;5;21;25m‚ʻ   \033[0m",
        "\033[38;5;21;1m  ‚ʻ‚ʻ\033[38;5;228;5m⚡\033[38;5;21;25mʻ‚ʻ   \033[0m",
    ]
    table = PrettyTable(["Thunderbolt", "Lightning"])
    for i in range(len(thunder1)):
        table.add_row([thunder1[i], thunder2[i]])
    return table


class TestMultiPattern:
    @pytest.mark.parametrize(
        ["pt", "expected_output", "test_type"],
        [
            (
                lf("city_data_prettytable"),
                """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
|   Darwin  | 112  |   120900   |      1714.7     |
|   Hobart  | 1357 |   205556   |      619.5      |
|   Sydney  | 2058 |  4336374   |      1214.8     |
| Melbourne | 1566 |  3806092   |      646.9      |
|   Perth   | 5386 |  1554769   |      869.4      |
+-----------+------+------------+-----------------+
""",
                "English Table",
            ),
            (
                lf("japanese_pretty_table"),
                """
+--------+------------+----------+
| Kanji  |  Hiragana  | English  |
+--------+------------+----------+
|  神戸  |   こうべ   |   Kobe   |
|  京都  |  きょうと  |  Kyoto   |
|  長崎  |  ながさき  | Nagasaki |
| 名古屋 |   なごや   |  Nagoya  |
|  大阪  |  おおさか  |  Osaka   |
|  札幌  |  さっぽろ  | Sapporo  |
|  東京  | とうきょう |  Tokyo   |
|  横浜  |  よこはま  | Yokohama |
+--------+------------+----------+

""",
                "Japanese table",
            ),
            (
                lf("emoji_pretty_table"),
                """
+-----------------+-----------------+
|   Thunderbolt   |    Lightning    |
+-----------------+-----------------+
|  \x1b[38;5;226m _`/""\x1b[38;5;250m.-.    \x1b[0m  |  \x1b[38;5;240;1m     .-.     \x1b[0m  |
|  \x1b[38;5;226m  ,\\_\x1b[38;5;250m(   ).  \x1b[0m  |  \x1b[38;5;240;1m    (   ).   \x1b[0m  |
|  \x1b[38;5;226m   /\x1b[38;5;250m(___(__) \x1b[0m  |  \x1b[38;5;240;1m   (___(__)  \x1b[0m  |
| \x1b[38;5;228;5m    ⚡\x1b[38;5;111;25mʻ ʻ\x1b[38;5;228;5m⚡\x1b[38;5;111;25mʻ ʻ \x1b[0m | \x1b[38;5;21;1m  ‚ʻ\x1b[38;5;228;5m⚡\x1b[38;5;21;25mʻ‚\x1b[38;5;228;5m⚡\x1b[38;5;21;25m‚ʻ   \x1b[0m |
|  \x1b[38;5;111m    ʻ ʻ ʻ ʻ  \x1b[0m  |  \x1b[38;5;21;1m  ‚ʻ‚ʻ\x1b[38;5;228;5m⚡\x1b[38;5;21;25mʻ‚ʻ   \x1b[0m |
+-----------------+-----------------+
            """,  # noqa: E501
                "Emoji table",
            ),
        ],
    )
    def test_multi_pattern_outputs(
        self, pt: PrettyTable, expected_output: str, test_type: str
    ) -> None:
        printed_table = pt.get_string()
        assert (
            printed_table.strip() == expected_output.strip()
        ), f"Error output for test output of type {test_type}"


def test_paginate() -> None:
    # Arrange
    t = helper_table(rows=7)
    expected_page_1 = """
+----+----------+---------+---------+
|    | Field 1  | Field 2 | Field 3 |
+----+----------+---------+---------+
| 1  | value 1  |  value2 |  value3 |
| 4  | value 4  |  value5 |  value6 |
| 7  | value 7  |  value8 |  value9 |
| 10 | value 10 | value11 | value12 |
+----+----------+---------+---------+
    """.strip()
    expected_page_2 = """
+----+----------+---------+---------+
|    | Field 1  | Field 2 | Field 3 |
+----+----------+---------+---------+
| 13 | value 13 | value14 | value15 |
| 16 | value 16 | value17 | value18 |
| 19 | value 19 | value20 | value21 |
+----+----------+---------+---------+
""".strip()

    # Act
    paginated = t.paginate(page_length=4)

    # Assert
    paginated = paginated.strip()
    assert paginated.startswith(expected_page_1)
    assert "\f" in paginated
    assert paginated.endswith(expected_page_2)

    # Act
    paginated = t.paginate(page_length=4, line_break="\n")

    # Assert
    assert "\f" not in paginated
    assert "\n" in paginated


def test_add_rows() -> None:
    """A table created with multiple add_row calls
    is the same as one created with a single add_rows
    """
    # Arrange
    table1 = PrettyTable(["A", "B", "C"])
    table2 = PrettyTable(["A", "B", "C"])
    table1.add_row([1, 2, 3])
    table1.add_row([4, 5, 6])
    rows = [
        [1, 2, 3],
        [4, 5, 6],
    ]

    # Act
    table2.add_rows(rows)

    # Assert
    assert str(table1) == str(table2)


def test_autoindex() -> None:
    """Testing that a table with a custom index row is
    equal to the one produced by the function
    .add_autoindex()
    """
    table1 = PrettyTable()
    table1.field_names = ["City name", "Area", "Population", "Annual Rainfall"]
    table1.add_row(["Adelaide", 1295, 1158259, 600.5])
    table1.add_row(["Brisbane", 5905, 1857594, 1146.4])
    table1.add_row(["Darwin", 112, 120900, 1714.7])
    table1.add_row(["Hobart", 1357, 205556, 619.5])
    table1.add_row(["Sydney", 2058, 4336374, 1214.8])
    table1.add_row(["Melbourne", 1566, 3806092, 646.9])
    table1.add_row(["Perth", 5386, 1554769, 869.4])
    table1.add_autoindex(fieldname="Test")

    table2 = PrettyTable()
    table2.field_names = ["Test", "City name", "Area", "Population", "Annual Rainfall"]
    table2.add_row([1, "Adelaide", 1295, 1158259, 600.5])
    table2.add_row([2, "Brisbane", 5905, 1857594, 1146.4])
    table2.add_row([3, "Darwin", 112, 120900, 1714.7])
    table2.add_row([4, "Hobart", 1357, 205556, 619.5])
    table2.add_row([5, "Sydney", 2058, 4336374, 1214.8])
    table2.add_row([6, "Melbourne", 1566, 3806092, 646.9])
    table2.add_row([7, "Perth", 5386, 1554769, 869.4])

    assert str(table1) == str(table2)


@pytest.fixture(scope="function")
def unpadded_pt() -> PrettyTable:
    table = PrettyTable(header=False, padding_width=0)
    table.add_row("abc")
    table.add_row("def")
    table.add_row("g..")
    return table


class TestUnpaddedTable:
    def test_unbordered(self, unpadded_pt: PrettyTable) -> None:
        unpadded_pt.border = False
        result = unpadded_pt.get_string()
        expected = """
abc
def
g..
"""
        assert result.strip() == expected.strip()

    def test_bordered(self, unpadded_pt: PrettyTable) -> None:
        unpadded_pt.border = True
        result = unpadded_pt.get_string()
        expected = """
+-+-+-+
|a|b|c|
|d|e|f|
|g|.|.|
+-+-+-+
"""
        assert result.strip() == expected.strip()


class TestCustomFormatter:
    def test_init_custom_format_is_empty(self) -> None:
        table = PrettyTable()
        assert table.custom_format == {}

    def test_init_custom_format_set_value(self) -> None:
        table = PrettyTable(
            custom_format={"col1": (lambda col_name, value: f"{value:.2}")}
        )
        assert len(table.custom_format) == 1

    def test_init_custom_format_throw_error_is_not_callable(self) -> None:
        with pytest.raises(ValueError) as e:
            PrettyTable(custom_format={"col1": "{:.2}"})

        assert "Invalid value for custom_format.col1. Must be a function." in str(
            e.value
        )

    def test_can_set_custom_format_from_property_setter(self) -> None:
        table = PrettyTable()
        table.custom_format = {"col1": (lambda col_name, value: f"{value:.2}")}
        assert len(table.custom_format) == 1

    def test_set_custom_format_to_none_set_empty_dict(self) -> None:
        table = PrettyTable()
        table.custom_format = None
        assert len(table.custom_format) == 0
        assert isinstance(table.custom_format, dict)

    def test_set_custom_format_invalid_type_throw_error(self) -> None:
        table = PrettyTable()
        with pytest.raises(TypeError) as e:
            table.custom_format = "Some String"
        assert "The custom_format property need to be a dictionary or callable" in str(
            e.value
        )

    def test_use_custom_formatter_for_int(
        self, city_data_prettytable: PrettyTable
    ) -> None:
        city_data_prettytable.custom_format["Annual Rainfall"] = lambda n, v: f"{v:.2f}"
        assert (
            city_data_prettytable.get_string().strip()
            == """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Adelaide | 1295 |  1158259   |      600.50     |
|  Brisbane | 5905 |  1857594   |     1146.40     |
|   Darwin  | 112  |   120900   |     1714.70     |
|   Hobart  | 1357 |   205556   |      619.50     |
|   Sydney  | 2058 |  4336374   |     1214.80     |
| Melbourne | 1566 |  3806092   |      646.90     |
|   Perth   | 5386 |  1554769   |      869.40     |
+-----------+------+------------+-----------------+
""".strip()
        )

    def test_custom_format_multi_type(self) -> None:
        table = PrettyTable(["col_date", "col_str", "col_float", "col_int"])
        table.add_row([dt.date(2021, 1, 1), "January", 12345.12345, 12345678])
        table.add_row([dt.date(2021, 2, 1), "February", 54321.12345, 87654321])
        table.custom_format["col_date"] = lambda f, v: v.strftime("%d %b %Y")
        table.custom_format["col_float"] = lambda f, v: f"{v:.3f}"
        table.custom_format["col_int"] = lambda f, v: f"{v:,}"
        assert (
            table.get_string().strip()
            == """
+-------------+----------+-----------+------------+
|   col_date  | col_str  | col_float |  col_int   |
+-------------+----------+-----------+------------+
| 01 Jan 2021 | January  | 12345.123 | 12,345,678 |
| 01 Feb 2021 | February | 54321.123 | 87,654,321 |
+-------------+----------+-----------+------------+
""".strip()
        )

    def test_custom_format_multi_type_using_on_function(self) -> None:
        table = PrettyTable(["col_date", "col_str", "col_float", "col_int"])
        table.add_row([dt.date(2021, 1, 1), "January", 12345.12345, 12345678])
        table.add_row([dt.date(2021, 2, 1), "February", 54321.12345, 87654321])

        def my_format(col: str, value: Any) -> str:
            if col == "col_date":
                return value.strftime("%d %b %Y")
            if col == "col_float":
                return f"{value:.3f}"
            if col == "col_int":
                return f"{value:,}"
            return str(value)

        table.custom_format = my_format
        assert (
            table.get_string().strip()
            == """
+-------------+----------+-----------+------------+
|   col_date  | col_str  | col_float |  col_int   |
+-------------+----------+-----------+------------+
| 01 Jan 2021 | January  | 12345.123 | 12,345,678 |
| 01 Feb 2021 | February | 54321.123 | 87,654,321 |
+-------------+----------+-----------+------------+
""".strip()
        )


class TestRepr:
    def test_default_repr(self, row_prettytable: PrettyTable) -> None:
        assert row_prettytable.__str__() == row_prettytable.__repr__()

    def test_jupyter_repr(self, row_prettytable: PrettyTable) -> None:
        assert row_prettytable._repr_html_() == row_prettytable.get_html_string()


class TestMinTableWidth:
    @pytest.mark.parametrize(
        "loops, fields, desired_width, border, internal_border",
        [
            (15, ["Test table"], 20, True, False),
            (16, ["Test table"], 21, True, False),
            (18, ["Test table", "Test table 2"], 40, True, False),
            (19, ["Test table", "Test table 2"], 41, True, False),
            (21, ["Test table", "Test col 2", "Test col 3"], 50, True, False),
            (22, ["Test table", "Test col 2", "Test col 3"], 51, True, False),
            (19, ["Test table"], 20, False, False),
            (20, ["Test table"], 21, False, False),
            (25, ["Test table", "Test table 2"], 40, False, False),
            (26, ["Test table", "Test table 2"], 41, False, False),
            (25, ["Test table", "Test col 2", "Test col 3"], 50, False, False),
            (26, ["Test table", "Test col 2", "Test col 3"], 51, False, False),
            (18, ["Test table"], 20, False, True),
            (19, ["Test table"], 21, False, True),
            (23, ["Test table", "Test table 2"], 40, False, True),
            (24, ["Test table", "Test table 2"], 41, False, True),
            (22, ["Test table", "Test col 2", "Test col 3"], 50, False, True),
            (23, ["Test table", "Test col 2", "Test col 3"], 51, False, True),
        ],
    )
    def test_min_table_width(
        self, loops, fields, desired_width, border, internal_border
    ) -> None:
        for col_width in range(loops):
            x = prettytable.PrettyTable()
            x.border = border
            x.preserve_internal_border = internal_border
            x.field_names = fields
            x.add_row(["X" * col_width] + ["" for _ in range(len(fields) - 1)])
            x.min_table_width = desired_width
            t = x.get_string()
            if border is False and internal_border is False:
                assert [len(x) for x in t.split("\n")] == [desired_width, desired_width]
            elif border is False and internal_border is True:
                assert [len(x) for x in t.split("\n")] == [
                    desired_width,
                    desired_width - 1,
                    desired_width,
                ]
            else:
                assert [len(x) for x in t.split("\n")] == [
                    desired_width,
                    desired_width,
                    desired_width,
                    desired_width,
                    desired_width,
                ]


class TestMaxTableWidth:
    def test_max_table_width(self) -> None:
        table = PrettyTable()
        table.max_table_width = 5
        table.add_row([0])

        # FIXME: Table is wider than table.max_table_width
        assert (
            table.get_string().strip()
            == """
+----+
| Fi |
+----+
| 0  |
+----+
""".strip()
        )

    def test_max_table_width_wide(self) -> None:
        table = PrettyTable()
        table.max_table_width = 52
        table.add_row(
            [
                0,
                0,
                0,
                0,
                0,
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
                "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam "
                "erat, sed diam voluptua",
            ]
        )

        assert (
            table.get_string().strip()
            == """
+---+---+---+---+---+------------------------------+
| F | F | F | F | F |           Field 6            |
+---+---+---+---+---+------------------------------+
| 0 | 0 | 0 | 0 | 0 | Lorem ipsum dolor sit amet,  |
|   |   |   |   |   | consetetur sadipscing elitr, |
|   |   |   |   |   |    sed diam nonumy eirmod    |
|   |   |   |   |   | tempor invidunt ut labore et |
|   |   |   |   |   | dolore magna aliquyam erat,  |
|   |   |   |   |   |      sed diam voluptua       |
+---+---+---+---+---+------------------------------+""".strip()
        )

    def test_max_table_width_wide2(self) -> None:
        table = PrettyTable()
        table.max_table_width = 70
        table.add_row(
            [
                "Lorem",
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam ",
                "ipsum",
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam ",
                "dolor",
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam ",
            ]
        )

        assert (
            table.get_string().strip()
            == """
+---+-----------------+---+-----------------+---+-----------------+
| F |     Field 2     | F |     Field 4     | F |     Field 6     |
+---+-----------------+---+-----------------+---+-----------------+
| L |   Lorem ipsum   | i |   Lorem ipsum   | d |   Lorem ipsum   |
| o | dolor sit amet, | p | dolor sit amet, | o | dolor sit amet, |
| r |    consetetur   | s |    consetetur   | l |    consetetur   |
| e |    sadipscing   | u |    sadipscing   | o |    sadipscing   |
| m | elitr, sed diam | m | elitr, sed diam | r | elitr, sed diam |
+---+-----------------+---+-----------------+---+-----------------+""".strip()
        )

    def test_max_table_width_wide_vrules_frame(self) -> None:
        table = PrettyTable()
        table.max_table_width = 52
        table.vrules = FRAME
        table.add_row(
            [
                0,
                0,
                0,
                0,
                0,
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
                "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam "
                "erat, sed diam voluptua",
            ]
        )

        assert (
            table.get_string().strip()
            == """
+--------------------------------------------------+
| F   F   F   F   F             Field 6            |
+--------------------------------------------------+
| 0   0   0   0   0   Lorem ipsum dolor sit amet,  |
|                     consetetur sadipscing elitr, |
|                        sed diam nonumy eirmod    |
|                     tempor invidunt ut labore et |
|                     dolore magna aliquyam erat,  |
|                          sed diam voluptua       |
+--------------------------------------------------+""".strip()
        )

    def test_max_table_width_wide_vrules_none(self) -> None:
        table = PrettyTable()
        table.max_table_width = 52
        table.vrules = NONE
        table.add_row(
            [
                0,
                0,
                0,
                0,
                0,
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
                "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam "
                "erat, sed diam voluptua",
            ]
        )

        assert (
            table.get_string().strip()
            == """
----------------------------------------------------
  F   F   F   F   F             Field 6             
----------------------------------------------------
  0   0   0   0   0   Lorem ipsum dolor sit amet,   
                      consetetur sadipscing elitr,  
                         sed diam nonumy eirmod     
                      tempor invidunt ut labore et  
                      dolore magna aliquyam erat,   
                           sed diam voluptua        
----------------------------------------------------""".strip()  # noqa: W291
        )


class TestRowEndSection:
    def test_row_end_section(self) -> None:
        table = PrettyTable()
        v = 1
        for row in range(4):
            if row % 2 == 0:
                table.add_row(
                    [f"value {v}", f"value{v+1}", f"value{v+2}"], divider=True
                )
            else:
                table.add_row(
                    [f"value {v}", f"value{v+1}", f"value{v+2}"], divider=False
                )
            v += 3
        table.del_row(0)
        assert (
            table.get_string().strip()
            == """
+----------+---------+---------+
| Field 1  | Field 2 | Field 3 |
+----------+---------+---------+
| value 4  |  value5 |  value6 |
| value 7  |  value8 |  value9 |
+----------+---------+---------+
| value 10 | value11 | value12 |
+----------+---------+---------+
""".strip()
        )


class TestClearing:
    def test_clear_rows(self, row_prettytable: PrettyTable) -> None:
        t = helper_table()
        t.add_row([0, "a", "b", "c"], divider=True)
        t.clear_rows()
        assert t.rows == []
        assert t.dividers == []
        assert t.field_names == ["", "Field 1", "Field 2", "Field 3"]

    def test_clear(self, row_prettytable: PrettyTable) -> None:
        t = helper_table()
        t.add_row([0, "a", "b", "c"], divider=True)
        t.clear()
        assert t.rows == []
        assert t.dividers == []
        assert t.field_names == []


class TestPreservingInternalBorders:
    def test_internal_border_preserved(self) -> None:
        pt = helper_table()
        pt.border = False
        pt.preserve_internal_border = True

        assert (
            pt.get_string().strip()
            == """
   | Field 1 | Field 2 | Field 3  
---+---------+---------+---------
 1 | value 1 |  value2 |  value3  
 4 | value 4 |  value5 |  value6  
 7 | value 7 |  value8 |  value9  
""".strip()  # noqa: W291
        )

    def test_internal_border_preserved_latex(self) -> None:
        pt = helper_table()
        pt.border = False
        pt.format = True
        pt.preserve_internal_border = True

        assert pt.get_latex_string().strip() == (
            "\\begin{tabular}{c|c|c|c}\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\end{tabular}"
        )

    def test_internal_border_preserved_html(self) -> None:
        pt = helper_table()
        pt.format = True
        pt.border = False
        pt.preserve_internal_border = True

        assert (
            pt.get_html_string().strip()
            == """
<table rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center"></th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 2</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value2</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value3</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">4</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 4</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value5</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value6</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">7</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 7</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value8</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value9</td>
        </tr>
    </tbody>
</table>
""".strip()  # noqa: E501
        )


class TestGeneralOutput:
    def test_copy(self) -> None:
        # Arrange
        t = helper_table()

        # Act
        t_copy = t.copy()

        # Assert
        assert t.get_string() == t_copy.get_string()

    def test_text(self) -> None:
        t = helper_table()
        assert t.get_formatted_string("text") == t.get_string()
        # test with default arg, too
        assert t.get_formatted_string() == t.get_string()
        # args passed through
        assert t.get_formatted_string(border=False) == t.get_string(border=False)

    def test_csv(self) -> None:
        t = helper_table()
        assert t.get_formatted_string("csv") == t.get_csv_string()
        # args passed through
        assert t.get_formatted_string("csv", border=False) == t.get_csv_string(
            border=False
        )

    def test_json(self) -> None:
        t = helper_table()
        assert t.get_formatted_string("json") == t.get_json_string()
        # args passed through
        assert t.get_formatted_string("json", border=False) == t.get_json_string(
            border=False
        )

    def test_html(self) -> None:
        t = helper_table()
        assert t.get_formatted_string("html") == t.get_html_string()
        # args passed through
        assert t.get_formatted_string("html", border=False) == t.get_html_string(
            border=False
        )

    def test_latex(self) -> None:
        t = helper_table()
        assert t.get_formatted_string("latex") == t.get_latex_string()
        # args passed through
        assert t.get_formatted_string("latex", border=False) == t.get_latex_string(
            border=False
        )

    def test_invalid(self) -> None:
        t = helper_table()
        with pytest.raises(ValueError):
            t.get_formatted_string("pdf")
