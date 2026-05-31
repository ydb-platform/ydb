from __future__ import annotations

import datetime as dt
import sqlite3
from collections.abc import Generator
from math import e, pi, sqrt
from typing import Any

import pytest
from pytest_lazy_fixtures import lf

import prettytable
from prettytable import (
    HRuleStyle,
    PrettyTable,
    RowType,
    TableStyle,
    VRuleStyle,
    from_db_cursor,
)


def test_version() -> None:
    assert isinstance(prettytable.__version__, str)
    assert prettytable.__version__[0].isdigit()
    assert prettytable.__version__.count(".") >= 2
    assert prettytable.__version__[-1].isdigit()


# Australian capital city data example table
CITY_DATA_HEADER = ["City name", "Area", "Population", "Annual Rainfall"]
CITY_DATA = [
    ["Adelaide", 1295, 1158259, 600.5],
    ["Brisbane", 5905, 1857594, 1146.4],
    ["Darwin", 112, 120900, 1714.7],
    ["Hobart", 1357, 205556, 619.5],
    ["Sydney", 2058, 4336374, 1214.8],
    ["Melbourne", 1566, 3806092, 646.9],
    ["Perth", 5386, 1554769, 869.4],
]


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
        table = PrettyTable(
            ["Field 1", "Field 2", "Field 3", "Field 4"], none_format="N/A"
        )
        table.add_row(["value 1", None, "None", ""])
        assert (
            table.get_string().strip()
            == """
+---------+---------+---------+---------+
| Field 1 | Field 2 | Field 3 | Field 4 |
+---------+---------+---------+---------+
| value 1 |   N/A   |   N/A   |         |
+---------+---------+---------+---------+
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
    def test_equivalence_mediawiki(
        self, left_hand: PrettyTable, right_hand: PrettyTable
    ) -> None:
        assert left_hand.get_mediawiki_string() == right_hand.get_mediawiki_string()


class TestDelete:
    def test_delete_column(self, col_prettytable: PrettyTable) -> None:
        col_prettytable.del_column("Area")

        assert (
            col_prettytable.get_string()
            == """+-----------+------------+-----------------+
| City name | Population | Annual Rainfall |
+-----------+------------+-----------------+
|  Adelaide |  1158259   |      600.5      |
|  Brisbane |  1857594   |      1146.4     |
|   Darwin  |   120900   |      1714.7     |
|   Hobart  |   205556   |      619.5      |
|   Sydney  |  4336374   |      1214.8     |
| Melbourne |  3806092   |      646.9      |
|   Perth   |  1554769   |      869.4      |
+-----------+------------+-----------------+"""
        )

    def test_delete_illegal_column_raises_error(
        self, col_prettytable: PrettyTable
    ) -> None:
        with pytest.raises(ValueError):
            col_prettytable.del_column("City not-a-name")

    def test_delete_row(self, city_data: PrettyTable) -> None:
        city_data.del_row(2)

        assert (
            city_data.get_string()
            == """+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
|   Hobart  | 1357 |   205556   |      619.5      |
|   Sydney  | 2058 |  4336374   |      1214.8     |
| Melbourne | 1566 |  3806092   |      646.9      |
|   Perth   | 5386 |  1554769   |      869.4      |
+-----------+------+------------+-----------------+"""
        )

    def test_delete_row_unavailable(self, city_data: PrettyTable) -> None:
        with pytest.raises(IndexError):
            city_data.del_row(10)


class TestFieldNameLessTable:
    """Make sure that building and stringing a table with no fieldnames works fine"""

    def test_can_string_ascii(self, field_name_less_table: PrettyTable) -> None:
        output = field_name_less_table.get_string()
        assert "|  Field 1  | Field 2 | Field 3 | Field 4 |" in output
        assert "|  Adelaide |   1295  | 1158259 |  600.5  |" in output

    def test_can_string_html(self, field_name_less_table: PrettyTable) -> None:
        output = field_name_less_table.get_html_string()
        assert "<th>Field 1</th>" in output
        assert "<td>Adelaide</td>" in output

    def test_can_string_latex(self, field_name_less_table: PrettyTable) -> None:
        output = field_name_less_table.get_latex_string()
        assert "Field 1 & Field 2 & Field 3 & Field 4 \\\\" in output
        assert "Adelaide & 1295 & 1158259 & 600.5 \\\\" in output

    def test_can_string_mediawiki(self, field_name_less_table: PrettyTable) -> None:
        output = field_name_less_table.get_mediawiki_string(header=True)
        assert "! Field 1 !! Field 2 !! Field 3 !! Field 4" in output
        assert "| Adelaide || 1295 || 1158259 || 600.5" in output

    def test_add_field_names_later(self, field_name_less_table: PrettyTable) -> None:
        field_name_less_table.field_names = CITY_DATA_HEADER
        assert (
            "City name | Area | Population | Annual Rainfall"
            in field_name_less_table.get_string()
        )


@pytest.fixture(scope="function")
def aligned_before_table() -> PrettyTable:
    table = PrettyTable()
    table.align = "r"
    table.field_names = CITY_DATA_HEADER
    for row in CITY_DATA:
        table.add_row(row)
    return table


@pytest.fixture(scope="function")
def aligned_after_table() -> PrettyTable:
    table = PrettyTable()
    table.field_names = CITY_DATA_HEADER
    for row in CITY_DATA:
        table.add_row(row)
    table.align = "r"
    return table


class TestAlignment:
    """Make sure alignment works regardless of when it was set"""

    def test_aligned_ascii(
        self, aligned_before_table: PrettyTable, aligned_after_table: PrettyTable
    ) -> None:
        assert aligned_before_table.get_string() == aligned_after_table.get_string()

    def test_aligned_html(
        self, aligned_before_table: PrettyTable, aligned_after_table: PrettyTable
    ) -> None:
        assert (
            aligned_before_table.get_html_string()
            == aligned_after_table.get_html_string()
        )

    def test_aligned_latex(
        self, aligned_before_table: PrettyTable, aligned_after_table: PrettyTable
    ) -> None:
        assert (
            aligned_before_table.get_latex_string()
            == aligned_after_table.get_latex_string()
        )

    def test_aligned_mediawiki(
        self, aligned_before_table: PrettyTable, aligned_after_table: PrettyTable
    ) -> None:
        assert aligned_before_table.get_mediawiki_string(
            header=True
        ) == aligned_after_table.get_mediawiki_string(header=True)


class TestOptionOverride:
    """Make sure all options are properly overwritten by get_string."""

    def test_border(self, city_data: PrettyTable) -> None:
        assert city_data.get_string() != city_data.get_string(border=False)

    def test_header(self, city_data: PrettyTable) -> None:
        assert city_data.get_string() != city_data.get_string(header=False)

    def test_hrules_all(self, city_data: PrettyTable) -> None:
        assert city_data.get_string() != city_data.get_string(hrules=HRuleStyle.ALL)

    def test_hrules_none(self, city_data: PrettyTable) -> None:
        assert city_data.get_string() != city_data.get_string(hrules=HRuleStyle.NONE)


class TestOptionAttribute:
    """Make sure all options which have an attribute interface work as they should.
    Also make sure option settings are copied correctly when a table is cloned by
    slicing."""

    def test_set_for_all_columns(self, city_data: PrettyTable) -> None:
        city_data.field_names = sorted(city_data.field_names)
        city_data.align = "l"
        city_data.max_width = 10
        city_data.start = 2
        city_data.end = 4
        city_data.sortby = "Area"
        city_data.reversesort = True
        city_data.header = True
        city_data.border = False
        city_data.hrules = HRuleStyle.ALL
        city_data.int_format = "4"
        city_data.float_format = "2.2"
        city_data.padding_width = 2
        city_data.left_padding_width = 2
        city_data.right_padding_width = 2
        city_data.vertical_char = "!"
        city_data.horizontal_char = "~"
        city_data.junction_char = "*"
        city_data.top_junction_char = "@"
        city_data.bottom_junction_char = "#"
        city_data.right_junction_char = "$"
        city_data.left_junction_char = "%"
        city_data.top_right_junction_char = "^"
        city_data.top_left_junction_char = "&"
        city_data.bottom_right_junction_char = "("
        city_data.bottom_left_junction_char = ")"
        city_data.format = True
        city_data.attributes = {"class": "prettytable"}
        assert city_data.get_string() == city_data[:].get_string()

    def test_set_for_one_column(self, city_data: PrettyTable) -> None:
        city_data.align["Rainfall"] = "l"
        city_data.max_width["Name"] = 10
        city_data.int_format["Population"] = "4"
        city_data.float_format["Area"] = "2.2"
        assert city_data.get_string() == city_data[:].get_string()

    def test_preserve_internal_border(self) -> None:
        table = PrettyTable(preserve_internal_border=True)
        assert table.preserve_internal_border is True

    def test_internal_border_preserved(self, helper_table: PrettyTable) -> None:
        helper_table.border = False
        helper_table.preserve_internal_border = True

        assert (
            helper_table.get_string().strip()
            == """
   | Field 1 | Field 2 | Field 3  
---+---------+---------+---------
 1 | value 1 |  value2 |  value3  
 4 | value 4 |  value5 |  value6  
 7 | value 7 |  value8 |  value9  
""".strip()  # noqa: W291
        )


@pytest.fixture(scope="module")
def db_cursor() -> Generator[sqlite3.Cursor]:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    yield cur
    cur.close()
    conn.close()


@pytest.fixture(scope="module")
def init_db(db_cursor: sqlite3.Cursor) -> Generator[Any]:
    db_cursor.execute(
        "CREATE TABLE cities "
        "(name TEXT, area INTEGER, population INTEGER, rainfall REAL)"
    )
    for row in CITY_DATA:
        db_cursor.execute(f"INSERT INTO cities VALUES {tuple(row)}")
    yield
    db_cursor.execute("DROP TABLE cities")


class TestBasic:
    """Some very basic tests."""

    def test_table_rows(self, city_data: PrettyTable) -> None:
        rows = city_data.rows
        assert len(rows) == 7
        assert rows[0] == CITY_DATA[0]

    def test_add_rows(self, city_data: PrettyTable) -> None:
        """A table created with multiple add_row calls is the same as one created
        with a single add_rows
        """
        table = PrettyTable(CITY_DATA_HEADER)
        table.add_rows(CITY_DATA)
        assert str(city_data) == str(table)

        table.add_rows([])
        assert str(city_data) == str(table)

    def _test_no_blank_lines(self, table: PrettyTable) -> None:
        string = table.get_string()
        lines = string.split("\n")
        assert "" not in lines

    def _test_all_length_equal(self, table: PrettyTable) -> None:
        string = table.get_string()
        lines = string.split("\n")
        lengths = {len(line) for line in lines}
        assert len(lengths) == 1

    def test_no_blank_lines(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_with_title(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.title = "My table"
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_with_title(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.title = "My table"
        self._test_all_length_equal(city_data)

    def test_all_lengths_equal_with_long_title(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length, even with a long title."""
        city_data.title = "My table (75 characters wide) " + "=" * 45
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_without_border(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.border = False
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_without_border(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.border = False
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_without_header(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.header = False
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_without_header(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.header = False
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_with_hrules_none(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.hrules = HRuleStyle.NONE
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_with_hrules_none(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.hrules = HRuleStyle.NONE
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_with_hrules_all(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.hrules = HRuleStyle.ALL
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_with_hrules_all(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.hrules = HRuleStyle.ALL
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_with_style_msword(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.set_style(TableStyle.MSWORD_FRIENDLY)
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_with_style_msword(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.set_style(TableStyle.MSWORD_FRIENDLY)
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_with_int_format(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.int_format = "04"
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_with_int_format(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.int_format = "04"
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_with_float_format(self, city_data: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        city_data.float_format = "6.2f"
        self._test_no_blank_lines(city_data)

    def test_all_lengths_equal_with_float_format(self, city_data: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        city_data.float_format = "6.2f"
        self._test_all_length_equal(city_data)

    def test_no_blank_lines_from_csv(self, city_data_from_csv: PrettyTable) -> None:
        """No table should ever have blank lines in it."""
        self._test_no_blank_lines(city_data_from_csv)

    def test_all_lengths_equal_from_csv(self, city_data_from_csv: PrettyTable) -> None:
        """All lines in a table should be of the same length."""
        self._test_all_length_equal(city_data_from_csv)

    def test_no_blank_lines_from_mediawiki(
        self, city_data_from_mediawiki: PrettyTable
    ) -> None:
        """No table should ever have blank lines in it."""
        self._test_no_blank_lines(city_data_from_mediawiki)

    def test_all_lengths_equal_from_mediawiki(
        self, city_data_from_mediawiki: PrettyTable
    ) -> None:
        """All lines in a table should be of the same length."""
        self._test_all_length_equal(city_data_from_mediawiki)

    def test_rowcount(self, city_data: PrettyTable) -> None:
        assert city_data.rowcount == 7

    def test_colcount(self, city_data: PrettyTable) -> None:
        assert city_data.colcount == 4

    def test_getitem(self, city_data: PrettyTable) -> None:
        assert (
            city_data[1].get_string()
            == """+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Brisbane | 5905 |  1857594   |      1146.4     |
+-----------+------+------------+-----------------+"""
        )

    def test_invalid_getitem(self, city_data: PrettyTable) -> None:
        with pytest.raises(IndexError):
            assert city_data[10]

    @pytest.mark.usefixtures("init_db")
    def test_no_blank_lines_from_db(self, db_cursor: sqlite3.Cursor) -> None:
        """No table should ever have blank lines in it."""
        db_cursor.execute("SELECT * FROM cities")
        table = from_db_cursor(db_cursor)
        assert table is not None
        self._test_no_blank_lines(table)

    @pytest.mark.usefixtures("init_db")
    def test_all_lengths_equal_from_db(self, db_cursor: sqlite3.Cursor) -> None:
        """No table should ever have blank lines in it."""
        db_cursor.execute("SELECT * FROM cities")
        table = from_db_cursor(db_cursor)
        assert table is not None
        self._test_all_length_equal(table)


class TestEmptyTable:
    """Make sure the print_empty option works"""

    def test_print_empty_true(self, city_data: PrettyTable) -> None:
        table = PrettyTable()
        table.field_names = CITY_DATA_HEADER

        assert table.get_string(print_empty=True) != ""
        assert table.get_string(print_empty=True) != city_data.get_string(
            print_empty=True
        )

    def test_print_empty_false(self, city_data: PrettyTable) -> None:
        table = PrettyTable()
        table.field_names = CITY_DATA_HEADER

        assert table.get_string(print_empty=False) == ""
        assert table.get_string(print_empty=False) != city_data.get_string(
            print_empty=False
        )

    def test_interaction_with_border(self) -> None:
        table = PrettyTable()
        table.field_names = CITY_DATA_HEADER

        assert table.get_string(border=False, print_empty=True) == ""


class TestSlicing:
    def test_slice_all(self, city_data: PrettyTable) -> None:
        table = city_data[:]
        assert city_data.get_string() == table.get_string()

    def test_slice_first_two_rows(self, city_data: PrettyTable) -> None:
        table = city_data[0:2]
        string = table.get_string()
        assert len(string.split("\n")) == 6
        for row_index in (0, 1):
            city = CITY_DATA[row_index][0]
            assert isinstance(city, str)
            assert city in string
        for row_index in (2, 3, 4, 5, 6):
            city = CITY_DATA[row_index][0]
            assert isinstance(city, str)
            assert city not in string

    def test_slice_last_two_rows(self, city_data: PrettyTable) -> None:
        table = city_data[-2:]
        string = table.get_string()
        assert len(string.split("\n")) == 6
        for row_index in (0, 1, 2, 3, 4):
            city = CITY_DATA[row_index][0]
            assert isinstance(city, str)
            assert city not in string
        for row_index in (5, 6):
            city = CITY_DATA[row_index][0]
            assert isinstance(city, str)
            assert city in string


class TestRowFilter:
    EXPECTED_RESULT = """+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
|   Sydney  | 2058 |  4336374   |      1214.8     |
| Melbourne | 1566 |  3806092   |      646.9      |
|   Perth   | 5386 |  1554769   |      869.4      |
+-----------+------+------------+-----------------+"""

    def filter_function(self, vals: RowType) -> bool:
        return vals[2] > 999999

    def test_row_filter(self, city_data: PrettyTable) -> None:
        city_data.row_filter = self.filter_function
        assert city_data.row_filter == self.filter_function
        assert self.EXPECTED_RESULT == city_data.get_string()

    def test_row_filter_at_class_declaration(self) -> None:
        table = PrettyTable(
            field_names=CITY_DATA_HEADER,
            row_filter=self.filter_function,
        )
        for row in CITY_DATA:
            table.add_row(row)
        assert table.row_filter == self.filter_function
        assert self.EXPECTED_RESULT == table.get_string().strip()


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


class TestColumnFormattingfromDict:
    def test_set_align_format(self, city_data: PrettyTable) -> None:
        city_data.align = {"Annual Rainfall": "r"}
        assert (
            city_data.get_string()
            == """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Adelaide | 1295 |  1158259   |           600.5 |
|  Brisbane | 5905 |  1857594   |          1146.4 |
|   Darwin  | 112  |   120900   |          1714.7 |
|   Hobart  | 1357 |   205556   |           619.5 |
|   Sydney  | 2058 |  4336374   |          1214.8 |
| Melbourne | 1566 |  3806092   |           646.9 |
|   Perth   | 5386 |  1554769   |           869.4 |
+-----------+------+------------+-----------------+
""".strip()
        )

    def test_set_valign_format(self, city_data: PrettyTable) -> None:
        table = PrettyTable(
            ["Field 1", "Field 2", "Field 3", "Field 4", "Field 5", "Field 6"],
        )
        table.valign = {"Field 1": "m"}
        table.max_width = {"Field 2": 20, "Field 4": 10, "Field 6": 10}
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
            table.get_string()
            == """
+---------+----------------------+---------+------------+---------+------------+
| Field 1 |       Field 2        | Field 3 |  Field 4   | Field 5 |  Field 6   |
+---------+----------------------+---------+------------+---------+------------+
|         |  Lorem ipsum dolor   |  ipsum  |   Lorem    |  dolor  |   Lorem    |
|         | sit amet, consetetur |         |   ipsum    |         |   ipsum    |
|         |  sadipscing elitr,   |         | dolor sit  |         | dolor sit  |
|  Lorem  |       sed diam       |         |   amet,    |         |   amet,    |
|         |                      |         | consetetur |         | consetetur |
|         |                      |         | sadipscing |         | sadipscing |
|         |                      |         | elitr, sed |         | elitr, sed |
|         |                      |         |    diam    |         |    diam    |
+---------+----------------------+---------+------------+---------+------------+
""".strip()
        )

    def test_max_width(
        self,
    ) -> None:
        table = PrettyTable(
            ["Field 1", "Field 2", "Field 3", "Field 4", "Field 5", "Field 6"],
        )
        table.max_width = {"Field 2": 20, "Field 4": 10, "Field 6": 10}
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
            table.get_string()
            == """
+---------+----------------------+---------+------------+---------+------------+
| Field 1 |       Field 2        | Field 3 |  Field 4   | Field 5 |  Field 6   |
+---------+----------------------+---------+------------+---------+------------+
|  Lorem  |  Lorem ipsum dolor   |  ipsum  |   Lorem    |  dolor  |   Lorem    |
|         | sit amet, consetetur |         |   ipsum    |         |   ipsum    |
|         |  sadipscing elitr,   |         | dolor sit  |         | dolor sit  |
|         |       sed diam       |         |   amet,    |         |   amet,    |
|         |                      |         | consetetur |         | consetetur |
|         |                      |         | sadipscing |         | sadipscing |
|         |                      |         | elitr, sed |         | elitr, sed |
|         |                      |         |    diam    |         |    diam    |
+---------+----------------------+---------+------------+---------+------------+
""".strip()
        )

    def test_min_width(self, city_data: PrettyTable) -> None:
        city_data.min_width = {
            "City name": 20,
            "Area": 10,
            "Population": 20,
            "Annual Rainfall": 20,
        }
        assert (
            city_data.get_string()
            == """
+----------------------+------------+----------------------+----------------------+
|      City name       |    Area    |      Population      |   Annual Rainfall    |
+----------------------+------------+----------------------+----------------------+
|       Adelaide       |    1295    |       1158259        |        600.5         |
|       Brisbane       |    5905    |       1857594        |        1146.4        |
|        Darwin        |    112     |        120900        |        1714.7        |
|        Hobart        |    1357    |        205556        |        619.5         |
|        Sydney        |    2058    |       4336374        |        1214.8        |
|      Melbourne       |    1566    |       3806092        |        646.9         |
|        Perth         |    5386    |       1554769        |        869.4         |
+----------------------+------------+----------------------+----------------------+
""".strip()
        )

    def test_set_int_format(self, city_data: PrettyTable) -> None:
        city_data.int_format = {"Population": "20"}
        assert (
            city_data.get_string()
            == """
+-----------+------+----------------------+-----------------+
| City name | Area |      Population      | Annual Rainfall |
+-----------+------+----------------------+-----------------+
|  Adelaide | 1295 |              1158259 |      600.5      |
|  Brisbane | 5905 |              1857594 |      1146.4     |
|   Darwin  | 112  |               120900 |      1714.7     |
|   Hobart  | 1357 |               205556 |      619.5      |
|   Sydney  | 2058 |              4336374 |      1214.8     |
| Melbourne | 1566 |              3806092 |      646.9      |
|   Perth   | 5386 |              1554769 |      869.4      |
+-----------+------+----------------------+-----------------+
""".strip()
        )

    def test_set_float_format(self, city_data: PrettyTable) -> None:
        city_data.float_format = {"Annual Rainfall": "4.2"}
        assert (
            city_data.get_string()
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

    def test_set_custom_format(self, city_data: PrettyTable) -> None:
        city_data.custom_format = {"Annual Rainfall": lambda f, v: f"{v:.2f}"}
        assert (
            city_data.get_string()
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

    def test_set_none_format(self, city_data: PrettyTable) -> None:
        city_data.clear_rows()
        city_data.add_row([None, None, None, None])
        city_data.none_format = {"Annual Rainfall": "N/A"}
        assert (
            city_data.get_string()
            == """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|    None   | None |    None    |       N/A       |
+-----------+------+------------+-----------------+
""".strip()
        )


class TestBreakLine:
    @pytest.mark.parametrize(
        ["rows", "hrule", "expected_result"],
        [
            (
                [["value 1", "value2\nsecond line"], ["value 3", "value4"]],
                HRuleStyle.ALL,
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
                HRuleStyle.ALL,
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
                HRuleStyle.FRAME,
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


class TestFromDB:
    @pytest.mark.usefixtures("init_db")
    def test_non_select_cursor(self, db_cursor: sqlite3.Cursor) -> None:
        db_cursor.execute(f"INSERT INTO cities VALUES {tuple(CITY_DATA[0])}")
        assert from_db_cursor(db_cursor) is None


class TestCsvOutput:
    def test_csv_output(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_csv_string(delimiter="\t", header=False) == (
            "1\tvalue 1\tvalue2\tvalue3\r\n"
            "4\tvalue 4\tvalue5\tvalue6\r\n"
            "7\tvalue 7\tvalue8\tvalue9\r\n"
        )
        assert helper_table.get_csv_string() == (
            ",Field 1,Field 2,Field 3\r\n"
            "1,value 1,value2,value3\r\n"
            "4,value 4,value5,value6\r\n"
            "7,value 7,value8,value9\r\n"
        )
        options = {"fields": ["Field 1", "Field 3"]}
        assert helper_table.get_csv_string(**options) == (
            "Field 1,Field 3\r\n"
            "value 1,value3\r\n"
            "value 4,value6\r\n"
            "value 7,value9\r\n"
        )


def test_paginate(city_data: PrettyTable) -> None:
    expected_page_1 = """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|  Adelaide | 1295 |  1158259   |      600.5      |
|  Brisbane | 5905 |  1857594   |      1146.4     |
|   Darwin  | 112  |   120900   |      1714.7     |
|   Hobart  | 1357 |   205556   |      619.5      |
+-----------+------+------------+-----------------+""".strip()
    expected_page_2 = """
+-----------+------+------------+-----------------+
| City name | Area | Population | Annual Rainfall |
+-----------+------+------------+-----------------+
|   Sydney  | 2058 |  4336374   |      1214.8     |
| Melbourne | 1566 |  3806092   |      646.9      |
|   Perth   | 5386 |  1554769   |      869.4      |
+-----------+------+------------+-----------------+""".strip()

    paginated = city_data.paginate(page_length=4).strip()
    assert paginated.startswith(expected_page_1)
    assert "\f" in paginated
    assert paginated.endswith(expected_page_2)

    paginated = city_data.paginate(page_length=4, line_break="\n")
    assert "\f" not in paginated
    assert "\n" in paginated


def test_autoindex(city_data: PrettyTable) -> None:
    """Testing that a table with a custom index row is
    equal to the one produced by the function
    .add_autoindex()
    """
    city_data.field_names = CITY_DATA_HEADER
    city_data.add_autoindex(fieldname="Test")

    table2 = PrettyTable()
    table2.field_names = ["Test"] + CITY_DATA_HEADER
    for idx, row in enumerate(CITY_DATA):
        table2.add_row([idx + 1] + row)

    assert str(city_data) == str(table2)


@pytest.fixture(scope="function")
def unpadded_pt() -> PrettyTable:
    table = PrettyTable(header=False, padding_width=0)
    table.add_row(list("abc"))
    table.add_row(list("def"))
    table.add_row(list("g.."))
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
        assert isinstance(table.custom_format, dict)

    def test_set_custom_format_invalid_type_throw_error(self) -> None:
        table = PrettyTable()
        with pytest.raises(TypeError) as e:
            table.custom_format = "Some String"  # type: ignore[assignment]
        assert "The custom_format property need to be a dictionary or callable" in str(
            e.value
        )

    def test_use_custom_formatter_for_int(self, city_data: PrettyTable) -> None:
        city_data.custom_format["Annual Rainfall"] = lambda n, v: f"{v:.2f}"
        assert (
            city_data.get_string().strip()
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


class TestBreakOnHyphens:
    row = [
        "bluedevil breeze breeze-gtk eos-bash-shared glib2 "
        "kactivitymanagerd kde-cli-tools kde-gtk-config kdecoration"
    ]
    EXPECTED_TRUE = """+------------------------------------------+
|                 Field 1                  |
+------------------------------------------+
|  bluedevil breeze breeze-gtk eos-bash-   |
| shared glib2 kactivitymanagerd kde-cli-  |
|     tools kde-gtk-config kdecoration     |
+------------------------------------------+"""
    EXPECTED_FALSE = """+------------------------------------------+
|                 Field 1                  |
+------------------------------------------+
|       bluedevil breeze breeze-gtk        |
| eos-bash-shared glib2 kactivitymanagerd  |
| kde-cli-tools kde-gtk-config kdecoration |
+------------------------------------------+"""

    def test_break_on_hyphens(self) -> None:
        table = PrettyTable(max_width=40)
        table.break_on_hyphens = False
        assert not table.break_on_hyphens
        table.add_row(self.row)
        assert table.get_string().strip() == self.EXPECTED_FALSE

    def test_break_on_hyphens_on_init(self) -> None:
        table = PrettyTable(max_width=40, break_on_hyphens=False)
        assert not table._break_on_hyphens
        assert not table.break_on_hyphens
        table.add_row(self.row)
        assert table.get_string().strip() == self.EXPECTED_FALSE

    def test_break_on_hyphens_default(self) -> None:
        table = PrettyTable(max_width=40)
        assert table.break_on_hyphens
        table.add_row(self.row)
        assert table.get_string().strip() == self.EXPECTED_TRUE


class TestWidth:
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
        self,
        loops: int,
        fields: list[str],
        desired_width: int,
        border: bool,
        internal_border: bool,
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

    @pytest.mark.parametrize("set_width_parameter", [True, False])
    def test_table_max_width_wo_header_width(self, set_width_parameter: bool) -> None:
        headers = [
            "A Field Name",
            "B Field Name",
            "D Field Name",
            "E Field Name",
            "F Field Name",
            "G Field Name",
            "H Field Name",
            "I Field Name",
            "J Field Name",
            "K Field Name",
            "L Field Name",
            "M Field Name",
        ]
        row = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        expected = """+---+---+---+---+---+---+---+---+---+---+----+----+
| A | B | D | E | F | G | H | I | J | K | L  | M  |
+---+---+---+---+---+---+---+---+---+---+----+----+
| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 |
+---+---+---+---+---+---+---+---+---+---+----+----+"""

        if set_width_parameter:
            table = PrettyTable(headers, use_header_width=False)
        else:
            table = PrettyTable(headers)
            table.use_header_width = False
        table.add_row(row)

        assert table.get_string() == expected

    def test_table_width_on_init_wo_columns(self) -> None:
        """See also #272"""
        table = PrettyTable(max_width=10)
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
+---------+------------+---------+------------+---------+------------+
| Field 1 |  Field 2   | Field 3 |  Field 4   | Field 5 |  Field 6   |
+---------+------------+---------+------------+---------+------------+
|  Lorem  |   Lorem    |  ipsum  |   Lorem    |  dolor  |   Lorem    |
|         |   ipsum    |         |   ipsum    |         |   ipsum    |
|         | dolor sit  |         | dolor sit  |         | dolor sit  |
|         |   amet,    |         |   amet,    |         |   amet,    |
|         | consetetur |         | consetetur |         | consetetur |
|         | sadipscing |         | sadipscing |         | sadipscing |
|         | elitr, sed |         | elitr, sed |         | elitr, sed |
|         |    diam    |         |    diam    |         |    diam    |
+---------+------------+---------+------------+---------+------------+""".strip()
        )

    def test_table_width_on_init_with_columns(self) -> None:
        """See also #272"""
        table = PrettyTable(
            ["Field 1", "Field 2", "Field 3", "Field 4", "Field 5", "Field 6"],
            max_width=10,
        )
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
+---------+------------+---------+------------+---------+------------+
| Field 1 |  Field 2   | Field 3 |  Field 4   | Field 5 |  Field 6   |
+---------+------------+---------+------------+---------+------------+
|  Lorem  |   Lorem    |  ipsum  |   Lorem    |  dolor  |   Lorem    |
|         |   ipsum    |         |   ipsum    |         |   ipsum    |
|         | dolor sit  |         | dolor sit  |         | dolor sit  |
|         |   amet,    |         |   amet,    |         |   amet,    |
|         | consetetur |         | consetetur |         | consetetur |
|         | sadipscing |         | sadipscing |         | sadipscing |
|         | elitr, sed |         | elitr, sed |         | elitr, sed |
|         |    diam    |         |    diam    |         |    diam    |
+---------+------------+---------+------------+---------+------------+""".strip()
        )

    def test_table_minwidth_on_init_with_columns(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"], min_width=20)
        table.add_row(
            [
                "Lorem",
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam ",
            ]
        )

        assert (
            table.get_string().strip()
            == """+----------------------+--------------------------------------------------------------------+
|       Field 1        |                              Field 2                               |
+----------------------+--------------------------------------------------------------------+
|        Lorem         | Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam  |
+----------------------+--------------------------------------------------------------------+"""  # noqa: E501
        )

    def test_table_min_max_width_on_init_with_columns(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"], min_width=20, max_width=40)
        table.add_row(
            [
                "Lorem",
                "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam ",
            ]
        )

        assert (
            table.get_string().strip()
            == """+----------------------+------------------------------------------+
|       Field 1        |                 Field 2                  |
+----------------------+------------------------------------------+
|        Lorem         |  Lorem ipsum dolor sit amet, consetetur  |
|                      |        sadipscing elitr, sed diam        |
+----------------------+------------------------------------------+"""
        )

    def test_table_float_formatting_on_init_wo_columns(self) -> None:
        """See also #243"""
        table = prettytable.PrettyTable(float_format="10.2")
        table.field_names = ["Metric", "Initial sol.", "Best sol."]
        table.add_rows([["foo", 1.0 / 3.0, 1.0 / 3.0]])

        assert (
            table.get_string().strip()
            == """
+--------+--------------+------------+
| Metric | Initial sol. | Best sol.  |
+--------+--------------+------------+
|  foo   |        0.33  |       0.33 |
+--------+--------------+------------+""".strip()
        )

    def test_max_table_width_wide_vrules_frame(self) -> None:
        table = PrettyTable()
        table.max_table_width = 52
        table.vrules = VRuleStyle.FRAME
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
        table.vrules = VRuleStyle.NONE
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


class TestFields:
    def test_fields_at_class_declaration(self) -> None:
        table = PrettyTable(
            field_names=CITY_DATA_HEADER,
            fields=["City name", "Annual Rainfall"],
        )
        for row in CITY_DATA:
            table.add_row(row)
        assert (
            """+-----------+-----------------+
| City name | Annual Rainfall |
+-----------+-----------------+
|  Adelaide |      600.5      |
|  Brisbane |      1146.4     |
|   Darwin  |      1714.7     |
|   Hobart  |      619.5      |
|   Sydney  |      1214.8     |
| Melbourne |      646.9      |
|   Perth   |      869.4      |
+-----------+-----------------+"""
            == table.get_string().strip()
        )

    def test_fields(self) -> None:
        table = PrettyTable()
        table.field_names = CITY_DATA_HEADER
        table.fields = ["City name", "Annual Rainfall"]
        for row in CITY_DATA:
            table.add_row(row)
        assert (
            """+-----------+-----------------+
| City name | Annual Rainfall |
+-----------+-----------------+
|  Adelaide |      600.5      |
|  Brisbane |      1146.4     |
|   Darwin  |      1714.7     |
|   Hobart  |      619.5      |
|   Sydney  |      1214.8     |
| Melbourne |      646.9      |
|   Perth   |      869.4      |
+-----------+-----------------+"""
            == table.get_string().strip()
        )


class TestGeneralOutput:
    def test_copy(self, helper_table: PrettyTable) -> None:
        t_copy = helper_table.copy()
        assert helper_table.get_string() == t_copy.get_string()

    def test_text(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_formatted_string("text") == helper_table.get_string()
        # test with default arg, too
        assert helper_table.get_formatted_string() == helper_table.get_string()
        # args passed through
        assert helper_table.get_formatted_string(
            border=False
        ) == helper_table.get_string(border=False)

    def test_csv(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_formatted_string("csv") == helper_table.get_csv_string()
        # args passed through
        assert helper_table.get_formatted_string(
            "csv", border=False
        ) == helper_table.get_csv_string(border=False)

    def test_json(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_formatted_string("json") == helper_table.get_json_string()
        )
        # args passed through
        assert helper_table.get_formatted_string(
            "json", border=False
        ) == helper_table.get_json_string(border=False)

    def test_html(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_formatted_string("html") == helper_table.get_html_string()
        )
        # args passed through
        assert helper_table.get_formatted_string(
            "html", border=False
        ) == helper_table.get_html_string(border=False)

    def test_latex(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_formatted_string("latex")
            == helper_table.get_latex_string()
        )
        # args passed through
        assert helper_table.get_formatted_string(
            "latex", border=False
        ) == helper_table.get_latex_string(border=False)

    def test_mediawiki(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_formatted_string(
            "mediawiki", border=False
        ) == helper_table.get_mediawiki_string(border=False)

    def test_invalid(self, helper_table: PrettyTable) -> None:
        with pytest.raises(ValueError):
            helper_table.get_formatted_string("pdf")


class TestDeprecations:
    @pytest.mark.parametrize(
        "module_name",
        [
            "prettytable",
            "prettytable.prettytable",
        ],
    )
    @pytest.mark.parametrize(
        "name",
        [
            "FRAME",
            "ALL",
            "NONE",
            "HEADER",
        ],
    )
    def test_hrule_constant_deprecations(self, module_name: str, name: str) -> None:
        with pytest.deprecated_call(match=f"the '{name}' constant is deprecated"):
            exec(f"from {module_name} import {name}")

    @pytest.mark.parametrize(
        "module_name",
        [
            "prettytable",
            "prettytable.prettytable",
        ],
    )
    @pytest.mark.parametrize(
        "name",
        [
            "DEFAULT",
            "MSWORD_FRIENDLY",
            "PLAIN_COLUMNS",
            "MARKDOWN",
            "ORGMODE",
            "DOUBLE_BORDER",
            "SINGLE_BORDER",
            "RANDOM",
        ],
    )
    def test_table_style_constant_deprecations(
        self, module_name: str, name: str
    ) -> None:
        with pytest.deprecated_call(match=f"the '{name}' constant is deprecated"):
            exec(f"from {module_name} import {name}")
