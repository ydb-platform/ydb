from __future__ import annotations

from prettytable import PrettyTable, TableStyle


class TestRowEndSection:
    EXPECTED_RESULT = """
┌──────────┬──────────┬──────────┐
│ Field 1  │ Field 2  │ Field 3  │
├──────────┼──────────┼──────────┤
│ value 4  │ value 5  │ value 6  │
│ value 7  │ value 8  │ value 9  │
├──────────┼──────────┼──────────┤
│ value 10 │ value 11 │ value 12 │
└──────────┴──────────┴──────────┘
""".strip()

    TEST_ROWS = [
        ["value 4", "value 5", "value 6"],
        ["value 7", "value 8", "value 9"],
        ["value 10", "value 11", "value 12"],
    ]

    def test_row_end_section_via_argument(self) -> None:
        table = PrettyTable()
        table.set_style(TableStyle.SINGLE_BORDER)
        table.add_row(self.TEST_ROWS[0])
        table.add_row(self.TEST_ROWS[1], divider=True)
        table.add_row(self.TEST_ROWS[2])
        assert table.get_string().strip() == self.EXPECTED_RESULT

    def test_row_end_section_via_method(self) -> None:
        table = PrettyTable()
        table.set_style(TableStyle.SINGLE_BORDER)
        table.add_row(self.TEST_ROWS[0])
        table.add_row(self.TEST_ROWS[1])
        table.add_divider()
        table.add_row(self.TEST_ROWS[2])
        assert table.get_string().strip() == self.EXPECTED_RESULT

    def test_add_rows_divider(self) -> None:
        """A table created with two add_rows calls, one with divider=True has a
        divider"""
        table = PrettyTable()
        table.set_style(TableStyle.SINGLE_BORDER)
        table.add_rows(self.TEST_ROWS[0:2], divider=True)
        table.add_rows(self.TEST_ROWS[2:])
        assert table.get_string().strip() == self.EXPECTED_RESULT


class TestClearing:
    def test_clear_rows(self, helper_table: PrettyTable) -> None:
        helper_table.add_row([0, "a", "b", "c"], divider=True)
        helper_table.clear_rows()
        assert helper_table.rows == []
        assert helper_table.dividers == []
        assert helper_table.field_names == ["", "Field 1", "Field 2", "Field 3"]

    def test_clear(self, helper_table: PrettyTable) -> None:
        helper_table.add_row([0, "a", "b", "c"], divider=True)
        helper_table.clear()
        assert helper_table.rows == []
        assert helper_table.dividers == []
        assert helper_table.field_names == []
