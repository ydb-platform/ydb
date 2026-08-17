from __future__ import annotations

from prettytable import PrettyTable, TableStyle


def test_rst_output_without_header(helper_table: PrettyTable) -> None:
    helper_table.set_style(TableStyle.RST)
    assert helper_table.get_string(header=False).strip() == """
+---+---------+--------+--------+
| 1 | value 1 | value2 | value3 |
+---+---------+--------+--------+
| 4 | value 4 | value5 | value6 |
+---+---------+--------+--------+
| 7 | value 7 | value8 | value9 |
+---+---------+--------+--------+
""".strip()


def test_rst_output_with_fields(helper_table: PrettyTable) -> None:
    helper_table.set_style(TableStyle.RST)
    assert helper_table.get_string(fields=["Field 1", "Field 3"]).strip() == """
+---------+---------+
| Field 1 | Field 3 |
+=========+=========+
| value 1 |  value3 |
+---------+---------+
| value 4 |  value6 |
+---------+---------+
| value 7 |  value9 |
+---------+---------+
""".strip()


def test_rst_header_uses_equals(helper_table: PrettyTable) -> None:
    # Arrange
    helper_table.set_style(TableStyle.RST)

    # Act
    result = helper_table.get_string()

    # Assert
    lines = result.splitlines()
    # The header separator (line after header row) should use "="
    header_sep = lines[2]
    assert "=" in header_sep
    assert header_sep.startswith("+")
    assert header_sep.endswith("+")
    # Data separators should use "-"
    data_sep = lines[4]
    assert "=" not in data_sep
    assert "-" in data_sep


def test_rst_style_does_not_leak(helper_table: PrettyTable) -> None:
    # Arrange
    original = helper_table.get_string()

    # Act
    helper_table.set_style(TableStyle.RST)
    helper_table.set_style(TableStyle.DEFAULT)

    # Assert
    assert helper_table.get_string() == original


def test_rst_output_with_multiline_title(helper_table: PrettyTable) -> None:
    # Arrange
    helper_table.set_style(TableStyle.RST)
    # Act
    helper_table.title = "Line 1\nLine 2"
    # Assert
    assert helper_table.get_string() == """
+---------------------------------+
|              Line 1             |
|              Line 2             |
+---+---------+---------+---------+
|   | Field 1 | Field 2 | Field 3 |
+===+=========+=========+=========+
| 1 | value 1 |  value2 |  value3 |
+---+---------+---------+---------+
| 4 | value 4 |  value5 |  value6 |
+---+---------+---------+---------+
| 7 | value 7 |  value8 |  value9 |
+---+---------+---------+---------+
""".strip()


def test_markdown_to_rst_does_not_leak(helper_table: PrettyTable) -> None:
    # Arrange
    helper_table.set_style(TableStyle.MARKDOWN)
    helper_table.set_style(TableStyle.RST)

    # Act
    result = helper_table.get_string()

    # Assert: Markdown's ":" alignment char should not appear in RST output
    assert ":" not in result
