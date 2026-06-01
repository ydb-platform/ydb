from __future__ import annotations

import pytest

from prettytable import HRuleStyle, PrettyTable, from_html, from_html_one


class TestHtmlConstructor:
    def test_html_and_back(self, city_data: PrettyTable) -> None:
        html_string = city_data.get_html_string()
        new_table = from_html(html_string)[0]
        assert new_table.get_string() == city_data.get_string()

    def test_html_one_and_back(self, city_data: PrettyTable) -> None:
        html_string = city_data.get_html_string()
        new_table = from_html_one(html_string)
        assert new_table.get_string() == city_data.get_string()

    def test_html_one_fail_on_many(self, city_data: PrettyTable) -> None:
        html_string = city_data.get_html_string()
        html_string += city_data.get_html_string()
        with pytest.raises(ValueError):
            from_html_one(html_string)


class TestHtmlOutput:
    def test_html_output(self, helper_table: PrettyTable) -> None:
        result = helper_table.get_html_string()
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

    def test_html_output_formatted(self, helper_table: PrettyTable) -> None:
        result = helper_table.get_html_string(format=True)
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

    def test_html_output_with_title(self, helper_table: PrettyTable) -> None:
        helper_table.title = "Title & Title"
        result = helper_table.get_html_string(
            attributes={"bgcolor": "red", "a<b": "1<2"}
        )
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

    def test_html_output_formatted_with_title(self, helper_table: PrettyTable) -> None:
        helper_table.title = "Title & Title"
        result = helper_table.get_html_string(
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

    def test_html_output_without_escaped_header(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.field_names = [
            "",
            "Field 1",
            "<em>Field 2</em>",
            "<a href='#'>Field 3</a>",
        ]
        result = empty_helper_table.get_html_string(escape_header=False)
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

    def test_html_output_without_escaped_data(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = empty_helper_table.get_html_string(escape_data=False)
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

    def test_html_output_with_escaped_header(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.field_names = [
            "",
            "Field 1",
            "<em>Field 2</em>",
            "<a href='#'>Field 3</a>",
        ]
        result = empty_helper_table.get_html_string(escape_header=True)
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

    def test_html_output_with_escaped_data(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = empty_helper_table.get_html_string(escape_data=True)
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

    def test_html_output_formatted_without_escaped_header(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.field_names = [
            "",
            "Field 1",
            "<em>Field 2</em>",
            "<a href='#'>Field 3</a>",
        ]
        result = empty_helper_table.get_html_string(escape_header=False, format=True)
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

    def test_html_output_formatted_without_escaped_data(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = empty_helper_table.get_html_string(escape_data=False, format=True)
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

    def test_html_output_formatted_with_escaped_header(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.field_names = [
            "",
            "Field 1",
            "<em>Field 2</em>",
            "<a href='#'>Field 3</a>",
        ]
        result = empty_helper_table.get_html_string(escape_header=True, format=True)
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

    def test_html_output_formatted_with_escaped_data(
        self, empty_helper_table: PrettyTable
    ) -> None:
        empty_helper_table.add_row(
            [
                1,
                "<b>value 1</b>",
                "<span style='text-decoration: underline;'>value2</span>",
                "<a href='#'>value3</a>",
            ]
        )
        result = empty_helper_table.get_html_string(escape_data=True, format=True)
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

    def test_table_formatted_html_autoindex(self) -> None:
        """See also #199"""
        table = PrettyTable(["Field 1", "Field 2", "Field 3"])
        for row in range(1, 3 * 3, 3):
            table.add_row(
                [f"value {row*100}", f"value {row+1*100}", f"value {row+2*100}"]
            )
        table.format = True
        table.add_autoindex("I")

        assert (
            table.get_html_string().strip()
            == """
<table frame="box" rules="cols">
    <thead>
        <tr>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">I</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 1</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 2</th>
            <th style="padding-left: 1em; padding-right: 1em; text-align: center">Field 3</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">1</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 100</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 101</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 201</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">2</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 400</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 104</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 204</td>
        </tr>
        <tr>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">3</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 700</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 107</td>
            <td style="padding-left: 1em; padding-right: 1em; text-align: center; vertical-align: top">value 207</td>
        </tr>
    </tbody>
</table>""".strip()  # noqa: E501
        )

    def test_internal_border_preserved_html(self, helper_table: PrettyTable) -> None:
        helper_table.format = True
        helper_table.border = False
        helper_table.preserve_internal_border = True

        assert (
            helper_table.get_html_string().strip()
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

    def test_break_line_html(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"])
        table.add_row(["value 1", "value2\nsecond line"])
        table.add_row(["value 3", "value4"])
        result = table.get_html_string(hrules=HRuleStyle.ALL)
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

    def test_break_line_xhtml(self) -> None:
        table = PrettyTable(["Field 1", "Field 2"])
        table.add_row(["value 1", "value2\nsecond line"])
        table.add_row(["value 3", "value4"])
        result = table.get_html_string(hrules=HRuleStyle.ALL, xhtml=True)
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
            <td>value2<br/>second line</td>
        </tr>
        <tr>
            <td>value 3</td>
            <td>value4</td>
        </tr>
    </tbody>
</table>
""".strip()
        )
