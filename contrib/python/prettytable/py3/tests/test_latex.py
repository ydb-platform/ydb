from __future__ import annotations

from prettytable import HRuleStyle, PrettyTable, VRuleStyle


class TestLatexOutput:
    def test_latex_output(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_latex_string() == (
            "\\begin{tabular}{cccc}\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\end{tabular}"
        )
        options = {"fields": ["Field 1", "Field 3"]}
        assert helper_table.get_latex_string(**options) == (
            "\\begin{tabular}{cc}\r\n"
            "Field 1 & Field 3 \\\\\r\n"
            "value 1 & value3 \\\\\r\n"
            "value 4 & value6 \\\\\r\n"
            "value 7 & value9 \\\\\r\n"
            "\\end{tabular}"
        )

    def test_latex_output_formatted(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_latex_string(format=True) == (
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
        assert helper_table.get_latex_string(format=True, **options) == (
            "\\begin{tabular}{|c|c|}\r\n"
            "\\hline\r\n"
            "Field 1 & Field 3 \\\\\r\n"
            "value 1 & value3 \\\\\r\n"
            "value 4 & value6 \\\\\r\n"
            "value 7 & value9 \\\\\r\n"
            "\\hline\r\n"
            "\\end{tabular}"
        )

        vrule_options: dict[str, VRuleStyle] = {"vrules": VRuleStyle.FRAME}
        assert helper_table.get_latex_string(format=True, **vrule_options) == (
            "\\begin{tabular}{|cccc|}\r\n"
            "\\hline\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\hline\r\n"
            "\\end{tabular}"
        )

        hrule_options: dict[str, HRuleStyle] = {"hrules": HRuleStyle.ALL}
        assert helper_table.get_latex_string(format=True, **hrule_options) == (
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

    def test_latex_output_header(self, helper_table: PrettyTable) -> None:
        assert helper_table.get_latex_string(format=True, hrules=HRuleStyle.HEADER) == (
            "\\begin{tabular}{|c|c|c|c|}\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "\\hline\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\end{tabular}"
        )

    def test_internal_border_preserved_latex(self, helper_table: PrettyTable) -> None:
        helper_table.border = False
        helper_table.format = True
        helper_table.preserve_internal_border = True

        assert helper_table.get_latex_string().strip() == (
            "\\begin{tabular}{c|c|c|c}\r\n"
            " & Field 1 & Field 2 & Field 3 \\\\\r\n"
            "1 & value 1 & value2 & value3 \\\\\r\n"
            "4 & value 4 & value5 & value6 \\\\\r\n"
            "7 & value 7 & value8 & value9 \\\\\r\n"
            "\\end{tabular}"
        )
