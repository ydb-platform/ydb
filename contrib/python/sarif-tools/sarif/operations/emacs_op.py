"""
Code for `sarif emacs` command.
"""

from datetime import datetime
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from sarif import sarif_file

_THIS_MODULE_PATH = os.path.dirname(__file__)

_TEMPLATES_PATH = os.path.join(_THIS_MODULE_PATH, "templates")

_ENV = Environment(
    loader=FileSystemLoader(searchpath=_TEMPLATES_PATH),
    autoescape=select_autoescape(),
)


def generate_compile(
    input_files: sarif_file.SarifFileSet,
    output: str,
    output_multiple_files: bool,
    date_val: datetime = datetime.now(),
):
    """
    Generate txt file from the input files.
    """
    output_file = output
    if output_multiple_files:
        for input_file in input_files:
            output_file_name = input_file.get_file_name_without_extension() + ".txt"
            print(
                "Writing results for",
                input_file.get_file_name(),
                "to",
                output_file_name,
            )
            _generate_single_txt(
                input_file, os.path.join(output, output_file_name), date_val
            )
        output_file = os.path.join(output, ".compile.txt")
    source_description = input_files.get_description()
    print(
        "Writing results for",
        source_description,
        "to",
        os.path.basename(output_file),
    )
    _generate_single_txt(input_files, output_file, date_val)


def _generate_single_txt(input_file, output_file, date_val):
    all_tools = input_file.get_distinct_tool_names()
    report = input_file.get_report()

    total_distinct_issue_codes = 0
    problems = []
    severities = report.get_severities()

    for severity in severities:
        distinct_issue_codes = report.get_issue_type_count_for_severity(severity)

        total_distinct_issue_codes += distinct_issue_codes

        severity_details = _enrich_details(
            report.get_issues_grouped_by_type_for_severity(severity)
        )

        severity_section = {
            "type": severity,
            "count": distinct_issue_codes,
            "details": severity_details,
        }

        problems.append(severity_section)

    filtered = None
    filter_stats = input_file.get_filter_stats()
    if filter_stats:
        filtered = f"Results were filtered by {filter_stats}."

    template = _ENV.get_template("sarif_emacs.txt")
    txt_content = template.render(
        report_type=", ".join(all_tools),
        report_date=date_val,
        severities=", ".join(severities),
        total=total_distinct_issue_codes,
        problems=problems,
        filtered=filtered,
    )

    with open(output_file, "wt", encoding="utf-8") as file_out:
        file_out.write(txt_content)


def _enrich_details(records_of_severity):
    return [
        {"code": key, "count": len(records), "details": records}
        for (key, records) in records_of_severity.items()
    ]
