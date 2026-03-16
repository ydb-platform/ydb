"""
Code for `sarif html` command.
"""

import base64
from datetime import datetime
import os
from typing import Union

from jinja2 import Environment, FileSystemLoader, select_autoescape

from sarif import charts, sarif_file

_THIS_MODULE_PATH = os.path.dirname(__file__)

_TEMPLATES_PATH = os.path.join(_THIS_MODULE_PATH, "templates")

_ENV = Environment(
    loader=FileSystemLoader(searchpath=_TEMPLATES_PATH),
    autoescape=select_autoescape(),
)


def generate_html(
    input_files: sarif_file.SarifFileSet,
    image_file: Union[str, None],
    output: str,
    output_multiple_files: bool,
    date_val: datetime = datetime.now(),
):
    """
    Generate HTML file from the input files.
    """
    if image_file:
        image_mime_type = "image/" + os.path.splitext(image_file)[-1]
        if image_mime_type == "image/jpg":
            image_mime_type = "image/jpeg"
        with open(image_file, "rb") as input_file:
            image_data = input_file.read()

        image_data_base64 = base64.b64encode(image_data).decode("utf-8")
    else:
        image_mime_type = None
        image_data_base64 = None

    output_file = output
    if output_multiple_files:
        for input_file in input_files:
            output_file_name = input_file.get_file_name_without_extension() + ".html"
            print(
                "Writing HTML report for",
                input_file.get_file_name(),
                "to",
                output_file_name,
            )
            _generate_single_html(
                input_file,
                os.path.join(output, output_file_name),
                date_val,
                image_mime_type,
                image_data_base64,
            )
        output_file = os.path.join(output, "static_analysis_output.html")
    source_description = input_files.get_description()
    print(
        "Writing HTML report for",
        source_description,
        "to",
        os.path.basename(output_file),
    )
    _generate_single_html(
        input_files, output_file, date_val, image_mime_type, image_data_base64
    )


def _generate_single_html(
    input_file, output_file, date_val, image_mime_type, image_data_base64
):
    all_tools = input_file.get_distinct_tool_names()
    report = input_file.get_report()

    total_distinct_issue_codes = 0
    problems = []
    severities = report.get_severities()

    for severity in severities:
        distinct_issue_codes = report.get_issue_type_count_for_severity(severity)

        total_distinct_issue_codes += distinct_issue_codes

        severity_details = _enrich_details(
            report.get_issues_grouped_by_type_for_severity(severity), input_file
        )

        severity_section = {
            "type": severity,
            "count": distinct_issue_codes,
            "details": severity_details,
        }

        problems.append(severity_section)

    chart_data = charts.generate_severity_pie_chart(report, output_file=None)
    if chart_data:
        chart_image_data_base64 = base64.b64encode(chart_data).decode("utf-8")
    else:
        chart_image_data_base64 = None

    filtered = None
    filter_stats = input_file.get_filter_stats()
    if filter_stats:
        filtered = f"Results were filtered by {filter_stats}."

    template = _ENV.get_template("sarif_summary.html")
    html_content = template.render(
        report_type=", ".join(all_tools),
        report_date=date_val,
        severities=", ".join(severities),
        total=total_distinct_issue_codes,
        problems=problems,
        image_mime_type=image_mime_type,
        image_data_base64=image_data_base64,
        chart_image_data_base64=chart_image_data_base64,
        filtered=filtered,
    )

    with open(output_file, "wt", encoding="utf-8") as file_out:
        file_out.write(html_content)


def _extract_help_links_from_rules(rules, link_to_desc, key):
    for rule in rules:
        if "helpUri" in rule:
            uri = rule["helpUri"]
            if uri not in link_to_desc:
                desc = rule.get("fullDescription", {}).get("text")
                if not desc:
                    desc = rule.get("name")
                if not desc:
                    desc = key
                link_to_desc[uri] = desc


def _enrich_details(records_of_severity, input_file):
    ret = []

    for key, records in records_of_severity.items():
        link_to_desc = {}
        for record in records:
            rule_id = record["Code"]
            rules = input_file.get_rules_by_id(rule_id)
            _extract_help_links_from_rules(rules, link_to_desc, key)
        links = [(desc, uri) for (uri, desc) in link_to_desc.items()]
        ret.append(
            {"code": key, "count": len(records), "links": links, "details": records}
        )
    return ret
