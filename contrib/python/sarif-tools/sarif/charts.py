"""
Functions for generating charts from SARIF data
"""

import io
import matplotlib.pyplot as plt


def generate_severity_pie_chart(report, output_file=None):
    """
    Generate a pie chart from the breakdown of issues by severity.
    The slices are ordered and plotted counter-clockwise.  The return
    value is truthy if the number of issues is not zero, False otherwise.
    If `output_file` is `None`, return the bytes of the pie chart image in
    png format.  Otherwise, write the bytes to the file specified (image
    format inferred from filename).
    """
    sizes = []
    labels = []
    explode = []
    for severity in report.get_severities():
        count = report.get_issue_count_for_severity(severity)
        if count > 0:
            sizes.append(count)
            labels.append(severity)
            explode.append(0.1)  # could add more logic to highlight specific severities

    any_issues = bool(sizes)
    if any_issues:
        _fig1, ax1 = plt.subplots()
        ax1.pie(
            sizes,
            explode=explode,
            labels=labels,
            autopct="%1.1f%%",
            shadow=True,
            startangle=90,
        )
        ax1.axis("equal")

        if output_file:
            plt.savefig(output_file)
        else:
            byte_buffer = io.BytesIO()
            plt.savefig(byte_buffer, format="png")
            return byte_buffer.getbuffer()
    return any_issues
