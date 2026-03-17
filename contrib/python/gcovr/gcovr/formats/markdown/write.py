# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

from typing import Any

from jinja2 import (
    Environment,
    PackageLoader,
)

from ...data_model.container import CoverageContainer
from ...data_model.stats import SummarizedStats
from ...options import Options
from ...utils import open_text_for_writing


def templates() -> Environment:
    """Get the template environment."""
    loader: PackageLoader = PackageLoader(
        "gcovr.formats.markdown",
        package_path="default",
    )

    return Environment(
        loader=loader,
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def write_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """Produce the gcovr report in markdown."""
    data = {
        "title": options.markdown_title,
        "heading_level": options.markdown_heading_level,
        "file_link": options.markdown_file_link,
        "summary": _summary_from_stats(covdata.stats, options),
    }

    sorted_keys = covdata.sort_coverage(
        sort_key=options.sort_key,
        sort_reverse=options.sort_reverse,
        by_metric="branch" if options.sort_branches else "line",
    )
    data["entries"] = list[dict[str, Any]]()
    for key in sorted_keys:
        summary = _summary_from_stats(covdata[key].stats, options)
        summary["filename"] = covdata[key].presentable_filename(options.root_filter)
        data["entries"].append(summary)

    markdown_string = templates().get_template("report_template.md.j2").render(**data)

    with open_text_for_writing(output_file, "coverage.md", encoding="utf-8") as fh:
        fh.write(markdown_string)


def write_summary_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """Produce the gcovr summary report in markdown."""
    data = {
        "title": options.markdown_title,
        "heading_level": options.markdown_heading_level,
        "summary": _summary_from_stats(covdata.stats, options),
    }

    markdown_string = templates().get_template("report_template.md.j2").render(**data)

    with open_text_for_writing(output_file, "coverage.md", encoding="utf-8") as fh:
        fh.write(markdown_string)


def _coverage_to_badge(
    coverage: float | None,
    medium_threshold: float,
    high_threshold: float,
    theme: str,
) -> str:
    if coverage is None:
        return "⚫"
    if coverage >= high_threshold:
        return "🔵" if theme == "blue" else "🟢"
    if coverage >= medium_threshold:
        return "🟡"

    return "🔴"


def _summary_from_stats(stats: SummarizedStats, options: Options) -> dict[str, Any]:
    summary = dict[str, Any]()

    summary["line_badge"] = _coverage_to_badge(
        stats.line.percent,
        options.medium_threshold_line,
        options.high_threshold_line,
        options.markdown_theme,
    )
    summary["function_badge"] = _coverage_to_badge(
        stats.function.percent,
        options.medium_threshold,
        options.high_threshold,
        options.markdown_theme,
    )
    summary["branch_badge"] = _coverage_to_badge(
        stats.branch.percent,
        options.medium_threshold_branch,
        options.high_threshold_branch,
        options.markdown_theme,
    )
    summary["line_covered"] = stats.line.covered
    summary["line_total"] = stats.line.total
    summary["line_percent"] = stats.line.percent_or(0.0)
    summary["function_covered"] = stats.function.covered
    summary["function_total"] = stats.function.total
    summary["function_percent"] = stats.function.percent_or(0.0)
    summary["branch_covered"] = stats.branch.covered
    summary["branch_total"] = stats.branch.total
    summary["branch_percent"] = stats.branch.percent_or(0.0)

    return summary
