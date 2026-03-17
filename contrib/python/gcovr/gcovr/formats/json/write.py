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

import os
from typing import Any

from ...data_model import version
from ...data_model.container import CoverageContainer
from ...options import Options
from ...utils import force_unix_separator, write_json_output

SUMMARY_FORMAT_VERSION = (
    # BEGIN summary version
    "0.6"
    # END summary version
)
KEY_SUMMARY_FORMAT_VERSION = "gcovr/summary_format_version"


def write_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """Produce an JSON report in the format partially compatible with gcov JSON output."""
    write_json_output(
        {
            "gcovr/format_version": version.FORMAT_VERSION,
            "files": covdata.serialize(options),
        },
        pretty=options.json_pretty,
        filename=output_file,
        default_filename="coverage.json",
    )


def write_summary_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """Produce gcovr JSON summary report."""

    json_dict = dict[str, Any]()

    json_dict["root"] = force_unix_separator(
        os.path.relpath(
            options.root,
            os.getcwd() if output_file == "-" else os.path.dirname(output_file),
        )
    )
    json_dict["gcovr/summary_format_version"] = SUMMARY_FORMAT_VERSION
    files = list[dict[str, Any]]()
    json_dict["files"] = files

    # Data
    sorted_keys = covdata.sort_coverage(
        sort_key=options.sort_key,
        sort_reverse=options.sort_reverse,
        by_metric="branch" if options.sort_branches else "line",
    )

    for key in sorted_keys:
        filename = covdata[key].presentable_filename(options.root_filter)
        if options.json_base:
            filename = "/".join([options.json_base, filename])

        files.append(
            {
                "filename": filename,
                **covdata[key].stats.serialize(None, options),
            }
        )

    # Footer & summary
    json_dict.update(covdata.stats.serialize(0.0, options))

    write_json_output(
        json_dict,
        pretty=options.json_summary_pretty,
        filename=output_file,
        default_filename="summary_coverage.json",
    )
