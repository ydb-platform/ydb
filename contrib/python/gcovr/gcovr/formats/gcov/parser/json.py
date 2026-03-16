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

"""
Handle parsing of the json ``.gcov.json.gz`` file format.

Other modules should only use the following items:
`parse_coverage()`

The behavior of this parser was informed by the following sources:

* the *Invoking Gcov* section in the GCC manual (version 11)
  <https://gcc.gnu.org/onlinedocs/gcc-14.1.0/gcc/Invoking-Gcov.html>
"""
# pylint: disable=too-many-lines

import os
from typing import Any, Iterator

from gcovr.utils import get_md5_hexdigest, read_source_file

from ....data_model.coverage import FileCoverage
from ....data_model.merging import FUNCTION_MAX_LINE_MERGE_OPTIONS, MergeOptions
from ....filter import Filter, is_file_excluded
from ....logging import LOGGER
from .common import (
    SUSPICIOUS_COUNTER,
    check_hits,
)

GCOV_JSON_VERSION = "2"


def parse_coverage(
    data_fname: str,
    gcov_json_data: dict[str, Any],
    *,
    include_filter: tuple[Filter, ...],
    exclude_filter: tuple[Filter, ...],
    source_encoding: str,
    ignore_parse_errors: set[str] | None,
    suspicious_hits_threshold: int = SUSPICIOUS_COUNTER,
    activate_trace_logging: bool = False,
) -> Iterator[tuple[FileCoverage, list[str]]]:
    """Process a GCOV JSON output."""

    # Check format version because the file can be created external
    if gcov_json_data["format_version"] != GCOV_JSON_VERSION:
        raise RuntimeError(
            f"Got wrong JSON format version {gcov_json_data['format_version']}, expected {GCOV_JSON_VERSION}"
        )

    for file in gcov_json_data["files"]:
        if not file["lines"] and not file["functions"]:
            if activate_trace_logging:
                LOGGER.trace(
                    "Skip data for file %s because no lines and functions defined.",
                    file["file"],
                )
            continue

        fname = os.path.normpath(
            os.path.join(gcov_json_data["current_working_directory"], file["file"])
        )

        if is_file_excluded("source file", fname, include_filter, exclude_filter):
            continue

        LOGGER.debug("Parsing coverage data for file %s", fname)

        max_line_number = (
            max(line["line_number"] for line in file["lines"]) if file["lines"] else 1
        )
        encoded_source_lines = read_source_file(source_encoding, fname, max_line_number)

        yield (
            _parse_file_node(
                data_fname,
                gcov_file_node=file,
                filename=fname,
                source_lines=encoded_source_lines,
                ignore_parse_errors=ignore_parse_errors,
                suspicious_hits_threshold=suspicious_hits_threshold,
                activate_trace_logging=activate_trace_logging,
            ),
            encoded_source_lines,
        )


def _parse_file_node(
    data_fname: str,
    *,
    gcov_file_node: dict[str, Any],
    filename: str,
    source_lines: list[str],
    ignore_parse_errors: set[str] | None,
    suspicious_hits_threshold: int = SUSPICIOUS_COUNTER,
    activate_trace_logging: bool = False,
) -> FileCoverage:
    """
    Extract coverage data from a json gcov report.

    Logging:
    Parse problems are reported as warnings.
    Coverage exclusion decisions are reported as verbose messages.

    Arguments:
        gcov_file_node: one of the "files" node in the gcov json format
        filename: for error reports
        source_lines: decoded source code lines, for reporting
        data_fname: source of this node, for reporting
        ignore_parse_errors: which errors should be converted to warnings

    Returns:
        The coverage data

    Raises:
        Any exceptions during parsing, unless ignore_parse_errors is set.
    """
    persistent_states: dict[str, Any] = {"location": (filename, 0)}

    if ignore_parse_errors is None:
        ignore_parse_errors = set()

    filecov = FileCoverage(data_fname, filename=filename)
    for line in gcov_file_node["lines"]:
        persistent_states.update(location=(filename, line["line_number"]))
        if activate_trace_logging:
            LOGGER.trace(
                "Reading %s of function %s",
                ":".join(str(e) for e in persistent_states["location"]),
                line.get("function_name"),
            )
        linecov = filecov.insert_line_coverage(
            str(data_fname),
            lineno=line["line_number"],
            count=check_hits(
                line["count"],
                source_lines[line["line_number"] - 1],
                ignore_parse_errors,
                suspicious_hits_threshold,
                persistent_states,
            ),
            function_name=line.get("function_name"),
            block_ids=line["block_ids"],
            md5=get_md5_hexdigest(
                source_lines[line["line_number"] - 1].encode("UTF-8")
            ),
        )
        for branch in line["branches"]:
            linecov.insert_branch_coverage(
                str(data_fname),
                branchno=None,
                count=check_hits(
                    branch["count"],
                    source_lines[line["line_number"] - 1],
                    ignore_parse_errors,
                    suspicious_hits_threshold,
                    persistent_states,
                ),
                source_block_id=branch["source_block_id"],
                fallthrough=branch["fallthrough"],
                throw=branch["throw"],
                destination_block_id=branch["destination_block_id"],
            )
        for index, condition in enumerate(line.get("conditions", [])):
            linecov.insert_condition_coverage(
                str(data_fname),
                conditionno=index,
                count=check_hits(
                    condition["count"],
                    source_lines[line["line_number"] - 1],
                    ignore_parse_errors,
                    suspicious_hits_threshold,
                    persistent_states,
                ),
                covered=condition["covered"],
                not_covered_true=condition["not_covered_true"],
                not_covered_false=condition["not_covered_false"],
            )
        for call in line.get("calls", []):
            linecov.insert_call_coverage(
                str(data_fname),
                callno=None,
                source_block_id=call["source_block_id"],
                destination_block_id=call["destination_block_id"],
                returned=call["returned"],
            )

    for function in gcov_file_node["functions"]:
        # Use 100% only if covered == total.
        if function["blocks_executed"] == function["blocks"]:
            blocks = 100.0
        else:
            # There is at least one uncovered item.
            # Round to 1 decimal and clamp to max 99.9%.
            ratio = function["blocks_executed"] / function["blocks"]
            blocks = min(99.9, round(ratio * 100.0, 1))

        filecov.insert_function_coverage(
            str(data_fname),
            MergeOptions(func_opts=FUNCTION_MAX_LINE_MERGE_OPTIONS),
            mangled_name=function["name"],
            demangled_name=function["demangled_name"],
            lineno=function["start_line"],
            count=function["execution_count"],
            blocks=blocks,
            start=(function["start_line"], function["start_column"]),
            end=(function["end_line"], function["end_column"]),
        )

    if (
        "negative_hits.warn_once_per_file" in persistent_states
        and persistent_states["negative_hits.warn_once_per_file"] > 1
    ):
        LOGGER.warning(
            "Ignored %d negative hits overall.",
            persistent_states["negative_hits.warn_once_per_file"],
        )

    if (
        "suspicious_hits.warn_once_per_file" in persistent_states
        and persistent_states["suspicious_hits.warn_once_per_file"] > 1
    ):
        LOGGER.warning(
            "Ignored %d suspicious hits overall.",
            persistent_states["suspicious_hits.warn_once_per_file"],
        )

    return filecov
