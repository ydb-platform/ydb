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
Handle parsing of the LLVM ``.json`` file format.

The behavior of this parser was informed by the following sources:

* the *Source-based Code Coverage* section in the LLVM manual (version 22)
  <https://clang.llvm.org/docs/SourceBasedCodeCoverage.html#creating-coverage-reports>
* the ``CoverageExporterJson.cpp`` source code in LLVM which contains the format description
  <https://github.com/llvm/llvm-project/blob/3b5b5c1ec4a3095ab096dd780e84d7ab81f3d7ff/llvm/tools/llvm-cov/CoverageExporterJson.cpp>
* the description of the coverage mapping format
  <https://releases.llvm.org/18.1.8/docs/CoverageMappingFormat.html>
"""

from dataclasses import dataclass
from enum import Enum
import json
import logging
import os
import re
import shlex
import subprocess  # nosec
from typing import Any


from ...exclusions import apply_all_exclusions, get_exclusion_options_from_options
from ...filter import is_file_excluded
from ...data_model.container import CoverageContainer
from ...data_model.coverage import FileCoverage
from ...data_model.merging import (
    get_merge_mode_from_options,
    FUNCTION_MAX_LINE_MERGE_OPTIONS,
    MergeOptions,
)
from ...decision_analysis import DecisionParser
from ...options import Options
from ...utils import get_md5_hexdigest, read_source_file, search_file, write_json_output

LOGGER = logging.getLogger("gcovr")

EXPECTED_TYPE = "llvm.coverage.json.export"
EXPECTED_MAJOR_VERSION = 2


#
#  Get coverage from already existing gcovr JSON files
#
def read_report(options: Options) -> CoverageContainer:
    """Read trace files into internal data model."""
    profraw_files = set()

    # Get data files
    if not options.search_paths:
        options.search_paths = [options.root]

    for search_path in options.search_paths:
        profraw_files.update(find_datafiles(search_path, options.exclude_directory))

    covdata = CoverageContainer()

    merge_options = get_merge_mode_from_options(options)
    for profraw_file in profraw_files:
        activate_trace_logging = not is_file_excluded(
            "trace",
            profraw_file,
            options.trace_include_filter,
            options.trace_exclude_filter,
        )
        if activate_trace_logging:
            LOGGER.trace("Processing file: %s", profraw_file)
        llvm_json_data = _llvm_profraw_to_json(options, profraw_file)

        if (current_type := llvm_json_data.get("type")) != EXPECTED_TYPE:
            raise AssertionError(
                f"Wrong JSON type, got {current_type} expected {EXPECTED_TYPE}."
            )
        if not (current_version := llvm_json_data.get("version", "")).startswith(
            f"{EXPECTED_MAJOR_VERSION}."
        ):
            raise AssertionError(
                f"Wrong major version, got {current_version or None} expected {EXPECTED_MAJOR_VERSION}.x.x."
            )

        covdata.merge(
            read_json(
                profraw_file,
                llvm_json_data,
                options,
                merge_options,
                version=tuple(
                    int(v) for v in llvm_json_data.get("version", "").split(".")
                ),
            ),
            merge_options,
        )

        if options.delete_input_files:
            for profraw_file in profraw_files:
                os.unlink(profraw_file)

    for filecov in covdata.values():
        source_lines = read_source_file(
            options.source_encoding,
            filecov.filename,
            max(linecov_collection.lineno for linecov_collection in filecov.lines()),
        )
        activate_trace_logging = not is_file_excluded(
            "trace",
            filecov.filename,
            options.trace_include_filter,
            options.trace_exclude_filter,
        )
        if activate_trace_logging:
            LOGGER.trace("Apply exclusions for %s", filecov.filename)
        apply_all_exclusions(
            filecov,
            lines=source_lines,
            options=get_exclusion_options_from_options(options),
        )
        for linecov_collection in filecov.lines():
            linecov_collection.md5 = get_md5_hexdigest(
                source_lines[linecov_collection.lineno - 1].encode("UTF-8")
            )

        if options.show_decision:
            decision_parser = DecisionParser(filecov, source_lines)
            decision_parser.parse_all_lines()

    return covdata


def find_datafiles(
    search_path: str, exclude_directory: list[re.Pattern[str]]
) -> list[str]:
    """Find .gcda and .gcno files under the given search path.

    The .gcno files will *only* produce uncovered results.
    However, that is useful information when a compilation unit
    is never actually exercised by the test code.
    So we ONLY return them if there's no corresponding .gcda file.
    """
    if os.path.isfile(search_path):
        LOGGER.debug(
            "Using given %s file %s", os.path.splitext(search_path)[1][1:], search_path
        )
        files = [search_path]
    else:
        LOGGER.debug("Scanning directory %s for profraw files...", search_path)
        files = list(
            search_file(
                lambda fname: re.compile(r".*\.profraw$").match(fname) is not None,
                search_path,
                exclude_directory=exclude_directory,
            )
        )
    LOGGER.debug("Found %d files", len(files))
    return files


def _llvm_profraw_to_json(options: Options, profraw: str) -> dict[str, Any]:
    """Transform the profraw file to to JSON and load it."""
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    env["LANGUAGE"] = "en_US"

    activate_trace_logging = not is_file_excluded(
        "trace", profraw, options.trace_include_filter, options.trace_exclude_filter
    )

    def run_cmd(cmd: list[str]) -> tuple[str, str]:
        """Run the given command."""

        tool = cmd[0]
        if activate_trace_logging:
            LOGGER.trace("Running %s: %s", tool, shlex.join(cmd))
        with subprocess.Popen(  # nosec # We know that we execute llvm-profdata tool
            cmd,
            env=env,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            out, err = process.communicate()
            if process.returncode == 0:
                if activate_trace_logging:
                    LOGGER.trace("STDERR >>%s<< End of STDERR", err)
                    LOGGER.trace("STDOUT >>%s<< End of STDOUT", out)
            else:
                raise RuntimeError(
                    f"{tool} returncode was {process.returncode}{' (exited by signal)' if process.returncode < 0 else ''}.\n"
                    f"STDOUT >>{out}<< End of STDOUT\n"
                    f"STDERR >>{err}<< End of STDERR"
                )
            return out, err

    profdata = os.path.splitext(profraw)[0] + ".profdata"
    try:
        run_cmd(
            [
                options.llvm_profdata_cmd,
                "merge",
                "--sparse",
                f"--output={profdata}",
                profraw,
            ]
        )
        json_string, _ = run_cmd(
            [
                options.llvm_profdata_cmd.replace("llvm-profdata", "llvm-cov"),
                "export",
                f"--instr-profile={profdata}",
                *options.llvm_cov_binaries,
            ]
        )
    finally:
        if not options.keep_intermediate_files and os.path.exists(profdata):
            os.remove(profdata)

    llvm_json_data: dict[str, Any] = json.loads(json_string)
    if options.keep_intermediate_files:
        write_json_output(
            llvm_json_data,
            pretty=True,
            filename=profdata + ".json",
            default_filename="",
        )
    return llvm_json_data


def read_json(
    data_source: str,
    json_data: dict[str, Any],
    options: Options,
    merge_options: MergeOptions,
    version: tuple[int, ...],
) -> CoverageContainer:
    """Read one clang JSON coverage report."""
    covdata = CoverageContainer()
    for data in json_data["data"]:
        if any(
            "mcdc_records" in entry
            for key in ("files", "functions")
            for entry in data[key]
        ):
            LOGGER.warning(
                "%s: Found 'mcdc_records' in exported JSON report. This is ignored by GCOVR.",
                data_source,
            )
        read_json_files(data_source, data, options, merge_options, covdata, version)
        read_json_functions(data_source, data, options, merge_options, covdata)

    for filecov in covdata.values():
        activate_trace_logging = not is_file_excluded(
            "trace",
            filecov.filename,
            options.trace_include_filter,
            options.trace_exclude_filter,
        )
        if activate_trace_logging:
            LOGGER.trace("Apply exclusions for %s", filecov.filename)
        apply_all_exclusions(
            filecov,
            lines=[],
            options=get_exclusion_options_from_options(options),
        )

    return covdata


def read_json_files(
    data_source: str,
    json_data: dict[str, Any],
    options: Options,
    merge_options: MergeOptions,
    covdata: CoverageContainer,
    version: tuple[int, ...],
) -> None:
    """Read the files from clang JSON coverage report."""
    file_data: dict[str, Any]
    branches_found = False
    for file_data in json_data["files"]:
        filename = file_data["filename"]
        if is_file_excluded(
            "source file", filename, options.include_filter, options.exclude_filter
        ):
            continue
        filecov = FileCoverage(data_source, filename=filename)
        segments_by_line = dict[int, list[Segment]]()
        for segment in [
            (Segment(*[*segment, False]) if version == (2, 0, 0) else Segment(*segment))
            for segment in file_data["segments"]
        ]:
            if segment.line not in segments_by_line:
                segments_by_line[segment.line] = list[Segment]()
            segments_by_line[segment.line].append(segment)

        for lineno, segments in segments_by_line.items():
            filecov.insert_line_coverage(
                data_source,
                merge_options,
                lineno=lineno,
                count=max(segment.count for segment in segments),
                function_name=None,
                block_ids=None,
                md5=None,
                excluded=False,
            )

        covdata.insert_file_coverage(filecov, merge_options)
        branches_by_line = dict[int, list[BranchRegion]]()
        branches_found |= "branches" in file_data
        for branch_region in [
            BranchRegion(*branch_region)
            for branch_region in file_data.get("branches", [])
        ]:
            if branch_region.line_start not in branches_by_line:
                branches_by_line[branch_region.line_start] = list[BranchRegion]()
            branches_by_line[branch_region.line_start].append(branch_region)

        for line, branch_regions in branches_by_line.items():
            linecov_collection = filecov.get_line(line)
            if linecov_collection is None:
                filecov.raise_data_error(
                    f"Missing line coverage for line {line} required by branch record."
                )
            linecov = list(linecov_collection.linecov())[0]
            for index, branch_region in enumerate(branch_regions):
                linecov.insert_branch_coverage(
                    data_source,
                    branchno=index * 2,
                    count=branch_region.true_execution_count,
                    fallthrough=False,
                    throw=False,
                    source_block_id=None,
                    destination_block_id=None,
                    excluded=False,
                )
                linecov.insert_branch_coverage(
                    data_source,
                    branchno=(index * 2) + 1,
                    count=branch_region.false_execution_count,
                    fallthrough=False,
                    throw=False,
                    source_block_id=None,
                    destination_block_id=None,
                    excluded=False,
                )

    if not branches_found:
        LOGGER.warning("No branches found in LLVM JSON, this needs at least clang 12.")


def read_json_functions(
    data_source: str,
    json_data: dict[str, Any],
    options: Options,
    merge_options: MergeOptions,
    covdata: CoverageContainer,
) -> None:
    """Read the functions from clang JSON coverage report."""
    function_data: dict[str, Any]
    for function_data in json_data["functions"]:
        filenames: list[str] = function_data["filenames"]
        if is_file_excluded(
            "source file", filenames[0], options.include_filter, options.exclude_filter
        ):
            continue

        filecov = FileCoverage(data_source, filename=filenames[0])
        function_regions = list(
            FunctionRegion(*region) for region in function_data["regions"]
        )
        function_regions_executed = len(
            list(
                function_regions
                for region in function_regions
                if region.execution_count != 0
            )
        )
        # Use 100% only if covered == total.
        if function_regions_executed == len(function_regions):
            blocks = 100.0
        else:
            # There is at least one uncovered item.
            # Round to 1 decimal and clamp to max 99.9%.
            ratio = function_regions_executed / len(function_regions)
            blocks = min(99.9, round(ratio * 100.0, 1))
        filecov.insert_function_coverage(
            data_source,
            MergeOptions(func_opts=FUNCTION_MAX_LINE_MERGE_OPTIONS),
            mangled_name=function_data["name"],
            demangled_name=None,
            lineno=function_regions[0].line_start,
            count=function_regions[0].execution_count,
            blocks=blocks,
            start=(
                function_regions[0].line_start,
                function_regions[0].column_start,
            ),
            end=(
                function_regions[-1].line_end,
                function_regions[-1].column_end,
            ),
        )

        covdata.insert_file_coverage(filecov, merge_options)


@dataclass
class Segment:
    """Representation of a segment."""

    line: int
    """The line where this segment begins."""
    col: int
    """The column where this segment begins."""
    count: int
    """The execution count, or zero if no count was recorded."""
    has_count: bool
    """When false, the segment was not instrumented or skipped."""
    is_region_entry: bool
    """Whether this enters a new region or returns to a previous count."""
    is_gap_region: bool
    """Whether this enters a gap region."""


class RegionKind(Enum):
    """The region kinds as described in https://github.com/llvm/llvm-project/blob/64b98967542d0128457154080f91c1ec4283eecb/llvm/include/llvm/ProfileData/Coverage/CoverageMapping.h#L233."""

    CODE_REGION = 0
    """A CodeRegion associates some code with a counter."""

    EXPANSION_REGION = 1
    """An ExpansionRegion represents a file expansion region that associates
    a source range with the expansion of a virtual source file, such as
    for a macro instantiation or #include file."""

    SKIPPED_REGION = 2
    """A SkippedRegion represents a source range with code that was skipped
    by a preprocessor or similar means."""

    GAP_REGION = 3
    """A GapRegion is like a CodeRegion, but its count is only set as the
    line execution count when it's the only region in the line."""

    BRANCH_REGION = 4
    """A BranchRegion represents leaf-level boolean expressions and is
    associated with two counters, each representing the number of times the
    expression evaluates to true or false."""

    MCDC_DECISION_REGION = 5
    """A DecisionRegion represents a top-level boolean expression and is
    associated with a variable length bitmap index and condition number."""

    MCDC_BRANCH_REGION = 6
    """A Branch Region can be extended to include IDs to facilitate MC/DC."""


@dataclass
class BranchRegion:
    """Representation of a branch region."""

    line_start: int
    """The starting line number of the region."""
    column_start: int
    """The starting column number of the region."""
    line_end: int
    """The ending line number of the region."""
    column_end: int
    """The ending column number of the region."""
    true_execution_count: int
    """Number of times the true branch was taken."""
    false_execution_count: int
    """Number of times the false branch was taken."""
    file_id: int
    """Identifier for the file containing the region."""
    expanded_file_id: int
    """Identifier for the expanded file, if applicable."""
    kind: RegionKind
    """Type of region (e.g., code, gap, etc.)."""


@dataclass
class FunctionRegion:
    """Representation of a function region."""

    line_start: int
    """The starting line number of the region."""
    column_start: int
    """The starting column number of the region."""
    line_end: int
    """The ending line number of the region."""
    column_end: int
    """The ending column number of the region."""
    execution_count: int
    """Number of times the region was executed."""
    file_id: int
    """Identifier for the file containing the region."""
    expanded_file_id: int
    """Identifier for the expanded file, if applicable."""
    kind: RegionKind
    """Type of region (e.g., code, gap, etc.)."""
