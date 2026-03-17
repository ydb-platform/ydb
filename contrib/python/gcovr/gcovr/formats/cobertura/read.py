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
from glob import glob
from lxml import etree  # nosec # We only write XML files

from ...data_model.container import CoverageContainer
from ...data_model.coverage import (
    BranchCoverage,
    FileCoverage,
    LineCoverage,
)
from ...data_model.merging import MergeOptions, get_merge_mode_from_options
from ...filter import is_file_excluded
from ...logging import LOGGER
from ...options import Options


#
#  Get coverage from already existing gcovr JSON files
#
def read_report(options: Options) -> CoverageContainer:
    """merge a coverage from multiple reports in the format
    compatible with Cobertura"""

    covdata = CoverageContainer()
    if len(options.cobertura_tracefile) == 0:
        return covdata

    datafiles = set()

    for trace_files_regex in options.cobertura_tracefile:
        trace_files = glob(trace_files_regex, recursive=True)
        if not trace_files:
            raise RuntimeError(
                "Bad --covertura-add-tracefile option.\n"
                "\tThe specified file does not exist."
            )

        for activate_trace_logging in trace_files:
            datafiles.add(os.path.normpath(activate_trace_logging))

    for data_sources in datafiles:
        LOGGER.debug("Processing XML file: %s", data_sources)

        try:
            root: etree._Element = etree.parse(data_sources).getroot()  # nosec # We parse the file given by the user
        except Exception as e:
            raise RuntimeError(f"Bad --cobertura-add-tracefile option.\n{e}") from None

        source_elem = root.find("./sources/source")
        if source_elem is None:
            raise AssertionError(f"No source directory defined in file {data_sources}")
        source_dir = str(source_elem.text)

        gcovr_file: etree._Element
        for gcovr_file in root.xpath("./packages//class"):  # type: ignore [assignment, union-attr]
            filename = gcovr_file.get("filename")
            if filename is None:  # pragma: no cover
                LOGGER.warning(
                    "Missing filename attribute in class element at %s:%s",
                    data_sources,
                    gcovr_file.sourceline,
                )
                continue

            filename = str(os.path.normpath(os.path.join(source_dir, filename)))
            if is_file_excluded(
                "source file", filename, options.include_filter, options.exclude_filter
            ):
                continue

            filecov = FileCoverage(data_sources, filename=filename)
            merge_options = get_merge_mode_from_options(options)
            xml_line: etree._Element
            for xml_line in gcovr_file.xpath("./lines//line"):  # type: ignore [assignment, union-attr]
                _insert_line_from_xml(filecov, data_sources, merge_options, xml_line)

            covdata.insert_file_coverage(filecov, merge_options)

    return covdata


def _insert_line_from_xml(
    filecov: FileCoverage,
    data_sources: str,
    merge_options: MergeOptions,
    xml_line: etree._Element,
) -> None:
    try:
        lineno = int(xml_line.get("number", ""))
    except Exception:  # pragma: no cover
        raise RuntimeError(
            "Bad --covertura-add-tracefile option.\n"
            f"'number' attribute is required and must be an integer: {etree.tostring(xml_line).decode()}\n"
        ) from None

    try:
        count = int(xml_line.get("hits", ""))
    except Exception:  # pragma: no cover
        raise RuntimeError(
            "Bad --covertura-add-tracefile option.\n"
            f"'hits' attribute is required and must be an integer: {etree.tostring(xml_line).decode()}\n"
        ) from None

    is_branch = xml_line.get("branch") == "true"
    branch_msg = xml_line.get("condition-coverage")
    linecov = filecov.insert_line_coverage(
        data_sources, merge_options, lineno=lineno, count=count, function_name=None
    )

    if is_branch and branch_msg is not None:
        try:
            [covered, total] = branch_msg[branch_msg.rfind("(") + 1 : -1].split("/")
            for i in range(int(total)):
                _branch_from_json(linecov, data_sources, i, i < int(covered))
        except AssertionError as exc:  # pragma: no cover
            LOGGER.warning(
                "Invalid branch information for line %s: %s", linecov.lineno, exc
            )


def _branch_from_json(
    linecov: LineCoverage, data_sources: str, branchno: int, is_covered: bool
) -> BranchCoverage:
    return linecov.insert_branch_coverage(
        data_sources,
        branchno=branchno,
        count=1 if is_covered else 0,
    )
