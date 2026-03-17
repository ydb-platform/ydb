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

from lxml import etree  # nosec # We only write XML files

from ...data_model.container import CoverageContainer
from ...data_model.stats import CoverageStat, DecisionCoverageStat
from ...logging import LOGGER
from ...options import Options
from ...utils import write_xml_output


def write_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """produce an XML report in the SonarQube generic coverage format"""

    if not any(
        filter(lambda filecov: filecov.condition_coverage().total > 0, covdata.values())  # type: ignore [arg-type]
    ) and (options.sonarqube_metric == "condition"):
        LOGGER.warning("No condition coverage data found.")

    root_elem = etree.Element("coverage")
    root_elem.set("version", "1")

    for _, filecov in sorted(covdata.items()):
        filename = filecov.presentable_filename(options.root_filter)

        file_node = etree.Element("file")
        file_node.set("path", filename)

        for linecov in filecov.linecov(sort=True):
            if linecov.is_reportable:
                line_node = etree.Element("lineToCover")
                line_node.set("lineNumber", str(linecov.lineno))
                line_node.set("covered", "true" if linecov.is_covered else "false")

                if options.sonarqube_metric != "line":
                    stat: CoverageStat | DecisionCoverageStat | None = None
                    if (
                        options.sonarqube_metric == "branch"
                        and linecov.has_reportable_branches
                    ):
                        stat = linecov.branch_coverage()
                    elif (
                        options.sonarqube_metric == "condition"
                        and linecov.has_reportable_conditions
                    ):
                        stat = linecov.condition_coverage()
                    elif options.sonarqube_metric == "decision" and linecov.decision:
                        stat = linecov.decision_coverage()

                    if stat:
                        line_node.set("branchesToCover", str(stat.total))
                        line_node.set("coveredBranches", str(stat.covered))

                file_node.append(line_node)

        root_elem.append(file_node)

    write_xml_output(
        root_elem,
        pretty=options.sonarqube_pretty,
        filename=output_file,
        default_filename="sonarqube.xml",
    )
