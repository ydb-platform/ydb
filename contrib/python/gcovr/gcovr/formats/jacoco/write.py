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

from dataclasses import dataclass
from lxml import etree  # nosec # We only write XML files

from ...data_model.container import CoverageContainer
from ...data_model.stats import CoverageStat, SummarizedStats
from ...options import Options
from ...utils import write_xml_output


def write_report(
    covdata: CoverageContainer, output_file: str, options: Options
) -> None:
    """produce an XML report in the JaCoCo format"""

    root_elem = etree.Element("report")
    root_elem.set("name", options.jacoco_report_name)

    # Generate the coverage output (on a per-package basis)
    packages = dict[str, PackageData]()

    for _, filecov in sorted(covdata.items()):
        filename = filecov.presentable_filename(options.root_filter)
        if "/" in filename:
            directory, fname = filename.rsplit("/", 1)
        else:
            directory, fname = "", filename

        package_data = packages.setdefault(
            directory,
            PackageData(
                {},
                SummarizedStats.new_empty(),
            ),
        )
        source_elem = etree.Element("sourcefile")
        source_elem.set("name", fname)

        for linecov in filecov.linecov(sort=True):
            line_elem = etree.SubElement(source_elem, "line")
            line_elem.set("nr", str(linecov.lineno))
            if linecov.is_reportable:
                stat = linecov.branch_coverage()
                if stat.total:
                    line_elem.set("mb", str(stat.total - stat.covered))
                    line_elem.set("cb", str(stat.covered))

        filecov_stats = filecov.stats
        add_counters(source_elem, filecov_stats)

        package_data.sources[fname] = source_elem
        package_data.stats += filecov_stats

    for package_name, package_data in sorted(packages.items()):
        package_elem = etree.SubElement(root_elem, "package")
        package_elem.set("name", package_name.replace("/", "."))
        for _, source_data in sorted(package_data.sources.items()):
            package_elem.append(source_data)
        add_counters(package_elem, package_data.stats)

    add_counters(root_elem, covdata.stats)

    write_xml_output(
        root_elem,
        pretty=options.jacoco_pretty,
        filename=output_file,
        default_filename="jacoco.xml",
        doctype="<!DOCTYPE report SYSTEM 'https://www.jacoco.org/jacoco/trunk/coverage/report.dtd'>",
    )


@dataclass
class PackageData:
    """Class holding package information."""

    sources: dict[str, etree._Element]
    stats: SummarizedStats


def add_counters(elem: etree._Element, stats: SummarizedStats) -> None:
    """Add the counter elements for the given stats."""

    def add_counter_element(element_type: str, stat: CoverageStat) -> None:
        """Add one stat element."""
        counter_elem = etree.SubElement(elem, "counter")
        counter_elem.set("type", element_type)
        counter_elem.set("missed", str(stat.total - stat.covered))
        counter_elem.set("covered", str(stat.covered))

    add_counter_element("LINE", stats.line)
    add_counter_element("BRANCH", stats.branch)
    add_counter_element("METHOD", stats.function)
