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

from ..data_model.container import CoverageContainer
from ..options import GcovrConfigOption, Options


class BaseHandler:
    """Base class for a format handler."""

    @classmethod
    def get_options(cls) -> list[GcovrConfigOption | str]:
        """Get the options of the format handler"""
        raise AssertionError("Function 'get_options' not implemented.")

    def __init__(self, options: Options) -> None:
        global_options = [
            "output",
            "timestamp",
            "root",
            "root_dir",
            "root_filter",
            "sort_branches",
            "sort_key",
            "sort_reverse",
            "search_paths",
            "source_encoding",
            "starting_dir",
            "include_filter",
            "exclude_filter",
            "trace_include_filter",
            "trace_exclude_filter",
        ]
        option_dict = {}
        for name in global_options + [
            o if isinstance(o, str) else o.name for o in self.__class__.get_options()
        ]:
            option_dict[name] = options.get(name)
        self.options = Options(**option_dict)

    def validate_options(self) -> None:
        """Validation of command line options"""

    def read_report(self) -> CoverageContainer:
        """Read a report in the format of the handler"""
        raise AssertionError("Function 'read_report' not implemented.")

    def write_report(self, covdata: CoverageContainer, output_file: str) -> None:
        """Write a report in the format of the handler"""
        raise AssertionError("Function 'write_report' not implemented.")

    def write_summary_report(
        self, covdata: CoverageContainer, output_file: str
    ) -> None:
        """Write a summary report in the format of the handler"""
        raise AssertionError("Function 'write_summary_report' not implemented.")
