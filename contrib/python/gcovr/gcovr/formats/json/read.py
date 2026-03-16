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

import gzip
import json
import os
from glob import glob

from ...filter import is_file_excluded

from ...data_model import version
from ...data_model.container import CoverageContainer
from ...data_model.merging import get_merge_mode_from_options
from ...logging import LOGGER
from ...options import Options
from ...utils import GZIP_SUFFIX


#
#  Get coverage from already existing gcovr JSON files
#
def read_report(options: Options) -> CoverageContainer:
    """Read trace files into internal data model."""

    covdata = CoverageContainer()
    if len(options.json_tracefile) != 0:
        datafiles = set()

        for trace_file_pattern in options.json_tracefile:
            trace_files = glob(trace_file_pattern, recursive=True)
            if not trace_files:
                raise RuntimeError(
                    f"Bad --json-add-tracefile={trace_file_pattern} option.\n"
                    "\tThe specified file does not exist."
                )

            for activate_trace_logging in trace_files:
                datafiles.add(os.path.normpath(activate_trace_logging))

        merge_options = get_merge_mode_from_options(options)
        for data_source in datafiles:
            activate_trace_logging = not is_file_excluded(
                "trace",
                data_source,
                options.trace_include_filter,
                options.trace_exclude_filter,
            )
            if activate_trace_logging:
                LOGGER.trace("Processing file: %s", data_source)

            if data_source.casefold().endswith(GZIP_SUFFIX):
                with gzip.open(data_source, "rt", encoding="utf-8") as fh:
                    gcovr_json_data = json.loads(fh.read())
            else:
                with open(data_source, encoding="utf-8") as json_file:
                    gcovr_json_data = json.load(json_file)

            format_version = str(gcovr_json_data["gcovr/format_version"])
            if format_version != version.FORMAT_VERSION:
                raise AssertionError(
                    f"Wrong format version, got {format_version} expected {version.FORMAT_VERSION}."
                )

            covdata.merge(
                CoverageContainer.deserialize(
                    data_source, gcovr_json_data["files"], options, merge_options
                ),
                merge_options,
            )

    return covdata
