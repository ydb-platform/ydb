#!/usr/bin/env python
# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

"""
For testing
"""

from eccodes import codes_bufr_new_from_file, codes_release


def count_bufrs(path):
    count = 0
    with open(path, "rb") as f:
        while True:
            handle = codes_bufr_new_from_file(f, headers_only=True)
            if handle is None:
                return count
            count += 1
            codes_release(handle)
