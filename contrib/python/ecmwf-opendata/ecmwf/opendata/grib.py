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

from eccodes import (
    codes_get_long,
    codes_get_string,
    codes_grib_new_from_file,
    codes_keys_iterator_delete,
    codes_keys_iterator_get_name,
    codes_keys_iterator_new,
    codes_keys_iterator_next,
    codes_release,
)


def grib_index(path):
    index = []
    with open(path, "rb") as f:
        while True:
            handle = codes_grib_new_from_file(f, headers_only=True)
            if handle is None:
                return index
            try:
                keys = {}
                iter = codes_keys_iterator_new(handle, "mars")
                try:
                    while codes_keys_iterator_next(iter):
                        name = codes_keys_iterator_get_name(iter)
                        if name.startswith("_"):
                            continue
                        value = codes_get_string(handle, name)
                        keys[name] = value

                    if "step" not in keys and "fcmonth" not in keys:
                        if keys["stream"] in ("mmsa",):
                            keys["fcmonth"] = str(
                                (codes_get_long(handle, "endStep") + 48) // (24 * 30)
                            )

                finally:
                    codes_keys_iterator_delete(iter)

                keys["param"] = codes_get_string(handle, "shortName")
                keys["_offset"] = codes_get_long(handle, "offset")
                keys["_length"] = codes_get_long(handle, "totalLength")

                index.append(keys)
            finally:
                codes_release(handle)


def count_gribs(path):
    count = 0
    with open(path, "rb") as f:
        while True:
            handle = codes_grib_new_from_file(f, headers_only=True)
            if handle is None:
                return count
            count += 1
            codes_release(handle)
