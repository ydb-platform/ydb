#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#
#

from .gribapi import *  # noqa
from .gribapi import __version__, lib

# The minimum recommended version for the ecCodes package
min_recommended_version_str = "2.39.0"
min_recommended_version_int = 23900

if lib.grib_get_api_version() < min_recommended_version_int:
    import warnings

    warnings.warn(
        "ecCodes {} or higher is recommended. "
        "You are running version {}".format(min_recommended_version_str, __version__)
    )
