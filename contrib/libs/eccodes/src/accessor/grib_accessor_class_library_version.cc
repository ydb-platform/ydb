/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_library_version.h"

grib_accessor_library_version_t _grib_accessor_library_version{};
grib_accessor* grib_accessor_library_version = &_grib_accessor_library_version;

int grib_accessor_library_version_t::unpack_string(char* val, size_t* len)
{
    char result[30] = {0,};
    size_t size = 0;

    int major    = ECCODES_MAJOR_VERSION;
    int minor    = ECCODES_MINOR_VERSION;
    int revision = ECCODES_REVISION_VERSION;

    snprintf(result, sizeof(result), "%d.%d.%d", major, minor, revision);
    size = sizeof(result);

    if (*len < size)
        return GRIB_ARRAY_TOO_SMALL;
    strcpy(val, result);

    *len = size;

    return GRIB_SUCCESS;
}

int grib_accessor_library_version_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_library_version_t::string_length()
{
    return 255;
}
