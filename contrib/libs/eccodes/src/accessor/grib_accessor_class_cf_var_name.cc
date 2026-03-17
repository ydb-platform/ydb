/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_cf_var_name.h"

grib_accessor_cf_var_name_t _grib_accessor_cf_var_name{};
grib_accessor* grib_accessor_cf_var_name = &_grib_accessor_cf_var_name;

void grib_accessor_cf_var_name_t::init(const long l, grib_arguments* arg)
{
    grib_accessor_ascii_t::init(l, arg);
    grib_handle* h = grib_handle_of_accessor(this);
    defaultKey_    = arg->get_name(h, 0);
}

int grib_accessor_cf_var_name_t::unpack_string(char* val, size_t* len)
{
    grib_handle* h       = grib_handle_of_accessor(this);
    char defaultKey[256] = {0,};
    size_t size       = sizeof(defaultKey) / sizeof(*defaultKey);
    char* pDefaultKey = defaultKey;

    int err = grib_get_string(h, defaultKey_, defaultKey, &size);
    if (err) return err;
    ECCODES_ASSERT(size > 0);
    ECCODES_ASSERT(strlen(defaultKey) > 0);

    if (STR_EQUAL(defaultKey, "~") || isdigit(defaultKey[0])) {
        // NetCDF variables cannot start with a digit
        long paramId = 0;
        err          = grib_get_long(h, "paramId", &paramId);
        if (err)
            snprintf(val, 1024, "%s", "unknown");
        else
            snprintf(val, 1024, "p%ld", paramId);
    }
    else {
        snprintf(val, 1024, "%s", pDefaultKey);
    }
    size = strlen(val);
    *len = size + 1;
    return GRIB_SUCCESS;
}

size_t grib_accessor_cf_var_name_t::string_length()
{
    return 1024;
}
