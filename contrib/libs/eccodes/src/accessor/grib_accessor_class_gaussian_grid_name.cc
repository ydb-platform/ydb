/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_gaussian_grid_name.h"

grib_accessor_gaussian_grid_name_t _grib_accessor_gaussian_grid_name{};
grib_accessor* grib_accessor_gaussian_grid_name = &_grib_accessor_gaussian_grid_name;

void grib_accessor_gaussian_grid_name_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    int n = 0;

    N_            = arg->get_name(parent_->h, n++);
    Ni_           = arg->get_name(parent_->h, n++);
    isOctahedral_ = arg->get_name(parent_->h, n++);
    length_       = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC;
}

long grib_accessor_gaussian_grid_name_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

#define MAX_GRIDNAME_LEN 16

int grib_accessor_gaussian_grid_name_t::unpack_string(char* v, size_t* len)
{
    long N = 0, Ni = 0;
    char tmp[MAX_GRIDNAME_LEN] = {0,};
    size_t length = 0;
    int ret       = GRIB_SUCCESS;

    if ((ret = grib_get_long_internal(parent_->h, N_, &N)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(parent_->h, Ni_, &Ni)) != GRIB_SUCCESS)
        return ret;

    if (Ni == GRIB_MISSING_LONG) {
        /* Reduced gaussian grid */
        long isOctahedral = 0;
        if ((ret = grib_get_long_internal(parent_->h, isOctahedral_, &isOctahedral)) != GRIB_SUCCESS)
            return ret;
        if (isOctahedral == 1) {
            snprintf(tmp, sizeof(tmp), "O%ld", N);
        }
        else {
            snprintf(tmp, sizeof(tmp), "N%ld", N); /* Classic */
        }
    }
    else {
        /* Regular gaussian grid */
        snprintf(tmp, sizeof(tmp), "F%ld", N);
    }
    length = strlen(tmp) + 1;

    if (*len < length) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, length, *len);
        *len = length;
        return GRIB_BUFFER_TOO_SMALL;
    }

    strcpy(v, tmp);
    *len = length;
    return GRIB_SUCCESS;
}

size_t grib_accessor_gaussian_grid_name_t::string_length()
{
    return MAX_GRIDNAME_LEN;
}
