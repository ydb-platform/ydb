/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_codetable_units.h"
#include "grib_accessor_class_codetable.h"

grib_accessor_codetable_units_t _grib_accessor_codetable_units{};
grib_accessor* grib_accessor_codetable_units = &_grib_accessor_codetable_units;

void grib_accessor_codetable_units_t::init(const long len, grib_arguments* params)
{
    grib_accessor_gen_t::init(len, params);

    int n      = 0;
    codetable_ = params->get_name(grib_handle_of_accessor(this), n++);
    length_    = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

long grib_accessor_codetable_units_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_codetable_units_t::unpack_string(char* buffer, size_t* len)
{
    grib_codetable* table = NULL;

    size_t size = 1;
    long value;
    int err = GRIB_SUCCESS;
    char tmp[1024];
    size_t l                      = sizeof(tmp);
    grib_accessor_codetable_t* ca = (grib_accessor_codetable_t*)grib_find_accessor(grib_handle_of_accessor(this), codetable_);

    if ((err = ((grib_accessor*)ca)->unpack_long(&value, &size)) != GRIB_SUCCESS)
        return err;

    table = ca->codetable();

    if (table && (value >= 0) && (value < table->size) && table->entries[value].units) {
        strcpy(tmp, table->entries[value].units);
    }
    else {
        snprintf(tmp, sizeof(tmp), "%d", (int)value);
    }

    l = strlen(tmp) + 1;

    if (*len < l) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, l, *len);
        *len = l;
        return GRIB_BUFFER_TOO_SMALL;
    }

    strcpy(buffer, tmp);
    *len = l;

    return GRIB_SUCCESS;
}
