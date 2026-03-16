/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_double.h"

grib_accessor_double_t _grib_accessor_double{};
grib_accessor* grib_accessor_double = &_grib_accessor_double;

long grib_accessor_double_t::get_native_type()
{
    return GRIB_TYPE_DOUBLE;
}

int grib_accessor_double_t::unpack_string(char* v, size_t* len)
{
    double val = 0;
    size_t l   = 1;
    char repres[1024];
    char format[32]         = "%g";
    grib_handle* h          = grib_handle_of_accessor(this);

    unpack_double(&val, &l);
    if ((val == GRIB_MISSING_DOUBLE) && ((flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0)) {
        snprintf(repres, sizeof(repres), "MISSING");
    }
    else {
        size_t size = sizeof(format);
        grib_get_string(h, "formatForDoubles", format, &size);
        snprintf(repres, sizeof(repres), format, val);
    }

    l = strlen(repres) + 1;

    if (l > *len) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, l, *len);
        *len = l;
        return GRIB_BUFFER_TOO_SMALL;
    }
    grib_context_log(context_, GRIB_LOG_DEBUG, "grib_accessor_long: Casting double %s to string  ", name_);

    *len = l;

    strcpy(v, repres);
    return GRIB_SUCCESS;
}

void grib_accessor_double_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_values(this);
}

int grib_accessor_double_t::compare(grib_accessor* b)
{
    int retval   = 0;
    double* aval = 0;
    double* bval = 0;

    size_t alen = 0;
    size_t blen = 0;
    long count  = 0;
    int err     = 0;

    err = value_count(&count);
    if (err)
        return err;
    alen = count;

    err = b->value_count(&count);
    if (err)
        return err;
    blen = count;

    if (alen != blen)
        return GRIB_COUNT_MISMATCH;

    aval = (double*)grib_context_malloc(context_, alen * sizeof(double));
    bval = (double*)grib_context_malloc(b->context_, blen * sizeof(double));

    unpack_double(aval, &alen);
    b->unpack_double(bval, &blen);
    retval = GRIB_SUCCESS;
    while (alen != 0) {
        if (*bval != *aval)
            retval = GRIB_DOUBLE_VALUE_MISMATCH;
        alen--;
    }

    grib_context_free(context_, aval);
    grib_context_free(b->context_, bval);

    return retval;
}

int grib_accessor_double_t::pack_missing()
{
    size_t len   = 1;
    double value = GRIB_MISSING_DOUBLE;

    if (flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING)
        return pack_double(&value, &len);
    return GRIB_VALUE_CANNOT_BE_MISSING;
}
