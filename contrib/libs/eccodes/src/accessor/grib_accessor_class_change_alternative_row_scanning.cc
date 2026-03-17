/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_change_alternative_row_scanning.h"

grib_accessor_change_alternative_row_scanning_t _grib_accessor_change_alternative_row_scanning{};
grib_accessor* grib_accessor_change_alternative_row_scanning = &_grib_accessor_change_alternative_row_scanning;

void grib_accessor_change_alternative_row_scanning_t::init(const long len, grib_arguments* args)
{
    grib_accessor_gen_t::init(len, args);
    int n = 0;

    values_                 = args->get_name(grib_handle_of_accessor(this), n++);
    Ni_                     = args->get_name(grib_handle_of_accessor(this), n++);
    Nj_                     = args->get_name(grib_handle_of_accessor(this), n++);
    alternativeRowScanning_ = args->get_name(grib_handle_of_accessor(this), n++);

    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

int grib_accessor_change_alternative_row_scanning_t::pack_long(const long* val, size_t* len)
{
    int err               = 0;
    const grib_context* c = context_;
    grib_handle* h        = grib_handle_of_accessor(this);
    long i, j, jr, theEnd, Ni, Nj, k, kp, alternativeRowScanning;
    size_t size    = 0;
    double* values = NULL;
    double tmp     = 0.0;

    if (*val == 0)
        return 0;

    /* Make sure Ni / Nj are not missing */
    if (grib_is_missing(h, Ni_, &err) && !err) {
        grib_context_log(c, GRIB_LOG_ERROR, "change_alternative_row_scanning: Key %s cannot be 'missing'!", Ni_);
        return GRIB_WRONG_GRID;
    }
    if (grib_is_missing(h, Nj_, &err) && !err) {
        grib_context_log(c, GRIB_LOG_ERROR, "change_alternative_row_scanning: Key %s cannot be 'missing'!", Nj_);
        return GRIB_WRONG_GRID;
    }

    if ((err = grib_get_long_internal(h, Ni_, &Ni)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, Nj_, &Nj)) != GRIB_SUCCESS)
        return err;
    if ((err = grib_get_long_internal(h, alternativeRowScanning_, &alternativeRowScanning)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return err;

    if (size > (size_t)(Ni * Nj)) {
        grib_context_log(c, GRIB_LOG_ERROR, "change_alternative_row_scanning: wrong values size!=Ni*Nj (%zu!=%ld*%ld)", size, Ni, Nj);
        return GRIB_WRONG_ARRAY_SIZE;
    }

    values = (double*)grib_context_malloc(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((err = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return err;
    }

    theEnd = Ni / 2;
    for (j = 0; j < Nj; j++) {
        jr = Ni * j;
        for (i = 0; i < theEnd; i++) {
            if (j % 2 == 1) {
                /* Swap first and last value on every odd row */
                k          = jr + i;
                kp         = jr + Ni - i - 1;
                tmp        = values[k];
                values[k]  = values[kp];
                values[kp] = tmp;
            }
        }
    }
    alternativeRowScanning = !alternativeRowScanning;
    if ((err = grib_set_long_internal(h, alternativeRowScanning_, alternativeRowScanning)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return err;
    }

    if ((err = grib_set_double_array_internal(h, values_, values, size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return err;
    }

    grib_context_free(c, values);

    return GRIB_SUCCESS;
}

long grib_accessor_change_alternative_row_scanning_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}

int grib_accessor_change_alternative_row_scanning_t::unpack_long(long* v, size_t* len)
{
    /* Decoding this accessor doesn't make sense so we return a dummy value */
    *v = -1;
    return GRIB_SUCCESS;
}
