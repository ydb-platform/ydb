/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_reference_value_error.h"

grib_accessor_reference_value_error_t _grib_accessor_reference_value_error{};
grib_accessor* grib_accessor_reference_value_error = &_grib_accessor_reference_value_error;

void grib_accessor_reference_value_error_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n = 0;

    referenceValue_ = c->get_name(grib_handle_of_accessor(this), n++);
    floatType_      = c->get_name(grib_handle_of_accessor(this), n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    length_ = 0;
}

int grib_accessor_reference_value_error_t::unpack_double(double* val, size_t* len)
{
    int ret               = GRIB_SUCCESS;
    double referenceValue = 0;

    if ((ret = grib_get_double_internal(grib_handle_of_accessor(this),
                                        referenceValue_, &referenceValue)) != GRIB_SUCCESS)
        return ret;

    if (!strcmp(floatType_, "ibm"))
        *val = grib_ibmfloat_error(referenceValue);
    else if (!strcmp(floatType_, "ieee"))
        *val = grib_ieeefloat_error(referenceValue);
    else
        ECCODES_ASSERT(1 == 0);

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return ret;
}
