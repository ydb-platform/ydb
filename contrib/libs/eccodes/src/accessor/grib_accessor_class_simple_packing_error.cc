/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_simple_packing_error.h"
#include "grib_scaling.h"

grib_accessor_simple_packing_error_t _grib_accessor_simple_packing_error{};
grib_accessor* grib_accessor_simple_packing_error = &_grib_accessor_simple_packing_error;

void grib_accessor_simple_packing_error_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    int n          = 0;
    grib_handle* h = grib_handle_of_accessor(this);

    bitsPerValue_       = c->get_name(h, n++);
    binaryScaleFactor_  = c->get_name(h, n++);
    decimalScaleFactor_ = c->get_name(h, n++);
    referenceValue_     = c->get_name(h, n++);
    floatType_          = c->get_name(h, n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    length_ = 0;
}

int grib_accessor_simple_packing_error_t::unpack_double(double* val, size_t* len)
{
    int ret                 = 0;
    long binaryScaleFactor  = 0;
    long bitsPerValue       = 0;
    long decimalScaleFactor = 0;
    double referenceValue   = 0;
    grib_handle* h          = grib_handle_of_accessor(this);

    if ((ret = grib_get_long_internal(h, binaryScaleFactor_, &binaryScaleFactor)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, bitsPerValue_, &bitsPerValue)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long_internal(h, decimalScaleFactor_, &decimalScaleFactor)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_double_internal(h, referenceValue_, &referenceValue)) != GRIB_SUCCESS)
        return ret;

    if (!strcmp(floatType_, "ibm"))
        *val = grib_ibmfloat_error(referenceValue);
    else if (!strcmp(floatType_, "ieee"))
        *val = grib_ieeefloat_error(referenceValue);
    else
        ECCODES_ASSERT(1 == 0);

    if (bitsPerValue != 0)
        *val = (*val + codes_power<double>(binaryScaleFactor, 2)) * codes_power<double>(-decimalScaleFactor, 10) * 0.5;

    if (ret == GRIB_SUCCESS)
        *len = 1;

    return ret;
}
