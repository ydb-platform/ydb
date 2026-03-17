/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_packing_type.h"

grib_accessor_packing_type_t _grib_accessor_packing_type{};
grib_accessor* grib_accessor_packing_type = &_grib_accessor_packing_type;

void grib_accessor_packing_type_t::init(const long l, grib_arguments* args)
{
    grib_accessor_gen_t::init(l, args);
    int n         = 0;
    values_       = args->get_name(grib_handle_of_accessor(this), n++);
    packing_type_ = args->get_name(grib_handle_of_accessor(this), n++);
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    length_ = 0;
}

size_t grib_accessor_packing_type_t::string_length()
{
    return 1024;
}

long grib_accessor_packing_type_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_packing_type_t::pack_string(const char* sval, size_t* len)
{
    grib_handle* h  = grib_handle_of_accessor(this);
    double* values  = NULL;
    grib_context* c = context_;
    size_t size     = 0;
    int err         = 0;

    if ((err = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return err;

    values = (double*)grib_context_malloc(c, size * sizeof(double));
    if (!values) return GRIB_OUT_OF_MEMORY;

    if ((err = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return err;
    }

    if ((err = grib_set_string_internal(h, packing_type_, sval, len)) != GRIB_SUCCESS) {
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

int grib_accessor_packing_type_t::unpack_string(char* val, size_t* len)
{
    grib_handle* h = grib_handle_of_accessor(this);

    return grib_get_string(h, packing_type_, val, len);
}
