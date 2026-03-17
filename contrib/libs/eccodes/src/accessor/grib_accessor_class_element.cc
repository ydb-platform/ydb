/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_element.h"

grib_accessor_element_t _grib_accessor_element{};
grib_accessor* grib_accessor_element = &_grib_accessor_element;

void grib_accessor_element_t::init(const long l, grib_arguments* c)
{
    grib_accessor_long_t::init(l, c);
    grib_handle* hand = grib_handle_of_accessor(this);

    int n    = 0;
    array_   = c->get_name(hand, n++);
    element_ = c->get_long(hand, n++);
}

static int check_element_index(const char* func, const char* array_name, long index, size_t size)
{
    const grib_context* c = grib_context_get_default();
    if (index < 0 || index >= size) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Invalid element index %ld for array '%s'. Value must be between 0 and %zu",
                         func, index, array_name, size - 1);
        return GRIB_INVALID_ARGUMENT;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_element_t::unpack_long(long* val, size_t* len)
{
    int ret               = 0;
    size_t size           = 0;
    long* ar              = NULL;
    const grib_context* c = context_;
    grib_handle* hand     = grib_handle_of_accessor(this);
    long index            = element_;

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    if ((ret = grib_get_size(hand, array_, &size)) != GRIB_SUCCESS)
        return ret;

    ar = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if (!ar) {
        grib_context_log(c, GRIB_LOG_ERROR, "Error allocating %zu bytes", size * sizeof(long));
        return GRIB_OUT_OF_MEMORY;
    }

    if ((ret = grib_get_long_array_internal(hand, array_, ar, &size)) != GRIB_SUCCESS)
        return ret;

    // An index of -x means the xth item from the end of the list, so ar[-1] means the last item in ar
    if (index < 0) {
        index = size + index;
    }

    if ((ret = check_element_index(__func__, array_, index, size)) != GRIB_SUCCESS) {
        goto the_end;
    }

    *val = ar[index];

the_end:
    grib_context_free(c, ar);
    return ret;
}

int grib_accessor_element_t::pack_long(const long* val, size_t* len)
{
    int ret               = 0;
    size_t size           = 0;
    long* ar              = NULL;
    const grib_context* c = context_;
    grib_handle* hand     = grib_handle_of_accessor(this);
    long index            = element_;

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    int atype = GRIB_TYPE_UNDEFINED;
    if (grib_get_native_type(hand, array_, &atype) == GRIB_SUCCESS && atype == GRIB_TYPE_DOUBLE) {
        const double dVal = *val;
        return pack_double(&dVal, len);
    }

    if ((ret = grib_get_size(hand, array_, &size)) != GRIB_SUCCESS)
        return ret;

    ar = (long*)grib_context_malloc_clear(c, size * sizeof(long));
    if (!ar) {
        grib_context_log(c, GRIB_LOG_ERROR, "Error allocating %zu bytes", size * sizeof(long));
        return GRIB_OUT_OF_MEMORY;
    }

    if ((ret = grib_get_long_array_internal(hand, array_, ar, &size)) != GRIB_SUCCESS)
        return ret;

    // An index of -x means the xth item from the end of the list, so ar[-1] means the last item in ar
    if (index < 0) {
        index = size + index;
    }

    if ((ret = check_element_index(__func__, array_, index, size)) != GRIB_SUCCESS) {
        goto the_end;
    }

    ECCODES_ASSERT(index >= 0);
    ECCODES_ASSERT(index < size);
    ar[index] = *val;

    if ((ret = grib_set_long_array_internal(hand, array_, ar, size)) != GRIB_SUCCESS)
        goto the_end;

the_end:
    grib_context_free(c, ar);
    return ret;
}

int grib_accessor_element_t::pack_double(const double* v, size_t* len)
{
    int ret               = 0;
    size_t size           = 0;
    double* ar            = NULL;
    const grib_context* c = context_;
    grib_handle* hand     = grib_handle_of_accessor(this);
    long index            = element_;

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    if ((ret = grib_get_size(hand, array_, &size)) != GRIB_SUCCESS)
        return ret;

    ar = (double*)grib_context_malloc_clear(c, size * sizeof(double));
    if (!ar) {
        grib_context_log(c, GRIB_LOG_ERROR, "Error allocating %zu bytes", size * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }

    if ((ret = grib_get_double_array_internal(hand, array_, ar, &size)) != GRIB_SUCCESS)
        return ret;

    // An index of -x means the xth item from the end of the list, so ar[-1] means the last item in ar
    if (index < 0) {
        index = size + index;
    }

    if ((ret = check_element_index(__func__, array_, index, size)) != GRIB_SUCCESS) {
        goto the_end;
    }

    ECCODES_ASSERT(index >= 0);
    ECCODES_ASSERT(index < size);
    ar[index] = *v;

    if ((ret = grib_set_double_array_internal(hand, array_, ar, size)) != GRIB_SUCCESS)
        goto the_end;

the_end:
    grib_context_free(c, ar);
    return ret;
}

int grib_accessor_element_t::unpack_double(double* val, size_t* len)
{
    int ret                 = 0;
    size_t size             = 0;
    double* ar              = NULL;
    const grib_context* c   = context_;
    const grib_handle* hand = grib_handle_of_accessor(this);
    long index              = element_;

    if (*len < 1) {
        ret = GRIB_ARRAY_TOO_SMALL;
        return ret;
    }

    if ((ret = grib_get_size(hand, array_, &size)) != GRIB_SUCCESS)
        return ret;

    ar = (double*)grib_context_malloc_clear(c, size * sizeof(double));
    if (!ar) {
        grib_context_log(c, GRIB_LOG_ERROR, "Error allocating %zu bytes", size * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }

    if ((ret = grib_get_double_array_internal(hand, array_, ar, &size)) != GRIB_SUCCESS)
        return ret;

    // An index of -x means the xth item from the end of the list, so ar[-1] means the last item in ar
    if (index < 0) {
        index = size + index;
    }

    if ((ret = check_element_index(__func__, array_, index, size)) != GRIB_SUCCESS) {
        goto the_end;
    }

    *val = ar[index];

the_end:
    grib_context_free(c, ar);
    return ret;
}
