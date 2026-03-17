/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_values.h"

grib_accessor_values_t _grib_accessor_values{};
grib_accessor* grib_accessor_values = &_grib_accessor_values;

long grib_accessor_values_t::init_length()
{
    int ret = 0;
    long seclen        = 0;
    long offsetsection = 0;
    long offsetdata    = 0;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), seclen_, &seclen)))
        return ret;

    if (seclen == 0) {
        /* printf("init_length seclen=0\n"); */
        return 0;
    }

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), offsetsection_, &offsetsection)))
        return ret;

    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), offsetdata_, &offsetdata)))
        return ret;

    /* When reparsing */
    if (offsetdata < offsetsection) {
        /* printf("init_length offsetdata < offsetsection=0\n"); */
        ECCODES_ASSERT(grib_handle_of_accessor(this)->loader);
        return 0;
    }

    return seclen - (offsetdata - offsetsection);
}

void grib_accessor_values_t::init(const long v, grib_arguments* params)
{
    grib_accessor_gen_t::init(v, params);
    carg_ = 0;

    seclen_        = params->get_name(grib_handle_of_accessor(this), carg_++);
    offsetdata_    = params->get_name(grib_handle_of_accessor(this), carg_++);
    offsetsection_ = params->get_name(grib_handle_of_accessor(this), carg_++);
    values_dirty_  = 1;

    length_ = init_length();
    /* ECCODES_ASSERT(length_ >=0); */
}

long grib_accessor_values_t::get_native_type()
{
    return GRIB_TYPE_DOUBLE;
}

void grib_accessor_values_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_values(this);
}

long grib_accessor_values_t::byte_count()
{
    grib_context_log(context_, GRIB_LOG_DEBUG, "byte_count of %s = %ld", name_, length_);
    return length_;
}

long grib_accessor_values_t::byte_offset()
{
    return offset_;
}

long grib_accessor_values_t::next_offset()
{
    return offset_ + length_;
}

void grib_accessor_values_t::update_size(size_t s)
{
    grib_context_log(context_, GRIB_LOG_DEBUG, "updating size of %s old %ld new %ld", name_, length_, s);
    length_ = s;
    ECCODES_ASSERT(length_ >= 0);
}

int grib_accessor_values_t::compare(grib_accessor* b)
{
    int retval   = 0;
    double* aval = 0;
    double* bval = 0;

    size_t alen = 0;
    size_t blen = 0;
    int err     = 0;
    long count  = 0;

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
    for (size_t i = 0; i < alen && retval == GRIB_SUCCESS; ++i) {
        if (aval[i] != bval[i]) retval = GRIB_DOUBLE_VALUE_MISMATCH;
    }

    grib_context_free(context_, aval);
    grib_context_free(b->context_, bval);

    return retval;
}

int grib_accessor_values_t::pack_long(const long* val, size_t* len)
{
    double* dval = (double*)grib_context_malloc(context_, *len * sizeof(double));

    for (size_t i = 0; i < *len; i++)
        dval[i] = (double)val[i];

    int ret = pack_double(dval, len);
    grib_context_free(context_, dval);

    values_dirty_ = 1;

    return ret;
}
