/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_long_vector.h"
#include "grib_accessor_class_abstract_long_vector.h"

grib_accessor_long_vector_t _grib_accessor_long_vector{};
grib_accessor* grib_accessor_long_vector = &_grib_accessor_long_vector;

void grib_accessor_long_vector_t::init(const long l, grib_arguments* c)
{
    grib_accessor_abstract_long_vector_t::init(l, c);
    grib_accessor* va                       = NULL;
    grib_accessor_abstract_long_vector_t* v = NULL;
    int n                                   = 0;

    vector_ = c->get_name(grib_handle_of_accessor(this), n++);
    va      = (grib_accessor*)grib_find_accessor(grib_handle_of_accessor(this), vector_);
    v       = (grib_accessor_abstract_long_vector_t*)va;

    index_ = c->get_long(grib_handle_of_accessor(this), n++);

    /* check index_ on init and never change it */
    ECCODES_ASSERT(index_ < v->number_of_elements_ && index_ >= 0);

    length_ = 0;
}

int grib_accessor_long_vector_t::unpack_long(long* val, size_t* len)
{
    size_t size = 0;
    int err     = 0;
    long* vector;
    grib_accessor* va                       = NULL;
    grib_accessor_abstract_long_vector_t* v = NULL;

    va = (grib_accessor*)grib_find_accessor(grib_handle_of_accessor(this), vector_);
    v  = (grib_accessor_abstract_long_vector_t*)va;

    /*TODO implement the dirty mechanism to avoid to unpack every time */
    err = grib_get_size(grib_handle_of_accessor(this), vector_, &size);
    if (err) return err;
    DEBUG_ASSERT(size > 0);
    vector = (long*)grib_context_malloc(context_, sizeof(long) * size);
    err    = va->unpack_long(vector, &size);
    grib_context_free(context_, vector);
    if (err) return err;

    *val = v->v_[index_];

    return GRIB_SUCCESS;
}

int grib_accessor_long_vector_t::unpack_double(double* val, size_t* len)
{
    long lval                               = 0;
    int err                                 = 0;
    grib_accessor* va                       = NULL;
    grib_accessor_abstract_long_vector_t* v = NULL;
    va                                      = (grib_accessor*)grib_find_accessor(grib_handle_of_accessor(this), vector_);
    v                                       = (grib_accessor_abstract_long_vector_t*)va;

    err = unpack_long(&lval, len);

    *val = (double)v->v_[index_];

    return err;
}

int grib_accessor_long_vector_t::pack_long(const long* val, size_t* len)
{
    int err                                 = 0;
    grib_accessor* va                       = NULL;
    grib_accessor_abstract_long_vector_t* v = NULL;

    va = (grib_accessor*)grib_find_accessor(grib_handle_of_accessor(this), vector_);
    v  = (grib_accessor_abstract_long_vector_t*)va;

    v->pack_index_ = index_;

    err = va->pack_long(val, len);
    return err;
}

long grib_accessor_long_vector_t::get_native_type()
{
    return GRIB_TYPE_LONG;
}
