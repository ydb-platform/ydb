/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessors_list.h"

grib_accessors_list* grib_accessors_list_create(grib_context* c)
{
    return (grib_accessors_list*)grib_context_malloc_clear(c, sizeof(grib_accessors_list));
}

void grib_accessors_list_delete(grib_context* c, grib_accessors_list* al)
{
    grib_accessors_list* tmp;
    while (al) {
        tmp = al->next_;
        grib_context_free(c, al);
        al = tmp;
    }
}

int grib_accessors_list::value_count(size_t* count)
{
    long lcount             = 0;
    *count                  = 0;
    grib_accessors_list* al = this;
    while (al) {
        al->accessor->value_count(&lcount);
        *count += lcount;
        al = al->next_;
    }
    return 0;
}

void grib_accessors_list::push(grib_accessor* a, int rank)
{
    const grib_context* c = a->context_;

    grib_accessors_list* last_acc = this->last();
    if (last_acc && last_acc->accessor) {
        last_acc->next_           = (grib_accessors_list*)grib_context_malloc_clear(c, sizeof(grib_accessors_list));
        last_acc->next_->accessor = a;
        last_acc->next_->prev_    = last_acc;
        last_acc->next_->rank_    = rank;
        this->last_               = last_acc->next_;
    }
    else {
        this->accessor = a;
        this->rank_    = rank;
        this->last_    = this;
    }
}

grib_accessors_list* grib_accessors_list::last()
{
    return last_;
}

grib_accessors_list::~grib_accessors_list()
{
    grib_accessors_list* tmp;
    grib_context* c = grib_context_get_default();

    grib_accessors_list* al = this;
    while (al) {
        tmp = al->next_;
        // grib_accessor_delete(c, al->accessor);
        grib_context_free(c, al);
        al = tmp;
    }
}

int grib_accessors_list::unpack_long(long* val, size_t* buffer_len)
{
    int err             = GRIB_SUCCESS;
    size_t unpacked_len = 0;
    size_t len          = 0;

    grib_accessors_list* al = this;
    while (al && err == GRIB_SUCCESS) {
        len = *buffer_len - unpacked_len;
        err = al->accessor->unpack_long(val + unpacked_len, &len);
        unpacked_len += len;
        al = al->next_;
    }

    *buffer_len = unpacked_len;
    return err;
}

int grib_accessors_list::unpack_double(double* val, size_t* buffer_len)
{
    int err             = GRIB_SUCCESS;
    size_t unpacked_len = 0;
    size_t len          = 0;

    grib_accessors_list* al = this;
    while (al && err == GRIB_SUCCESS) {
        len = *buffer_len - unpacked_len;
        err = al->accessor->unpack_double(val + unpacked_len, &len);
        unpacked_len += len;
        al = al->next_;
    }

    *buffer_len = unpacked_len;
    return err;
}

int grib_accessors_list::unpack_float(float* val, size_t* buffer_len)
{
    int err             = GRIB_SUCCESS;
    size_t unpacked_len = 0;
    size_t len          = 0;

    grib_accessors_list* al = this;
    while (al && err == GRIB_SUCCESS) {
        len = *buffer_len - unpacked_len;
        err = al->accessor->unpack_float(val + unpacked_len, &len);
        unpacked_len += len;
        al = al->next_;
    }

    *buffer_len = unpacked_len;
    return err;
}

int grib_accessors_list::unpack_string(char** val, size_t* buffer_len)
{
    int err             = GRIB_SUCCESS;
    size_t unpacked_len = 0;
    size_t len          = 0;

    grib_accessors_list* al = this;
    while (al && err == GRIB_SUCCESS) {
        len = *buffer_len - unpacked_len;
        err = al->accessor->unpack_string_array(val + unpacked_len, &len);
        unpacked_len += len;
        al = al->next_;
    }

    *buffer_len = unpacked_len;
    return err;
}
