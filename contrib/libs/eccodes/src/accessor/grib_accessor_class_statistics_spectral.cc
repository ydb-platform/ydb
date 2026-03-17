/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_statistics_spectral.h"

grib_accessor_statistics_spectral_t _grib_accessor_statistics_spectral{};
grib_accessor* grib_accessor_statistics_spectral = &_grib_accessor_statistics_spectral;

void grib_accessor_statistics_spectral_t::init(const long l, grib_arguments* c)
{
    grib_accessor_abstract_vector_t::init(l, c);
    int n = 0;
    grib_handle* h  = grib_handle_of_accessor(this);

    values_ = c->get_name(h, n++);
    J_      = c->get_name(h, n++);
    K_      = c->get_name(h, n++);
    M_      = c->get_name(h, n++);
    JS_     = c->get_name(h, n++);

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;

    number_of_elements_ = 4;
    v_ = (double*)grib_context_malloc(context_, sizeof(double) * number_of_elements_);

    length_ = 0;
    dirty_  = 1;
}

int grib_accessor_statistics_spectral_t::unpack_double(double* val, size_t* len)
{
    int ret = 0, i = 0;
    double* values;
    size_t size = 0;
    long J, K, M, N;
    double avg, enorm, sd;
    grib_context* c = context_;
    grib_handle* h  = grib_handle_of_accessor(this);

    if (!dirty_)
        return GRIB_SUCCESS;

    if (*len != number_of_elements_)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long(h, J_, &J)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long(h, K_, &K)) != GRIB_SUCCESS)
        return ret;

    if ((ret = grib_get_long(h, M_, &M)) != GRIB_SUCCESS)
        return ret;

    if (J != M || M != K)
        return GRIB_NOT_IMPLEMENTED;

    N = (M + 1) * (M + 2) / 2;

    if (2 * N != size) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "wrong number of components for spherical harmonics %ld != %ld", 2 * N, size);
        return GRIB_WRONG_ARRAY_SIZE;
    }

    values = (double*)grib_context_malloc(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((ret = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    avg = values[0];
    sd  = 0;

    for (i = 2; i < 2 * J; i += 2)
        sd += values[i] * values[i];

    for (i = 2 * J; i < size; i += 2)
        sd += 2 * values[i] * values[i] + 2 * values[i + 1] * values[i + 1];

    enorm = sd + avg * avg;

    sd    = sqrt(sd);
    enorm = sqrt(enorm);

    dirty_ = 0;

    grib_context_free(c, values);

    v_[0] = avg;
    v_[1] = enorm;
    v_[2] = sd;
    v_[3] = sd == 0 ? 1 : 0;

    for (i = 0; i < number_of_elements_; i++)
        val[i] = v_[i];

    return ret;
}

int grib_accessor_statistics_spectral_t::value_count(long* count)
{
    *count = number_of_elements_;
    return GRIB_SUCCESS;
}

void grib_accessor_statistics_spectral_t::destroy(grib_context* c)
{
    grib_context_free(c, v_);
    grib_accessor_abstract_vector_t::destroy(c);
}

int grib_accessor_statistics_spectral_t::compare(grib_accessor* b)
{
    int retval   = GRIB_SUCCESS;
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

    b->dirty_ = 1;
    dirty_    = 1;

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
