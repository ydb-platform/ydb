/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1end_of_interval_monthly.h"

grib_accessor_g1end_of_interval_monthly_t _grib_accessor_g1end_of_interval_monthly{};
grib_accessor* grib_accessor_g1end_of_interval_monthly = &_grib_accessor_g1end_of_interval_monthly;

void grib_accessor_g1end_of_interval_monthly_t::init(const long l, grib_arguments* c)
{
    grib_accessor_abstract_vector_t::init(l, c);
    int n = 0;

    verifyingMonth_ = c->get_name(grib_handle_of_accessor(this), n++);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;

    number_of_elements_ = 6;
    v_                  = (double*)grib_context_malloc(context_, sizeof(double) * number_of_elements_);

    length_ = 0;
    dirty_  = 1;
}

int grib_accessor_g1end_of_interval_monthly_t::unpack_double(double* val, size_t* len)
{
    int ret                = 0;
    char verifyingMonth[7] = {0,};
    size_t slen = 7;
    long year = 0, month = 0, date = 0;
    const long mdays[] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    long days          = 0;

    if (!dirty_)
        return GRIB_SUCCESS;

    if (*len != (size_t)number_of_elements_)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_string(grib_handle_of_accessor(this), verifyingMonth_, verifyingMonth, &slen)) != GRIB_SUCCESS)
        return ret;

    date = atoi(verifyingMonth);
    if (date < 0) {
        return GRIB_INVALID_ARGUMENT;
    }
    year  = date / 100;
    month = date - year * 100;
    if (month == 2) {
        days = 28;
        if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0))
            days = 29;
    }
    else {
        if (month < 1 || month > 12) return GRIB_INVALID_ARGUMENT;
        days = mdays[month - 1];
    }
    v_[0] = year;
    v_[1] = month;

    v_[2] = days;
    v_[3] = 24;
    v_[4] = 00;
    v_[5] = 00;

    dirty_ = 0;

    val[0] = v_[0];
    val[1] = v_[1];
    val[2] = v_[2];
    val[3] = v_[3];
    val[4] = v_[4];
    val[5] = v_[5];

    return ret;
}

int grib_accessor_g1end_of_interval_monthly_t::value_count(long* count)
{
    *count = number_of_elements_;
    return 0;
}

void grib_accessor_g1end_of_interval_monthly_t::destroy(grib_context* c)
{
    grib_context_free(c, v_);
    grib_accessor_abstract_vector_t::destroy(c);
}

int grib_accessor_g1end_of_interval_monthly_t::compare(grib_accessor* b)
{
    int retval   = GRIB_SUCCESS;
    double* aval = 0;
    double* bval = 0;

    long count  = 0;
    size_t alen = 0;
    size_t blen = 0;

    int err = value_count(&count);
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

    err = unpack_double(aval, &alen);
    if (err) return err;
    err = b->unpack_double(bval, &blen);
    if (err) return err;

    for (size_t i = 0; i < alen && retval == GRIB_SUCCESS; ++i) {
        if (aval[i] != bval[i]) retval = GRIB_DOUBLE_VALUE_MISMATCH;
    }

    grib_context_free(context_, aval);
    grib_context_free(b->context_, bval);

    return retval;
}
