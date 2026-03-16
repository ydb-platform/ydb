/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_statistics.h"

grib_accessor_statistics_t _grib_accessor_statistics{};
grib_accessor* grib_accessor_statistics = &_grib_accessor_statistics;

void grib_accessor_statistics_t::init(const long l, grib_arguments* c)
{
    grib_accessor_abstract_vector_t::init(l, c);
    int n = 0;

    missing_value_ = c->get_name(grib_handle_of_accessor(this), n++);
    values_        = c->get_name(grib_handle_of_accessor(this), n++);
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    flags_ |= GRIB_ACCESSOR_FLAG_FUNCTION;
    flags_ |= GRIB_ACCESSOR_FLAG_HIDDEN;

    number_of_elements_ = 8;
    v_                  = (double*)grib_context_malloc(context_,
                                                       sizeof(double) * number_of_elements_);

    length_ = 0;
    dirty_  = 1;
}

int grib_accessor_statistics_t::unpack_double(double* val, size_t* len)
{
    int ret        = 0;
    double* values = NULL;
    size_t i = 0, size = 0, real_size = 0;
    double max, min, avg, sd, value, skew, kurt, m2 = 0, m3 = 0, m4 = 0;
    double missing            = 0;
    long missingValuesPresent = 0;
    size_t number_of_missing  = 0;
    grib_context* c           = context_;
    grib_handle* h            = grib_handle_of_accessor(this);

    if (!dirty_)
        return GRIB_SUCCESS;

    if (*len != number_of_elements_)
        return GRIB_ARRAY_TOO_SMALL;

    if ((ret = grib_get_size(h, values_, &size)) != GRIB_SUCCESS)
        return ret;

    grib_context_log(context_, GRIB_LOG_DEBUG,
                     "grib_accessor_statistics_t: computing statistics for %d values", size);

    if ((ret = grib_get_double(h, missing_value_, &missing)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_get_long_internal(h, "missingValuesPresent", &missingValuesPresent)) != GRIB_SUCCESS)
        return ret;

    values = (double*)grib_context_malloc_clear(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((ret = grib_get_double_array_internal(h, values_, values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }

    number_of_missing = 0;
    if (missingValuesPresent) {
        i = 0;
        while (i < size && values[i] == missing) {
            i++;
            number_of_missing++;
        }
        if (number_of_missing == size) {
            /* ECC-649: All values are missing */
            min = max = avg = missing;
        }
        else {
            max = values[i];
            min = values[i];
            avg = values[i];
            for (i = number_of_missing + 1; i < size; i++) {
                value = values[i];
                if (value > max && value != missing)
                    max = value;
                if (value < min && value != missing)
                    min = value;
                if (value != missing)
                    avg += value;
                else
                    number_of_missing++;
            }
        }
    }
    else {
        max = values[0];
        min = values[0];
        avg = values[0];
        for (i = 1; i < size; i++) {
            value = values[i];
            if (value > max)
                max = value;
            if (value < min)
                min = value;
            avg += value;
        }
    }

    /* Don't divide by zero if all values are missing! */
    if (size != number_of_missing) {
        avg /= (size - number_of_missing);
    }

    sd   = 0;
    skew = 0;
    kurt = 0;
    for (i = 0; i < size; i++) {
        int valueNotMissing = 1;
        if (missingValuesPresent) {
            valueNotMissing = (values[i] != missing);
        }
        if (valueNotMissing) {
            double v   = values[i] - avg;
            double tmp = v * v;
            m2 += tmp;
            m3 += v * tmp;
            m4 += tmp * tmp;
        }
    }

    real_size = size - number_of_missing;
    if (real_size != 0) {
        m2 /= real_size;
        m3 /= real_size;
        m4 /= real_size;
        sd = sqrt(m2);
    }
    if (m2 != 0) {
        skew = m3 / (sd * sd * sd);
        kurt = m4 / (m2 * m2) - 3.0;
    }
    // printf("\ngrib_accessor_class_statistics_t::unpack_double   Computed. So setting dirty to 0....... \n");
    dirty_ = 0;

    grib_context_free(c, values);

    v_[0] = max;
    v_[1] = min;
    v_[2] = avg;
    v_[3] = number_of_missing;
    v_[4] = sd;
    v_[5] = skew;
    v_[6] = kurt;
    v_[7] = sd == 0 ? 1 : 0;

    for (i = 0; i < number_of_elements_; i++)
        val[i] = v_[i];

    return ret;
}

int grib_accessor_statistics_t::value_count(long* count)
{
    *count = number_of_elements_;
    return 0;
}

void grib_accessor_statistics_t::destroy(grib_context* c)
{
    grib_context_free(c, v_);
    grib_accessor_abstract_vector_t::destroy(c);
}

int grib_accessor_statistics_t::compare(grib_accessor* b)
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

int grib_accessor_statistics_t::unpack_string(char* v, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}
