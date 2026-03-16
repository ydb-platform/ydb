/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_data_g2simple_packing_with_preprocessing.h"
#define DIRECT  0
#define INVERSE 1

grib_accessor_data_g2simple_packing_with_preprocessing_t _grib_accessor_data_g2simple_packing_with_preprocessing{};
grib_accessor* grib_accessor_data_g2simple_packing_with_preprocessing = &_grib_accessor_data_g2simple_packing_with_preprocessing;

void grib_accessor_data_g2simple_packing_with_preprocessing_t::init(const long v, grib_arguments* args)
{
    grib_accessor_data_g2simple_packing_t::init(v, args);
    pre_processing_           = args->get_name(grib_handle_of_accessor(this), carg_++);
    pre_processing_parameter_ = args->get_name(grib_handle_of_accessor(this), carg_++);
    flags_ |= GRIB_ACCESSOR_FLAG_DATA;
}

int grib_accessor_data_g2simple_packing_with_preprocessing_t::value_count(long* n_vals)
{
    *n_vals = 0;

    return grib_get_long_internal(grib_handle_of_accessor(this), number_of_values_, n_vals);
}

static int pre_processing_func(double* values, long length, long pre_processing,
                               double* pre_processing_parameter, int mode)
{
    int i = 0, ret = 0;
    double min      = values[0];
    double next_min = values[0];
    ECCODES_ASSERT(length > 0);

    switch (pre_processing) {
        /* NONE */
        case 0:
            break;
        /* LOGARITHM */
        case 1:
            if (mode == DIRECT) {
                for (i = 0; i < length; i++) {
                    if (values[i] < min)
                        min = values[i];
                    if (values[i] > next_min)
                        next_min = values[i];
                }
                for (i = 0; i < length; i++) {
                    if (values[i] > min && values[i] < next_min)
                        next_min = values[i];
                }
                if (min > 0) {
                    *pre_processing_parameter = 0;
                    for (i = 0; i < length; i++) {
                        DEBUG_ASSERT(values[i] > 0);
                        values[i] = log(values[i]);
                    }
                }
                else {
                    double ppp                = 0;
                    *pre_processing_parameter = next_min - 2 * min;
                    if (next_min == min)
                        return ret;
                    ppp = *pre_processing_parameter;
                    for (i = 0; i < length; i++) {
                        DEBUG_ASSERT((values[i] + ppp) > 0);
                        values[i] = log(values[i] + ppp);
                    }
                }
            }
            else {
                ECCODES_ASSERT(mode == INVERSE);
                if (*pre_processing_parameter == 0) {
                    for (i = 0; i < length; i++)
                        values[i] = exp(values[i]);
                }
                else {
                    for (i = 0; i < length; i++)
                        values[i] = exp(values[i]) - *pre_processing_parameter;
                }
            }
            break;
        default:
            ret = GRIB_NOT_IMPLEMENTED;
            break;
    }

    return ret;
}

int grib_accessor_data_g2simple_packing_with_preprocessing_t::unpack_double(double* val, size_t* len)
{
    size_t n_vals = 0;
    long nn       = 0;
    int err       = 0;

    long pre_processing;
    double pre_processing_parameter;

    err    = value_count(&nn);
    n_vals = nn;
    if (err)
        return err;

    if (n_vals == 0) {
        *len = 0;
        return GRIB_SUCCESS;
    }

    dirty_ = 0;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), pre_processing_, &pre_processing)) != GRIB_SUCCESS) {
        return err;
    }

    if ((err = grib_get_double_internal(grib_handle_of_accessor(this), pre_processing_parameter_, &pre_processing_parameter)) != GRIB_SUCCESS) {
        return err;
    }

    err = grib_accessor_data_simple_packing_t::unpack_double(val, &n_vals);
    if (err != GRIB_SUCCESS)
        return err;

    err = pre_processing_func(val, n_vals, pre_processing, &pre_processing_parameter, INVERSE);
    if (err != GRIB_SUCCESS)
        return err;

    *len = (long)n_vals;

    return err;
}

int grib_accessor_data_g2simple_packing_with_preprocessing_t::pack_double(const double* val, size_t* len)
{
    size_t n_vals = *len;
    int err       = 0;

    long pre_processing             = 0;
    double pre_processing_parameter = 0;

    dirty_ = 1;

    if ((err = grib_get_long_internal(grib_handle_of_accessor(this), pre_processing_, &pre_processing)) != GRIB_SUCCESS)
        return err;

    err = pre_processing_func((double*)val, n_vals, pre_processing, &pre_processing_parameter, DIRECT);
    if (err != GRIB_SUCCESS)
        return err;

    err = grib_accessor_data_g2simple_packing_t::pack_double(val, len);
    if (err != GRIB_SUCCESS)
        return err;

    if ((err = grib_set_double_internal(grib_handle_of_accessor(this), pre_processing_parameter_, pre_processing_parameter)) != GRIB_SUCCESS)
        return err;

    if ((err = grib_set_long_internal(grib_handle_of_accessor(this), number_of_values_, n_vals)) != GRIB_SUCCESS)
        return err;

    return GRIB_SUCCESS;
}
