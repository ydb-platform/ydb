/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_message_is_valid.h"
#include <cstdio>

grib_accessor_message_is_valid_t _grib_accessor_message_is_valid{};
grib_accessor* grib_accessor_message_is_valid = &_grib_accessor_message_is_valid;

static const char* TITLE = "Message validity checks";

void grib_accessor_message_is_valid_t::init(const long l, grib_arguments* arg)
{
    grib_accessor_long_t::init(l, arg);

    grib_handle* h = grib_handle_of_accessor(this);
    product_ = arg->get_name(h, 0);
    edition_ = 0;

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
    length_ = 0;
}

int grib_accessor_message_is_valid_t::check_field_values()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    int ret = GRIB_SUCCESS;
    double* values = NULL;
    size_t size = 0;
    grib_context* c = handle_->context;

    if ((ret = grib_get_size(handle_, "values", &size)) != GRIB_SUCCESS)
        return ret;
    values = (double*)grib_context_malloc_clear(c, size * sizeof(double));
    if (!values)
        return GRIB_OUT_OF_MEMORY;

    if ((ret = grib_get_double_array(handle_, "values", values, &size)) != GRIB_SUCCESS) {
        grib_context_free(c, values);
        return ret;
    }
    grib_context_free(c, values);
    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_grid_pl_array()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    int ret = GRIB_SUCCESS;
    long Ni = 0,plpresent  = 0;
    long* pl = NULL; // pl array
    size_t plsize = 0;
    grib_context* c = handle_->context;

    // is there a PL array?
    ret = grib_get_long(handle_, "PLPresent", &plpresent);
    if (ret != GRIB_SUCCESS || plpresent == 0)
        return GRIB_SUCCESS; // No PL array. So nothing to do

    char gridType[128] = {0,};
    size_t len = 128;
    ret = grib_get_string_internal(handle_, "gridType", gridType, &len);
    if (ret != GRIB_SUCCESS) return ret;

    if ((ret = grib_get_size(handle_, "pl", &plsize)) != GRIB_SUCCESS)
        return ret;
    if (plsize == 0) { // pl array must have at least one element
        return GRIB_WRONG_GRID;
    }

    // If we have a PL array and PLPresent=true, then Ni must be missing
    ret = grib_get_long(handle_, "Ni", &Ni);
    if (ret == GRIB_SUCCESS && Ni != GRIB_MISSING_LONG) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Invalid Ni: If there is a PL array, Ni must be set to MISSING", TITLE);
        return GRIB_WRONG_GRID;
    }

    pl = (long*)grib_context_malloc_clear(c, sizeof(long) * plsize);
    if (!pl) return GRIB_OUT_OF_MEMORY;
    if ((ret = grib_get_long_array_internal(handle_, "pl", pl, &plsize)) != GRIB_SUCCESS)
        return ret;

    long numberOfDataPoints = 0;
    if ((ret = grib_get_long_internal(handle_, "numberOfDataPoints", &numberOfDataPoints)) != GRIB_SUCCESS)
        return ret;
    size_t sum_pl = 0;
    for (size_t j = 0; j < plsize; j++) sum_pl += pl[j];

    if (STR_EQUAL(gridType, "reduced_ll")) {
        // For reduced_ll grids, sum(pl) must equal numberOfDataPoints in every situation
        if (sum_pl != (size_t)numberOfDataPoints) {
            grib_context_log(c, GRIB_LOG_ERROR, "%s: Sum of PL array (=%zu) must equal numberOfDataPoints (=%ld)",
                             TITLE, sum_pl, numberOfDataPoints);
            grib_context_free(c, pl);
            return GRIB_WRONG_GRID;
        }
    }
    else {
        // Other reduced grids
        // Unfortunately in our archive we have reduced_ll grids with zeroes in the pl!
        for (size_t j = 0; j < plsize; j++) {
            if (pl[j] == 0) {
                grib_context_log(c, GRIB_LOG_ERROR, "%s: Invalid PL array: entry at index=%zu is zero", TITLE, j);
                grib_context_free(c, pl);
                return GRIB_WRONG_GRID;
            }
        }
        if (sum_pl < (size_t)numberOfDataPoints) {
            grib_context_log(c, GRIB_LOG_ERROR, "%s: Sum of PL array (=%zu) cannot be less than numberOfDataPoints (=%ld)",
                    TITLE, sum_pl, numberOfDataPoints);
            grib_context_free(c, pl);
            return GRIB_WRONG_GRID;
        }
    }

    grib_context_free(c, pl);

    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_geoiterator()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);
    int err = 0;

#if defined(HAVE_GEOGRAPHY)
    grib_iterator* iter = grib_iterator_new(handle_, 0, &err);
    if (err == GRIB_NOT_IMPLEMENTED || err == GRIB_SUCCESS) {
        grib_iterator_delete(iter);
        return GRIB_SUCCESS; // GRIB_NOT_IMPLEMENTED is OK e.g., for spectral fields
    }

    grib_context_log(handle_->context, GRIB_LOG_ERROR, "%s: %s", TITLE, grib_get_error_message(err));
    grib_iterator_delete(iter);
#endif

    return err;
}

int grib_accessor_message_is_valid_t::check_7777()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    if (!grib_is_defined(handle_, "7777")) {
        return GRIB_7777_NOT_FOUND;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_surface_keys()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    int err = 0;
    const grib_context* c = handle_->context;

    if (edition_ != 2) return GRIB_SUCCESS;

    if (!grib_is_defined(handle_, "typeOfFirstFixedSurface"))
        return GRIB_SUCCESS; // nothing to do

    long stype=0;
    grib_get_long_internal(handle_, "typeOfFirstFixedSurface", &stype);
    int sfac_missing = grib_is_missing(handle_, "scaleFactorOfFirstFixedSurface", &err);
    int sval_missing = grib_is_missing(handle_, "scaledValueOfFirstFixedSurface", &err);
    if ((stype == 255 && !sfac_missing) || (stype == 255 && !sval_missing)) {
        grib_context_log(c, GRIB_LOG_ERROR,
            "%s: First fixed surface: If the type of surface is missing so should its scaleFactor/scaledValue keys", TITLE);
        return GRIB_INVALID_KEY_VALUE;
    }
    if (sfac_missing != sval_missing) {
        grib_context_log(c, GRIB_LOG_ERROR,
            "%s: First fixed surface: If the scale factor is missing so should the scaled value and vice versa", TITLE);
        return GRIB_INVALID_KEY_VALUE;
    }
    if (stype != 255) {
        int required_val = codes_grib_surface_type_requires_value(edition_, stype, &err);
        if (err) return err;
        if (required_val && (sfac_missing || sval_missing)) {
            char name[128] = {0,};
            size_t len = 128;
            grib_get_string(handle_, "nameOfFirstFixedSurface", name, &len);
            grib_context_log(c, GRIB_LOG_ERROR, "%s: First fixed surface: Type %ld (%s) requires a level", TITLE, stype, name);
            return GRIB_INVALID_KEY_VALUE;
        }
        // TODO(masn): generalise this. Need to check with DGOV
        if (stype == 1 && (!sfac_missing || !sval_missing)) {
            grib_context_log(c, GRIB_LOG_ERROR,
                "%s: First fixed surface: If type=%ld, scaleFactor/scaledValue keys must be set to missing", TITLE, stype);
            return GRIB_INVALID_KEY_VALUE;
        }
    }

    grib_get_long_internal(handle_, "typeOfSecondFixedSurface", &stype);
    sfac_missing = grib_is_missing(handle_, "scaleFactorOfSecondFixedSurface", &err);
    sval_missing = grib_is_missing(handle_, "scaledValueOfSecondFixedSurface", &err);
    if ((stype == 255 && !sfac_missing) || (stype == 255 && !sval_missing)) {
        grib_context_log(c, GRIB_LOG_ERROR,
            "%s: Second fixed surface: If the type of surface is missing so should its scaleFactor/scaledValue keys", TITLE);
        return GRIB_INVALID_KEY_VALUE;
    }
    if (sfac_missing != sval_missing) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Second fixed surface: If the scale factor is missing so should the scaled value and vice versa", TITLE);
        return GRIB_INVALID_KEY_VALUE;
    }
    if (stype != 255) {
        int required_val = codes_grib_surface_type_requires_value(edition_, stype, &err);
        if (err) return err;
        if (required_val && (sfac_missing || sval_missing)) {
            char name[128] = {0,};
            size_t len = 128;
            grib_get_string(handle_, "nameOfSecondFixedSurface", name, &len);
            grib_context_log(c, GRIB_LOG_ERROR, "%s: Second fixed surface: Type %ld (%s) requires a level", TITLE, stype, name);
            return GRIB_INVALID_KEY_VALUE;
        }
    }

    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_steps()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    char stepType[32] = {0,};
    size_t size = sizeof(stepType) / sizeof(*stepType);
    int err = grib_get_string_internal(handle_, "stepType", stepType, &size);
    if (err) return err;
    if (!STR_EQUAL(stepType, "instant")) {
        // check start and end steps
        long startStep = 0;
        err = grib_get_long_internal(handle_, "startStep", &startStep);
        if (err) return err;
        long endStep = 0;
        err = grib_get_long_internal(handle_, "endStep", &endStep);
        if (err) return err;
        if (startStep > endStep) {
            grib_context_log(handle_->context, GRIB_LOG_ERROR, "%s: Invalid step: startStep > endStep (%ld > %ld)", TITLE, startStep, endStep);
            return GRIB_WRONG_STEP;
        }
        // Accumulations, average etc
        // TODO(masn): Generalise this rule. Beware of index and stdanom where start == end!
        if ( STR_EQUAL(stepType, "accum") || STR_EQUAL(stepType, "avg") || STR_EQUAL(stepType, "min") || STR_EQUAL(stepType, "max") ) {
            if (startStep == endStep) {
                grib_context_log(handle_->context, GRIB_LOG_ERROR,
                    "%s: Invalid steps: stepType=%s but startStep=endStep", TITLE, stepType, startStep, endStep);
                return GRIB_WRONG_STEP;
            }
        }
    }
    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_section_numbers(const int* sec_nums, size_t N)
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    for (size_t i = 0; i < N; ++i) {
        char sname[16] = {0,};
        const int sec_num = sec_nums[i];
        snprintf(sname, sizeof(sname), "section_%d", sec_num);
        if (!grib_is_defined(handle_, sname)) {
            grib_context_log(handle_->context, GRIB_LOG_ERROR, "%s: GRIB%ld: Section %d is missing!", TITLE, edition_, sec_num);
            return GRIB_INVALID_MESSAGE;
        }
    }
    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_namespace_keys()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);
    const char* ns = "ls";
    grib_keys_iterator* kiter = grib_keys_iterator_new(handle_, /*flags=*/0, ns);
    if (!kiter) return GRIB_DECODING_ERROR;
    int count = 0;
    while (grib_keys_iterator_next(kiter)) {
        ++count;
        const char* name = grib_keys_iterator_get_name(kiter);
        int type = 0;
        grib_get_native_type(handle_, name, &type);
        if ( STR_EQUAL(grib_get_type_name(type), "unknown") ) {
            grib_context_log(handle_->context, GRIB_LOG_ERROR, "%s: Key %s has unknown type", TITLE, name);
            return GRIB_DECODING_ERROR;
        }
    }
    if (count == 0) {
        grib_context_log(handle_->context, GRIB_LOG_ERROR, "%s: Message has no keys in the '%s' namespace", TITLE, ns);
        return GRIB_DECODING_ERROR;
    }
    grib_keys_iterator_delete(kiter);
    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_sections()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);
    int err = 0;
    if (edition_ == 1) {
        const int grib1_section_nums[] = {1, 2, 4}; // section 3 is optional
        const size_t N = sizeof(grib1_section_nums) / sizeof(grib1_section_nums[0]);
        err = check_section_numbers(grib1_section_nums, N);
        if (err) return err;
    }
    else if (edition_ == 2) {
        const int grib2_section_nums[] = {1, 3, 4, 5, 6, 7, 8}; // section 2 is optional
        const size_t N = sizeof(grib2_section_nums) / sizeof(grib2_section_nums[0]);
        err = check_section_numbers(grib2_section_nums, N);
        if (err) return err;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_message_is_valid_t::check_parameter()
{
    grib_context_log(handle_->context, GRIB_LOG_DEBUG, "%s: %s", TITLE, __func__);

    int err = 0;
    long centre = 0;
    err = grib_get_long_internal(handle_, "centre", &centre);
    if (err) return err;
    if (centre == 98) {
        // make sure paramId is valid for ECMWF data
        long paramId = 0;
        err = grib_get_long_internal(handle_, "paramId", &paramId);
        if (err) return err;
        if (paramId == 0) {
            grib_context_log(handle_->context, GRIB_LOG_ERROR, "%s: Key paramId is 0 (parameter is not mapped)", TITLE);
            return GRIB_INVALID_MESSAGE;
        }
    }

    return err;
}

int grib_accessor_message_is_valid_t::unpack_long(long* val, size_t* len)
{
    typedef int (grib_accessor_message_is_valid_t::*check_func)();
    static check_func check_functions[] = {
        &grib_accessor_message_is_valid_t::check_sections,
        &grib_accessor_message_is_valid_t::check_field_values,
        &grib_accessor_message_is_valid_t::check_grid_pl_array,
        &grib_accessor_message_is_valid_t::check_geoiterator,
        &grib_accessor_message_is_valid_t::check_steps,
        &grib_accessor_message_is_valid_t::check_7777,
        &grib_accessor_message_is_valid_t::check_namespace_keys,
        &grib_accessor_message_is_valid_t::check_surface_keys,
        &grib_accessor_message_is_valid_t::check_parameter
    };

    int err = 0;
    handle_ = grib_handle_of_accessor(this);

    *len = 1;
    *val = 1; // Assume message is valid

    char product[32] = {0,};
    size_t size = sizeof(product) / sizeof(*product);
    err = grib_get_string(handle_, product_, product, &size);
    if (err) return err;

    if (!STR_EQUAL(product, "GRIB")) {
        grib_context_log(handle_->context, GRIB_LOG_ERROR, "Validity checks only implemented for GRIB messages");
        *val = 0;
        return GRIB_NOT_IMPLEMENTED;
    }

    err = grib_get_long_internal(handle_, "edition", &edition_);
    if (err) return err;

    const size_t num_check_functions = sizeof(check_functions) / sizeof(check_functions[0]);
    for (size_t i = 0; i < num_check_functions; ++i) {
        check_func cfunc = check_functions[i];
        err = (this->*cfunc)();
        if (err) {
            *val = 0; // check failed
        }
    }

    return GRIB_SUCCESS;
}
