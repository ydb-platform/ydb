/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_trim.h"

grib_accessor_trim_t _grib_accessor_trim{};
grib_accessor* grib_accessor_trim = &_grib_accessor_trim;

void grib_accessor_trim_t::init(const long l, grib_arguments* arg)
{
    grib_accessor_ascii_t::init(l, arg);
    int n          = 0;
    grib_handle* h = grib_handle_of_accessor(this);

    input_      = arg->get_name(h, n++);
    trim_left_  = arg->get_long(h, n++);
    trim_right_ = arg->get_long(h, n++);
    DEBUG_ASSERT(trim_left_ == 0 || trim_left_ == 1);
    DEBUG_ASSERT(trim_right_ == 0 || trim_right_ == 1);
}

int grib_accessor_trim_t::unpack_string(char* val, size_t* len)
{
    int err         = 0;
    grib_handle* h  = grib_handle_of_accessor(this);
    char input[256] = {0,};
    size_t size  = sizeof(input) / sizeof(*input);
    char* pInput = input;

    err = grib_get_string(h, input_, input, &size);
    if (err) return err;

    string_lrtrim(&pInput, trim_left_, trim_right_);
    snprintf(val, 1024, "%s", pInput);
    size = strlen(val);
    *len = size + 1;
    return GRIB_SUCCESS;
}

int grib_accessor_trim_t::pack_string(const char* val, size_t* len)
{
    char input[256] = {0,};

    size_t inputLen = 256;
    char buf[256]   = {0,};
    char* pBuf = NULL;
    int err;
    grib_handle* h                = grib_handle_of_accessor(this);
    grib_accessor* inputAccesstor = grib_find_accessor(h, input_);
    if (!inputAccesstor) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Accessor for %s not found", input_);
        return GRIB_NOT_FOUND;
    }

    if ((err = grib_get_string(h, input_, input, &inputLen)) != GRIB_SUCCESS)
        return err;

    snprintf(buf, sizeof(buf), "%s", val);
    pBuf = buf;
    string_lrtrim(&pBuf, trim_left_, trim_right_);

    return inputAccesstor->pack_string(pBuf, len);
}

size_t grib_accessor_trim_t::string_length()
{
    return 1024;
}
