/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_julian_date.h"

grib_accessor_julian_date_t _grib_accessor_julian_date{};
grib_accessor* grib_accessor_julian_date = &_grib_accessor_julian_date;

void grib_accessor_julian_date_t::init(const long l, grib_arguments* c)
{
    grib_accessor_double_t::init(l, c);
    grib_handle* h = grib_handle_of_accessor(this);
    const int arg_count = c->get_count();
    ECCODES_ASSERT( arg_count == 2 || arg_count == 6);

    int n = 0;
    year_  = c->get_name(h, n++);
    month_ = c->get_name(h, n++);

    day_ = c->get_name(h, n++);
    if (day_ == NULL) {
        hour_   = 0;
        minute_ = 0;
        second_ = 0;
        ymd_    = year_;
        hms_    = month_;
        year_   = 0;
        month_  = 0;
    }
    else {
        ymd_    = 0;
        hms_    = 0;
        hour_   = c->get_name(h, n++);
        minute_ = c->get_name(h, n++);
        second_ = c->get_name(h, n++);
    }
    sep_[0] = ' ';
    sep_[1] = 0;
    sep_[2] = 0;
    sep_[3] = 0;
    sep_[4] = 0;

    length_ = 0;
}

void grib_accessor_julian_date_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

int grib_accessor_julian_date_t::unpack_double(double* val, size_t* len)
{
    int ret = 0;
    long hour, minute, second;
    long year, month, day, ymd, hms;
    grib_handle* h = grib_handle_of_accessor(this);

    if (ymd_ == NULL) {
        ret = grib_get_long(h, year_, &year);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, month_, &month);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, day_, &day);
        if (ret != GRIB_SUCCESS)
            return ret;

        ret = grib_get_long(h, hour_, &hour);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, minute_, &minute);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, second_, &second);
        if (ret != GRIB_SUCCESS)
            return ret;
    }
    else {
        ret = grib_get_long(h, ymd_, &ymd);
        if (ret != GRIB_SUCCESS)
            return ret;
        year = ymd / 10000;
        ymd %= 10000;
        month = ymd / 100;
        ymd %= 100;
        day = ymd;

        ret = grib_get_long(h, hms_, &hms);
        if (ret != GRIB_SUCCESS)
            return ret;
        hour = hms / 10000;
        hms %= 10000;
        minute = hms / 100;
        hms %= 100;
        second = hms;
    }

    ret = grib_datetime_to_julian(year, month, day, hour, minute, second, val);

    return ret;
}

int grib_accessor_julian_date_t::pack_double(const double* val, size_t* len)
{
    int ret     = 0;
    long hour   = 0;
    long minute = 0;
    long second = 0;
    long ymd = 0, hms = 0;
    long year, month, day;
    grib_handle* h = grib_handle_of_accessor(this);

    ret = grib_julian_to_datetime(*val, &year, &month, &day, &hour, &minute, &second);
    if (ret != 0)
        return ret;

    if (ymd_ == NULL) {
        ret = grib_set_long(h, year_, year);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, month_, month);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, day_, day);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, hour_, hour);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, minute_, minute);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, second_, second);
        if (ret != 0)
            return ret;
    }
    else {
        ymd = year * 10000 + month * 100 + day;
        ret = grib_set_long(h, ymd_, ymd);
        if (ret != 0)
            return ret;

        hms = hour * 10000 + minute * 100 + second;
        ret = grib_set_long(h, hms_, hms);
        if (ret != 0)
            return ret;
    }

    return ret;
}

int grib_accessor_julian_date_t::unpack_string(char* val, size_t* len)
{
    int ret = 0;
    long hour, minute, second;
    long year, month, day, ymd, hms;
    char* sep      = sep_;
    grib_handle* h = grib_handle_of_accessor(this);

    if (*len < 15)
        return GRIB_BUFFER_TOO_SMALL;

    if (ymd_ == NULL) {
        ret = grib_get_long(h, year_, &year);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, month_, &month);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, day_, &day);
        if (ret != GRIB_SUCCESS)
            return ret;

        ret = grib_get_long(h, hour_, &hour);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, minute_, &minute);
        if (ret != GRIB_SUCCESS)
            return ret;
        ret = grib_get_long(h, second_, &second);
        if (ret != GRIB_SUCCESS)
            return ret;
    }
    else {
        ret = grib_get_long(h, ymd_, &ymd);
        if (ret != GRIB_SUCCESS)
            return ret;
        year = ymd / 10000;
        ymd %= 10000;
        month = ymd / 100;
        ymd %= 100;
        day = ymd;

        ret = grib_get_long(h, hms_, &hms);
        if (ret != GRIB_SUCCESS)
            return ret;

        // ECC-1999: If hms_ is passed in in 'hhmm' format
        // its largest value would be 2459
        if (hms < 2500) {
            hms *= 100; // convert to seconds e.g., 1205 -> 120500
        }

        hour = hms / 10000;
        hms %= 10000;
        minute = hms / 100;
        hms %= 100;
        second = hms;
    }

    if (sep[1] != 0 && sep[2] != 0 && sep[3] != 0 && sep[4] != 0) {
        snprintf(val, 1024, "%04ld%c%02ld%c%02ld%c%02ld%c%02ld%c%02ld",
                 year, sep[0], month, sep[1], day, sep[2], hour, sep[3], minute, sep[4], second);
    }
    else if (sep[0] != 0) {
        snprintf(val, 1024, "%04ld%02ld%02ld%c%02ld%02ld%02ld", year, month, day, sep[0], hour, minute, second);
    }
    else {
        snprintf(val, 1024, "%04ld%02ld%02ld%02ld%02ld%02ld", year, month, day, hour, minute, second);
    }
    *len = strlen(val) + 1;
    return ret;
}

int grib_accessor_julian_date_t::pack_string(const char* val, size_t* len)
{
    int ret = 0;
    long hour, minute, second;
    long year, month, day, ymd, hms;
    char* sep      = sep_;
    grib_handle* h = grib_handle_of_accessor(this);

    ret = sscanf(val, "%04ld%c%02ld%c%02ld%c%02ld%c%02ld%c%02ld",
                 &year, &sep[0], &month, &sep[1], &day, &sep[2], &hour, &sep[3], &minute, &sep[4], &second);
    if (ret != 11) {
        if (strlen(val) == 15) {
            ret = sscanf(val, "%04ld%02ld%02ld%c%02ld%02ld%02ld", &year, &month, &day, &sep[0], &hour, &minute, &second);
            if (ret != 7) {
                grib_context_log(h->context, GRIB_LOG_ERROR, " Wrong date time format. Please use \"YYYY-MM-DD hh:mm:ss\"");
                return GRIB_INVALID_KEY_VALUE;
            }
            sep[1] = 0;
            sep[2] = 0;
            sep[3] = 0;
            sep[4] = 0;
        }
        else {
            ret = sscanf(val, "%04ld%02ld%02ld%02ld%02ld%02ld", &year, &month, &day, &hour, &minute, &second);
            if (ret != 6) {
                grib_context_log(h->context, GRIB_LOG_ERROR, " Wrong date time format. Please use \"YYYY-MM-DD hh:mm:ss\"");
                return GRIB_INVALID_KEY_VALUE;
            }
            sep[0] = 0;
            sep[1] = 0;
            sep[2] = 0;
            sep[3] = 0;
            sep[4] = 0;
        }
    }

    if (ymd_ == NULL) {
        ret = grib_set_long(h, year_, year);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, month_, month);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, day_, day);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, hour_, hour);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, minute_, minute);
        if (ret != 0)
            return ret;
        ret = grib_set_long(h, second_, second);
        if (ret != 0)
            return ret;
    }
    else {
        ymd = year * 10000 + month * 100 + day;
        ret = grib_set_long(h, ymd_, ymd);
        if (ret != 0)
            return ret;

        hms = hour * 10000 + minute * 100 + second;
        ret = grib_set_long(h, hms_, hms);
        if (ret != 0)
            return ret;
    }

    return ret;
}

int grib_accessor_julian_date_t::unpack_long(long* val, size_t* len)
{
    grib_context_log(context_, GRIB_LOG_ERROR, " Cannot unpack %s as long", name_);
    return GRIB_NOT_IMPLEMENTED;
}
int grib_accessor_julian_date_t::pack_long(const long* v, size_t* len)
{
    grib_context_log(context_, GRIB_LOG_ERROR, " Cannot pack %s as long", name_);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_julian_date_t::pack_expression(grib_expression* e)
{
    size_t len        = 1;
    long lval         = 0;
    double dval       = 0;
    const char* cval  = NULL;
    int ret           = 0;
    grib_handle* hand = grib_handle_of_accessor(this);

    switch (e->native_type(hand)) {
        case GRIB_TYPE_LONG: {
            len = 1;
            ret = e->evaluate_long(hand, &lval);
            if (ret != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to set %s as long", name_);
                return ret;
            }
            /*if (hand->context->debug)
                    printf("ECCODES DEBUG grib_accessor_gen::pack_expression %s %ld\n", name_ ,lval);*/
            return pack_long(&lval, &len);
        }

        case GRIB_TYPE_DOUBLE: {
            len = 1;
            ret = e->evaluate_double(hand, &dval);
            /*if (hand->context->debug)
                    printf("ECCODES DEBUG grib_accessor_gen::pack_expression %s %g\n", name_ , dval);*/
            return pack_double(&dval, &len);
        }

        case GRIB_TYPE_STRING: {
            char tmp[1024];
            len  = sizeof(tmp);
            cval = e->evaluate_string(hand, tmp, &len, &ret);
            if (ret != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to set %s as string", name_);
                return ret;
            }
            len = strlen(cval);
            /*if (hand->context->debug)
                    printf("ECCODES DEBUG grib_accessor_gen::pack_expression %s %s\n", name_ , cval);*/
            return pack_string(cval, &len);
        }
    }

    return GRIB_NOT_IMPLEMENTED;
}
