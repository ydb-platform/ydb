/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_sprintf.h"

grib_accessor_sprintf_t _grib_accessor_sprintf{};
grib_accessor* grib_accessor_sprintf = &_grib_accessor_sprintf;

void grib_accessor_sprintf_t::init(const long l, grib_arguments* c)
{
    grib_accessor_ascii_t::init(l, c);
    args_ = c;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_sprintf_t::unpack_string(char* val, size_t* len)
{
    char result[1024];
    char tempBuffer[2048];
    char sres[1024];
    long ires            = 0;
    double dres          = 0;
    int i                = 0;
    size_t replen        = 1024;
    int ret              = GRIB_SUCCESS;
    int carg             = 0;
    int is_missing       = 0;
    const char* uname    = NULL;
    const char* tempname = NULL;
    size_t uname_len     = 0;

    uname = args_->get_string(grib_handle_of_accessor(this), carg++);
    snprintf(result, sizeof(result), "%s", "");
    uname_len = strlen(uname);

    for (i = 0; i < uname_len; i++) {
        if (uname[i] == '%') {
            int precision = 999;
            i++;
            if (uname[i] == '.') {
                char *theEnd = NULL, *start;
                start        = (char*)&(uname[++i]);
                precision    = strtol(start, &theEnd, 10);
                ECCODES_ASSERT(*theEnd != 0);
                while (uname[i] != *theEnd)
                    i++;
            }
            switch (uname[i]) {
                case 'd':
                    tempname = args_->get_name(grib_handle_of_accessor(this), carg++);

                    if ((ret = grib_get_long_internal(grib_handle_of_accessor(this), tempname, &ires)) != GRIB_SUCCESS)
                        return ret;
                    /* Bug GRIB-56: Check to see if the key is missing */
                    is_missing = grib_is_missing(grib_handle_of_accessor(this), tempname, &ret);
                    if (ret != GRIB_SUCCESS)
                        return ret;
                    if (is_missing) {
                        snprintf(tempBuffer, sizeof(tempBuffer), "%sMISSING", result);
                        strcpy(result, tempBuffer);
                    }
                    else {
                        /* Not missing so print it */
                        if (precision != 999) {
                            snprintf(tempBuffer, sizeof(tempBuffer), "%s%.*ld", result, precision, ires);
                            strcpy(result, tempBuffer);
                        }
                        else {
                            snprintf(tempBuffer, sizeof(tempBuffer), "%s%ld", result, ires);
                            strcpy(result, tempBuffer);
                        }
                    }
                    break;

                case 'g':
                    tempname = args_->get_name(grib_handle_of_accessor(this), carg++);
                    if ((ret = grib_get_double_internal(grib_handle_of_accessor(this), tempname, &dres)) != GRIB_SUCCESS)
                        return ret;
                    snprintf(tempBuffer, sizeof(tempBuffer), "%s%g", result, dres);
                    strcpy(result, tempBuffer);

                    break;

                case 's':
                    tempname = args_->get_name(grib_handle_of_accessor(this), carg++);
                    if ((ret = grib_get_string_internal(grib_handle_of_accessor(this), tempname, sres, &replen)) != GRIB_SUCCESS)
                        return ret;
                    snprintf(tempBuffer, sizeof(tempBuffer), "%s%s", result, sres);
                    strcpy(result, tempBuffer);
                    replen = 1024;
            }
        }
        else {
            snprintf(tempBuffer, sizeof(tempBuffer), "%s%c", result, uname[i]);
            strcpy(result, tempBuffer);
        }
    }

    replen = strlen(result) + 1;

    if (*len < replen) {
        *len = replen;
        return GRIB_ARRAY_TOO_SMALL;
    }
    *len = replen;

    snprintf(val, 1024, "%s", result);

    return GRIB_SUCCESS;
}

int grib_accessor_sprintf_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_sprintf_t::string_length()
{
    return 1024;
}
