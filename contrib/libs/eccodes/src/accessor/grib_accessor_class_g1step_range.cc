/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_g1step_range.h"

grib_accessor_g1step_range_t _grib_accessor_g1step_range{};
grib_accessor* grib_accessor_g1step_range = &_grib_accessor_g1step_range;

void grib_accessor_g1step_range_t::init(const long l, grib_arguments* c)
{
    grib_accessor_abstract_long_vector_t::init(l, c);
    grib_handle* h      = grib_handle_of_accessor(this);
    int n               = 0;
    p1_                 = c->get_name(h, n++);
    p2_                 = c->get_name(h, n++);
    timeRangeIndicator_ = c->get_name(h, n++);
    unit_               = c->get_name(h, n++);
    step_unit_          = c->get_name(h, n++);
    stepType_           = c->get_name(h, n++);
    patch_fp_precip_    = c->get_name(h, n++);
    error_on_units_     = 1;

    number_of_elements_ = 2;
    v_                  = (long*)grib_context_malloc_clear(h->context,
                                                           sizeof(long) * number_of_elements_);
    pack_index_         = -1;
    dirty_              = 1;

    length_ = 0;
}

void grib_accessor_g1step_range_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

static const int u2s1[] = {
    60,                   /* (0) minutes */
    3600,                 /* (1) hour    */
    86400,                /* (2) day     */
    2592000,              /* (3) month     */
    -1,                   /* (4) */
    -1,                   /* (5) */
    -1,                   /* (6) */
    -1,                   /* (7) */
    -1,                   /* (8) */
    -1,                   /* (9) */
    10800,                /* (10) 3 hours */
    21600,                /* (11) 6 hours */
    43200,                /* (12) 12 hours */
    900,                  /* (13) 15 minutes  */
    1800,                 /* (14) 30 minutes */
    1 /* (15) seconds  */ /* See ECC-316 */
};

static const int units_index[] = {
    1, 0, 10, 11, 12, 2, 0, 13, 14, 15
};

static const int u2s[] = {
    60,      /* (0) minutes */
    3600,    /* (1) hour    */
    86400,   /* (2) day     */
    2592000, /* (3) month     */
    -1,      /* (4) */
    -1,      /* (5) */
    -1,      /* (6) */
    -1,      /* (7) */
    -1,      /* (8) */
    -1,      /* (9) */
    10800,   /* (10) 3 hours */
    21600,   /* (11) 6 hours */
    43200,   /* (12) 12 hours */
    1,       /* (13) seconds  */
    900,     /* (14) 15 minutes  */
    1800     /* (15) 30 minutes  */
};

int grib_accessor_g1step_range_t::grib_g1_step_get_steps(long* start, long* theEnd)
{
    int err                            = 0;
    long p1 = 0, p2 = 0, unit = 0, timeRangeIndicator = 0, timeRangeIndicatorFromStepRange = 0;
    long step_unit    = 1;
    char stepType[20] = {0,};
    size_t stepTypeLen = 20;
    long newstart, newend;
    int factor = 1;
    long u2sf, u2sf_step_unit;
    grib_handle* hand = grib_handle_of_accessor(this);

    if (step_unit_ != NULL)
        grib_get_long_internal(hand, step_unit_, &step_unit);

    if (err != GRIB_SUCCESS)
        return err;

    err = grib_get_long_internal(hand, unit_, &unit);
    if (err)
        return err;
    if (unit == 254) {
        unit = 15; /* See ECC-316: WMO says 254 is for 'seconds' but we use 15! */
    }

    err = grib_get_long_internal(hand, p1_, &p1);
    if (err)
        return err;

    err = grib_get_long_internal(hand, p2_, &p2);
    if (err)
        return err;

    err = grib_get_long_internal(hand, timeRangeIndicator_, &timeRangeIndicator);
    if (err)
        return err;

    /* TODO move to the def file */
    err = grib_get_long(hand, "timeRangeIndicatorFromStepRange", &timeRangeIndicatorFromStepRange);
    if (err) return err;

    if (timeRangeIndicatorFromStepRange == 10)
        timeRangeIndicator = timeRangeIndicatorFromStepRange;

    if (stepType_) {
        err = grib_get_string_internal(hand, stepType_, stepType, &stepTypeLen);
        if (err)
            return err;
    }
    else
        snprintf(stepType, sizeof(stepType), "unknown");

    *start  = p1;
    *theEnd = p2;

    if (timeRangeIndicator == 10)
        *start = *theEnd = (p1 << 8) | (p2 << 0);
    else if (!strcmp(stepType, "instant"))
        *start = *theEnd = p1;
    else if (!strcmp(stepType, "accum") && timeRangeIndicator == 0) {
        *start  = 0;
        *theEnd = p1;
    }

    if (u2s1[unit] == u2s[step_unit] || (*start == 0 && *theEnd == 0))
        return 0;

    newstart = (*start) * u2s1[unit];
    newend   = (*theEnd) * u2s1[unit];

    if (newstart < 0 || newend < 0) {
        factor = 60;
        u2sf   = u2s1[unit] / factor;
        if (u2s1[unit] % factor)
            return GRIB_DECODING_ERROR;
        newstart       = (*start) * u2sf;
        newend         = (*theEnd) * u2sf;
        u2sf_step_unit = u2s[step_unit] / factor;
        if (u2s[step_unit] % factor)
            return GRIB_DECODING_ERROR;
    }
    else {
        u2sf_step_unit = u2s[step_unit];
    }

    if (newstart % u2sf_step_unit != 0 || newend % u2sf_step_unit != 0) {
        return GRIB_DECODING_ERROR;
    }
    else {
        *start  = newstart / u2sf_step_unit;
        *theEnd = newend / u2sf_step_unit;
    }

    return 0;
}

int grib_accessor_g1step_range_t::unpack_string(char* val, size_t* len)
{
    char buf[100];
    size_t size = 0;
    long start = 0, theEnd = 0;
    long timeRangeIndicator = 0;
    long unit;
    int err           = 0;
    char stepType[20] = {0,};
    size_t stepTypeLen = 20;
    grib_handle* hand  = grib_handle_of_accessor(this);

    if ((err = grib_g1_step_get_steps(&start, &theEnd)) != GRIB_SUCCESS) {
        size_t step_unit_string_len = 10;
        char step_unit_string[10];

        if (step_unit_ != NULL)
            grib_get_string(hand, step_unit_, step_unit_string, &step_unit_string_len);
        else
            snprintf(step_unit_string, sizeof(step_unit_string), "h");

        if (error_on_units_) {
            grib_get_long_internal(hand, unit_, &unit);
            if (unit == 254) {
                unit = 15; /* See ECC-316 */
            }
            grib_set_long_internal(hand, step_unit_, unit);
            grib_context_log(context_, GRIB_LOG_ERROR,
                             "unable to represent the step in %s\n                    Hint: try changing the step units",
                             step_unit_string);
        }
        return err;
    }

    err = grib_get_long_internal(hand, timeRangeIndicator_, &timeRangeIndicator);
    if (err)
        return err;

    if (stepType_) {
        err = grib_get_string_internal(hand, stepType_, stepType, &stepTypeLen);
        if (err)
            return err;
    }
    else
        snprintf(stepType, sizeof(stepType), "unknown");

    /* Patch for old forecast probabilities */
    if (patch_fp_precip_) {
        start += 24;
    }

    if (strcmp(stepType, "instant") == 0) {
        snprintf(buf, sizeof(buf), "%ld", start);
    }
    else if ((strcmp(stepType, "avgfc") == 0) ||
             (strcmp(stepType, "avgua") == 0) ||
             (strcmp(stepType, "avgia") == 0) ||
             (strcmp(stepType, "varins") == 0)) {
        snprintf(buf, sizeof(buf), "%ld", start);
    }
    else if (
        (strcmp(stepType, "accum") == 0) ||
        (strcmp(stepType, "avg") == 0) ||
        (strcmp(stepType, "min") == 0) ||
        (strcmp(stepType, "max") == 0) ||
        (strcmp(stepType, "rms") == 0) ||
        (strcmp(stepType, "diff") == 0) ||
        (strcmp(stepType, "avgas") == 0) ||
        (strcmp(stepType, "avgad") == 0) ||
        (strcmp(stepType, "avgid") == 0) ||
        (strcmp(stepType, "varas") == 0) ||
        (strcmp(stepType, "varad") == 0)) {
        if (start == theEnd) {
            snprintf(buf, sizeof(buf), "%ld", theEnd);
        }
        else {
            snprintf(buf, sizeof(buf), "%ld-%ld", start, theEnd);
        }
    }
    else {
        grib_context_log(context_, GRIB_LOG_ERROR, "Unknown stepType=[%s] timeRangeIndicator=[%ld]", stepType, timeRangeIndicator);
        return GRIB_NOT_IMPLEMENTED;
    }

    size = strlen(buf) + 1;

    if (*len < size)
        return GRIB_ARRAY_TOO_SMALL;

    *len = size;

    memcpy(val, buf, size);

    return GRIB_SUCCESS;
}

static int grib_g1_step_apply_units(
    const long* start, const long* theEnd, const long* step_unit,
    long* P1, long* P2, long* unit,
    const int max, const int instant)
{
    int j = 0;
    long start_sec, end_sec;
    int index     = 0;
    int max_index = sizeof(units_index) / sizeof(*units_index);

    while (*unit != units_index[index] && index != max_index)
        index++;

    start_sec = *start * u2s[*step_unit];
    *P2       = 0;

    if (instant) {
        *unit = units_index[0];
        for (j = index; j < max_index; j++) {
            if (start_sec % u2s1[*unit] == 0 &&
                (*P1 = start_sec / u2s1[*unit]) <= max)
                return 0;
            *unit = units_index[j];
        }
        for (j = 0; j < index; j++) {
            if (start_sec % u2s1[*unit] == 0 &&
                (*P1 = start_sec / u2s1[*unit]) <= max)
                return 0;
            *unit = units_index[j];
        }
    }
    else {
        end_sec = *theEnd * u2s[*step_unit];
        *unit   = units_index[0];
        for (j = index; j < max_index; j++) {
            if (start_sec % u2s1[*unit] == 0 &&
                end_sec % u2s1[*unit] == 0 &&
                (*P1 = start_sec / u2s1[*unit]) <= max &&
                (*P2 = end_sec / u2s1[*unit]) <= max)
                return 0;
            *unit = units_index[j];
        }
        for (j = 0; j < index; j++) {
            if (start_sec % u2s1[*unit] == 0 &&
                end_sec % u2s1[*unit] == 0 &&
                (*P1 = start_sec / u2s1[*unit]) <= max &&
                (*P2 = end_sec / u2s1[*unit]) <= max)
                return 0;
            *unit = units_index[j];
        }
    }

    return GRIB_WRONG_STEP;
}

int grib_accessor_g1step_range_t::pack_string(const char* val, size_t* len)
{
    grib_handle* h          = grib_handle_of_accessor(this);
    long timeRangeIndicator = 0, P1 = 0, P2 = 0;
    long start = 0, theEnd = -1, unit = 0, ounit = 0, step_unit = 1;
    int ret = 0;
    long end_sec, start_sec;
    char *p = NULL, *q = NULL;
    int instant       = 0;
    char stepType[20] = {0,};
    size_t stepTypeLen = 20;

    if (stepType_) {
        ret = grib_get_string_internal(h, stepType_, stepType, &stepTypeLen);
        if (ret)
            return ret;
    }
    else
        snprintf(stepType, sizeof(stepType), "unknown");

    if ((ret = grib_set_long_internal(h, "timeRangeIndicatorFromStepRange", -1)))
        return ret;

    /* don't change timeRangeIndicator when setting step EXCEPT IF instant*/
    if ((ret = grib_get_long_internal(h, timeRangeIndicator_, &timeRangeIndicator)))
        return ret;

    instant = (strcmp(stepType, "instant") == 0) ? 1 : 0;

    if ((ret = grib_get_long_internal(h, unit_, &unit)))
        return ret;
    if (unit == 254) {
        unit = 15; /* See ECC-316 */
    }

    if (step_unit_ != NULL && (ret = grib_get_long_internal(h, step_unit_, &step_unit)))
        return ret;

    ounit = unit;

    start  = strtol(val, &p, 10);
    theEnd = start;
    if (*p != 0)
        theEnd = strtol(++p, &q, 10);

    if (start == 0 && theEnd == 0) {
        if ((ret = grib_set_long_internal(h, p1_, start)) != GRIB_SUCCESS)
            return ret;
        ret = grib_set_long_internal(h, p2_, theEnd);
        return ret;
    }
    end_sec   = theEnd * u2s[step_unit];
    start_sec = start * u2s[step_unit];

    if ((end_sec > 918000 || start_sec > 918000) &&
        h->context->gribex_mode_on && instant) {
        timeRangeIndicator = 10;
        if ((ret = grib_set_long_internal(h, timeRangeIndicator_, 10)))
            return ret;
        /* TODO move to the def file*/
        if ((ret = grib_set_long_internal(h, "timeRangeIndicatorFromStepRange", 10)))
            return ret;
    }

    if (timeRangeIndicator == 10) {
        /*
         * timeRangeIndicator = 10 means 'P1 occupies octets 19 and 20' i.e. 16 bits
         */
        long off                   = 0;
        grib_accessor* p1_accessor = NULL;
        if (theEnd != start && !h->context->gribex_mode_on) {
            if (h->context->gribex_mode_on == 0) {
                grib_context_log(h->context, GRIB_LOG_ERROR,
                                 "Unable to set %s: end must be equal to start when timeRangeIndicator=10",
                                 name_);
                return GRIB_WRONG_STEP;
            }
            else
                start = theEnd;
        }
        if ((ret = grib_g1_step_apply_units(&start, &theEnd, &step_unit, &P1, &P2, &unit, 65535, instant)) != GRIB_SUCCESS) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "unable to find units to set %s=%s", name_, val);
            return ret;
        }

        p1_accessor = grib_find_accessor(grib_handle_of_accessor(this), p1_);
        if (p1_accessor == NULL) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "unable to find accessor %s", p1_);
            return GRIB_NOT_FOUND;
        }
        off = p1_accessor->offset_ * 8;
        /* Note: here we assume the key P2 is one octet and immediately follows P1. Hence 16 bits */

        ret = grib_encode_unsigned_long(grib_handle_of_accessor(this)->buffer->data, P1, &off, 16);
        if (ret != 0)
            return ret;

        if (h->context->debug) {
            long dp1, dp2;
            grib_get_long(h, p1_, &dp1);
            grib_get_long(h, p2_, &dp2);
            fprintf(stderr, "ECCODES DEBUG pack_string: P1=%ld P2=%ld (as two octets => %ld)\n", dp1, dp2, P1);
        }

        if (ounit != unit)
            ret = grib_set_long_internal(h, unit_, unit);

        return ret;
    }

    if ((ret = grib_g1_step_apply_units(&start, &theEnd, &step_unit, &P1, &P2, &unit, 255, instant)) != GRIB_SUCCESS) {
        if (instant || h->context->gribex_mode_on) {
            long off                   = 0;
            grib_accessor* p1_accessor = NULL;
            if ((ret = grib_set_long_internal(h, timeRangeIndicator_, 10)))
                return ret;
            /* TODO move to the def file*/
            if ((ret = grib_set_long_internal(h, "timeRangeIndicatorFromStepRange", 10)))
                return ret;
            if (theEnd != start && !h->context->gribex_mode_on) {
                grib_context_log(h->context, GRIB_LOG_ERROR,
                                 "Unable to set %s: end must be equal to start when timeRangeIndicator=10",
                                 name_);
                return GRIB_WRONG_STEP;
            }
            else
                start = theEnd;

            if ((ret = grib_g1_step_apply_units(&start, &theEnd, &step_unit, &P1, &P2, &unit, 65535, instant)) != GRIB_SUCCESS) {
                grib_context_log(h->context, GRIB_LOG_ERROR, "unable to find units to set %s=%s", name_, val);
                return ret;
            }

            p1_accessor = grib_find_accessor(grib_handle_of_accessor(this), p1_);
            if (p1_accessor == NULL) {
                grib_context_log(h->context, GRIB_LOG_ERROR, "unable to find accessor %s", p1_);
                return GRIB_NOT_FOUND;
            }
            off = p1_accessor->offset_ * 8;
            /* Note:  case for timeRangeIndicator of 10
             * We assume the key P2 is one octet and immediately follows P1. Hence 16 bits
             */
            ret = grib_encode_unsigned_long(grib_handle_of_accessor(this)->buffer->data, P1, &off, 16);
            if (ret != 0)
                return ret;

            if (h->context->debug) {
                long dp1, dp2;
                grib_get_long(h, p1_, &dp1);
                grib_get_long(h, p2_, &dp2);
                fprintf(stderr, "ECCODES DEBUG pack_string: P1=%ld P2=%ld (as two octets => %ld)\n", dp1, dp2, P1);
            }

            if (ounit != unit)
                ret = grib_set_long_internal(h, unit_, unit);
        }

        if (ret == GRIB_WRONG_STEP) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "Failed to set %s=%s: Keys P1 and P2 are one octet each (Range 0 to 255)", name_, val);
        }
        return ret;
    }

    if (ounit != unit)
        if ((ret = grib_set_long_internal(h, unit_, unit)) != GRIB_SUCCESS)
            return ret;

    if ((ret = grib_set_long_internal(h, p1_, P1)) != GRIB_SUCCESS)
        return ret;
    if ((ret = grib_set_long_internal(h, p2_, P2)) != GRIB_SUCCESS)
        return ret;

    v_[0]  = start;
    v_[1]  = theEnd;
    dirty_ = 0;

    return 0;
}

int grib_accessor_g1step_range_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_g1step_range_t::string_length()
{
    return 255;
}

int grib_accessor_g1step_range_t::pack_long(const long* val, size_t* len)
{
    char buff[256];
    size_t bufflen    = 100;
    char sval[100]    = { 0 };
    char* p           = sval;
    size_t svallen    = 100;
    char stepType[20] = {0,};
    size_t stepTypeLen = 20;
    long step_unit     = 0;
    int err            = 0;

    if (stepType_) {
        err = grib_get_string_internal(grib_handle_of_accessor(this), stepType_, stepType, &stepTypeLen);
        if (err)
            return err;
    }
    else
        snprintf(stepType, sizeof(stepType), "unknown");

    if (step_unit_ != NULL && (err = grib_get_long_internal(grib_handle_of_accessor(this), step_unit_, &step_unit)))
        return err;

    switch (pack_index_) {
        case -1:
            pack_index_ = -1;
            snprintf(buff, sizeof(buff), "%ld", *val);
            return pack_string(buff, &bufflen);
        case 0:
            pack_index_     = -1;
            error_on_units_ = 0;
            unpack_string(sval, &svallen);
            error_on_units_ = 1;
            while (*p != '-' && *p != '\0')
                p++;
            if (*p == '-') {
                snprintf(buff, sizeof(buff), "%ld-%s", *val, ++p);
            }
            else {
                if (strcmp(stepType, "instant") && strcmp(stepType, "avgd")) {
                    snprintf(buff, sizeof(buff), "%ld-%s", *val, sval);
                }
                else {
                    snprintf(buff, sizeof(buff), "%ld", *val);
                }
            }
            return pack_string(buff, &bufflen);
        case 1:
            pack_index_     = -1;
            error_on_units_ = 0;
            unpack_string(sval, &svallen);
            error_on_units_ = 1;
            while (*p != '-' && *p != '\0')
                p++;
            if (*p == '-') {
                *p = '\0';
                snprintf(buff, sizeof(buff), "%s-%ld", sval, *val);
            }
            else {
                if (strcmp(stepType, "instant") && strcmp(stepType, "avgd")) {
                    snprintf(buff, sizeof(buff), "%s-%ld", sval, *val);
                }
                else {
                    snprintf(buff, sizeof(buff), "%ld", *val);
                }
            }
            return pack_string(buff, &bufflen);
        default:
            ECCODES_ASSERT(pack_index_ < 2);
            break;
    }

    return GRIB_INTERNAL_ERROR;
}

int grib_accessor_g1step_range_t::unpack_long(long* val, size_t* len)
{
    char buff[100];
    size_t bufflen = 100;
    long start, theEnd;
    char* p = buff;
    char* q = NULL;
    int err = 0;

    /*TODO implement dirty*/
    if ((err = unpack_string(buff, &bufflen)) != GRIB_SUCCESS)
        return err;

    start  = strtol(buff, &p, 10);
    theEnd = start;
    if (*p != 0)
        theEnd = strtol(++p, &q, 10);

    if (pack_index_ == 1)
        *val = start;
    else
        *val = theEnd;

    v_[0]  = start;
    v_[1]  = theEnd;
    dirty_ = 0;

    if (start > theEnd) {
        // For now just a warning. Will later change to an error
        fprintf(stderr, "ECCODES WARNING :  endStep < startStep (%ld < %ld)\n", theEnd, start);
        //grib_context_log(context_, GRIB_LOG_ERROR, "endStep < startStep (%ld < %ld)", theEnd, start);
        //return GRIB_WRONG_STEP;
    }
    return GRIB_SUCCESS;
}

long grib_accessor_g1step_range_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

void grib_accessor_g1step_range_t::destroy(grib_context* c)
{
    grib_context_free(c, v_);
    grib_accessor_abstract_long_vector_t::destroy(c);
}
