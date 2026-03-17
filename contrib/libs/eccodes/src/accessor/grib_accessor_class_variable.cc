/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_variable.h"
#include <limits.h>

grib_accessor_variable_t _grib_accessor_variable{};
grib_accessor* grib_accessor_variable = &_grib_accessor_variable;

//
// This accessor is used for:
//  constant
//  transient
//

#define MAX_VARIABLE_STRING_LENGTH 255

void grib_accessor_variable_t::init(const long length, grib_arguments* args)
{
    grib_accessor_gen_t::init(length, args);

    grib_handle* hand           = grib_handle_of_accessor(this);
    grib_expression* expression = nullptr;
    if (args) expression = args->get_expression(hand, 0);
    const char* p               = 0;
    size_t len                  = 1;
    long l                      = 0;
    int ret                     = 0;
    double d                    = 0;
    cname_                      = NULL;
    dval_                       = 0;
    fval_                       = 0;
    cval_                       = NULL;
    type_                       = GRIB_TYPE_UNDEFINED;

    length_ = 0;
    if (type_ == GRIB_TYPE_UNDEFINED && expression) {
        type_ = expression->native_type(hand);

        switch (type_) {
            case GRIB_TYPE_DOUBLE:
                expression->evaluate_double(hand, &d);
                pack_double(&d, &len);
                break;

            case GRIB_TYPE_LONG:
                expression->evaluate_long(hand, &l);
                pack_long(&l, &len);
                break;

            default: {
                char tmp[1024];
                len = sizeof(tmp);
                p   = expression->evaluate_string(hand, tmp, &len, &ret);
                if (ret != GRIB_SUCCESS) {
                    grib_context_log(context_, GRIB_LOG_ERROR, "Unable to evaluate %s as string: %s",
                                     name_, grib_get_error_message(ret));
                    return;
                }
                len = strlen(p) + 1;
                pack_string(p, &len);
                break;
            }
        }
    }
}

void grib_accessor_variable_t::accessor_variable_set_type(int type)
{
    type_ = type;
}

void grib_accessor_variable_t::dump(eccodes::Dumper* dumper)
{
    switch (type_) {
        case GRIB_TYPE_DOUBLE:
            dumper->dump_double(this, NULL);
            break;

        case GRIB_TYPE_LONG:
            dumper->dump_long(this, NULL);
            break;

        default:
            dumper->dump_string(this, NULL);
            break;
    }
}

int grib_accessor_variable_t::pack_double(const double* val, size_t* len)
{
    const double dval = *val;

    if (*len != 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains 1 value", name_);
        *len = 1;
        return GRIB_ARRAY_TOO_SMALL;
    }

    // if (std::isnan(dval)) {
    //     grib_context_log(a->context, GRIB_LOG_ERROR, "%s: Invalid number for %s: %g", __func__, name_ , dval);
    //     return GRIB_INVALID_ARGUMENT;
    // }

    dval_ = dval;
    if (dval < (double)LONG_MIN || dval > (double)LONG_MAX)
        type_ = GRIB_TYPE_DOUBLE;
    else
        type_ = ((long)dval == dval) ? GRIB_TYPE_LONG : GRIB_TYPE_DOUBLE;

    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::pack_float(const float* val, size_t* len)
{
    const double fval = *val;

    if (*len != 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains 1 value", name_);
        *len = 1;
        return GRIB_ARRAY_TOO_SMALL;
    }

    fval_ = fval;
    if (fval < (float)LONG_MIN || fval > (float)LONG_MAX)
        type_ = GRIB_TYPE_DOUBLE;
    else
        type_ = ((long)fval == fval) ? GRIB_TYPE_LONG : GRIB_TYPE_DOUBLE;

    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::pack_long(const long* val, size_t* len)
{
    if (*len != 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s it contains 1 value", name_);
        *len = 1;
        return GRIB_ARRAY_TOO_SMALL;
    }

    dval_ = *val;
    fval_ = *val;
    type_ = GRIB_TYPE_LONG;

    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::unpack_double(double* val, size_t* len)
{
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains %d values", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    *val = dval_;
    *len = 1;
    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::unpack_float(float* val, size_t* len)
{
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s, it contains %d values", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    *val = fval_;
    *len = 1;
    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::unpack_long(long* val, size_t* len)
{
    if (*len < 1) {
        grib_context_log(context_, GRIB_LOG_ERROR, "Wrong size for %s it contains %d values ", name_, 1);
        *len = 0;
        return GRIB_ARRAY_TOO_SMALL;
    }
    *val = (long)dval_;
    *len = 1;
    return GRIB_SUCCESS;
}

long grib_accessor_variable_t::get_native_type()
{
    return type_;
}

void grib_accessor_variable_t::destroy(grib_context* c)
{
    int i = 0;

    grib_context_free(c, cval_);
    if (cname_)
        grib_context_free(c, cname_); /* ECC-765 */

    /* Note: BUFR operator descriptors are variables and have attributes so need to free them */
    while (i < MAX_ACCESSOR_ATTRIBUTES && attributes_[i]) {
        attributes_[i]->destroy(c);
        delete attributes_[i];
        attributes_[i] = nullptr;
        ++i;
    }

    grib_accessor_gen_t::destroy(c);
}

int grib_accessor_variable_t::unpack_string(char* val, size_t* len)
{
    char buf[80];
    char* p     = buf;
    size_t slen = 0;

    if (type_ == GRIB_TYPE_STRING) {
        p = cval_;
    }
    else {
        snprintf(p, 64, "%g", dval_);
    }

    slen = strlen(p) + 1;
    if (*len < slen) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, slen, *len);
        *len = slen;
        return GRIB_BUFFER_TOO_SMALL;
    }
    strcpy(val, p);
    *len = slen;

    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::pack_string(const char* val, size_t* len)
{
    const grib_context* c = context_;

    grib_context_free(c, cval_);
    cval_  = grib_context_strdup(c, val);
    dval_  = atof(val);
    fval_  = atof(val);
    type_  = GRIB_TYPE_STRING;
    cname_ = NULL;
    return GRIB_SUCCESS;
}

int grib_accessor_variable_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_variable_t::string_length()
{
    if (type_ == GRIB_TYPE_STRING)
        return strlen(cval_);
    else
        return MAX_VARIABLE_STRING_LENGTH;
}

long grib_accessor_variable_t::byte_count()
{
    return length_;
}

int grib_accessor_variable_t::compare(grib_accessor* b)
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

grib_accessor* grib_accessor_variable_t::make_clone(grib_section* s, int* err)
{
    grib_accessor* the_clone                   = NULL;
    grib_accessor_variable_t* variableAccessor = NULL;
    grib_action creator;
    creator.op_         = (char*)"variable";
    creator.name_space_ = (char*)"";
    creator.set_        = 0;

    creator.name_             = grib_context_strdup(context_, name_);
    the_clone                = grib_accessor_factory(s, &creator, 0, NULL);
    the_clone->parent_       = NULL;
    the_clone->h_            = s->h;
    the_clone->flags_        = flags_;
    variableAccessor         = (grib_accessor_variable_t*)the_clone;
    variableAccessor->cname_ = creator.name_; /* ECC-765: Store for later freeing memory */

    *err                    = 0;
    variableAccessor->type_ = type_;
    if (type_ == GRIB_TYPE_STRING && cval_ != NULL) {
        variableAccessor->cval_ = grib_context_strdup(context_, cval_);
    }
    else {
        variableAccessor->dval_ = dval_;
        variableAccessor->fval_ = fval_;
    }

    return the_clone;
}
