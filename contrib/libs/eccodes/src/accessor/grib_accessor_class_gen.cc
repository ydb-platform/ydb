/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_gen.h"
#include "grib_accessor_class.h"
#include <stdexcept>

grib_accessor_gen_t _grib_accessor_gen = grib_accessor_gen_t{};
grib_accessor* grib_accessor_gen       = &_grib_accessor_gen;

void grib_accessor_gen_t::init_accessor(const long len, grib_arguments* args)
{
    init(len, args);
}

void grib_accessor_gen_t::init(const long len, grib_arguments* param)
{
    grib_action* act = (grib_action*)(creator_);
    if (flags_ & GRIB_ACCESSOR_FLAG_TRANSIENT) {
        length_ = 0;
        if (!vvalue_)
            vvalue_ = (grib_virtual_value*)grib_context_malloc_clear(context_, sizeof(grib_virtual_value));
        vvalue_->type   = get_native_type();
        vvalue_->length = len;
        if (act->default_value_ != NULL) {
            const char* p = 0;
            size_t s_len  = 1;
            long l;
            int ret = 0;
            double d;
            char tmp[1024];
            grib_expression* expression = act->default_value_->get_expression(grib_handle_of_accessor(this), 0);
            int type                    = expression->native_type(grib_handle_of_accessor(this));
            switch (type) {
                    // TODO(maee): add single-precision case

                case GRIB_TYPE_DOUBLE:
                    expression->evaluate_double(grib_handle_of_accessor(this), &d);
                    pack_double(&d, &s_len);
                    break;

                case GRIB_TYPE_LONG:
                    expression->evaluate_long(grib_handle_of_accessor(this), &l);
                    pack_long(&l, &s_len);
                    break;

                default:
                    s_len = sizeof(tmp);
                    p     = expression->evaluate_string(grib_handle_of_accessor(this), tmp, &s_len, &ret);
                    if (ret != GRIB_SUCCESS) {
                        grib_context_log(context_, GRIB_LOG_ERROR, "Unable to evaluate %s as string", name_);
                        ECCODES_ASSERT(0);
                    }
                    s_len = strlen(p) + 1;
                    pack_string(p, &s_len);
                    break;
            }
        }
    }
    else {
        length_ = len;
    }
}

void grib_accessor_gen_t::dump(eccodes::Dumper* dumper)
{
    const int type = get_native_type();
    switch (type) {
        case GRIB_TYPE_STRING:
            dumper->dump_string(this, NULL);
            break;
        case GRIB_TYPE_DOUBLE:
            dumper->dump_double(this, NULL);
            break;
        case GRIB_TYPE_LONG:
            dumper->dump_long(this, NULL);
            break;
        default:
            dumper->dump_bytes(this, NULL);
    }
}

long grib_accessor_gen_t::next_offset()
{
    return offset_ + length_;
}

int grib_accessor_gen_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_gen_t::string_length()
{
    return 1024;
}

long grib_accessor_gen_t::byte_count()
{
    return length_;
}

long grib_accessor_gen_t::get_native_type()
{
    grib_context_log(context_,
                     GRIB_LOG_ERROR,
                     "Accessor %s [%s] must implement 'get_native_type'",
                     name_,
                     class_name_);
    return GRIB_TYPE_UNDEFINED;
}

long grib_accessor_gen_t::byte_offset()
{
    return offset_;
}

int grib_accessor_gen_t::unpack_bytes(unsigned char* val, size_t* len)
{
    const unsigned char* buf = grib_handle_of_accessor(this)->buffer->data;
    const long length        = byte_count();
    const long offset        = byte_offset();

    if (*len < length) {
        grib_context_log(context_,
                         GRIB_LOG_ERROR,
                         "Wrong size for %s, it is %ld bytes long",
                         name_,
                         length);
        *len = length;
        return GRIB_ARRAY_TOO_SMALL;
    }

    memcpy(val, buf + offset, length);
    *len = length;

    return GRIB_SUCCESS;
}

int grib_accessor_gen_t::clear()
{
    unsigned char* buf = grib_handle_of_accessor(this)->buffer->data;
    const long length  = byte_count();
    const long offset  = byte_offset();

    memset(buf + offset, 0, length);

    return GRIB_SUCCESS;
}

int grib_accessor_gen_t::unpack_long(long* v, size_t* len)
{
    is_overridden_[UNPACK_LONG] = 0;
    int type                    = GRIB_TYPE_UNDEFINED;
    if (is_overridden_[UNPACK_DOUBLE] == 1) {
        double val = 0.0;
        size_t l   = 1;
        unpack_double(&val, &l);
        if (is_overridden_[UNPACK_DOUBLE] == 1) {
            if (val == GRIB_MISSING_DOUBLE)
                *v = GRIB_MISSING_LONG;
            else
                *v = (long)val;
            grib_context_log(
                context_, GRIB_LOG_DEBUG, "Casting double %s to long", name_);
            return GRIB_SUCCESS;
        }
    }

    if (is_overridden_[UNPACK_STRING] == 1) {
        char val[1024];
        size_t l   = sizeof(val);
        char* last = NULL;
        unpack_string(val, &l);

        if (is_overridden_[UNPACK_STRING] == 1) {
            *v = strtol(val, &last, 10);

            if (*last == 0) {
                grib_context_log(
                    context_, GRIB_LOG_DEBUG, "Casting string %s to long", name_);
                return GRIB_SUCCESS;
            }
        }
    }

    grib_context_log(
        context_, GRIB_LOG_ERROR, "Cannot unpack key '%s' as long", name_);
    if (grib_get_native_type(grib_handle_of_accessor(this), name_, &type) ==
        GRIB_SUCCESS) {
        grib_context_log(context_,
                         GRIB_LOG_ERROR,
                         "Hint: Try unpacking as %s",
                         grib_get_type_name(type));
    }
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::unpack_double(double* v, size_t* len)
{
    return unpack_helper<double>(this, v, len);
}

int grib_accessor_gen_t::unpack_float(float* v, size_t* len)
{
    return unpack_helper<float>(this, v, len);
}

int grib_accessor_gen_t::unpack_string(char* v, size_t* len)
{
    is_overridden_[UNPACK_STRING] = 0;

    int err = 0;
    if (is_overridden_[UNPACK_DOUBLE] == 1) {
        double val = 0.0;
        size_t l   = 1;
        err        = unpack_double(&val, &l);
        if (is_overridden_[UNPACK_DOUBLE] == 1) {
            if (err)
                return err;
            snprintf(v, 64, "%g", val);
            *len = strlen(v);
            grib_context_log(
                context_, GRIB_LOG_DEBUG, "Casting double %s to string", name_);
            return GRIB_SUCCESS;
        }
    }

    if (is_overridden_[UNPACK_LONG] == 1) {
        long val = 0;
        size_t l = 1;
        err      = unpack_long(&val, &l);
        if (is_overridden_[UNPACK_LONG] == 1) {
            if (err)
                return err;
            snprintf(v, 64, "%ld", val);
            *len = strlen(v);
            grib_context_log(
                context_, GRIB_LOG_DEBUG, "Casting long %s to string\n", name_);
            return GRIB_SUCCESS;
        }
    }

    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::unpack_string_array(char** v, size_t* len)
{
    size_t length = 0;

    int err = grib_get_string_length_acc(this, &length);
    if (err)
        return err;
    v[0] = (char*)grib_context_malloc_clear(context_, length);
    unpack_string(v[0], &length);  // TODO(masn): check return value
    *len = 1;

    return GRIB_SUCCESS;
}

int grib_accessor_gen_t::pack_expression(grib_expression* e)
{
    size_t len        = 1;
    long lval         = 0;
    double dval       = 0;
    const char* cval  = NULL;
    int ret           = 0;
    grib_handle* hand = grib_handle_of_accessor(this);


    // Use the native type of the expression not the accessor
    switch (e->native_type(hand)) {
        case GRIB_TYPE_LONG: {
            len = 1;
            ret = e->evaluate_long(hand, &lval);
            if (ret != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to set %s as long (from %s)",
                                 name_, e->class_name());
                return ret;
            }
            /*if (hand->context->debug)
                printf("ECCODES DEBUG grib_accessor_gen_t::pack_expression %s %ld\n", name,lval);*/
            return pack_long(&lval, &len);
        }

        case GRIB_TYPE_DOUBLE: {
            len = 1;
            ret = e->evaluate_double(hand, &dval);
            if (ret != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to set %s as double (from %s)",
                                 name_, e->class_name());
                return ret;
            }
            /*if (hand->context->debug)
                printf("ECCODES DEBUG grib_accessor_gen_t::pack_expression %s %g\n", name, dval);*/
            return pack_double(&dval, &len);
        }

        case GRIB_TYPE_STRING: {
            char tmp[1024];
            len  = sizeof(tmp);
            cval = e->evaluate_string(hand, tmp, &len, &ret);
            if (ret != GRIB_SUCCESS) {
                grib_context_log(context_, GRIB_LOG_ERROR, "Unable to set %s as string (from %s)",
                                 name_, e->class_name());
                return ret;
            }
            len = strlen(cval);
            /*if (hand->context->debug)
                printf("ECCODES DEBUG grib_accessor_gen_t::pack_expression %s %s\n", name, cval);*/
            return pack_string(cval, &len);
        }
    }

    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::pack_long(const long* v, size_t* len)
{
    is_overridden_[PACK_LONG] = 0;
    grib_context* c           = context_;
    if (is_overridden_[PACK_DOUBLE]) {
        double* val = (double*)grib_context_malloc(c, *len * (sizeof(double)));
        if (!val) {
            grib_context_log(c,
                             GRIB_LOG_ERROR,
                             "Unable to allocate %zu bytes",
                             *len * (sizeof(double)));
            return GRIB_OUT_OF_MEMORY;
        }
        for (size_t i = 0; i < *len; i++)
            val[i] = v[i];
        int ret = pack_double(val, len);
        grib_context_free(c, val);
        if (is_overridden_[PACK_DOUBLE]) {
            return ret;
        }
    }

    grib_context_log(
        c, GRIB_LOG_ERROR, "Should not pack '%s' as an integer", name_);
    if (is_overridden_[PACK_STRING]) {
        grib_context_log(c, GRIB_LOG_ERROR, "Try packing as a string");
    }

    return GRIB_NOT_IMPLEMENTED;
}

int pack_double_array_as_long(grib_accessor* a, const double* v, size_t* len)
{
    grib_context* c = a->context_;
    int ret         = GRIB_SUCCESS;
    size_t numBytes = *len * (sizeof(long));
    long* lValues   = (long*)grib_context_malloc(c, numBytes);
    if (!lValues) {
        grib_context_log(
            c, GRIB_LOG_ERROR, "Unable to allocate %ld bytes", numBytes);
        return GRIB_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < *len; i++)
        lValues[i] = (long)v[i]; /* convert from double to long */
    ret = a->pack_long(lValues, len);
    grib_context_free(c, lValues);
    return ret;
}

int grib_accessor_gen_t::pack_double(const double* v, size_t* len)
{
    is_overridden_[PACK_DOUBLE] = 0;
    grib_context* c             = context_;

    if (is_overridden_[PACK_LONG] || strcmp(class_name_, "codetable") == 0) {
        /* ECC-648: Special case of codetable */
        return pack_double_array_as_long(this, v, len);
    }

    grib_context_log(
        c, GRIB_LOG_ERROR, "Should not pack '%s' as a double", name_);
    if (is_overridden_[PACK_STRING]) {
        grib_context_log(c, GRIB_LOG_ERROR, "Try packing as a string");
    }
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::pack_string_array(const char** v, size_t* len)
{
    int err           = 0;
    size_t length     = 0;
    grib_accessor* as = 0;

    as     = this;
    long i = (long)*len - 1;
    while (as && i >= 0) {
        length = strlen(v[i]);
        err    = as->pack_string(v[i], &length);
        if (err)
            return err;
        --i;
        as = as->same_;
    }
    return GRIB_SUCCESS;
}

int grib_accessor_gen_t::pack_string(const char* v, size_t* len)
{
    is_overridden_[PACK_STRING] = 0;
    if (is_overridden_[PACK_DOUBLE]) {
        size_t l     = 1;
        char* endPtr = NULL; /* for error handling */
        double val   = strtod(v, &endPtr);
        if (*endPtr) {
            grib_context_log(context_,
                             GRIB_LOG_ERROR,
                             "%s: Invalid value (%s) for key '%s'. String cannot be "
                             "converted to a double",
                             __func__,
                             v,
                             name_);
            return GRIB_WRONG_TYPE;
        }
        return pack_double(&val, &l);
    }

    if (is_overridden_[PACK_LONG]) {
        size_t l = 1;
        long val = atol(v);
        int err  = pack_long(&val, &l);
        if (is_overridden_[PACK_LONG]) {
            return err;
        }
    }

    grib_context_log(
        context_, GRIB_LOG_ERROR, "Should not pack '%s' as string", name_);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::pack_bytes(const unsigned char* val, size_t* len)
{
    const size_t length = *len;
    if (length_ != length) {
        grib_context_log(
            context_,
            GRIB_LOG_ERROR,
            "pack_bytes: Wrong size (%zu) for %s. It is %ld bytes long",
            length,
            name_,
            length_);
        return GRIB_BUFFER_TOO_SMALL;
    }
    grib_buffer_replace(this, val, length, 1, 1);
    return GRIB_SUCCESS;
}

void grib_accessor_gen_t::destroy(grib_context* ct)
{
    grib_dependency_remove_observed(this);
    grib_dependency_remove_observer(this);
    if (vvalue_ != NULL) {
        grib_context_free(ct, vvalue_);
        vvalue_ = NULL;
    }
    /*grib_context_log(ct,GRIB_LOG_DEBUG,"address=%p",a);*/
}

grib_section* grib_accessor_gen_t::sub_section()
{
    return NULL;
}

int grib_accessor_gen_t::notify_change(grib_accessor* observed)
{
    /* Default behaviour is to notify creator */
    return creator_->notify_change(this, observed);
}

void grib_accessor_gen_t::update_size(size_t s)
{
    grib_context_log(context_,
                     GRIB_LOG_FATAL,
                     "Accessor %s [%s] must implement 'update_size'",
                     name_,
                     class_name_);
}

grib_accessor* grib_accessor_gen_t::next_accessor()
{
    return next(this, 1);
}

grib_accessor* grib_accessor_gen_t::next(grib_accessor* a, int mod)
{
    grib_accessor* next = NULL;
    if (a->next_) {
        next = a->next_;
    }
    else {
        if (a->parent_->owner)
            next = a->parent_->owner->next(a->parent_->owner, 0);
    }
    return next;
}

int grib_accessor_gen_t::compare(grib_accessor* b)
{
    return GRIB_NOT_IMPLEMENTED;
}

/* Redefined in all padding */

size_t grib_accessor_gen_t::preferred_size(int from_handle)
{
    return length_;
}

int grib_accessor_gen_t::is_missing_internal()
{
    return is_missing();
}

int grib_accessor_gen_t::is_missing()
{
    int i              = 0;
    int is_missing     = 1;
    unsigned char ones = 0xff;
    unsigned char* v   = NULL;

    if (flags_ & GRIB_ACCESSOR_FLAG_TRANSIENT) {
        if (vvalue_ == NULL) {
            grib_context_log(context_,
                             GRIB_LOG_ERROR,
                             "%s internal error (flags=0x%lX)",
                             name_,
                             flags_);
            ECCODES_ASSERT(!"grib_accessor_gen_t::is_missing(): vvalue == NULL");
            return 0;
        }
        return vvalue_->missing;
    }
    ECCODES_ASSERT(length_ >= 0);

    v = grib_handle_of_accessor(this)->buffer->data + offset_;

    for (i = 0; i < length_; i++) {
        if (*v != ones) {
            is_missing = 0;
            break;
        }
        v++;
    }

    return is_missing;
}

int grib_accessor_gen_t::unpack_double_element(size_t i, double* val)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::unpack_double_element_set(const size_t* index_array,
                                                   size_t len,
                                                   double* val_array)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_gen_t::unpack_double_subarray(double* val,
                                                size_t start,
                                                size_t len)
{
    return GRIB_NOT_IMPLEMENTED;
}

grib_accessor* grib_accessor_gen_t::clone(grib_section* s, int* err)
{
    grib_context* ct = context_;
    grib_context_log(ct, GRIB_LOG_DEBUG, "clone %s ==> %s", class_name_, name_);
    return make_clone(s, err);
}

grib_accessor* grib_accessor_gen_t::make_clone(grib_section* s, int* err)
{
    *err = GRIB_NOT_IMPLEMENTED;
    return NULL;
}

long grib_accessor_gen_t::get_next_position_offset()
{
    return next_offset();
}

grib_accessor_gen_t::~grib_accessor_gen_t() {}

void grib_accessor_gen_t::post_init()
{
    return;
}
int grib_accessor_gen_t::pack_missing()
{
    throw std::runtime_error("grib_accessor_gen_t::pack_missing not implemented");
}
int grib_accessor_gen_t::pack_float(const float*, size_t* len)
{
    throw std::runtime_error("grib_accessor_gen_t::pack_float not implemented");
}
void grib_accessor_gen_t::resize(size_t)
{
    throw std::runtime_error("grib_accessor_gen_t::resize not implemented");
}
int grib_accessor_gen_t::nearest_smaller_value(double, double*)
{
    throw std::runtime_error(
        "grib_accessor_gen_t::nearest_smaller_value not implemented");
}
int grib_accessor_gen_t::unpack_float_element(size_t, float*)
{
    throw std::runtime_error(
        "grib_accessor_gen_t::unpack_float_element not implemented");
}
int unpack_element_set(const size_t*, size_t, double*)
{
    throw std::runtime_error(
        "grib_accessor_gen_t::unpack_element_set not implemented");
}
int grib_accessor_gen_t::unpack_float_element_set(const size_t*,
                                                  size_t,
                                                  float*)
{
    throw std::runtime_error(
        "grib_accessor_gen_t::unpack_float_element_set not implemented");
}
