/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_group.h"

grib_accessor_group_t _grib_accessor_group{};
grib_accessor* grib_accessor_group = &_grib_accessor_group;

void grib_accessor_group_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    const grib_buffer* buffer = grib_handle_of_accessor(this)->buffer;

    size_t i = 0;
    unsigned char* v;
    const char* s = arg ? arg->get_string(grib_handle_of_accessor(this), 0) : nullptr;

    if (s && strlen(s) > 1) {
        grib_context_log(context_, GRIB_LOG_WARNING,
                         "Using only first character as group end of %s not the string %s", name_, s);
    }

    endCharacter_ = s ? s[0] : 0;

    v = buffer->data + offset_;
    i = 0;
    if (s) {
        while (*v != endCharacter_ && i <= buffer->ulength) {
            if (*v > 126)
                *v = 32;
            v++;
            i++;
        }
    }
    else {
        while (*v > 32 && *v != 61 && *v < 127 && i <= buffer->ulength) {
            v++;
            i++;
        }
    }
    length_ = i;

    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

int grib_accessor_group_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_group_t::string_length()
{
    return length_;
}

void grib_accessor_group_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

long grib_accessor_group_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_group_t::unpack_string(char* val, size_t* len)
{
    long i         = 0;
    size_t l       = length_ + 1;
    grib_handle* h = grib_handle_of_accessor(this);

    if (*len < l) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, l, *len);
        *len = l;
        return GRIB_ARRAY_TOO_SMALL;
    }

    for (i = 0; i < length_; i++)
        val[i] = h->buffer->data[offset_ + i];
    val[i] = 0;
    *len   = i;
    return GRIB_SUCCESS;
}

int grib_accessor_group_t::unpack_long(long* v, size_t* len)
{
    char val[1024] = {0,};
    size_t l   = sizeof(val);
    size_t i   = 0;
    char* last = NULL;
    int err    = unpack_string(val, &l);
    if (err)
        return err;

    i = 0;
    while (i < l - 1 && val[i] == ' ')
        i++;

    if (val[i] == 0) {
        *v = 0;
        return 0;
    }
    if (val[i + 1] == ' ' && i < l - 2)
        val[i + 1] = 0;

    *v = strtol(val, &last, 10);

    grib_context_log(context_, GRIB_LOG_DEBUG, "Casting string %s to long", name_);
    return GRIB_SUCCESS;
}

int grib_accessor_group_t::unpack_double(double* v, size_t* len)
{
    char val[1024];
    size_t l   = sizeof(val);
    char* last = NULL;
    unpack_string(val, &l);
    *v = strtod(val, &last);

    if (*last == 0) {
        grib_context_log(context_, GRIB_LOG_DEBUG, "Casting string %s to long", name_);
        return GRIB_SUCCESS;
    }

    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_group_t::compare(grib_accessor* b)
{
    grib_context_log(context_, GRIB_LOG_ERROR, "%s:%s not implemented", __func__, name_);
    return GRIB_NOT_IMPLEMENTED;

    // int retval = 0;
    // char* aval = 0;
    // char* bval = 0;
    // int err    = 0;
    // size_t alen = 0;
    // size_t blen = 0;
    // long count  = 0;
    // err = value_count(&count);    // if (err) return err;
    // alen = count;
    // err = b->value_count(&count);    // if (err) return err;
    // blen = count;
    // if (alen != blen) return GRIB_COUNT_MISMATCH;
    // aval = (char*)grib_context_malloc(context , alen * sizeof(char));
    // bval = (char*)grib_context_malloc(b->context, blen * sizeof(char));
    // unpack_string(aval, &alen);    // b->unpack_string(bval, &blen);    // retval = GRIB_SUCCESS;
    // if (strcmp(aval, bval)) retval = GRIB_STRING_VALUE_MISMATCH;
    // grib_context_free(context , aval);
    // grib_context_free(b->context, bval);
    // return retval;
}

long grib_accessor_group_t::next_offset()
{
    return offset_ + length_;
}
