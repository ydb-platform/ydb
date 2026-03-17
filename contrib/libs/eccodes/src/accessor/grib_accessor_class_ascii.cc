/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_ascii.h"

grib_accessor_ascii_t _grib_accessor_ascii{};
grib_accessor* grib_accessor_ascii = &_grib_accessor_ascii;

void grib_accessor_ascii_t::init(const long len, grib_arguments* arg)
{
    grib_accessor_gen_t::init(len, arg);
    length_ = len;
    ECCODES_ASSERT(length_ >= 0);
}

int grib_accessor_ascii_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

size_t grib_accessor_ascii_t::string_length()
{
    return length_;
}

void grib_accessor_ascii_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

long grib_accessor_ascii_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_ascii_t::unpack_string(char* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    const size_t alen = length_;

    if (*len < (alen + 1)) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (len=%zu)",
                         class_name_, name_, alen + 1, *len);
        *len = alen + 1;
        return GRIB_BUFFER_TOO_SMALL;
    }

    size_t i = 0;
    for (i = 0; i < alen; i++)
        val[i] = hand->buffer->data[offset_ + i];
    val[i] = 0;
    *len   = i;
    return GRIB_SUCCESS;
}

int grib_accessor_ascii_t::pack_string(const char* val, size_t* len)
{
    grib_handle* hand = grib_handle_of_accessor(this);
    const size_t alen = length_;
    if (*len > (alen + 1)) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "%s: Buffer too small for %s. It is %zu bytes long (input string len=%zu)",
                         class_name_, name_, alen, *len);
        *len = alen;
        return GRIB_BUFFER_TOO_SMALL;
    }

    for (size_t i = 0; i < alen; i++) {
        if (i < *len)
            hand->buffer->data[offset_ + i] = val[i];
        else
            hand->buffer->data[offset_ + i] = 0;
    }

    // TODO(masn): Make this an error.
    // But we have to allow this case unfortunately as returning an error breaks
    // clients e.g. grib1 local def 40 has marsDomain of 2 bytes but local def 21
    // has the same key with 1 byte! Legacy stuff that cannot be changed easily.
    // So at least issue a warning
    if (*len > alen) {
        // Decode the string and compare with the incoming value
        size_t size = 0;
        if (grib_get_string_length_acc(this, &size) == GRIB_SUCCESS) {
            char* value = (char*)grib_context_malloc_clear(context_, size);
            if (value) {
                if (this->unpack_string(value, &size) == GRIB_SUCCESS && !STR_EQUAL(val, value)) {
                    fprintf(stderr, "ECCODES WARNING :  String input '%s' truncated to '%s'. Key %s is %zu byte(s)\n",
                            val, value, name_, alen);
                }
                grib_context_free(context_, value);
            }
        }
    }

    return GRIB_SUCCESS;
}

int grib_accessor_ascii_t::pack_long(const long* v, size_t* len)
{
    grib_context_log(this->context_, GRIB_LOG_ERROR, "Should not pack %s as long (It's a string)", name_);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_ascii_t::pack_double(const double* v, size_t* len)
{
    grib_context_log(this->context_, GRIB_LOG_ERROR, "Should not pack %s as double (It's a string)", name_);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_ascii_t::unpack_long(long* v, size_t* len)
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

    grib_context_log(this->context_, GRIB_LOG_DEBUG, "Casting string %s to long", name_);
    return GRIB_SUCCESS;
}

int grib_accessor_ascii_t::unpack_double(double* v, size_t* len)
{
    char val[1024];
    size_t l   = sizeof(val);
    char* last = NULL;

    int err = unpack_string(val, &l);
    if (err) return err;

    *v = strtod(val, &last);

    if (*last == 0) {
        grib_context_log(this->context_, GRIB_LOG_DEBUG, "Casting string %s to long", name_);
        return GRIB_SUCCESS;
    }

    grib_context_log(this->context_, GRIB_LOG_WARNING, "Cannot unpack %s as double. Hint: Try unpacking as string", name_);

    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_ascii_t::compare(grib_accessor* b)
{
    int retval = 0;
    char* aval = 0;
    char* bval = 0;
    int err    = 0;

    size_t alen = length_ + 1;
    size_t blen = b->length_ + 1;

    if (alen != blen)
        return GRIB_COUNT_MISMATCH;

    aval = (char*)grib_context_malloc(context_, alen * sizeof(char));
    bval = (char*)grib_context_malloc(b->context_, blen * sizeof(char));

    err = unpack_string(aval, &alen);
    if (err) return err;
    err = b->unpack_string(bval, &blen);
    if (err) return err;

    retval = GRIB_SUCCESS;
    if (!STR_EQUAL(aval, bval))
        retval = GRIB_STRING_VALUE_MISMATCH;

    grib_context_free(context_, aval);
    grib_context_free(b->context_, bval);

    return retval;
}
