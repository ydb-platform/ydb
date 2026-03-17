/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_hash_array.h"
#include "action_class_hash_array.h"

grib_accessor_hash_array_t _grib_accessor_hash_array{};
grib_accessor* grib_accessor_hash_array = &_grib_accessor_hash_array;

#define MAX_HASH_ARRAY_STRING_LENGTH 255

void grib_accessor_hash_array_t::init(const long len, grib_arguments* args)
{
    grib_accessor_gen_t::init(len, args);
    length_ = 0;
    key_    = 0;
    ha_     = NULL;
}

void grib_accessor_hash_array_t::dump(eccodes::Dumper* dumper)
{
    dumper->dump_string(this, NULL);
}

int grib_accessor_hash_array_t::pack_double(const double* val, size_t* len)
{
    char s[200] = {0,};
    snprintf(s, sizeof(s), "%g", *val);
    key_ = grib_context_strdup(context_, s);
    ha_  = 0;
    return GRIB_SUCCESS;
}

int grib_accessor_hash_array_t::pack_long(const long* val, size_t* len)
{
    char s[200] = {0,};
    snprintf(s, sizeof(s), "%ld", *val);
    if (key_)
        grib_context_free(context_, key_);
    key_ = grib_context_strdup(context_, s);
    ha_  = 0;
    return GRIB_SUCCESS;
}

int grib_accessor_hash_array_t::pack_string(const char* v, size_t* len)
{
    key_ = grib_context_strdup(context_, v);
    ha_  = 0;
    return GRIB_SUCCESS;
}

int grib_accessor_hash_array_t::unpack_double(double* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

grib_hash_array_value* grib_accessor_hash_array_t::find_hash_value(int* err)
{
    grib_hash_array_value* ha_ret    = 0;
    grib_hash_array_value* ha        = NULL;

    eccodes::action::HashArray* hash_array = dynamic_cast<eccodes::action::HashArray*>(creator_);

    ha = hash_array->get_hash_array(grib_handle_of_accessor(this));
    if (!ha) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "unable to get hash value for %s", creator_->name_);
        *err = GRIB_HASH_ARRAY_NO_MATCH;
        return NULL;
    }

    *err = GRIB_SUCCESS;

    ECCODES_ASSERT(ha != NULL);
    if (!key_) {
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "unable to get hash value for %s, set before getting", creator_->name_);
        *err = GRIB_HASH_ARRAY_NO_MATCH;
        return NULL;
    }

    ha_ret = (grib_hash_array_value*)grib_trie_get(ha->index, key_);
    if (!ha_ret)
        ha_ret = (grib_hash_array_value*)grib_trie_get(ha->index, "default");

    if (!ha_ret) {
        *err = GRIB_HASH_ARRAY_NO_MATCH;
        grib_context_log(context_, GRIB_LOG_ERROR,
                         "hash_array: no match for %s=%s",
                         creator_->name_, key_);
        const char* full_path = hash_array->get_hash_array_full_path();
        if (full_path) {
            grib_context_log(context_, GRIB_LOG_ERROR, "hash_array: file path = %s", full_path);
        }
        grib_context_log(context_, GRIB_LOG_ERROR, "Hint: Check the key 'masterTablesVersionNumber'");
        return NULL;
    }
    return ha_ret;
}

int grib_accessor_hash_array_t::unpack_long(long* val, size_t* len)
{
    grib_hash_array_value* ha = 0;
    int err                   = 0;
    size_t i                  = 0;

    if (!ha_) {
        ha = find_hash_value(&err);
        if (err)
            return err;
        ha_ = ha;
    }

    switch (ha_->type) {
        case GRIB_HASH_ARRAY_TYPE_INTEGER:
            if (*len < ha_->iarray->n) {
                return GRIB_ARRAY_TOO_SMALL;
            }
            *len = ha_->iarray->n;
            for (i = 0; i < *len; i++)
                val[i] = ha_->iarray->v[i];
            break;

        default:
            return GRIB_NOT_IMPLEMENTED;
    }

    return GRIB_SUCCESS;
}

long grib_accessor_hash_array_t::get_native_type()
{
    int type = GRIB_TYPE_STRING;
    if (flags_ & GRIB_ACCESSOR_FLAG_LONG_TYPE)
        type = GRIB_TYPE_LONG;

    return type;
}

void grib_accessor_hash_array_t::destroy(grib_context* c)
{
    if (key_)
        grib_context_free(c, key_);
    grib_accessor_gen_t::destroy(c);
}

int grib_accessor_hash_array_t::unpack_string(char* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

size_t grib_accessor_hash_array_t::string_length()
{
    return MAX_HASH_ARRAY_STRING_LENGTH;
}

int grib_accessor_hash_array_t::value_count(long* count)
{
    int err                   = 0;
    grib_hash_array_value* ha = 0;

    if (!ha_) {
        ha = find_hash_value(&err);
        if (err)
            return err;
        ha_ = ha;
    }

    *count = ha_->iarray->n;
    return err;
}

int grib_accessor_hash_array_t::compare(grib_accessor* b)
{
    return GRIB_NOT_IMPLEMENTED;
}
