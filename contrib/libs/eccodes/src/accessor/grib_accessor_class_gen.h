/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include "grib_api_internal.h"
#include "grib_accessor.h"
#include "grib_value.h"
#include <bitset>


class grib_accessor_gen_t : public grib_accessor
{
public:
    grib_accessor_gen_t() :
        grib_accessor{} { class_name_ = "gen"; }
    ~grib_accessor_gen_t();

    void init_accessor(const long, grib_arguments*) override;  // TODO: Implement
    grib_accessor* create_empty_accessor() override { return new grib_accessor_gen_t{}; }
    grib_section* sub_section() override;
    long get_native_type() override;
    int pack_missing() override;
    int is_missing() override;
    int is_missing_internal() override;  // TODO: Implement
    int pack_bytes(const unsigned char*, size_t* len) override;
    int pack_double(const double* val, size_t* len) override;
    int pack_float(const float* val, size_t* len) override;
    int pack_long(const long* val, size_t* len) override;
    int pack_string(const char*, size_t* len) override;
    int pack_string_array(const char**, size_t* len) override;
    int pack_expression(grib_expression*) override;
    int unpack_bytes(unsigned char*, size_t* len) override;
    int unpack_double(double* val, size_t* len) override;
    int unpack_float(float* val, size_t* len) override;
    int unpack_long(long* val, size_t* len) override;
    int unpack_string(char*, size_t* len) override;
    int unpack_string_array(char**, size_t* len) override;
    size_t string_length() override;
    long byte_count() override;
    long byte_offset() override;
    long get_next_position_offset() override;  // TODO: Implement
    long next_offset() override;
    int value_count(long*) override;
    void destroy(grib_context*) override;
    void dump(eccodes::Dumper*) override;
    void init(const long, grib_arguments*) override;
    void post_init() override;
    int notify_change(grib_accessor* changed) override;
    void update_size(size_t) override;
    size_t preferred_size(int) override;
    void resize(size_t) override;
    int nearest_smaller_value(double, double*) override;
    grib_accessor* next(grib_accessor*, int) override;
    int compare(grib_accessor* a) override;
    int unpack_double_element(size_t i, double* val) override;
    int unpack_float_element(size_t i, float* val) override;
    int unpack_double_element_set(const size_t* index_array, size_t len, double* val_array) override;
    int unpack_float_element_set(const size_t* index_array, size_t len, float* val_array) override;
    int unpack_double_subarray(double* val, size_t start, size_t len) override;
    int clear() override;
    grib_accessor* clone(grib_section* s, int* err) override;  // TODO: Implement
    grib_accessor* make_clone(grib_section*, int*) override;
    grib_accessor* next_accessor() override;  // TODO: Implement

private:
    enum
    {
        PACK_DOUBLE,
        PACK_FLOAT,
        PACK_LONG,
        PACK_STRING,
        UNPACK_DOUBLE,
        UNPACK_FLOAT,
        UNPACK_LONG,
        UNPACK_STRING,
    };
    std::bitset<8> is_overridden_ = 0b11111111;

    template <typename T>
    int unpack_helper(grib_accessor* a, T* v, size_t* len);
};


template <typename T>
int grib_accessor_gen_t::unpack_helper(grib_accessor* a, T* v, size_t* len)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");
    int type          = GRIB_TYPE_UNDEFINED;
    const char* Tname = type_to_string<T>(*v);

    if constexpr (std::is_same_v<T, float>) {
        is_overridden_[UNPACK_FLOAT] = 0;
    }
    else if constexpr (std::is_same_v<T, double>) {
        is_overridden_[UNPACK_DOUBLE] = 0;
    }

    if (is_overridden_[UNPACK_LONG]) {
        long val = 0;
        size_t l = 1;
        a->unpack_long(&val, &l);
        if (is_overridden_[UNPACK_LONG]) {
            *v = val;
            grib_context_log(a->context_, GRIB_LOG_DEBUG, "Casting long %s to %s", a->name_, Tname);
            return GRIB_SUCCESS;
        }
    }

    if (is_overridden_[UNPACK_STRING]) {
        char val[1024];
        size_t l   = sizeof(val);
        char* last = NULL;
        a->unpack_string(val, &l);
        if (is_overridden_[UNPACK_STRING]) {
            *v = strtod(val, &last);
            if (*last == 0) { /* conversion of string to double worked */
                grib_context_log(a->context_, GRIB_LOG_DEBUG, "Casting string %s to %s", a->name_, Tname);
                return GRIB_SUCCESS;
            }
        }
    }

    grib_context_log(a->context_, GRIB_LOG_ERROR, "Cannot unpack key '%s' as %s", a->name_, Tname);
    if (grib_get_native_type(grib_handle_of_accessor(a), a->name_, &type) == GRIB_SUCCESS) {
        grib_context_log(a->context_, GRIB_LOG_ERROR, "Hint: Try unpacking as %s", grib_get_type_name(type));
    }

    return GRIB_NOT_IMPLEMENTED;
}
