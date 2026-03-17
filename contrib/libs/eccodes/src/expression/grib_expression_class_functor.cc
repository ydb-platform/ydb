/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_expression_class_functor.h"
#include <string>
#include <algorithm>


namespace eccodes::expression
{

// See ECC-1936. We cannot use strcasestr (not on Windows and non-standard)
static bool string_contains_case(const char* haystack, const char* needle, bool case_sensitive)
{
    std::string copy_haystack = haystack;
    std::string copy_needle   = needle;

    if (!case_sensitive) {
        // Convert both strings to lowercase if we don't care about case
        std::transform(copy_needle.begin(), copy_needle.end(), copy_needle.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        std::transform(copy_haystack.begin(), copy_haystack.end(), copy_haystack.begin(),
                       [](unsigned char c) { return std::tolower(c); });
    }
    // Perform the search
    return copy_haystack.find(copy_needle) != std::string::npos;
}

int Functor::evaluate_long(grib_handle* h, long* lres) const
{
    // if (STR_EQUAL(name_, "lookup")) {
    //     return GRIB_SUCCESS;
    // }

    if (STR_EQUAL(name_, "new")) {
        *lres = h->loader != NULL;
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "abs")) {
        const grib_expression* exp = args_ ? args_->get_expression(h, 0) : nullptr;
        if (exp) {
            long lval = 0;
            int ret = exp->evaluate_long(h, &lval);
            *lres = abs(lval);
            return ret;
        }
        return GRIB_INVALID_ARGUMENT;
    }

    if (STR_EQUAL(name_, "size")) {
        *lres = 0;
        const char* keyName = args_ ? args_->get_name(h, 0) : nullptr;
        if (keyName) {
            size_t size = 0;
            int err = grib_get_size(h, keyName, &size);
            if (err) return err;
            *lres = (long)size;
            return GRIB_SUCCESS;
        }
        return GRIB_INVALID_ARGUMENT;
    }

    if (STR_EQUAL(name_, "debug_mode")) {
        const int n = args_ ? args_->get_count() : 0;
        if (n != 1) return GRIB_INVALID_ARGUMENT;
        const int dmode = args_ ? args_->get_long(h, 0) : 0;
        grib_context_set_debug(0, dmode);
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "dump_content")) {
        const int n = args_ ? args_->get_count() : 0;
        if (n != 1) {
            grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Please provide an argument e.g. wmo", name_);
            return GRIB_INVALID_ARGUMENT;
        }
        const char* dump_mode = args_->get_string(h, 0);
        if (dump_mode) {
            int dump_flags = 0;
            if (STR_EQUAL(dump_mode, "wmo")) { // mimic grib_dump -O
                dump_flags = GRIB_DUMP_FLAG_CODED | GRIB_DUMP_FLAG_OCTET | GRIB_DUMP_FLAG_VALUES | GRIB_DUMP_FLAG_READ_ONLY;
            }
            if (STR_EQUAL(dump_mode, "debug")) { // mimic grib_dump -Da
                dump_flags = GRIB_DUMP_FLAG_VALUES | GRIB_DUMP_FLAG_READ_ONLY | GRIB_DUMP_FLAG_ALIASES;
            }
            grib_dump_content(h, stdout, dump_mode, dump_flags, 0);
            *lres = 1;
            return GRIB_SUCCESS;
        }
    }

    if (STR_EQUAL(name_, "missing")) {
        const char* keyName = args_ ? args_->get_name(h, 0) : nullptr;
        if (keyName) {
            long val = 0;
            int err  = 0;
            if (h->product_kind == PRODUCT_BUFR) {
                int ismiss = grib_is_missing(h, keyName, &err);
                if (err) return err;
                *lres = ismiss;
                return GRIB_SUCCESS;
            }
            err = grib_get_long_internal(h, keyName, &val);
            if (err) return err;
            // Note: This does not cope with keys like typeOfSecondFixedSurface
            // which are codetable entries with values like 255: this value is
            // not classed as 'missing'!
            // (See ECC-594)
            *lres = (val == GRIB_MISSING_LONG);
            return GRIB_SUCCESS;
        }
        else {
            // No arguments means return the actual integer missing value
            *lres = GRIB_MISSING_LONG;
        }
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "defined")) {
        const char* keyName = args_ ? args_->get_name(h, 0) : nullptr;
        if (keyName) {
            const grib_accessor* a = grib_find_accessor(h, keyName);
            *lres = a != NULL ? 1 : 0;
            return GRIB_SUCCESS;
        }
        *lres = 0;
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "environment_variable")) {
        // ECC-1520: This implementation has some limitations:
        // 1. Cannot distinguish between environment variable NOT SET
        //    and SET but equal to 0
        // 2. Cannot deal with string values
        const char* p = args_ ? args_->get_name(h, 0) : nullptr;
        if (p) {
            const char* env = getenv(p);
            if (env) {
                long lval = 0;
                if (string_to_long(env, &lval, 1) == GRIB_SUCCESS) {
                    *lres = lval;
                    return GRIB_SUCCESS;
                }
            }
        }
        *lres = 0;
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "changed")) {
        *lres = 1;
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "contains")) {
        *lres = 0;
        const int n = args_ ? args_->get_count() : 0;
        if (n != 3) return GRIB_INVALID_ARGUMENT;
        const char* keyName = args_ ? args_->get_name(h, 0) : nullptr;
        if (!keyName) return GRIB_INVALID_ARGUMENT;
        int type = 0;
        int err = grib_get_native_type(h, keyName, &type);
        if (err) return err;
        if (type == GRIB_TYPE_STRING) {
            char keyValue[254] = {0,};
            size_t len = sizeof(keyValue);
            err = grib_get_string(h, keyName, keyValue, &len);
            if (err) return err;
            const char* sValue = args_->get_string(h, 1);
            if (!sValue) return GRIB_INVALID_ARGUMENT;
            const bool case_sens = args_->get_long(h, 2) == 0; // 0=case-sensitive, 1=case-insensitive
            const bool contains = string_contains_case(keyValue, sValue, case_sens);
            if (contains) {
                *lres = 1;
                return GRIB_SUCCESS;
            }
        } else {
            // For now only keys of type string supported
            return GRIB_INVALID_ARGUMENT;
        }
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "is_one_of")) {
        *lres = 0;
        const char* keyName = args_->get_name(h, 0);
        if (!keyName) return GRIB_INVALID_ARGUMENT;
        int type = 0;
        int err = grib_get_native_type(h, keyName, &type);
        if (err) return err;
        int n = args_->get_count();
        if (type == GRIB_TYPE_STRING) {
            char keyValue[254] = {0,};
            size_t len = sizeof(keyValue);
            err = grib_get_string(h, keyName, keyValue, &len);
            if (err) return err;
            for (int i = 1; i < n; ++i) { // skip 1st argument which is the key
                const char* sValue = args_->get_string(h, i);
                if (sValue && STR_EQUAL(keyValue, sValue)) {
                    *lres = 1;
                    return GRIB_SUCCESS;
                }
            }
        }
        else if (type == GRIB_TYPE_LONG) {
            long keyValue = 0;
            err = grib_get_long(h, keyName, &keyValue);
            if (err) return err;
            for (int i = 1; i < n; ++i) { // skip 1st argument which is the key
                long lValue = args_->get_long(h, i);
                if (keyValue == lValue) {
                    *lres = 1;
                    return GRIB_SUCCESS;
                }
            }
        }
        else if (type == GRIB_TYPE_DOUBLE) {
            return GRIB_NOT_IMPLEMENTED;
        }
        return GRIB_SUCCESS;
    }

    if (STR_EQUAL(name_, "gribex_mode_on")) {
        *lres = h->context->gribex_mode_on ? 1 : 0;
        return GRIB_SUCCESS;
    }

    grib_context_log(h->context, GRIB_LOG_ERROR, "grib_expression_class_functor::%s failed for '%s'", __func__, name_);
    return GRIB_NOT_IMPLEMENTED;
}

void Functor::print(grib_context* c,grib_handle* f, FILE* out) const
{
    fprintf(out, "%s(", name_);
    // grib_expression_print(c,args_,f);
    fprintf(out, ")");
}

void Functor::destroy(grib_context* c)
{
    grib_context_free_persistent(c, name_);
    grib_arguments_free(c, args_);
}

void Functor::add_dependency(grib_accessor* observer)
{
    if (strcmp(name_, "defined"))
        grib_dependency_observe_arguments(observer, args_);
}

Functor::Functor(grib_context* c, const char* name, grib_arguments* args)
{
    name_ = grib_context_strdup_persistent(c, name);
    args_ = args;
}

int Functor::native_type(grib_handle* h) const
{
    return GRIB_TYPE_LONG;
}

}  // namespace eccodes::expression


grib_expression* new_func_expression(grib_context* c, const char* name, grib_arguments* args) {
    return new eccodes::expression::Functor(c, name, args);
}
