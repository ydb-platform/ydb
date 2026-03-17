/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */
/***************************************************************************
 * Jean Baptiste Filippi - 01.11.2005
 ***************************************************************************/

#include "grib_api_internal.h"
#include "grib_value.h"
//#include "grib_accessor.h"
#include <float.h>
#include <limits>
#include <type_traits>

/* Note: A fast cut-down version of strcmp which does NOT return -1 */
/* 0 means input strings are equal and 1 means not equal */
GRIB_INLINE static int grib_inline_strcmp(const char* a, const char* b)
{
    if (*a != *b)
        return 1;
    while ((*a != 0 && *b != 0) && *(a) == *(b)) {
        a++;
        b++;
    }
    return (*a == 0 && *b == 0) ? 0 : 1;
}

// Debug utility function to track GRIB packing/repacking issues
template <typename T>
static void print_debug_info__set_array(grib_handle* h, const char* func, const char* name, const T* val, size_t length)
{
    size_t N = 7, i = 0;
    T minVal = std::numeric_limits<T>::max();
    T maxVal = -std::numeric_limits<T>::max();
    double missingValue = 0;
    ECCODES_ASSERT( h->context->debug );

    if (grib_get_double(h, "missingValue", &missingValue)!=GRIB_SUCCESS) {
        missingValue = 9999.0;
    }

    if (length <= N)
        N = length;
    fprintf(stderr, "ECCODES DEBUG %s h=%p key=%s, %zu entries (", func, (void*)h, name, length);
    for (i = 0; i < N; ++i) {
        if (i != 0) fprintf(stderr,", ");
        fprintf(stderr, "%.10g", val[i]);
    }
    if (N >= length) fprintf(stderr, ") ");
    else fprintf(stderr, "...) ");
    for (i = 0; i < length; ++i) {
        if (val[i] == (T)missingValue) continue;
        if (val[i] < minVal) minVal = val[i];
        if (val[i] > maxVal) maxVal = val[i];
    }
    fprintf(stderr, "min=%.10g, max=%.10g\n",minVal,maxVal);
}

static void print_error_no_accessor(const grib_context* c, const char* name)
{
    grib_context_log(c, GRIB_LOG_ERROR, "Unable to find accessor %s", name);
    const char* dpath = getenv("ECCODES_DEFINITION_PATH");
    if (dpath != NULL) {
        grib_context_log(c, GRIB_LOG_ERROR,
            "Hint: This could be a symptom of an issue with your definitions.\n\t"
            "The environment variable ECCODES_DEFINITION_PATH is defined and set to '%s'.\n\t"
            "Please use the latest definitions.", dpath);
    }
}

int grib_set_expression(grib_handle* h, const char* name, grib_expression* e)
{
    grib_accessor* a = grib_find_accessor(h, name);
    int ret          = GRIB_SUCCESS;

    if (a) {
        if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
            return GRIB_READ_ONLY;

        ret = a->pack_expression(e);
        if (ret == GRIB_SUCCESS) {
            return grib_dependency_notify_change(a);
        }
        return ret;
    }
    return GRIB_NOT_FOUND;
}

int grib_set_long_internal(grib_handle* h, const char* name, long val)
{
    const grib_context* c  = h->context;
    int ret          = GRIB_SUCCESS;
    grib_accessor* a = NULL;
    size_t l         = 1;

    a = grib_find_accessor(h, name);

    if (h->context->debug)
        fprintf(stderr, "ECCODES DEBUG grib_set_long_internal h=%p %s=%ld\n", (void*)h, name, val);

    if (a) {
        ret = a->pack_long(&val, &l);
        if (ret == GRIB_SUCCESS) {
            return grib_dependency_notify_change(a);
        }

        grib_context_log(c, GRIB_LOG_ERROR, "Unable to set %s=%ld as long (%s)",
                         name, val, grib_get_error_message(ret));
        return ret;
    }

    print_error_no_accessor(c, name);
    //grib_context_log(c, GRIB_LOG_ERROR, "Unable to find accessor %s", name);
    return GRIB_NOT_FOUND;
}

int grib_set_long(grib_handle* h, const char* name, long val)
{
    int ret          = GRIB_SUCCESS;
    grib_accessor* a = NULL;
    size_t l         = 1;

    a = grib_find_accessor(h, name);

    if (a) {
        if (h->context->debug) {
            if (strcmp(name, a->name_) != 0)
                fprintf(stderr, "ECCODES DEBUG grib_set_long h=%p %s=%ld (a->name_=%s)\n", (void*)h, name, val, a->name_);
            else
                fprintf(stderr, "ECCODES DEBUG grib_set_long h=%p %s=%ld\n", (void*)h, name, val);
        }

        if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
            return GRIB_READ_ONLY;

        ret = a->pack_long(&val, &l);
        if (ret == GRIB_SUCCESS)
            return grib_dependency_notify_change(a);

        return ret;
    }

    if (h->context->debug) {
        fprintf(stderr, "ECCODES DEBUG grib_set_long h=%p %s=%ld (Key not found)\n", (void*)h, name, val);
    }

    return GRIB_NOT_FOUND;
}

int grib_set_double_internal(grib_handle* h, const char* name, double val)
{
    int ret          = GRIB_SUCCESS;
    grib_accessor* a = NULL;
    size_t l         = 1;

    a = grib_find_accessor(h, name);

    if (h->context->debug)
        fprintf(stderr, "ECCODES DEBUG grib_set_double_internal h=%p %s=%.10g\n", (void*)h, name, val);

    if (a) {
        ret = a->pack_double(&val, &l);
        if (ret == GRIB_SUCCESS) {
            return grib_dependency_notify_change(a);
        }

        grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to set %s=%g as double (%s)",
                         name, val, grib_get_error_message(ret));
        return ret;
    }

    print_error_no_accessor(h->context, name);
    //grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to find accessor %s", name);
    return GRIB_NOT_FOUND;
}

typedef struct grib_key_err grib_key_err;
struct grib_key_err
{
    char* name;
    int err;
    grib_key_err* next;
};

int grib_copy_namespace(grib_handle* dest, const char* name, grib_handle* src)
{
    int* err = NULL;
    int type, error_code = 0;
    size_t len;
    char* sval            = NULL;
    unsigned char* uval   = NULL;
    double* dval          = NULL;
    long* lval            = NULL;
    grib_key_err* key_err = NULL;
    grib_key_err* first   = NULL;
    int todo = 1, count = 0;

    grib_keys_iterator* iter = NULL;

    if (!dest || !src)
        return GRIB_NULL_HANDLE;

    iter = grib_keys_iterator_new(src, 0, name);

    if (!iter) {
        grib_context_log(src->context, GRIB_LOG_ERROR, "grib_copy_namespace: Unable to get iterator for %s", name);
        return GRIB_INTERNAL_ERROR;
    }

    while (grib_keys_iterator_next(iter)) {
        grib_key_err* k = (grib_key_err*)grib_context_malloc_clear(src->context, sizeof(grib_key_err));
        k->err          = GRIB_NOT_FOUND;
        k->name         = grib_context_strdup(src->context, grib_keys_iterator_get_name(iter));
        if (key_err == NULL) {
            key_err = k;
            first   = k;
        }
        else {
            key_err->next = k;
            key_err       = key_err->next;
        }
    }

    count = 0;
    todo  = 1;
    while (todo && count < 4) {
        grib_accessor* a = NULL;
        key_err          = first;
        while (key_err) {
            const char* key = key_err->name;
            err = &(key_err->err);

            if (*err == GRIB_SUCCESS) {
                key_err = key_err->next;
                continue;
            }

            if ((a = grib_find_accessor(dest, key)) == NULL) {
                key_err->err = GRIB_NOT_FOUND;
                key_err      = key_err->next;
                continue;
            }

            if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
                key_err->err = GRIB_SUCCESS;
                key_err      = key_err->next;
                continue;
            }

            if (grib_is_missing(src, key, err) && *err == 0 && (*err = grib_set_missing(dest, key))) {
                if (*err != GRIB_SUCCESS && *err != GRIB_NOT_FOUND)
                    return *err;
                key_err = key_err->next;
                continue;
            }

            if ((*err = grib_get_native_type(dest, key, &type)) != GRIB_SUCCESS) {
                if (*err != GRIB_SUCCESS && *err != GRIB_NOT_FOUND)
                    return *err;
                key_err = key_err->next;
                continue;
            }

            if ((*err = grib_get_size(src, key, &len)) != GRIB_SUCCESS)
                return *err;

            switch (type) {
                case GRIB_TYPE_STRING:
                    len  = 1024;
                    sval = (char*)grib_context_malloc(src->context, len * sizeof(char));

                    if ((*err = grib_get_string(src, key, sval, &len)) != GRIB_SUCCESS)
                        return *err;

                    if ((*err = grib_set_string(dest, key, sval, &len)) != GRIB_SUCCESS)
                        return *err;

                    grib_context_free(src->context, sval);
                    break;

                case GRIB_TYPE_LONG:
                    lval = (long*)grib_context_malloc(src->context, len * sizeof(long));

                    if ((*err = grib_get_long_array(src, key, lval, &len)) != GRIB_SUCCESS)
                        return *err;

                    if ((*err = grib_set_long_array(dest, key, lval, len)) != GRIB_SUCCESS)
                        return *err;

                    grib_context_free(src->context, lval);
                    break;

                case GRIB_TYPE_DOUBLE:
                    dval = (double*)grib_context_malloc(src->context, len * sizeof(double));

                    if ((*err = grib_get_double_array(src, key, dval, &len)) != GRIB_SUCCESS)
                        return *err;

                    if ((*err = grib_set_double_array(dest, key, dval, len)) != GRIB_SUCCESS)
                        return *err;

                    grib_context_free(src->context, dval);
                    break;

                case GRIB_TYPE_BYTES:
                    len = 1024;
                    uval = (unsigned char*)grib_context_malloc(src->context, len * sizeof(unsigned char));

                    if ((*err = grib_get_bytes(src, key, uval, &len)) != GRIB_SUCCESS)
                        return *err;

                    if ((*err = grib_get_bytes(dest, key, uval, &len)) != GRIB_SUCCESS)
                        return *err;

                    grib_context_free(src->context, uval);

                    break;

                default:
                    break;
            }
            key_err = key_err->next;
        }
        count++;
        key_err = first;
        todo    = 0;
        while (key_err) {
            if (key_err->err == GRIB_NOT_FOUND) {
                todo = 1;
                break;
            }
            key_err = key_err->next;
        }
    }
    if (err)
        error_code = *err; // copy the error code before cleanup
    grib_keys_iterator_delete(iter);
    key_err = first;
    while (key_err) {
        grib_key_err* next = key_err->next;
        grib_context_free(src->context, key_err->name);
        grib_context_free(src->context, key_err);
        key_err = next;
    }

    return error_code;
}

int grib_set_double(grib_handle* h, const char* name, double val)
{
    int ret          = GRIB_SUCCESS;
    grib_accessor* a = NULL;
    size_t l         = 1;

    a = grib_find_accessor(h, name);

    if (a) {
        if (h->context->debug) {
            if (strcmp(name, a->name_)!=0)
                fprintf(stderr, "ECCODES DEBUG grib_set_double h=%p %s=%.10g (a->name_=%s)\n", (void*)h, name, val, a->name_);
            else
                fprintf(stderr, "ECCODES DEBUG grib_set_double h=%p %s=%.10g\n", (void*)h, name, val);
        }

        if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
            return GRIB_READ_ONLY;

        ret = a->pack_double(&val, &l);
        if (ret == GRIB_SUCCESS)
            return grib_dependency_notify_change(a);

        return ret;
    }
    return GRIB_NOT_FOUND;
}

int grib_set_string_internal(grib_handle* h, const char* name,
                             const char* val, size_t* length)
{
    int ret          = GRIB_SUCCESS;
    grib_accessor* a = NULL;

    a = grib_find_accessor(h, name);

    if (h->context->debug)
        fprintf(stderr, "ECCODES DEBUG grib_set_string_internal h=%p %s=%s\n", (void*)h, name, val);

    if (a) {
        ret = a->pack_string(val, length);
        if (ret == GRIB_SUCCESS) {
            return grib_dependency_notify_change(a);
        }

        grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to set %s=%s as string (%s)",
                         name, val, grib_get_error_message(ret));
        return ret;
    }

    print_error_no_accessor(h->context, name);
    //grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to find accessor %s", name);
    return GRIB_NOT_FOUND;
}

// Return 1 if we dealt with specific packing type changes and nothing more needs doing.
// Return 0 if further action is needed
static int preprocess_packingType_change(grib_handle* h, const char* keyname, const char* keyval)
{
    int err = 0;
    char input_packing_type[100] = {0,};
    size_t len = sizeof(input_packing_type);

    if (grib_inline_strcmp(keyname, "packingType") == 0) {
        if (strcmp(keyval, "grid_ccsds")==0) {
            // ECC-2021: packingType being changed to CCSDS
            long isGridded = -1;
            if ((err = grib_get_long(h, "isGridded", &isGridded)) == GRIB_SUCCESS && isGridded == 0) {
                if (h->context->debug) {
                    fprintf(stderr, "ECCODES DEBUG grib_set_string packingType: "
                            "CCSDS packing does not apply to spectral fields. Packing not changed\n");
                }
                return 1;  /* Dealt with - no further action needed */
            }
        }
        /* Second order doesn't have a proper representation for constant fields.
           So best not to do the change of packing type.
           Use strncmp to catch all flavours of 2nd order packing e.g. grid_second_order_boustrophedonic */
        if (strncmp(keyval, "grid_second_order", 17) == 0) {
            // packingType being changed to grid_second_order
            long bitsPerValue   = 0;
            size_t numCodedVals = 0;
            err = grib_get_long(h, "bitsPerValue", &bitsPerValue);
            if (!err && bitsPerValue == 0) {
                /* ECC-1219: packingType conversion from grid_ieee to grid_second_order.
                 * Normally having a bitsPerValue of 0 means a constant field but this is
                 * not so for IEEE packing which can be non-constant but always has bitsPerValue==0! */
                len = sizeof(input_packing_type);
                grib_get_string(h, "packingType", input_packing_type, &len);
                if (strcmp(input_packing_type, "grid_ieee") != 0) {
                    /* Not IEEE, so bitsPerValue==0 really means constant field */
                    if (h->context->debug) {
                        fprintf(stderr, "ECCODES DEBUG grib_set_string packingType: "
                                "Constant field cannot be encoded in second order. Packing not changed\n");
                    }
                    return 1; /* Dealt with - no further action needed */
                }
            }
            /* GRIB-883: check if there are enough coded values */
            err = grib_get_size(h, "codedValues", &numCodedVals);
            if (!err && numCodedVals < 3) {
                if (h->context->debug) {
                    fprintf(stderr, "ECCODES DEBUG grib_set_string packingType: "
                            "Not enough coded values for second order. Packing not changed\n");
                }
                return 1; /* Dealt with - no further action needed */
            }
        }

        /* ECC-1407: Are we changing from IEEE to CCSDS or Simple? */
        if (strcmp(keyval, "grid_simple")==0 || strcmp(keyval, "grid_ccsds")==0) {
            grib_get_string(h, "packingType", input_packing_type, &len);
            if (strcmp(input_packing_type, "grid_ieee") == 0) {
                const long max_bpv = 32; /* Cannot do any higher */
                grib_set_long(h, "bitsPerValue", max_bpv);
                //long accuracy = 0;
                //err = grib_get_long(h, "accuracy", &accuracy);
                //if (!err) grib_set_long(h, "bitsPerValue", accuracy);
            }
        }
    }
    return 0;  /* Further action is needed */
}

static void postprocess_packingType_change(grib_handle* h, const char* keyname, const char* keyval)
{
    if (grib_inline_strcmp(keyname, "packingType") == 0) {
        long is_experimental = 0, is_deprecated = 0;
        if (grib_get_long(h, "isTemplateExperimental", &is_experimental) == GRIB_SUCCESS && is_experimental == 1) {
            fprintf(stderr, "ECCODES WARNING :  The template for %s=%s is experimental. "
                            "This template was not validated at the time of publication.\n",
                    keyname, keyval);
            return;
        }
        if (grib_get_long(h, "isTemplateDeprecated", &is_deprecated) == GRIB_SUCCESS && is_deprecated == 1) {
            fprintf(stderr, "ECCODES WARNING :  The template for %s=%s is deprecated.\n", keyname, keyval);
        }
    }
}

int grib_set_string(grib_handle* h, const char* name, const char* val, size_t* length)
{
    int ret          = 0;
    grib_accessor* a = NULL;

    int processed = preprocess_packingType_change(h, name, val);
    if (processed)
        return GRIB_SUCCESS;  // Dealt with - no further action needed

    a = grib_find_accessor(h, name);

    if (a) {
        if (h->context->debug) {
            if (strcmp(name, a->name_)!=0)
                fprintf(stderr, "ECCODES DEBUG grib_set_string h=%p %s=|%s| (a->name_=%s)\n", (void*)h, name, val, a->name_);
            else
                fprintf(stderr, "ECCODES DEBUG grib_set_string h=%p %s=|%s|\n", (void*)h, name, val);
        }

        if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
            return GRIB_READ_ONLY;

        ret = a->pack_string(val, length);
        if (ret == GRIB_SUCCESS) {
            postprocess_packingType_change(h, name, val);
            return grib_dependency_notify_change(a);
        }
        return ret;
    }

    if (h->context->debug) {
        fprintf(stderr, "ECCODES DEBUG grib_set_string %s=|%s| (Key not found)\n", name, val);
    }

    return GRIB_NOT_FOUND;
}

int grib_set_string_array(grib_handle* h, const char* name, const char** val, size_t length)
{
    int ret = 0;
    grib_accessor* a;

    a = grib_find_accessor(h, name);

    if (h->context->debug) {
        fprintf(stderr, "ECCODES DEBUG grib_set_string_array h=%p key=%s %zu values\n", (void*)h, name, length);
    }

    if (a) {
        if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
            return GRIB_READ_ONLY;

        ret = a->pack_string_array(val, &length);
        if (ret == GRIB_SUCCESS) {
            return grib_dependency_notify_change(a);
        }
        return ret;
    }
    return GRIB_NOT_FOUND;
}

// int grib_set_bytes_internal(grib_handle* h, const char* name, const unsigned char* val, size_t* length)
// {
//     int ret          = GRIB_SUCCESS;
//     grib_accessor* a = NULL;
//     a = grib_find_accessor(h, name);
//     if (a) {
//         ret = a->pack_bytes(val, length);
//         if (ret == GRIB_SUCCESS) {
//             return grib_dependency_notify_change(a);
//         }
//         grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to set %s=%s as bytes (%s)",
//                          name, val, grib_get_error_message(ret));
//         return ret;
//     }
//     grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to find accessor %s", name);
//     return GRIB_NOT_FOUND;
// }

int grib_set_bytes(grib_handle* h, const char* name, const unsigned char* val, size_t* length)
{
    int ret          = 0;
    grib_accessor* a = grib_find_accessor(h, name);

    if (a) {
        // if(a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
        // return GRIB_READ_ONLY;

        ret = a->pack_bytes(val, length);
        if (ret == GRIB_SUCCESS) {
            return grib_dependency_notify_change(a);
        }
        return ret;
    }
    return GRIB_NOT_FOUND;
}

// int grib_clear(grib_handle* h, const char* name)
// {
//     int ret          = 0;
//     grib_accessor* a = NULL;
//     a = grib_find_accessor(h, name);
//     if (a) {
//         if (a->length_ == 0)
//             return 0;
//         if ((ret = a->grib_pack_zero()) != GRIB_SUCCESS)
//             grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to clear %s (%s)",
//                              name, grib_get_error_message(ret));
//         return ret;
//     }
//     /*grib_context_log(h->context,GRIB_LOG_ERROR,"Unable to find accessor %s",name);*/
//     return GRIB_NOT_FOUND;
// }

int grib_set_missing(grib_handle* h, const char* name)
{
    int ret          = 0;
    grib_accessor* a = NULL;

    a = grib_find_accessor(h, name);

    if (a) {
        if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
            return GRIB_READ_ONLY;

        if (grib_accessor_can_be_missing(a, &ret)) {
            if (h->context->debug)
                fprintf(stderr, "ECCODES DEBUG grib_set_missing h=%p %s\n", (void*)h, name);

            ret = a->pack_missing();
            if (ret == GRIB_SUCCESS)
                return grib_dependency_notify_change(a);
        }
        else
            ret = GRIB_VALUE_CANNOT_BE_MISSING;

        grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to set %s=missing (%s)",
                         name, grib_get_error_message(ret));
        return ret;
    }

    grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to find accessor %s", name);
    return GRIB_NOT_FOUND;
}

int grib_is_missing_long(grib_accessor* a, long x)
{
    int ret = (a == NULL || (a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING)) && (x == GRIB_MISSING_LONG) ? 1 : 0;
    return ret;
}
int grib_is_missing_double(grib_accessor* a, double x)
{
    int ret = (a == NULL || (a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING)) && (x == GRIB_MISSING_DOUBLE) ? 1 : 0;
    return ret;
}

int grib_is_missing_string(grib_accessor* a, const unsigned char* x, size_t len)
{
    // For a string value to be missing, every character has to be */
    // all 1's (i.e. 0xFF) */
    // Note: An empty string is also classified as missing */
    int ret;
    size_t i = 0;

    if (len == 0)
        return 1; // empty string
    ret = 1;
    for (i = 0; i < len; i++) {
        if (x[i] != 0xFF) {
            ret = 0;
            break;
        }
    }

    if (!a) return ret;

    ret = ( ((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) && ret == 1) ) ? 1 : 0;
    return ret;
}

int grib_accessor_is_missing(grib_accessor* a, int* err)
{
    *err = GRIB_SUCCESS;
    if (a) {
        if (a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING)
            return a->is_missing_internal();
        else
            return 0;
    }
    else {
        *err = GRIB_NOT_FOUND;
        return 1;
    }
}

int grib_accessor_can_be_missing(grib_accessor* a, int* err)
{
    if (a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) {
        return 1;
    }
    if (STR_EQUAL(a->class_name_, "codetable")) {
        // Special case of Code Table keys
        // The vast majority have a 'Missing' entry
        return 1;
    }
    return 0;
}

int grib_is_missing(const grib_handle* h, const char* name, int* err)
{
    grib_accessor* a = grib_find_accessor(h, name);
    return grib_accessor_is_missing(a, err);
}

// Return true if the given key exists (is defined) in our grib message
int grib_is_defined(const grib_handle* h, const char* name)
{
    const grib_accessor* a = grib_find_accessor(h, name);
    return (a ? 1 : 0);
}

int grib_set_flag(grib_handle* h, const char* name, unsigned long flag)
{
    grib_accessor* a = grib_find_accessor(h, name);

    if (!a)
        return GRIB_NOT_FOUND;

    a->flags_ |= flag;

    return GRIB_SUCCESS;
}

static int _grib_set_double_array_internal(grib_handle* h, grib_accessor* a,
                                           const double* val, size_t buffer_len, size_t* encoded_length, int check)
{
    if (a) {
        int err = _grib_set_double_array_internal(h, a->same_, val, buffer_len, encoded_length, check);

        if (check && (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY))
            return GRIB_READ_ONLY;

        if (err == GRIB_SUCCESS) {
            size_t len = buffer_len - *encoded_length;
            if (len) {
                err = a->pack_double(val + *encoded_length, &len);
                *encoded_length += len;
                if (err == GRIB_SUCCESS) {
                    // See ECC-778
                    return grib_dependency_notify_change_h(h, a);
                }
            }
            else {
                grib_get_size(h, a->name_, encoded_length);
                err = GRIB_WRONG_ARRAY_SIZE;
            }
        }

        return err;
    }
    else {
        return GRIB_SUCCESS;
    }
}

static int _grib_set_double_array(grib_handle* h, const char* name,
                                  const double* val, size_t length, int check)
{
    size_t encoded   = 0;
    grib_accessor* a = grib_find_accessor(h, name);
    int err          = 0;

    if (!a)
        return GRIB_NOT_FOUND;
    if (name[0] == '/' || name[0] == '#') {
        if (check && (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY))
            return GRIB_READ_ONLY;
        err     = a->pack_double(val, &length);
        encoded = length;
    }
    else
        err = _grib_set_double_array_internal(h, a, val, length, &encoded, check);

    if (err == GRIB_SUCCESS && length > encoded)
        err = GRIB_ARRAY_TOO_SMALL;

    if (err == GRIB_SUCCESS)
        return grib_dependency_notify_change_h(h, a); // See ECC-778

    return err;
}

int grib_set_double_array_internal(grib_handle* h, const char* name, const double* val, size_t length)
{
    int ret = 0;

    if (h->context->debug) {
        print_debug_info__set_array(h, __func__, name, val, length);
    }

    if (length == 0) {
        grib_accessor* a = grib_find_accessor(h, name);
        ret              = a->pack_double(val, &length);
    }
    else {
        ret = _grib_set_double_array(h, name, val, length, 0);
    }

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to set double array '%s' (%s)",
                         name, grib_get_error_message(ret));
    //if (h->context->debug) fprintf(stderr,"ECCODES DEBUG grib_set_double_array_internal key=%s --DONE\n",name);
    return ret;
}

int grib_set_float_array_internal(grib_handle* h, const char* name, const float* val, size_t length)
{
    return GRIB_NOT_IMPLEMENTED;
}

static int __grib_set_double_array(grib_handle* h, const char* name, const double* val, size_t length, int check)
{
    double v = 0;
    size_t i = 0;

    if (h->context->debug) {
        print_debug_info__set_array(h, __func__, name, val, length);
    }

    if (length == 0) {
        grib_accessor* a = grib_find_accessor(h, name);
        return a->pack_double(val, &length);
    }

    /*second order doesn't have a proper representation for constant fields
      the best is not to do the change of packing type if the field is constant
     */
    if (!strcmp(name, "values") || !strcmp(name, "codedValues")) {
        double missingValue;
        int ret      = 0;
        int constant = 0;

        ret = grib_get_double(h, "missingValue", &missingValue);
        if (ret)
            missingValue = 9999;

        v        = missingValue;
        constant = 1;
        for (i = 0; i < length; i++) {
            if (val[i] != missingValue) {
                if (v == missingValue) {
                    v = val[i];
                }
                else if (v != val[i]) {
                    constant = 0;
                    break;
                }
            }
        }
        if (constant) {
            char packingType[50] = {0,};
            size_t slen = 50;

            grib_get_string(h, "packingType", packingType, &slen);
            if (!strcmp(packingType, "grid_second_order") ||
                !strcmp(packingType, "grid_second_order_no_SPD") ||
                !strcmp(packingType, "grid_second_order_SPD1") ||
                !strcmp(packingType, "grid_second_order_SPD2") ||
                !strcmp(packingType, "grid_second_order_SPD3")) {
                slen = 11; /*length of 'grid_simple' */
                if (h->context->debug) {
                    fprintf(stderr, "ECCODES DEBUG __grib_set_double_array: Cannot use second order packing for constant fields. Using simple packing\n");
                }
                ret = grib_set_string(h, "packingType", "grid_simple", &slen);
                if (ret != GRIB_SUCCESS) {
                    if (h->context->debug) {
                        fprintf(stderr, "ECCODES DEBUG __grib_set_double_array: could not switch to simple packing!\n");
                    }
                }
            }
        }
    }

    return _grib_set_double_array(h, name, val, length, check);
}

int grib_set_force_double_array(grib_handle* h, const char* name, const double* val, size_t length)
{
    /* GRIB-285: Same as grib_set_double_array but allows setting of READ-ONLY keys like codedValues */
    /* Use with great caution!! */
    return __grib_set_double_array(h, name, val, length, /*check=*/0);
}
int grib_set_force_float_array(grib_handle* h, const char* name, const float* val, size_t length)
{
    /* GRIB-285: Same as grib_set_float_array but allows setting of READ-ONLY keys like codedValues */
    /* Use with great caution!! */
    //return __grib_set_float_array(h, name, val, length, /*check=*/0);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_set_double_array(grib_handle* h, const char* name, const double* val, size_t length)
{
    return __grib_set_double_array(h, name, val, length, /*check=*/1);
}
int grib_set_float_array(grib_handle* h, const char* name, const float* val, size_t length)
{
    //return __grib_set_float_array(h, name, val, length, /*check=*/1);
    return GRIB_NOT_IMPLEMENTED;
}

static int _grib_set_long_array_internal(grib_handle* h, grib_accessor* a, const long* val, size_t buffer_len, size_t* encoded_length, int check)
{
    if (a) {
        int err = _grib_set_long_array_internal(h, a->same_, val, buffer_len, encoded_length, check);

        if (check && (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY))
            return GRIB_READ_ONLY;

        if (err == GRIB_SUCCESS) {
            size_t len = buffer_len - *encoded_length;
            if (len) {
                err = a->pack_long(val + *encoded_length, &len);
                *encoded_length += len;
            }
            else {
                grib_get_size(h, a->name_, encoded_length);
                err = GRIB_WRONG_ARRAY_SIZE;
            }
        }

        return err;
    }
    else {
        return GRIB_SUCCESS;
    }
}

static int _grib_set_long_array(grib_handle* h, const char* name, const long* val, size_t length, int check)
{
    size_t encoded   = 0;
    grib_accessor* a = grib_find_accessor(h, name);
    int err          = 0;

    if (!a)
        return GRIB_NOT_FOUND;

    if (h->context->debug) {
        size_t i = 0;
        size_t N = 5;
        if (length <= N)
            N = length;
        fprintf(stderr, "ECCODES DEBUG _grib_set_long_array h=%p key=%s %zu values (", (void*)h, name, length);
        for (i = 0; i < N; ++i)
            fprintf(stderr, " %ld,", val[i]);
        if (N >= length)
            fprintf(stderr, " )\n");
        else
            fprintf(stderr, " ... )\n");
    }

    if (name[0] == '/' || name[0] == '#') {
        if (check && (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY))
            return GRIB_READ_ONLY;
        err     = a->pack_long(val, &length);
        encoded = length;
    }
    else
        err = _grib_set_long_array_internal(h, a, val, length, &encoded, check);

    if (err == GRIB_SUCCESS && length > encoded)
        err = GRIB_ARRAY_TOO_SMALL;

    if (err == GRIB_SUCCESS)
        return grib_dependency_notify_change(a);

    return err;
}

int grib_set_long_array_internal(grib_handle* h, const char* name, const long* val, size_t length)
{
    int ret = _grib_set_long_array(h, name, val, length, 0);
    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to set long array '%s' (%s)",
                         name, grib_get_error_message(ret));
    return ret;
}

int grib_set_long_array(grib_handle* h, const char* name, const long* val, size_t length)
{
    return _grib_set_long_array(h, name, val, length, 1);
}

int grib_get_long_internal(grib_handle* h, const char* name, long* val)
{
    int ret = grib_get_long(h, name, val);

    if (ret != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as long (%s)",
                         name, grib_get_error_message(ret));
    }

    return ret;
}

// int grib_is_in_dump(const grib_handle* h, const char* name)
// {
//     const grib_accessor* a = grib_find_accessor(h, name);
//     if (a != NULL && (a->flags_ & GRIB_ACCESSOR_FLAG_DUMP))
//         return 1;
//     else
//         return 0;
// }

// int grib_attributes_count(const grib_accessor* a, size_t* size)
// {
//     if (a) {
//         *size = 0;
//         while (a->attributes_[*size] != NULL) {
//             (*size)++;
//         }
//         return GRIB_SUCCESS;
//     }
//     return GRIB_NOT_FOUND;
// }

int grib_get_long(const grib_handle* h, const char* name, long* val)
{
    size_t length           = 1;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->accessor->unpack_long(val, &length);
        grib_context_free(h->context, al);
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        ret = a->unpack_long(val, &length);
    }
    return ret;
}

int grib_get_double_internal(grib_handle* h, const char* name, double* val)
{
    int ret = grib_get_double(h, name, val);

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as double (%s)",
                         name, grib_get_error_message(ret));

    return ret;
}

int grib_get_double(const grib_handle* h, const char* name, double* val)
{
    size_t length           = 1;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->accessor->unpack_double(val, &length);
        grib_context_free(h->context, al);
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        ret = a->unpack_double(val, &length);
    }
    return ret;
}

int grib_get_float(const grib_handle* h, const char* name, float* val)
{
    size_t length           = 1;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->accessor->unpack_float(val, &length);
        grib_context_free(h->context, al);
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        ret = a->unpack_float(val, &length);
    }
    return ret;
}

int grib_get_double_element_internal(grib_handle* h, const char* name, int i, double* val)
{
    int ret = grib_get_double_element(h, name, i, val);

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as double element (%s)",
                         name, grib_get_error_message(ret));

    return ret;
}

int grib_get_double_element(const grib_handle* h, const char* name, int i, double* val)
{
    grib_accessor* act = grib_find_accessor(h, name);

    if (act) {
        return act->unpack_double_element(i, val);
    }
    return GRIB_NOT_FOUND;
}
int grib_get_float_element(const grib_handle* h, const char* name, int i, float* val)
{
    grib_accessor* act = grib_find_accessor(h, name);

    if (act) {
        return act->unpack_float_element(i, val);
    }
    return GRIB_NOT_FOUND;
}

int grib_get_double_element_set_internal(grib_handle* h, const char* name, const size_t* index_array, size_t len, double* val_array)
{
    int ret = grib_get_double_element_set(h, name, index_array, len, val_array);

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as double element set (%s)",
                         name, grib_get_error_message(ret));

    return ret;
}
int grib_get_float_element_set_internal(grib_handle* h, const char* name, const size_t* index_array, size_t len, float* val_array)
{
    int ret = grib_get_float_element_set(h, name, index_array, len, val_array);

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as float element set (%s)",
                         name, grib_get_error_message(ret));

    return ret;
}

int grib_get_double_element_set(const grib_handle* h, const char* name, const size_t* index_array, size_t len, double* val_array)
{
    grib_accessor* acc = grib_find_accessor(h, name);

    if (acc) {
        return acc->unpack_double_element_set(index_array, len, val_array);
    }
    return GRIB_NOT_FOUND;
}
int grib_get_float_element_set(const grib_handle* h, const char* name, const size_t* index_array, size_t len, float* val_array)
{
    grib_accessor* acc = grib_find_accessor(h, name);

    if (acc) {
        return acc->unpack_float_element_set(index_array, len, val_array);
    }
    return GRIB_NOT_FOUND;
}

int grib_get_double_elements(const grib_handle* h, const char* name, const int* index_array, long len, double* val_array)
{
    double* values = 0;
    int err        = 0;
    size_t size = 0, num_bytes = 0;
    long j             = 0;
    grib_accessor* act = NULL;

    act = grib_find_accessor(h, name);
    if (!act)
        return GRIB_NOT_FOUND;

    err = grib_get_size_acc(h, act, &size);

    if (err != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Cannot get size of %s", __func__, name);
        return err;
    }

    /* Check index array has valid values */
    for (j = 0; j < len; j++) {
        const int anIndex = index_array[j];
        if (anIndex < 0 || anIndex >= size) {
            grib_context_log(h->context, GRIB_LOG_ERROR,
                             "%s: Index out of range: %d (should be between 0 and %zu)", __func__, anIndex, size - 1);
            return GRIB_INVALID_ARGUMENT;
        }
    }

    num_bytes = size * sizeof(double);
    values    = (double*)grib_context_malloc(h->context, num_bytes);
    if (!values) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "%s: Unable to allocate %zu bytes", __func__, num_bytes);
        return GRIB_OUT_OF_MEMORY;
    }

    err = act->unpack_double(values, &size);
    if (!err) {
        for (j = 0; j < len; j++) {
            val_array[j] = values[index_array[j]];
        }
    }

    grib_context_free(h->context, values);

    return err;
}
int grib_get_float_elements(const grib_handle* h, const char* name, const int* index_array, long len, float* val_array)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_get_string_internal(grib_handle* h, const char* name, char* val, size_t* length)
{
    int ret = grib_get_string(h, name, val, length);

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as string (%s)",
                         name, grib_get_error_message(ret));

    return ret;
}

int grib_get_string(const grib_handle* h, const char* name, char* val, size_t* length)
{
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->accessor->unpack_string(val, length);
        grib_context_free(h->context, al);
        return ret;
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        return a->unpack_string(val, length);
    }
}

// int grib_get_bytes_internal(const grib_handle* h, const char* name, unsigned char* val, size_t* length)
// {
//     int ret = grib_get_bytes(h, name, val, length);
//     if (ret != GRIB_SUCCESS)
//         grib_context_log(h->context, GRIB_LOG_ERROR,
//                          "Unable to get %s as bytes (%s)",
//                          name, grib_get_error_message(ret));
//     return ret;
// }

int grib_get_bytes(const grib_handle* h, const char* name, unsigned char* val, size_t* length)
{
    int err            = 0;
    grib_accessor* act = grib_find_accessor(h, name);
    err                = act ? act->unpack_bytes(val, length) : GRIB_NOT_FOUND;
    if (err)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "grib_get_bytes %s failed %s", name, grib_get_error_message(err));
    return err;
}

int grib_get_native_type(const grib_handle* h, const char* name, int* type)
{
    grib_accessors_list* al = NULL;
    grib_accessor* a        = NULL;
    *type                   = GRIB_TYPE_UNDEFINED;

    DEBUG_ASSERT(name != NULL && strlen(name) > 0);

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        *type = al->accessor->get_native_type();
        grib_context_free(h->context, al);
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        *type = a->get_native_type();
    }

    return GRIB_SUCCESS;
}

// const char* grib_get_accessor_class_name(grib_handle* h, const char* name)
// {
//     grib_accessor* act = grib_find_accessor(h, name);
//     return act ? act->cclass->name : NULL;
// }

template <typename T>
static int _grib_get_array_internal(const grib_handle* h, grib_accessor* a, T* val, size_t buffer_len, size_t* decoded_length)
{
    static_assert(std::is_floating_point<T>::value, "Requires floating point numbers");
    if (a) {
        int err = _grib_get_array_internal<T>(h, a->same_, val, buffer_len, decoded_length);

        if (err == GRIB_SUCCESS) {
            size_t len = buffer_len - *decoded_length;
            if constexpr (std::is_same<T, double>::value) {
                err = a->unpack_double(val + *decoded_length, &len);
            }
            else if constexpr (std::is_same<T, float>::value) {
                err = a->unpack_float(val + *decoded_length, &len);
            }
            *decoded_length += len;
        }

        return err;
    }
    else {
        return GRIB_SUCCESS;
    }
}

int grib_get_double_array_internal(const grib_handle* h, const char* name, double* val, size_t* length)
{
    return grib_get_array_internal<double>(h, name, val, length);
}

int grib_get_float_array_internal(const grib_handle* h, const char* name, float* val, size_t* length)
{
    return grib_get_array_internal<float>(h, name, val, length);
}

int grib_get_double_array(const grib_handle* h, const char* name, double* val, size_t* length)
{
    size_t len              = *length;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->unpack_double(val, length);
        grib_accessors_list_delete(h->context, al);
        return ret;
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        if (name[0] == '#') {
            return a->unpack_double(val, length);
        }
        else {
            *length = 0;
            return _grib_get_array_internal<double>(h, a, val, len, length);
        }
    }
}

int grib_get_float_array(const grib_handle* h, const char* name, float* val, size_t *length)
{
    size_t len = *length;
    grib_accessor* a = grib_find_accessor(h, name);
    if (!a) return GRIB_NOT_FOUND;

    //[> TODO: For now only GRIB supported... no BUFR keys <]
    if (h->product_kind != PRODUCT_GRIB) {
        //grib_context_log(h->context, GRIB_LOG_ERROR, "grib_get_float_array only supported for GRIB");
        return GRIB_NOT_IMPLEMENTED;
    }
    ECCODES_ASSERT(name[0]!='/');
    ECCODES_ASSERT(name[0]!='#');
    *length = 0;
    return _grib_get_array_internal<float>(h,a,val,len,length);
}

template <>
int grib_get_array<float>(const grib_handle* h, const char* name, float* val, size_t *length)
{
    return grib_get_float_array(h, name, val, length);
}

template <>
int grib_get_array<double>(const grib_handle* h, const char* name, double* val, size_t* length)
{
    return grib_get_double_array(h, name, val, length);
}

int grib_get_string_length_acc(grib_accessor* a, size_t* size)
{
    size_t s = 0;

    *size = 0;
    while (a) {
        s = a->string_length();
        if (s > *size)
            *size = s;
        a = a->same_;
    }
    (*size) += 1;

    return GRIB_SUCCESS;
}

int grib_get_string_length(const grib_handle* h, const char* name, size_t* size)
{
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = grib_get_string_length_acc(al->accessor, size);
        grib_context_free(h->context, al);
        return ret;
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        return grib_get_string_length_acc(a, size);
    }
}

int grib_get_size_acc(const grib_handle* h, grib_accessor* a, size_t* size)
{
    long count = 0;
    int err    = 0;

    if (!a)
        return GRIB_NOT_FOUND;

    *size = 0;
    while (a) {
        if (err == 0) {
            err = a->value_count(&count);
            if (err)
                return err;
            *size += count;
        }
        a = a->same_;
    }
    return GRIB_SUCCESS;
}

int grib_get_size(const grib_handle* ch, const char* name, size_t* size)
{
    grib_handle* h          = (grib_handle*)ch;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;
    *size                   = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->value_count(size);
        grib_accessors_list_delete(h->context, al);
        return ret;
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        if (name[0] == '#') {
            long count = *size;
            ret        = a->value_count(&count);
            *size      = count;
            return ret;
        }
        else
            return grib_get_size_acc(h, a, size);
    }
}

int grib_get_length(const grib_handle* h, const char* name, size_t* length)
{
    return grib_get_string_length(h, name, length);
}

// int grib_get_count(grib_handle* h, const char* name, size_t* size)
// {
//     grib_accessor* a = grib_find_accessor(h, name);
//     if (!a)
//         return GRIB_NOT_FOUND;
//     *size = 0;
//     while (a) {
//         (*size)++;
//         a = a->same_;
//     }
//     return GRIB_SUCCESS;
// }

int grib_get_offset(const grib_handle* ch, const char* key, size_t* val)
{
    const grib_handle* h     = (grib_handle*)ch;
    grib_accessor* act = grib_find_accessor(h, key);
    if (act) {
        *val = (size_t) act->byte_offset();
        return GRIB_SUCCESS;
    }
    return GRIB_NOT_FOUND;
}

static int grib_get_string_array_internal_(const grib_handle* h, grib_accessor* a, char** val, size_t buffer_len, size_t* decoded_length)
{
    if (a) {
        int err = grib_get_string_array_internal_(h, a->same_, val, buffer_len, decoded_length);

        if (err == GRIB_SUCCESS) {
            size_t len = buffer_len - *decoded_length;
            err        = a->unpack_string_array(val + *decoded_length, &len);
            *decoded_length += len;
        }

        return err;
    }
    else {
        return GRIB_SUCCESS;
    }
}

int grib_get_string_array(const grib_handle* h, const char* name, char** val, size_t* length)
{
    size_t len              = *length;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->unpack_string(val, length);
        grib_context_free(h->context, al);
        return ret;
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        if (name[0] == '#') {
            return a->unpack_string_array(val, length);
        }
        else {
            *length = 0;
            return grib_get_string_array_internal_(h, a, val, len, length);
        }
    }
}

static int _grib_get_long_array_internal(const grib_handle* h, grib_accessor* a, long* val, size_t buffer_len, size_t* decoded_length)
{
    if (a) {
        int err = _grib_get_long_array_internal(h, a->same_, val, buffer_len, decoded_length);

        if (err == GRIB_SUCCESS) {
            size_t len = buffer_len - *decoded_length;
            err        = a->unpack_long(val + *decoded_length, &len);
            *decoded_length += len;
        }

        return err;
    }
    else {
        return GRIB_SUCCESS;
    }
}

int grib_get_long_array_internal(grib_handle* h, const char* name, long* val, size_t* length)
{
    int ret = grib_get_long_array(h, name, val, length);

    if (ret != GRIB_SUCCESS)
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Unable to get %s as long array (%s)",
                         name, grib_get_error_message(ret));

    return ret;
}

int grib_get_long_array(const grib_handle* h, const char* name, long* val, size_t* length)
{
    size_t len              = *length;
    grib_accessor* a        = NULL;
    grib_accessors_list* al = NULL;
    int ret                 = 0;

    if (name[0] == '/') {
        al = grib_find_accessors_list(h, name);
        if (!al)
            return GRIB_NOT_FOUND;
        ret = al->unpack_long(val, length);
        grib_context_free(h->context, al);
    }
    else {
        a = grib_find_accessor(h, name);
        if (!a)
            return GRIB_NOT_FOUND;
        if (name[0] == '#') {
            return a->unpack_long(val, length);
        }
        else {
            *length = 0;
            return _grib_get_long_array_internal(h, a, val, len, length);
        }
    }
    return ret;
}

// static void grib_clean_key_value(grib_context* c, grib_key_value_list* kv)
// {
//     if (kv->long_value)
//         grib_context_free(c, kv->long_value);
//     kv->long_value = NULL;
//     if (kv->double_value)
//         grib_context_free(c, kv->double_value);
//     kv->double_value = NULL;
//     if (kv->string_value)
//         grib_context_free(c, kv->string_value);
//     kv->string_value = NULL;
//     if (kv->namespace_value)
//         grib_key_value_list_delete(c, kv->namespace_value);
//     kv->namespace_value = NULL;
//     kv->error           = 0;
//     kv->has_value       = 0;
//     kv->size            = 0;
// }

// static int grib_get_key_value(grib_handle* h, grib_key_value_list* kv)
// {
//     int err                   = 0;
//     size_t size               = 0;
//     grib_keys_iterator* iter  = NULL;
//     grib_key_value_list* list = NULL;
//     if (kv->has_value)
//         grib_clean_key_value(h->context, kv);
//     err = grib_get_size(h, kv->name, &size);
//     if (err) {
//         kv->error = err;
//         return err;
//     }
//     if (size == 0)
//         size = 512;
//     switch (kv->type) {
//         case GRIB_TYPE_LONG:
//             kv->long_value = (long*)grib_context_malloc_clear(h->context, size * sizeof(long));
//             err            = grib_get_long_array(h, kv->name, kv->long_value, &size);
//             kv->error      = err;
//             break;
//         case GRIB_TYPE_DOUBLE:
//             kv->double_value = (double*)grib_context_malloc_clear(h->context, size * sizeof(double));
//             err              = grib_get_double_array(h, kv->name, kv->double_value, &size);
//             kv->error        = err;
//             break;
//         case GRIB_TYPE_STRING:
//             grib_get_string_length(h, kv->name, &size);
//             kv->string_value = (char*)grib_context_malloc_clear(h->context, size * sizeof(char));
//             err              = grib_get_string(h, kv->name, kv->string_value, &size);
//             kv->error        = err;
//             break;
//         case GRIB_TYPE_BYTES:
//             kv->string_value = (char*)grib_context_malloc_clear(h->context, size * sizeof(char));
//             err              = grib_get_bytes(h, kv->name, (unsigned char*)kv->string_value, &size);
//             kv->error        = err;
//             break;
//         case CODES_NAMESPACE:
//             iter                = grib_keys_iterator_new(h, 0, kv->name);
//             list                = (grib_key_value_list*)grib_context_malloc_clear(h->context, sizeof(grib_key_value_list));
//             kv->namespace_value = list;
//             while (grib_keys_iterator_next(iter)) {
//                 list->name = grib_keys_iterator_get_name(iter);
//                 err        = grib_get_native_type(h, list->name, &(list->type));
//                 if (err)
//                     return err;
//                 err = grib_get_key_value(h, list);
//                 if (err)
//                     return err;
//                 list->next = (grib_key_value_list*)grib_context_malloc_clear(h->context, sizeof(grib_key_value_list));
//                 list       = list->next;
//             }
//             grib_keys_iterator_delete(iter);
//             break;
//         default:
//             err = grib_get_native_type(h, kv->name, &(kv->type));
//             if (err)
//                 return err;
//             err = grib_get_key_value(h, kv);
//             break;
//     }
//     kv->has_value = 1;
//     return err;
// }

// grib_key_value_list* grib_key_value_list_clone(grib_context* c, grib_key_value_list* list)
// {
//     grib_key_value_list* next      = list;
//     grib_key_value_list* the_clone = (grib_key_value_list*)grib_context_malloc_clear(c, sizeof(grib_key_value_list));
//     grib_key_value_list* p         = the_clone;
//     while (next && next->name) {
//         p->name = grib_context_strdup(c, next->name);
//         p->type = next->type;
//         next    = next->next;
//     }
//     return the_clone;
// }

// void grib_key_value_list_delete(grib_context* c, grib_key_value_list* kvl)
// {
//     grib_key_value_list* next = kvl;
//     grib_key_value_list* p    = NULL;
//     while (next) {
//         p = next->next;
//         if (next->type == CODES_NAMESPACE)
//             grib_key_value_list_delete(c, next->namespace_value);
//         grib_clean_key_value(c, next);
//         grib_context_free(c, next);
//         next = p;
//     }
// }

// int grib_get_key_value_list(grib_handle* h, grib_key_value_list* list)
// {
//     int ret                  = 0;
//     grib_key_value_list* kvl = list;
//     while (kvl) {
//         ret = grib_get_key_value(h, kvl);
//         kvl = kvl->next;
//     }
//     return ret;
// }

// int grib_get_values(grib_handle* h, grib_values* args, size_t count)
// {
//     int ret = 0;
//     int i   = 0;
//     for (i = 0; i < count; i++) {
//         char buff[1024] = {0,};
//         size_t len = sizeof(buff) / sizeof(*buff);
//         if (!args[i].name) {
//             args[i].error = GRIB_INVALID_ARGUMENT;
//             continue;
//         }
//         if (args[i].type == 0) {
//             args[i].error = grib_get_native_type(h, args[i].name, &(args[i].type));
//             if (args[i].error != GRIB_SUCCESS)
//                 ret = args[i].error;
//         }
//         switch (args[i].type) {
//             case GRIB_TYPE_LONG:
//                 args[i].error = grib_get_long(h, args[i].name, &(args[i].long_value));
//                 if (args[i].error != GRIB_SUCCESS)
//                     ret = args[i].error;
//                 break;
//             case GRIB_TYPE_DOUBLE:
//                 args[i].error = grib_get_double(h, args[i].name, &(args[i].double_value));
//                 if (args[i].error != GRIB_SUCCESS)
//                     ret = args[i].error;
//                 break;
//             case GRIB_TYPE_STRING:
//                 args[i].error        = grib_get_string(h, args[i].name, buff, &len);
//                 args[i].string_value = strdup(buff);
//                 if (args[i].error != GRIB_SUCCESS)
//                     ret = args[i].error;
//                 break;
//             default:
//                 args[i].error        = grib_get_string(h, args[i].name, buff, &len);
//                 args[i].string_value = strdup(buff);
//                 if (args[i].error != GRIB_SUCCESS)
//                     ret = args[i].error;
//                 break;
//         }
//     }
//     return ret;
// }

int grib_set_values(grib_handle* h, grib_values* args, size_t count)
{
    // The default behaviour is to print any error messages (not silent)
    return grib_set_values_silent(h, args, count, /*silent=*/0);
}

int grib_set_values_silent(grib_handle* h, grib_values* args, size_t count, int silent)
{
    int i, error = 0;
    int err = 0;
    size_t len;
    int more  = 1;
    int stack = h->values_stack++;

    ECCODES_ASSERT(h->values_stack < MAX_SET_VALUES - 1);

    h->values[stack]       = args;
    h->values_count[stack] = count;

    if (h->context->debug) {
        for (i = 0; i < count; i++) {
            grib_print_values("ECCODES DEBUG about to set key/value pair", &args[i], stderr, 1);
        }
    }

    for (i = 0; i < count; i++)
        args[i].error = GRIB_NOT_FOUND;

    while (more) {
        more = 0;
        for (i = 0; i < count; i++) {
            if (args[i].error != GRIB_NOT_FOUND)
                continue;

            switch (args[i].type) {
                case GRIB_TYPE_LONG:
                    error         = grib_set_long(h, args[i].name, args[i].long_value);
                    args[i].error = error;
                    if (args[i].error == GRIB_SUCCESS)
                        more = 1;
                    break;

                case GRIB_TYPE_DOUBLE:
                    args[i].error = grib_set_double(h, args[i].name, args[i].double_value);
                    if (args[i].error == GRIB_SUCCESS)
                        more = 1;
                    break;

                case GRIB_TYPE_STRING:
                    len           = strlen(args[i].string_value);
                    args[i].error = grib_set_string(h, args[i].name, args[i].string_value, &len);
                    if (args[i].error == GRIB_SUCCESS)
                        more = 1;
                    break;

                case GRIB_TYPE_MISSING:
                    args[i].error = grib_set_missing(h, args[i].name);
                    if (args[i].error == GRIB_SUCCESS)
                        more = 1;
                    break;

                default:
                    if (!silent)
                        grib_context_log(h->context, GRIB_LOG_ERROR, "grib_set_values[%d] %s invalid type %d", i, args[i].name, args[i].type);
                    args[i].error = GRIB_INVALID_ARGUMENT;
                    break;
            }
            // if (args[i].error != GRIB_SUCCESS)
            //   grib_context_log(h->context,GRIB_LOG_ERROR,"Unable to set %s (%s)",args[i].name,grib_get_error_message(args[i].error));
        }
    }

    h->values[stack]       = NULL;
    h->values_count[stack] = 0;

    h->values_stack--;

    for (i = 0; i < count; i++) {
        if (args[i].error != GRIB_SUCCESS) {
            if (!silent) {
                grib_context_log(h->context, GRIB_LOG_ERROR,
                                 "grib_set_values[%d] %s (type=%s) failed: %s (message %d)",
                                 i, args[i].name, grib_get_type_name(args[i].type),
                                 grib_get_error_message(args[i].error), h->context->handle_file_count);
            }
            err = err == GRIB_SUCCESS ? args[i].error : err;
        }
    }

    return err;
}

int grib_get_nearest_smaller_value(grib_handle* h, const char* name,
                                   double val, double* nearest)
{
    grib_accessor* act = grib_find_accessor(h, name);
    ECCODES_ASSERT(act);
    return act->nearest_smaller_value(val, nearest);
}

void grib_print_values(const char* title, const grib_values* values, FILE* out, int count)
{
    ECCODES_ASSERT(values);
    for (int i = 0; i < count; ++i) {
        const grib_values aVal = values[i];
        fprintf(out, "%s: %s=", title, aVal.name);
        switch (aVal.type) {
            case GRIB_TYPE_LONG:
                fprintf(out, "%ld", aVal.long_value);
                break;
            case GRIB_TYPE_DOUBLE:
                fprintf(out, "%g", aVal.double_value);
                break;
            case GRIB_TYPE_STRING:
                fprintf(out, "%s", aVal.string_value);
                break;
        }
        fprintf(out, " (type=%s)", grib_get_type_name(aVal.type));
        if (aVal.error) fprintf(out, "\t(%s)\n", grib_get_error_message(aVal.error));
        else            fprintf(out, "\n");
    }
}

int grib_values_check(grib_handle* h, grib_values* values, int count)
{
    int i = 0;
    long long_value;
    double double_value;
    unsigned char ubuff[1024] = {0,};
    char buff[1024] = {0,};
    size_t len = 1024;

    for (i = 0; i < count; i++) {
        if (values[i].type == 0) {
            values[i].error = GRIB_INVALID_TYPE;
            return values[i].error;
        }

        switch (values[i].type) {
            case GRIB_TYPE_LONG:
                values[i].error = grib_get_long(h, values[i].name, &long_value);
                if (values[i].error != GRIB_SUCCESS)
                    return values[i].error;
                if (long_value != values[i].long_value) {
                    values[i].error = GRIB_VALUE_DIFFERENT;
                    return values[i].error;
                }
                break;

            case GRIB_TYPE_DOUBLE:
                values[i].error = grib_get_double(h, values[i].name, &double_value);
                if (values[i].error != GRIB_SUCCESS)
                    return values[i].error;
                if (double_value != values[i].double_value) {
                    values[i].error = GRIB_VALUE_DIFFERENT;
                    return values[i].error;
                }
                break;

            case GRIB_TYPE_STRING:
                values[i].error = grib_get_string(h, values[i].name, buff, &len);
                if (values[i].error != GRIB_SUCCESS)
                    return values[i].error;
                if (strcmp(values[i].string_value, buff)) {
                    values[i].error = GRIB_VALUE_DIFFERENT;
                    return values[i].error;
                }
                break;

            case GRIB_TYPE_BYTES:
                values[i].error = grib_get_bytes(h, values[i].name, ubuff, &len);
                if (values[i].error != GRIB_SUCCESS)
                    return values[i].error;
                if (memcmp(values[i].string_value, ubuff, len)) {
                    values[i].error = GRIB_VALUE_DIFFERENT;
                    return values[i].error;
                }
                break;

            default:
                values[i].error = GRIB_INVALID_TYPE;
                return values[i].error;
        }
    }

    return 0;
}

int codes_copy_key(grib_handle* h1, grib_handle* h2, const char* key, int type)
{
    double d;
    double* ad;
    long l;
    long* al;
    char* s     = 0;
    char** as   = 0;
    size_t len1 = 0, len = 0;
    int err = 0;

    if (type != GRIB_TYPE_DOUBLE &&
        type != GRIB_TYPE_LONG &&
        type != GRIB_TYPE_STRING) {
        err = grib_get_native_type(h1, key, &type);
        if (err)
            return err;
    }

    err = grib_get_size(h1, key, &len1);
    if (err)
        return err;

    switch (type) {
        case GRIB_TYPE_DOUBLE:
            if (len1 == 1) {
                err = grib_get_double(h1, key, &d);
                if (err)
                    return err;
                grib_context_log(h1->context, GRIB_LOG_DEBUG, "codes_copy_key double: %s=%g\n", key, d);
                err = grib_set_double(h2, key, d);
                return err;
            }
            else {
                ad  = (double*)grib_context_malloc_clear(h1->context, len1 * sizeof(double));
                err = grib_get_double_array(h1, key, ad, &len1);
                if (err)
                    return err;
                err = grib_set_double_array(h2, key, ad, len1);
                grib_context_free(h1->context, ad);
                return err;
            }
            break;
        case GRIB_TYPE_LONG:
            if (len1 == 1) {
                err = grib_get_long(h1, key, &l);
                if (err)
                    return err;
                grib_context_log(h1->context, GRIB_LOG_DEBUG, "codes_copy_key long: %s=%ld\n", key, l);
                err = grib_set_long(h2, key, l);
                return err;
            }
            else {
                al  = (long*)grib_context_malloc_clear(h1->context, len1 * sizeof(long));
                err = grib_get_long_array(h1, key, al, &len1);
                if (err)
                    return err;
                err = grib_set_long_array(h2, key, al, len1);
                grib_context_free(h1->context, al);
                return err;
            }
            break;
        case GRIB_TYPE_STRING:
            err = grib_get_string_length(h1, key, &len);
            if (err)
                return err;
            if (len1 == 1) {
                s   = (char*)grib_context_malloc_clear(h1->context, len);
                err = grib_get_string(h1, key, s, &len);
                if (err)
                    return err;
                grib_context_log(h1->context, GRIB_LOG_DEBUG, "codes_copy_key str: %s=%s\n", key, s);
                err = grib_set_string(h2, key, s, &len);
                grib_context_free(h1->context, s);
                return err;
            }
            else {
                as  = (char**)grib_context_malloc_clear(h1->context, len1 * sizeof(char*));
                err = grib_get_string_array(h1, key, as, &len1);
                if (err)
                    return err;
                err = grib_set_string_array(h2, key, (const char**)as, len1);
                return err;
            }
            break;
        default:
            return GRIB_INVALID_TYPE;
    }
}

int codes_compare_key(grib_handle* h1, grib_handle* h2, const char* key, int compare_flags)
{
    grib_accessor* a1 = grib_find_accessor(h1, key);
    if (!a1) {
        grib_context_log(h1->context, GRIB_LOG_ERROR, "Key %s not found in first message", key);
        return GRIB_NOT_FOUND;
    }
    grib_accessor* a2 = grib_find_accessor(h2, key);
    if (!a2) {
        grib_context_log(h1->context, GRIB_LOG_ERROR, "Key %s not found in second message", key);
        return GRIB_NOT_FOUND;
    }

    return a1->compare_accessors(a2, GRIB_COMPARE_TYPES);
}
