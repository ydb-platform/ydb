/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"


grib_hash_array_value* grib_integer_hash_array_value_new(const char* name, grib_iarray* array)
{
    grib_context* c = grib_context_get_default();
    grib_hash_array_value* v = (grib_hash_array_value*)grib_context_malloc_clear_persistent(c, sizeof(grib_hash_array_value));

    v->name   = grib_context_strdup_persistent(c, name);
    v->type   = GRIB_HASH_ARRAY_TYPE_INTEGER;
    v->iarray = array;
    return v;
}

// grib_hash_array_value* grib_double_hash_array_value_new(grib_context* c, const char* name, grib_darray* array)
// {
//     grib_hash_array_value* v = (grib_hash_array_value*)grib_context_malloc_clear_persistent(c, sizeof(grib_hash_array_value));
//     v->name   = grib_context_strdup_persistent(c, name);
//     v->type   = GRIB_HASH_ARRAY_TYPE_DOUBLE;
//     v->darray = array;
//     return v;
// }

// void grib_hash_array_value_delete(grib_context* c, grib_hash_array_value* v)
// {
//     switch (v->type) {
//         case GRIB_HASH_ARRAY_TYPE_INTEGER:
//             grib_iarray_delete(v->iarray);
//             break;
//         case GRIB_HASH_ARRAY_TYPE_DOUBLE:
//             grib_darray_delete(c, v->darray);
//             break;
//         default:
//             grib_context_log(c, GRIB_LOG_ERROR,
//                              "wrong type in grib_hash_array_value_delete");
//     }
//     grib_context_free_persistent(c, v->name);
//     grib_context_free_persistent(c, v);
// }
