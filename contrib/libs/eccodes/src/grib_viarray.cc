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

// For debugging purposes
void grib_viarray_print(const char* title, const grib_viarray* viarray)
{
    size_t i;
    char text[100] = {0,};
    const size_t textLen = sizeof(text);
    ECCODES_ASSERT(viarray);
    printf("%s: viarray.n=%zu\n", title, viarray->n);
    for (i = 0; i < viarray->n; i++) {
        snprintf(text, textLen, " viarray->v[%zu]", i);
        grib_iarray_print(text, viarray->v[i]);
    }
    printf("\n");
}

grib_viarray* grib_viarray_new(size_t size, size_t incsize)
{
    grib_viarray* v = NULL;
    grib_context* c = grib_context_get_default();
    v = (grib_viarray*)grib_context_malloc_clear(c, sizeof(grib_viarray));
    if (!v) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "%s: Unable to allocate %zu bytes", __func__, sizeof(grib_viarray));
        return NULL;
    }
    v->size    = size;
    v->n       = 0;
    v->incsize = incsize;

    v->v       = (grib_iarray**)grib_context_malloc_clear(c, sizeof(grib_iarray*) * size);
    if (!v->v) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "%s: Unable to allocate %zu bytes", __func__, sizeof(grib_iarray*) * size);
        return NULL;
    }
    return v;
}

static grib_viarray* grib_viarray_resize(grib_viarray* v)
{
    const size_t newsize = v->incsize + v->size;
    grib_context* c = grib_context_get_default();

    v->v    = (grib_iarray**)grib_context_realloc(c, v->v, newsize * sizeof(grib_iarray*));
    v->size = newsize;
    if (!v->v) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "%s: Unable to allocate %zu bytes", __func__, sizeof(grib_iarray*) * newsize);
        return NULL;
    }
    return v;
}

grib_viarray* grib_viarray_push(grib_viarray* v, grib_iarray* val)
{
    size_t start_size    = 100;
    size_t start_incsize = 100;
    if (!v)
        v = grib_viarray_new(start_size, start_incsize);

    if (v->n >= v->size)
        v = grib_viarray_resize(v);
    v->v[v->n] = val;
    v->n++;
    return v;
}

void grib_viarray_delete(grib_viarray* v)
{
    if (!v)
        return;
    grib_context* c = grib_context_get_default();
    if (v->v)
        grib_context_free(c, v->v);
    grib_context_free(c, v);
}

void grib_viarray_delete_content(grib_viarray* v)
{
    size_t i=0;
    if (!v || !v->v)
        return;

    for (i = 0; i < v->n; i++) {
        grib_iarray_delete(v->v[i]);
        v->v[i] = 0;
    }
    v->n = 0;
}

// grib_iarray** grib_viarray_get_array(grib_context* c, grib_viarray* v)
// {
//     grib_iarray** ret;
//     size_t i = 0;
//     if (!v)
//         return NULL;
//     ret = (grib_iarray**)grib_context_malloc_clear(c, sizeof(grib_iarray*) * v->n);
//     for (i = 0; i < v->n; i++)
//         ret[i] = v->v[i];
//     return ret;
// }

// size_t grib_viarray_used_size(grib_viarray* v)
// {
//     return v->n;
// }
