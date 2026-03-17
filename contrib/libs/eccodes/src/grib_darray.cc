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
void grib_darray_print(const char* title, const grib_darray* darray)
{
    size_t i;
    ECCODES_ASSERT(darray);
    printf("%s: darray.size=%zu  darray.n=%zu  \t", title, darray->size, darray->n);
    for (i = 0; i < darray->n; i++) {
        printf("darray[%zu]=%g\t", i, darray->v[i]);
    }
    printf("\n");
}

// grib_darray* grib_darray_new_from_array(double* src_array, size_t size)
// {
//     size_t i;
//     grib_darray* v;
// c = grib_context_get_default();
//     v = grib_darray_new(c, size, 100);
//     for (i = 0; i < size; i++)
//         v->v[i] = src_array[i];
//     v->n       = size;
//     return v;
// }

grib_darray* grib_darray_new(size_t size, size_t incsize)
{
    grib_darray* v = NULL;
    grib_context* c = grib_context_get_default();
    v = (grib_darray*)grib_context_malloc_clear(c, sizeof(grib_darray));
    if (!v) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate %zu bytes", __func__, sizeof(grib_darray));
        return NULL;
    }
    v->size    = size;
    v->n       = 0;
    v->incsize = incsize;
    v->v       = (double*)grib_context_malloc_clear(c, sizeof(double) * size);
    if (!v->v) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate %zu bytes", __func__, sizeof(double) * size);
        return NULL;
    }
    return v;
}

static grib_darray* grib_darray_resize(grib_darray* v)
{
    const size_t newsize = v->incsize + v->size;
    grib_context* c = grib_context_get_default();

    v->v    = (double*)grib_context_realloc(c, v->v, newsize * sizeof(double));
    v->size = newsize;
    if (!v->v) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "%s: Unable to allocate %zu bytes", __func__, sizeof(double) * newsize);
        return NULL;
    }
    return v;
}

grib_darray* grib_darray_push(grib_darray* v, double val)
{
    size_t start_size    = 100;
    size_t start_incsize = 100;

    if (!v)
        v = grib_darray_new(start_size, start_incsize);

    if (v->n >= v->size)
        v = grib_darray_resize(v);
    v->v[v->n] = val;
    v->n++;
    return v;
}

void grib_darray_delete(grib_darray* v)
{
    if (!v)
        return;
    grib_context* c = grib_context_get_default();
    if (v->v)
        grib_context_free(c, v->v);
    grib_context_free(c, v);
}

// double* grib_darray_get_array(grib_context* c, grib_darray* v)
// {
//     double* ret = NULL;
//     size_t i = 0;
//     if (!v)
//         return NULL;
//     ret = (double*)grib_context_malloc_clear(c, sizeof(double) * v->n);
//     for (i = 0; i < v->n; i++)
//         ret[i] = v->v[i];
//     return ret;
// }

int grib_darray_is_constant(grib_darray* v, double epsilon)
{
    size_t i = 0;
    double val = 0;
    if (v->n == 1)
        return 1;

    val = v->v[0];
    for (i = 1; i < v->n; i++) {
        if (fabs(val - v->v[i]) > epsilon)
            return 0;
    }
    return 1;
}

size_t grib_darray_used_size(grib_darray* v)
{
    return v->n;
}
