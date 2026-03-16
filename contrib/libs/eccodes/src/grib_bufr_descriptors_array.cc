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

#define DYN_ARRAY_SIZE_INIT 200 /* Initial size for grib_bufr_descriptors_array_new */
#define DYN_ARRAY_SIZE_INCR 400 /* Increment size for the above */

bufr_descriptors_array* grib_bufr_descriptors_array_new(size_t size, size_t incsize)
{
    bufr_descriptors_array* v = NULL;

    grib_context* c = grib_context_get_default();

    v = (bufr_descriptors_array*)grib_context_malloc(c, sizeof(bufr_descriptors_array));
    if (!v) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate %zu bytes", __func__, sizeof(bufr_descriptors_array));
        return NULL;
    }
    v->context             = c;
    v->size                = size;
    v->n                   = 0;
    v->incsize             = incsize;
    v->v                   = (bufr_descriptor**)grib_context_malloc(c, sizeof(bufr_descriptor*) * size);
    v->number_of_pop_front = 0;
    if (!v->v) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "%s: Unable to allocate %zu bytes", __func__, sizeof(bufr_descriptor) * size);
        return NULL;
    }
    return v;
}

// bufr_descriptor* grib_bufr_descriptors_array_pop(bufr_descriptors_array* a)
// {
//     a->n -= 1;
//     return a->v[a->n];
// }

bufr_descriptor* grib_bufr_descriptors_array_pop_front(bufr_descriptors_array* a)
{
    bufr_descriptor* v = a->v[0];
    if (a->n == 0) {
        DEBUG_ASSERT(0);
    }
    a->n--;
    a->v++;
    a->number_of_pop_front++;

    return v;
}

static bufr_descriptors_array* grib_bufr_descriptors_array_resize_to(bufr_descriptors_array* v, size_t newsize)
{
    bufr_descriptor** newv;
    size_t i;
    grib_context* c = v->context;

    if (newsize < v->size)
        return v;

    if (!c)
        c = grib_context_get_default();

    newv = (bufr_descriptor**)grib_context_malloc_clear(c, newsize * sizeof(bufr_descriptor*));
    if (!newv) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate %zu bytes", __func__, sizeof(bufr_descriptor*) * newsize);
        return NULL;
    }

    for (i = 0; i < v->n; i++)
        newv[i] = v->v[i];

    v->v -= v->number_of_pop_front;
    grib_context_free(c, v->v);

    v->v                   = newv;
    v->size                = newsize;
    v->number_of_pop_front = 0;

    return v;
}

static bufr_descriptors_array* grib_bufr_descriptors_array_resize(bufr_descriptors_array* v)
{
    const size_t newsize = v->incsize + v->size;
    return grib_bufr_descriptors_array_resize_to(v, newsize);
}

bufr_descriptors_array* grib_bufr_descriptors_array_push(bufr_descriptors_array* v, bufr_descriptor* val)
{
    if (!v) {
        size_t start_size    = DYN_ARRAY_SIZE_INIT;
        size_t start_incsize = DYN_ARRAY_SIZE_INCR;
        v                    = grib_bufr_descriptors_array_new(start_size, start_incsize);
    }

    if (v->n >= v->size - v->number_of_pop_front)
        v = grib_bufr_descriptors_array_resize(v);

    v->v[v->n] = val;
    v->n++;
    return v;
}

bufr_descriptors_array* grib_bufr_descriptors_array_append(bufr_descriptors_array* v, bufr_descriptors_array* ar)
{
    size_t i;
    bufr_descriptor* vv = 0;

    if (!v) {
        size_t start_size    = DYN_ARRAY_SIZE_INIT;
        size_t start_incsize = DYN_ARRAY_SIZE_INCR;
        v                    = grib_bufr_descriptors_array_new(start_size, start_incsize);
    }

    for (i = 0; i < ar->n; i++) {
        vv = grib_bufr_descriptor_clone(ar->v[i]);
        grib_bufr_descriptors_array_push(v, vv);
    }

    grib_bufr_descriptors_array_delete(ar);
    /* ar = 0; */

    return v;
}

// bufr_descriptors_array* grib_bufr_descriptors_array_push_front(bufr_descriptors_array* v, bufr_descriptor* val)
// {
//     size_t i = 0;
//     if (!v) {
//         size_t start_size    = DYN_ARRAY_SIZE_INIT;
//         size_t start_incsize = DYN_ARRAY_SIZE_INCR;
//         v                    = grib_bufr_descriptors_array_new(start_size, start_incsize);
//     }
//     if (v->number_of_pop_front) {
//         v->v--;
//         v->number_of_pop_front--;
//     }
//     else {
//         if (v->n >= v->size)
//             v = grib_bufr_descriptors_array_resize(v);
//         for (i = v->n; i > 0; i--)
//             v[i] = v[i - 1];
//     }
//     v->v[0] = val;
//     v->n++;
//     return v;
// }

bufr_descriptor* grib_bufr_descriptors_array_get(bufr_descriptors_array* a, size_t i)
{
    return a->v[i];
}

// void grib_bufr_descriptors_array_set(bufr_descriptors_array* a, size_t i, bufr_descriptor* v)
// {
//     a->v[i] = v;
// }

void grib_bufr_descriptors_array_delete(bufr_descriptors_array* v)
{
    grib_context* c;

    if (!v)
        return;
    c = v->context;

    grib_bufr_descriptors_array_delete_array(v);

    grib_context_free(c, v);
}

void grib_bufr_descriptors_array_delete_array(bufr_descriptors_array* v)
{
    grib_context* c = NULL;
    size_t i = 0;
    bufr_descriptor** vv = NULL;

    if (!v)
        return;
    c = v->context;

    if (v->v) {
        vv = v->v;
        for (i = 0; i < v->n; i++) {
            grib_bufr_descriptor_delete(vv[i]);
        }
        vv = v->v - v->number_of_pop_front;
        grib_context_free(c, vv);
    }
}

// bufr_descriptor** grib_bufr_descriptors_array_get_array(bufr_descriptors_array* v)
// {
//     bufr_descriptor** vv;
//     size_t i;
//     grib_context* c = grib_context_get_default();
//     vv = (bufr_descriptor**)grib_context_malloc_clear(c, sizeof(bufr_descriptor*) * v->n);
//     for (i = 0; i < v->n; i++)
//         vv[i] = grib_bufr_descriptor_clone(v->v[i]);
//     return vv;
// }

size_t grib_bufr_descriptors_array_used_size(bufr_descriptors_array* v)
{
    return v->n;
}
