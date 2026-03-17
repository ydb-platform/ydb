/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

/*
 *
 * Description: routines for GRIB indexing from a set of files
 *
 */
#include "grib_api_internal.h"
#define GRIB_START_ARRAY_SIZE 5000
#define GRIB_ARRAY_INCREMENT 1000

#define SWAP(a, b) \
    temp = (a);    \
    (a)  = (b);    \
    (b)  = temp;

#define GRIB_ORDER_BY_ASC 1
#define GRIB_ORDER_BY_DESC -1

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

static grib_fieldset* grib_fieldset_create_from_keys(grib_context* c, const char** keys, int nkeys, int* err);
static grib_fieldset* grib_fieldset_create_from_order_by(grib_context* c, grib_order_by* ob,
                                                         int* err);
static int grib_fieldset_resize(grib_fieldset* set, size_t newsize);
static void grib_trim(char** x);
static grib_order_by* grib_fieldset_new_order_by(grib_context* c, const char* z);
static void grib_fieldset_sort(grib_fieldset* set, int theStart, int theEnd);
static grib_int_array* grib_fieldset_create_int_array(grib_context* c, size_t size);
static int grib_fieldset_resize_int_array(grib_int_array* a, size_t newsize);
static void grib_fieldset_delete_int_array(grib_int_array* f);
static grib_field** grib_fieldset_create_fields(grib_context* c, size_t size);
static void grib_fieldset_delete_fields(grib_fieldset* set);
static int grib_fieldset_resize_fields(grib_fieldset* set, size_t newsize);
static int grib_fieldset_set_order_by(grib_fieldset* set, grib_order_by* ob);


/* --------------- grib_column functions ------------------*/
static int grib_fieldset_new_column(grib_fieldset* set, int id, char* key, int type)
{
    grib_column* column = NULL;
    grib_context* c;
    int err = 0;

    if (!set)
        return GRIB_INVALID_ARGUMENT;

    c = set->context;

    set->columns[id].errors = (int*)grib_context_malloc_clear(c, sizeof(int) * GRIB_START_ARRAY_SIZE);

    switch (type) {
        case GRIB_TYPE_LONG:
            set->columns[id].long_values = (long*)grib_context_malloc_clear(c, sizeof(long) * GRIB_START_ARRAY_SIZE);
            if (!set->columns[id].long_values) {
                grib_context_log(c, GRIB_LOG_ERROR,
                                 "%s: Error allocating %zu bytes", __func__, sizeof(long) * GRIB_START_ARRAY_SIZE);
                err = GRIB_OUT_OF_MEMORY;
                return err;
            }
            break;
        case GRIB_TYPE_DOUBLE:
            set->columns[id].double_values = (double*)grib_context_malloc_clear(c, sizeof(double) * GRIB_START_ARRAY_SIZE);
            if (!set->columns[id].double_values) {
                grib_context_log(c, GRIB_LOG_ERROR,
                                 "%s: Error allocating %zu bytes", __func__, sizeof(double) * GRIB_START_ARRAY_SIZE);
                err = GRIB_OUT_OF_MEMORY;
                return err;
            }
            break;
        case GRIB_TYPE_STRING:
            set->columns[id].string_values = (char**)grib_context_malloc_clear(c, sizeof(char*) * GRIB_START_ARRAY_SIZE);
            if (!set->columns[id].string_values) {
                grib_context_log(c, GRIB_LOG_ERROR,
                                 "%s: Error allocating %zu bytes", __func__, sizeof(char*) * GRIB_START_ARRAY_SIZE);
                err = GRIB_OUT_OF_MEMORY;
                return err;
            }
            break;
        default:
            grib_context_log(c, GRIB_LOG_ERROR,
                             "grib_fieldset_new_column: Unknown column type %d", type);
            grib_context_free(c, column);
            return err;
    }

    set->columns[id].name              = grib_context_strdup(c, key);
    set->columns[id].type              = type;
    set->columns[id].values_array_size = GRIB_START_ARRAY_SIZE;
    set->columns[id].size              = 0;
    return err;
}

static void grib_fieldset_delete_columns(grib_fieldset* set)
{
    if (!set)
        return;

    const grib_context* c = set->context;

    for (size_t i = 0; i < set->columns_size; i++) {
        int j = 0;
        switch (set->columns[i].type) {
            case GRIB_TYPE_LONG:
                grib_context_free(c, set->columns[i].long_values);
                break;
            case GRIB_TYPE_DOUBLE:
                grib_context_free(c, set->columns[i].double_values);
                break;
            case GRIB_TYPE_STRING:
                for (j = 0; j < set->columns[i].size; j++)
                    grib_context_free(c, set->columns[i].string_values[j]);
                grib_context_free(c, set->columns[i].string_values);
                break;
            default:
                grib_context_log(c, GRIB_LOG_ERROR,
                                 "grib_fieldset_new_column: Unknown column type %d", set->columns[i].type);
        }
        grib_context_free(c, set->columns[i].errors);
        grib_context_free(c, set->columns[i].name);
    }
    grib_context_free(c, set->columns);
}

static int grib_fieldset_columns_resize(grib_fieldset* set, size_t newsize)
{
    if (!set || !set->columns)
        return GRIB_INVALID_ARGUMENT;

    double* newdoubles = NULL;
    long* newlongs     = NULL;
    char** newstrings  = NULL;
    int* newerrors     = NULL;
    const grib_context* c = set->context;

    if (newsize <= set->columns[0].values_array_size)
        return 0;

    for (size_t i = 0; i < set->columns_size; i++) {
        switch (set->columns[i].type) {
            case GRIB_TYPE_LONG:
                newlongs = (long*)grib_context_realloc(c, set->columns[i].long_values,
                                                       newsize * sizeof(long));
                if (!newlongs) {
                    grib_context_log(c, GRIB_LOG_ERROR,
                                     "%s: Error allocating %zu bytes", __func__, newsize - set->columns[i].values_array_size);
                    return GRIB_OUT_OF_MEMORY;
                }
                else
                    set->columns[i].long_values = newlongs;
                break;
            case GRIB_TYPE_DOUBLE:
                newdoubles = (double*)grib_context_realloc(c, set->columns[i].double_values,
                                                           newsize * sizeof(double));
                if (!newdoubles) {
                    grib_context_log(c, GRIB_LOG_ERROR,
                                     "%s: Error allocating %zu bytes", __func__, newsize - set->columns[i].values_array_size);
                    return GRIB_OUT_OF_MEMORY;
                }
                else
                    set->columns[i].double_values = newdoubles;
                break;
            case GRIB_TYPE_STRING:
                newstrings = (char**)grib_context_realloc(c, set->columns[i].string_values,
                                                          newsize * sizeof(char*));
                if (!newstrings) {
                    grib_context_log(c, GRIB_LOG_ERROR,
                                     "%s: Error allocating %zu bytes", __func__, newsize - set->columns[i].values_array_size);
                    return GRIB_OUT_OF_MEMORY;
                }
                else
                    set->columns[i].string_values = newstrings;
                break;
        }
        newerrors = (int*)grib_context_realloc(c, set->columns[i].errors, newsize * sizeof(int));
        if (!newerrors) {
            grib_context_log(c, GRIB_LOG_ERROR,
                             "%s: Error allocating %zu bytes", __func__, newsize * sizeof(int));
            return GRIB_OUT_OF_MEMORY;
        }
        else
            set->columns[i].errors = newerrors;

        set->columns[i].values_array_size = newsize;
    }

    return GRIB_SUCCESS;
}

static int grib_fieldset_column_copy_from_handle(grib_handle* h, grib_fieldset* set, int i)
{
    int err     = 0;
    long lval   = 0;
    double dval = 0;
    char sval[1024];
    size_t slen = 1024;
    if (!set || !h || set->columns[i].type == 0)
        return GRIB_INVALID_ARGUMENT;

    if (set->columns[i].size >= set->columns[i].values_array_size)
        grib_fieldset_columns_resize(set, set->columns[i].values_array_size + GRIB_ARRAY_INCREMENT);

    switch (set->columns[i].type) {
        case GRIB_TYPE_LONG:
            err                                               = grib_get_long(h, set->columns[i].name, &lval);
            set->columns[i].long_values[set->columns[i].size] = lval;
            break;
        case GRIB_TYPE_DOUBLE:
            err                                                 = grib_get_double(h, set->columns[i].name, &dval);
            set->columns[i].double_values[set->columns[i].size] = dval;
            break;
        case GRIB_TYPE_STRING:
            err                                                 = grib_get_string(h, set->columns[i].name, sval, &slen);
            set->columns[i].string_values[set->columns[i].size] = grib_context_strdup(h->context, sval);
            break;
    }

    set->columns[i].errors[set->columns[i].size] = err;
    set->columns[i].size++;

    return err;
}

/* --------------- grib_fieldset functions ------------------*/
grib_fieldset* grib_fieldset_new_from_files(grib_context* c, const char* filenames[],
                                            int nfiles, const char** keys, int nkeys,
                                            const char* where_string, const char* order_by_string, int* err)
{
    int i             = 0;
    int ret           = GRIB_SUCCESS;
    grib_order_by* ob = NULL;

    grib_fieldset* set = NULL;

    if (!c)
        c = grib_context_get_default();

    if (((!keys || nkeys == 0) && !order_by_string) || !filenames) {
        *err = GRIB_INVALID_ARGUMENT;
        return NULL;
    }

    if (order_by_string) {
        ob = grib_fieldset_new_order_by(c, order_by_string);
        if (!ob) {
            *err = GRIB_INVALID_ORDERBY;
            return NULL;
        }
    }

    if (!keys || nkeys == 0) {
        set = grib_fieldset_create_from_order_by(c, ob, err);
    }
    else {
        set = grib_fieldset_create_from_keys(c, keys, nkeys, err);
    }

    *err = GRIB_SUCCESS;
    for (i = 0; i < nfiles; i++) {
        ret = grib_fieldset_add(set, filenames[i]);
        if (ret != GRIB_SUCCESS) {
            *err = ret;
            return NULL;
        }
    }

    if (where_string) {
        ret = grib_fieldset_apply_where(set, where_string);
        if (ret != GRIB_SUCCESS) {
            *err = ret;
            return NULL;
        }
    }

    if (order_by_string) {
        if (!set->order_by && ob)
            *err = grib_fieldset_set_order_by(set, ob);
        if (*err != GRIB_SUCCESS)
            return NULL;
        grib_fieldset_sort(set, 0, set->size - 1);
        grib_fieldset_rewind(set);
    }

    return set;
}

static grib_fieldset* grib_fieldset_create_from_keys(grib_context* c, const char** keys, int nkeys, int* err)
{
    grib_fieldset* set = NULL;
    size_t msize = 0, size = 0;
    int i            = 0;
    int type         = 0;
    int default_type = GRIB_TYPE_STRING;

    if (!c)
        c = grib_context_get_default();

    size = GRIB_START_ARRAY_SIZE;

    msize = sizeof(grib_fieldset);
    set   = (grib_fieldset*)grib_context_malloc_clear(c, msize);
    if (!set) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Error allocating %zu bytes", __func__, msize);
        return NULL;
    }

    set->context           = c;
    set->fields_array_size = size;
    set->size              = 0;
    set->current           = -1;
    set->fields            = 0;
    set->filter            = 0;
    set->order             = 0;
    set->columns           = 0;
    set->where             = 0;
    set->order_by          = 0;

    set->fields = grib_fieldset_create_fields(set->context, size);

    set->order  = grib_fieldset_create_int_array(c, size);
    set->filter = grib_fieldset_create_int_array(c, size);
    for (i = 0; i < set->filter->size; i++)
        set->filter->el[i] = i;

    set->columns = (grib_column*)grib_context_malloc_clear(c, sizeof(grib_column) * nkeys);
    if (!set->columns) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: memory allocation error", __func__);
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }
    for (i = 0; i < nkeys; i++) {
        char* key = grib_context_strdup(c, keys[i]);
        char* p   = key;
        while (*p != ':' && *p != '\0')
            p++;
        if (*p == ':') {
            type = grib_type_to_int(*(p + 1));
            *p   = '\0';
        }
        else {
            type = default_type;
        }
        //if (type==0) type = default_type;
        *err = grib_fieldset_new_column(set, i, key, type);
        grib_context_free(c, key);
    }

    set->columns_size = nkeys;

    return set;
}

static grib_fieldset* grib_fieldset_create_from_order_by(grib_context* c, grib_order_by* ob, int* err)
{
    const char** keys   = NULL;
    size_t nkeys        = 0;
    int i               = 0;
    grib_fieldset* set  = NULL;
    grib_order_by* next = ob;

    while (next) {
        nkeys++;
        next = next->next;
    }

    keys = (const char**)grib_context_malloc_clear(c, nkeys * sizeof(char*));

    next = ob;
    i    = 0;
    while (next) {
        keys[i++] = next->key;
        next      = next->next;
    }

    set = grib_fieldset_create_from_keys(c, keys, nkeys, err);
    grib_context_free(c, (char*)keys);

    return set;
}

// Experimental: Needs more work
int grib_fieldset_apply_where(grib_fieldset* set, const char* where_string)
{
    // grib_math* m = NULL;
    // m = grib_math_new(set->context, where_string, &err);
    // if (err || !m) return err;
    // print_math(m);
    // printf("\n");
    // grib_math_delete(set->context, m);
    return GRIB_NOT_IMPLEMENTED;
}

int grib_fieldset_apply_order_by(grib_fieldset* set, const char* order_by_string)
{
    int err           = 0;
    grib_order_by* ob = NULL;

    if (!set)
        return GRIB_INVALID_ARGUMENT;

    if (set->order_by) {
        grib_fieldset_delete_order_by(set->context, set->order_by);
        set->order_by = 0;
    }

    ob = grib_fieldset_new_order_by(set->context, order_by_string);
    if ((err = grib_fieldset_set_order_by(set, ob)) != GRIB_SUCCESS)
        return err;

    if (set->order_by)
        grib_fieldset_sort(set, 0, set->size - 1);

    grib_fieldset_rewind(set);

    return err;
}

static int grib_fieldset_compare(grib_fieldset* set, const int* i, const int* j)
{
    int ret           = 0;
    double d          = 0;
    int idkey         = 0;
    grib_order_by* ob = NULL;
    int ii = 0, jj = 0;
    int* order = NULL;

    if (!set || !set->order_by)
        return GRIB_INVALID_ARGUMENT;
    ob    = set->order_by;
    order = set->order->el;

    ii = *(set->filter->el + *(order + *i));
    jj = *(set->filter->el + *(order + *j));

    while (ob) {
        idkey = ob->idkey;
        switch (set->columns[idkey].type) {
            case GRIB_TYPE_STRING:
                ret = strcmp(set->columns[idkey].string_values[ii],
                             set->columns[idkey].string_values[jj]);
                break;

            case GRIB_TYPE_DOUBLE:
                d = set->columns[idkey].double_values[ii] -
                    set->columns[idkey].double_values[jj];
                if (d > 0)
                    ret = 1;
                else if (d == 0)
                    ret = 0;
                else
                    ret = -1;
                break;

            case GRIB_TYPE_LONG:
                ret = set->columns[idkey].long_values[ii] -
                      set->columns[idkey].long_values[jj];
                break;
            default:
                return GRIB_INVALID_TYPE;
        }
        if (ret != 0) {
            ret *= ob->mode;
            break;
        }
        ob = ob->next;
    }

    return ret;
}

static void grib_fieldset_sort(grib_fieldset* set, int theStart, int theEnd)
{
    double temp;
    int l = 0, r = 0;
    if (theEnd > theStart) {
        l = theStart + 1;
        r = theEnd;
        while (l < r) {
            if (grib_fieldset_compare(set, &l, &theStart) <= 0) {
                l++;
            }
            else if (grib_fieldset_compare(set, &r, &theStart) >= 0) {
                r--;
            }
            else {
                SWAP(set->order->el[l], set->order->el[r])
            }
        }

        if (grib_fieldset_compare(set, &l, &theStart) < 0) {
            SWAP(set->order->el[l], set->order->el[theStart])
            l--;
        }
        else {
            l--;
            SWAP(set->order->el[l], set->order->el[theStart])
        }

        grib_fieldset_sort(set, theStart, l);
        grib_fieldset_sort(set, r, theEnd);
    }
}

void grib_fieldset_delete_order_by(grib_context* c, grib_order_by* order_by)
{
    grib_order_by* ob = order_by;

    if (!c)
        c = grib_context_get_default();

    while (order_by) {
        if (order_by->key)
            free(order_by->key);
        ob       = order_by;
        order_by = order_by->next;
        grib_context_free(c, ob);
    }

    return;
}

static grib_order_by* grib_fieldset_new_order_by(grib_context* c, const char* obstr)
{
    char *t1 = 0, *t2 = 0, *p = 0;
    int id  = 0;
    char* z = NULL;
    char* lasts = NULL;
    int mode, mode_default = GRIB_ORDER_BY_ASC;
    grib_order_by *ob, *sob;

    if (!obstr)
        return NULL;

    z = grib_context_strdup(c, obstr);
    if (!z)
        return 0;
    grib_trim(&z);

    if (strlen(z) == 0) {
        return 0;
    }

    ob        = (grib_order_by*)grib_context_malloc_clear(c, sizeof(grib_order_by));
    sob       = ob;
    ob->key   = 0;
    ob->idkey = 0;
    ob->mode  = 0;
    ob->next  = 0;

    t1 = strtok_r(z, ",", &lasts);

    while (t1) {
        grib_trim(&t1);
        t2 = grib_context_strdup(c, t1);
        p  = t2;
        while (*p != ' ' && *p != '\0')
            p++;
        mode = mode_default;
        if (p != t2) {
            while (*p == ' ')
                p++;
            if (*p != '\0') {
                *(p - 1) = '\0';
                if (strncmp(p, "asc", 3) == 0)
                    mode = GRIB_ORDER_BY_ASC;
                else if (strncmp(p, "desc", 4) == 0)
                    mode = GRIB_ORDER_BY_DESC;
                else
                    grib_context_log(c, GRIB_LOG_ERROR, "grib_fieldset_new_order_by: Invalid sort specifier: %s", p);
            }
            grib_trim(&p);
        }
        grib_trim(&t2);
        id = -1;
        t1 = strtok_r(NULL, ",", &lasts);

        if (ob->key) {
            ob->next = (grib_order_by*)grib_context_malloc_clear(c, sizeof(grib_order_by));
            ob       = ob->next;
            ob->key  = 0;
            ob->next = 0;
        }
        ob->mode  = mode;
        ob->key   = t2;
        ob->idkey = id;
    }

    if (z)
        grib_context_free(c, z);
    return sob;
}

void grib_fieldset_delete(grib_fieldset* set)
{
    grib_context* c = NULL;
    if (!set)
        return;

    c = set->context;

    grib_fieldset_delete_columns(set);

    grib_fieldset_delete_fields(set);
    grib_fieldset_delete_int_array(set->order);
    grib_fieldset_delete_int_array(set->filter);
    grib_fieldset_delete_order_by(c, set->order_by);

    grib_context_free(c, set);
}

int grib_fieldset_add(grib_fieldset* set, const char* filename)
{
    int ret        = GRIB_SUCCESS;
    int err        = 0;
    int i          = 0;
    grib_handle* h = NULL;
    /* int nkeys; */
    grib_file* file;
    double offset   = 0;
    long length     = 0;
    grib_context* c = NULL;

    if (!set || !filename)
        return GRIB_INVALID_ARGUMENT;
    c = set->context;

    /* nkeys=set->columns_size; */

    file = grib_file_open(filename, "r", &err);
    if (!file || !file->handle)
        return err;

    while ((h = grib_handle_new_from_file(c, file->handle, &ret)) != NULL || ret != GRIB_SUCCESS) {
        if (!h)
            return ret;

        err = GRIB_SUCCESS;
        for (i = 0; i < set->columns_size; i++) {
            err = grib_fieldset_column_copy_from_handle(h, set, i);
            if (err != GRIB_SUCCESS)
                ret = err;
        }
        if (err == GRIB_SUCCESS || err == GRIB_NOT_FOUND) {
            if (set->fields_array_size < set->columns[0].values_array_size) {
                ret = grib_fieldset_resize(set, set->columns[0].values_array_size);
                if (ret != GRIB_SUCCESS)
                    return ret;
            }
            offset                       = 0;
            grib_get_double(h, "offset", &offset);
            set->fields[set->size]       = (grib_field*)grib_context_malloc_clear(c, sizeof(grib_field));
            set->fields[set->size]->file = file;
            file->refcount++;
            set->fields[set->size]->offset = (off_t)offset;
            grib_get_long(h, "totalLength", &length);
            set->fields[set->size]->length = length;
            set->filter->el[set->size]     = set->size;
            set->order->el[set->size]      = set->size;
            set->size                      = set->columns[0].size;
        }
        grib_handle_delete(h);
    }
    if (h)
        grib_handle_delete(h);

    grib_file_close(file->name, 0, &err);

    grib_fieldset_rewind(set);

    return ret;
}

static int grib_fieldset_resize(grib_fieldset* set, size_t newsize)
{
    int err = 0;

    err = grib_fieldset_resize_fields(set, newsize);
    if (err)
        return err;
    err = grib_fieldset_resize_int_array(set->order, newsize);
    if (err)
        return err;
    err = grib_fieldset_resize_int_array(set->filter, newsize);
    if (err)
        return err;

    set->fields_array_size = newsize;

    return GRIB_SUCCESS;
}

void grib_fieldset_rewind(grib_fieldset* set)
{
    if (set)
        set->current = 0;
}

grib_handle* grib_fieldset_next_handle(grib_fieldset* set, int* err)
{
    grib_handle* h;
    *err = GRIB_SUCCESS;
    h    = grib_fieldset_retrieve(set, set->current, err);
    if (*err == GRIB_SUCCESS) {
        set->current++;
    }
    return h;
}

int grib_fieldset_count(const grib_fieldset* set)
{
    return set->size;
}

grib_handle* grib_fieldset_retrieve(grib_fieldset* set, int i, int* err)
{
    grib_handle* h    = NULL;
    grib_field* field = NULL;
    *err              = GRIB_SUCCESS;
    if (!set) {
        *err = GRIB_INVALID_ARGUMENT;
        return NULL;
    }
    if (i >= set->size)
        return NULL;

    field = set->fields[set->filter->el[set->order->el[i]]];
    grib_file_open(field->file->name, "r", err);
    if (*err != GRIB_SUCCESS)
        return NULL;

    fseeko(field->file->handle, field->offset, SEEK_SET);
    h = grib_handle_new_from_file(set->context, field->file->handle, err);
    if (*err != GRIB_SUCCESS)
        return NULL;

    grib_file_close(field->file->name, 0, err);

    return h;
}

static grib_int_array* grib_fieldset_create_int_array(grib_context* c, size_t size)
{
    grib_int_array* a;
    int i = 0;

    if (!c)
        c = grib_context_get_default();

    a = (grib_int_array*)grib_context_malloc_clear(c, sizeof(grib_int_array));
    if (!a) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "grib_fieldset_create_int_array: Cannot malloc %ld bytes",
                         sizeof(grib_int_array));
        return NULL;
    }

    a->el = (int*)grib_context_malloc_clear(c, sizeof(int) * size);
    if (!a->el) {
        grib_context_log(c, GRIB_LOG_ERROR,
                         "grib_fieldset_create_int_array: Cannot malloc %ld bytes",
                         sizeof(int) * size);
        return NULL;
    }

    a->size    = size;
    a->context = c;
    for (i = 0; i < size; i++)
        a->el[i] = i;

    return a;
}

static int grib_fieldset_resize_int_array(grib_int_array* a, size_t newsize)
{
    int* el;
    int err = 0;
    if (!a)
        return GRIB_INVALID_ARGUMENT;

    newsize = newsize * sizeof(int);

    el = (int*)grib_context_realloc(a->context, a->el, newsize);
    if (!el) {
        grib_context_log(a->context, GRIB_LOG_ERROR,
                         "%s: Error allocating %zu bytes", __func__, newsize);
        return GRIB_OUT_OF_MEMORY;
    }
    else
        a->el = el;
    a->size = newsize;
    return err;
}

static void grib_fieldset_delete_int_array(grib_int_array* f)
{
    if (!f) return;

    const grib_context* c = f->context;
    grib_context_free(c, f->el);
    grib_context_free(c, f);
}

static grib_field** grib_fieldset_create_fields(grib_context* c, size_t size)
{
    int i;
    grib_field** fields = (grib_field**)grib_context_malloc_clear(c, size * sizeof(grib_field*));
    if (!fields)
        return NULL;
    for (i = 0; i < size; i++)
        fields[i] = 0;
    return fields;
}

static int grib_fieldset_resize_fields(grib_fieldset* set, size_t newsize)
{
    int err = 0;
    int i;
    grib_field** fields;
    if (!set)
        return GRIB_INVALID_ARGUMENT;

    fields = (grib_field**)grib_context_realloc(set->context, set->fields, newsize * sizeof(grib_field*));
    if (!fields) {
        grib_context_log(set->context, GRIB_LOG_ERROR,
                         "%s: Error allocating %zu bytes", __func__, newsize * sizeof(grib_field*));
        return GRIB_OUT_OF_MEMORY;
    }
    else
        set->fields = fields;

    for (i = set->fields_array_size; i < newsize; i++)
        set->fields[i] = 0;

    set->fields_array_size = newsize;
    return err;
}

static void grib_fieldset_delete_fields(grib_fieldset* set)
{
    int i;
    for (i = 0; i < set->size; i++) {
        if (!set->fields[i])
            continue;
        set->fields[i]->file->refcount--;
        /* See GRIB-1010: force file close */
        {
            /* int err = 0; */
            /* grib_file_close(set->fields[i]->file->name, 1, &err); */
        }
        grib_context_free(set->context, set->fields[i]);
    }
    grib_context_free(set->context, set->fields);
}

static void grib_trim(char** x)
{
    char* p = NULL;
    while (**x == ' ')
        (*x)++;
    if (**x == '\0')
        return;
    p = (*x) + strlen(*x) - 1;
    while (*p == ' ') {
        *p = '\0';
        p--;
    }
    if (*p == ' ')
        *p = '\0';
}

static int grib_fieldset_set_order_by(grib_fieldset* set, grib_order_by* ob)
{
    grib_order_by* next = ob;
    char* p             = NULL;
    int i               = 0;

    while (next) {
        next->idkey = -1;
        p           = next->key;
        while (*p != 0 && *p != ':')
            p++;
        if (*p == ':')
            *p = 0;
        for (i = 0; i < set->columns_size; i++) {
            if (!set->columns[i].name) {  //ECC-1562
                grib_context_log(set->context, GRIB_LOG_ERROR, "grib_fieldset_set_order_by: Invalid type for key=%s", next->key);
                return GRIB_INVALID_TYPE;
            }
            if (!grib_inline_strcmp(next->key, set->columns[i].name)) {
                next->idkey = i;
                break;
            }
        }
        if (next->idkey == -1) {
            grib_context_log(set->context, GRIB_LOG_ERROR,
                             "grib_fieldset_set_order_by: "
                             "Unable to apply the order by. Key missing from the fieldset.");
            return GRIB_MISSING_KEY;
        }
        next = next->next;
    }

    set->order_by = ob;

    return GRIB_SUCCESS;
}
