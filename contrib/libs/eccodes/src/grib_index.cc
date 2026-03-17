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
#include <map>
#include <string>

#define UNDEF_LONG   -99999
#define UNDEF_DOUBLE -99999

#define NULL_MARKER 0
#define NOT_NULL_MARKER 255

#define GRIB_KEY_UNDEF "undef"

/* #if GRIB_PTHREADS */
// static pthread_once_t once  = PTHREAD_ONCE_INIT;
// static pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
// static pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;

// static void init() {
//     pthread_mutexattr_t attr;
//     pthread_mutexattr_init(&attr);
//     pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
//     pthread_mutex_init(&mutex1,&attr);
//     pthread_mutex_init(&mutex2,&attr);
//     pthread_mutexattr_destroy(&attr);

// }
// /* #elif GRIB_OMP_THREADS */
// static int once = 0;
// static omp_nest_lock_t mutex1;
// static omp_nest_lock_t mutex2;

// static void init()
// {
//     GRIB_OMP_CRITICAL(lock_grib_index_c)
//     {
//         if (once == 0)
//         {
//             omp_init_nest_lock(&mutex1);
//             omp_init_nest_lock(&mutex2);
//             once = 1;
//         }
//     }
// }

static const char* mars_keys =
    "mars.date,mars.time,mars.expver,mars.stream,mars.class,mars.type,"
    "mars.step,mars.param,mars.levtype,mars.levelist,mars.number,mars.iteration,"
    "mars.domain,mars.fcmonth,mars.fcperiod,mars.hdate,mars.method,"
    "mars.model,mars.origin,mars.quantile,mars.range,mars.refdate,mars.direction,mars.frequency";


/* See GRIB-32: start off ID with -1 as it is incremented before being used */
static int grib_filesid = -1;
static int index_count;
static long values_count = 0;

static int codes_index_add_file_internal(grib_index* index, const char* filename, int message_type);

static char* get_key(char** keys, int* type)
{
    char* key = NULL;
    char* p   = NULL;

    if (keys == NULL || *keys == 0)
        return NULL;
    *type = GRIB_TYPE_UNDEFINED;
    p     = *keys;
    while (*p == ' ')
        p++;

    while (*p != 0 && *p != ':' && *p != ',')
        p++;
    if (*p == ':') {
        *type = grib_type_to_int(*(p + 1));
        *p    = 0;
        p++;
        while (*p != 0 && *p != ',') {
            *(p++) = 0;
        }
    }
    else
        *type = GRIB_TYPE_UNDEFINED;
    if (*p) {
        *p = 0;
        p++;
    }
    key   = *keys;
    *keys = *p == 0 ? NULL : p;
    return key;
}

static int compare_long(const void* a, const void* b)
{
    long* arg1 = (long*)a;
    long* arg2 = (long*)b;
    if (*arg1 == *arg2)
        return 0;

    return *arg1 < *arg2 ? -1 : 1;
}

static int compare_double(const void* a, const void* b)
{
    double* arg1 = (double*)a;
    double* arg2 = (double*)b;
    if (*arg1 == *arg2)
        return 0;

    return *arg1 < *arg2 ? -1 : 1;
}

static int compare_string(const void* a, const void* b)
{
    char* arg1 = *(char* const*)a;
    char* arg2 = *(char* const*)b;

    while (*arg1 != 0 && *arg2 != 0 && *arg1 == *arg2) {
        arg1++;
        arg2++;
    }

    if (*arg1 == *arg2)
        return 0;

    return *arg1 < *arg2 ? -1 : 1;
}

static int grib_index_keys_compress(grib_context* c, grib_index* index, int* compress)
{
    grib_index_key* keys = index->keys->next;
    grib_index_key* prev = index->keys;
    int level            = 0;

    if (!keys)
        return 0;

    level = 1;
    while (keys) {
        if (keys->values_count == 1) {
            prev->next = keys->next;
            grib_context_free(c, keys->name);
            grib_context_free(c, keys);
            keys            = prev->next;
            compress[level] = 1;
            level++;
        }
        else {
            prev            = keys;
            keys            = keys->next;
            compress[level] = 0;
            level++;
        }
    }

    if (index->keys->values_count == 1) {
        keys        = index->keys;
        index->keys = index->keys->next;
        grib_context_free(c, keys->name);
        grib_context_free(c, keys);
        compress[0] = 1;
    }
    else
        compress[0] = 0;

    return 0;
}

static int grib_index_fields_compress(grib_context* c,
                                      grib_field_tree* fields, grib_field_tree* prev, int level, int* compress)
{
    if (!fields)
        return 0;

    if (!prev) {
        if (fields->next)
            grib_index_fields_compress(c, fields->next, 0, level, compress);
        level++;
        return grib_index_fields_compress(c, fields->next_level, fields, level, compress);
    }

    if (compress[level]) {
        if (!fields->next_level)
            prev->field = fields->field;

        prev->next_level = fields->next_level;
        grib_context_free(c, fields->value);
        grib_context_free(c, fields);
        level++;
        grib_index_fields_compress(c, prev->next_level, prev, level, compress);
    }
    else {
        grib_field_tree* next;
        next = fields->next;
        level++;
        while (next) {
            grib_index_fields_compress(c, next->next_level, next, level, compress);
            next = next->next;
        }
        grib_index_fields_compress(c, fields->next_level, fields, level, compress);
    }

    return 0;
}

int grib_index_compress(grib_index* index)
{
    int err           = 0;
    grib_context* c   = index->context;
    int compress[200] = {0,};

    if (!index->keys->next)
        return 0;

    err = grib_index_keys_compress(c, index, compress);
    if (err) return err;

    err = grib_index_fields_compress(c, index->fields, 0, 0, compress);
    if (err) return err;

    if (!index->fields->next) {
        grib_field_tree* next_level = index->fields->next_level;
        grib_context_free(c, index->fields->value);
        grib_context_free(c, index->fields);
        index->fields = next_level;
    }
    return 0;
}

static grib_index_key* grib_index_new_key(grib_context* c, grib_index_key* keys,
                                          const char* key, int type, int* err)
{
    grib_index_key *next = NULL, *current = NULL;
    grib_string_list* values = NULL;

    next = (grib_index_key*)grib_context_malloc_clear(c, sizeof(grib_index_key));
    if (!next) {
        grib_context_log(c, GRIB_LOG_ERROR, "Unable to allocate %zu bytes", sizeof(grib_index_key));
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }
    values = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
    if (!values) {
        grib_context_log(c, GRIB_LOG_ERROR, "Unable to allocate %zu bytes", sizeof(grib_string_list));
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }

    next->values = values;

    if (!keys) {
        keys    = next;
        current = keys;
    }
    else {
        current = keys;
        while (current->next)
            current = current->next;
        current->next = next;
        current       = current->next;
    }

    current->type = type;
    current->name = grib_context_strdup(c, key);
    return keys;
}

static int grib_read_uchar(FILE* fh, unsigned char* val)
{
    if (fread(val, sizeof(unsigned char), 1, fh) < 1) {
        if (feof(fh))
            return GRIB_END_OF_FILE;
        else
            return GRIB_IO_PROBLEM;
    }
    return GRIB_SUCCESS;
}

static int grib_read_short(FILE* fh, short* val)
{
    if (fread(val, sizeof(short), 1, fh) < 1) {
        if (feof(fh))
            return GRIB_END_OF_FILE;
        else
            return GRIB_IO_PROBLEM;
    }
    return GRIB_SUCCESS;
}

// static int grib_read_long(FILE* fh, long* val)
// {
//     if (fread(val, sizeof(long), 1, fh) < 1) {
//         if (feof(fh))
//             return GRIB_END_OF_FILE;
//         else
//             return GRIB_IO_PROBLEM;
//     }
//     return GRIB_SUCCESS;
// }

static int grib_read_unsigned_long(FILE* fh, unsigned long* val)
{
    if (fread(val, sizeof(long), 1, fh) < 1) {
        if (feof(fh))
            return GRIB_END_OF_FILE;
        else
            return GRIB_IO_PROBLEM;
    }
    return GRIB_SUCCESS;
}

static int grib_write_uchar(FILE* fh, unsigned char val)
{
    if (fwrite(&val, sizeof(unsigned char), 1, fh) < 1)
        return GRIB_IO_PROBLEM;
    return GRIB_SUCCESS;
}

static int grib_write_short(FILE* fh, short val)
{
    if (fwrite(&val, sizeof(short), 1, fh) < 1)
        return GRIB_IO_PROBLEM;
    return GRIB_SUCCESS;
}

// static int grib_write_long(FILE* fh, long val)
// {
//     if (fwrite(&val, sizeof(long), 1, fh) < 1)
//         return GRIB_IO_PROBLEM;
//     return GRIB_SUCCESS;
// }

static int grib_write_unsigned_long(FILE* fh, unsigned long val)
{
    if (fwrite(&val, sizeof(long), 1, fh) < 1)
        return GRIB_IO_PROBLEM;
    return GRIB_SUCCESS;
}

static int grib_write_string(FILE* fh, const char* s)
{
    size_t len = 0;
    if (s == NULL)
        return GRIB_IO_PROBLEM;
    len = strlen(s);
    grib_write_uchar(fh, (unsigned char)len);
    if (fwrite(s, 1, len, fh) < len)
        return GRIB_IO_PROBLEM;
    return GRIB_SUCCESS;
}

static int grib_write_identifier(FILE* fh, const char* ID)
{
    return grib_write_string(fh, ID);
}

static int grib_write_null_marker(FILE* fh)
{
    return grib_write_uchar(fh, NULL_MARKER);
}

static int grib_write_not_null_marker(FILE* fh)
{
    return grib_write_uchar(fh, NOT_NULL_MARKER);
}

static char* grib_read_string(grib_context* c, FILE* fh, int* err)
{
    unsigned char len = 0;
    char* s           = NULL;
    *err              = grib_read_uchar(fh, &len);

    if (*err)
        return NULL;
    s = (char*)grib_context_malloc_clear(c, len + 1);
    if (fread(s, len, 1, fh) < 1) {
        if (feof(fh))
            *err = GRIB_END_OF_FILE;
        else
            *err = GRIB_IO_PROBLEM;
        return NULL;
    }
    s[len] = 0;

    return s;
}

static int grib_write_field(FILE* fh, grib_field* field)
{
    int err;
    if (!field)
        return grib_write_null_marker(fh);

    err = grib_write_not_null_marker(fh);
    if (err)
        return err;

    err = grib_write_short(fh, field->file->id);
    if (err)
        return err;

    err = grib_write_unsigned_long(fh, field->offset);
    if (err)
        return err;

    err = grib_write_unsigned_long(fh, field->length);
    if (err)
        return err;

    err = grib_write_field(fh, field->next);
    if (err)
        return err;

    return GRIB_SUCCESS;
}

static grib_field* grib_read_field(grib_context* c, FILE* fh, grib_file** files, int* err)
{
    grib_field* field = NULL;
    short file_id;
    unsigned char marker = 0;
    unsigned long offset = 0;
    unsigned long length = 0;

    *err = grib_read_uchar(fh, &marker);
    if (marker == NULL_MARKER)
        return NULL;
    if (marker != NOT_NULL_MARKER) {
        *err = GRIB_CORRUPTED_INDEX;
        return NULL;
    }

    index_count++;
    field = (grib_field*)grib_context_malloc(c, sizeof(grib_field));
    *err  = grib_read_short(fh, &file_id);
    if (*err)
        return NULL;

    field->file = files[file_id];

    *err          = grib_read_unsigned_long(fh, &offset);
    field->offset = offset;
    if (*err)
        return NULL;

    *err          = grib_read_unsigned_long(fh, &length);
    field->length = length;
    if (*err)
        return NULL;

    field->next = grib_read_field(c, fh, files, err);

    return field;
}

static int grib_write_field_tree(FILE* fh, grib_field_tree* tree)
{
    int err = 0;

    if (!tree)
        return grib_write_null_marker(fh);

    err = grib_write_not_null_marker(fh);
    if (err)
        return err;

    err = grib_write_field(fh, tree->field);
    if (err)
        return err;

    err = grib_write_string(fh, tree->value);
    if (err)
        return err;

    err = grib_write_field_tree(fh, tree->next_level);
    if (err)
        return err;

    err = grib_write_field_tree(fh, tree->next);
    if (err)
        return err;
    return GRIB_SUCCESS;
}

grib_field_tree* grib_read_field_tree(grib_context* c, FILE* fh, grib_file** files, int* err)
{
    grib_field_tree* tree = NULL;
    unsigned char marker  = 0;
    *err                  = grib_read_uchar(fh, &marker);

    if (marker == NULL_MARKER)
        return NULL;
    if (marker != NOT_NULL_MARKER) {
        *err = GRIB_CORRUPTED_INDEX;
        return NULL;
    }

    tree        = (grib_field_tree*)grib_context_malloc(c, sizeof(grib_field_tree));
    tree->field = grib_read_field(c, fh, files, err);
    if (*err)
        return NULL;

    tree->value = grib_read_string(c, fh, err);
    if (*err)
        return NULL;

    tree->next_level = grib_read_field_tree(c, fh, files, err);
    if (*err)
        return NULL;

    tree->next = grib_read_field_tree(c, fh, files, err);
    if (*err)
        return NULL;

    return tree;
}

grib_index* grib_index_new(grib_context* c, const char* key, int* err)
{
    grib_index* index;
    grib_index_key* keys = NULL;
    char* q;
    int type;
    char* p;

    if (!strcmp(key, "mars"))
        return grib_index_new(c, mars_keys, err);

    p = grib_context_strdup(c, key);
    q = p;

    *err = 0;
    if (!c)
        c = grib_context_get_default();

    index = (grib_index*)grib_context_malloc_clear(c, sizeof(grib_index));
    if (!index) {
        grib_context_log(c, GRIB_LOG_ERROR, "Unable to create index");
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }
    index->context = c;
    index->product_kind = PRODUCT_GRIB;
    index->unpack_bufr = 0;

    while ((key = get_key(&p, &type)) != NULL) {
        keys = grib_index_new_key(c, keys, key, type, err);
        if (*err)
            return NULL;
    }
    index->keys   = keys;
    index->fields = (grib_field_tree*)grib_context_malloc_clear(c,
                                                                sizeof(grib_field_tree));
    if (!index->fields) {
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }

    grib_context_free(c, q);
    return index;
}

static void grib_index_values_delete(grib_context* c, grib_string_list* values)
{
    if (!values)
        return;

    grib_index_values_delete(c, values->next);
    grib_context_free(c, values->value);
    grib_context_free(c, values);

    return;
}

static void grib_index_key_delete(grib_context* c, grib_index_key* keys)
{
    if (!keys)
        return;

    grib_index_key_delete(c, keys->next);

    grib_index_values_delete(c, keys->values);
    grib_index_values_delete(c, keys->current);
    grib_context_free(c, keys->name);
    grib_context_free(c, keys);
}

static grib_string_list* grib_read_key_values(grib_context* c, FILE* fh, int* err)
{
    grib_string_list* values;
    unsigned char marker = 0;

    *err = grib_read_uchar(fh, &marker);
    if (marker == NULL_MARKER)
        return NULL;
    if (marker != NOT_NULL_MARKER) {
        *err = GRIB_CORRUPTED_INDEX;
        return NULL;
    }

    values_count++;

    values        = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
    values->value = grib_read_string(c, fh, err);
    if (*err)
        return NULL;

    values->next = grib_read_key_values(c, fh, err);
    if (*err)
        return NULL;

    return values;
}

static int grib_write_key_values(FILE* fh, grib_string_list* values)
{
    int err = 0;

    if (!values)
        return grib_write_null_marker(fh);

    err = grib_write_not_null_marker(fh);
    if (err)
        return err;

    err = grib_write_string(fh, values->value);
    if (err)
        return err;

    err = grib_write_key_values(fh, values->next);
    if (err)
        return err;

    return GRIB_SUCCESS;
}

static grib_index_key* grib_read_index_keys(grib_context* c, FILE* fh, int* err)
{
    grib_index_key* keys = NULL;
    unsigned char marker = 0;
    unsigned char type   = 0;

    if (!c)
        c = grib_context_get_default();

    *err = grib_read_uchar(fh, &marker);
    if (marker == NULL_MARKER)
        return NULL;
    if (marker != NOT_NULL_MARKER) {
        *err = GRIB_CORRUPTED_INDEX;
        return NULL;
    }

    keys       = (grib_index_key*)grib_context_malloc_clear(c, sizeof(grib_index_key));
    keys->name = grib_read_string(c, fh, err);
    if (*err)
        return NULL;

    *err       = grib_read_uchar(fh, &type);
    keys->type = type;
    if (*err)
        return NULL;

    values_count = 0;
    keys->values = grib_read_key_values(c, fh, err);
    if (*err)
        return NULL;

    keys->values_count = values_count;

    keys->next = grib_read_index_keys(c, fh, err);
    if (*err)
        return NULL;

    return keys;
}

static int grib_write_index_keys(FILE* fh, grib_index_key* keys)
{
    int err = 0;

    if (!keys)
        return grib_write_null_marker(fh);

    err = grib_write_not_null_marker(fh);
    if (err)
        return err;

    err = grib_write_string(fh, keys->name);
    if (err)
        return err;

    err = grib_write_uchar(fh, (unsigned char)keys->type);
    if (err)
        return err;

    err = grib_write_key_values(fh, keys->values);
    if (err)
        return err;

    err = grib_write_index_keys(fh, keys->next);
    if (err)
        return err;

    return GRIB_SUCCESS;
}

static void grib_field_delete(grib_context* c, grib_field* field)
{
    int err = 0;

    if (!field)
        return;

    grib_field_delete(c, field->next);

    if (field->file) {
        grib_file_close(field->file->name, 0, &err);
        field->file = NULL;
    }

    grib_context_free(c, field);
}

static void grib_field_tree_delete(grib_context* c, grib_field_tree* tree)
{
    if (!tree)
        return;

    grib_field_delete(c, tree->field);
    grib_context_free(c, tree->value);

    grib_field_tree_delete(c, tree->next_level);

    grib_field_tree_delete(c, tree->next);

    grib_context_free(c, tree);
}

static void grib_field_list_delete(grib_context* c, grib_field_list* field_list)
{
    grib_field_list* p = field_list;
    if (!field_list)
        return;
    while (p) {
        grib_field_list* q = p;
        p                  = p->next;
        grib_context_free(c, q);
    }
}

void grib_index_delete(grib_index* index)
{
    grib_file* file = index->files;
    grib_index_key_delete(index->context, index->keys);
    grib_field_tree_delete(index->context, index->fields);
    grib_field_list_delete(index->context, index->fieldset);
    while (file) {
        grib_file* f = file;
        file         = file->next;
        grib_file_pool_delete_clone(f);
    }
    grib_context_free(index->context, index);
}

static int grib_write_files(FILE* fh, grib_file* files)
{
    int err;
    if (!files)
        return grib_write_null_marker(fh);

    err = grib_write_not_null_marker(fh);
    if (err)
        return err;

    err = grib_write_string(fh, files->name);
    if (err)
        return err;

    err = grib_write_short(fh, (short)files->id);
    if (err)
        return err;

    return grib_write_files(fh, files->next);
}

static grib_file* grib_read_files(grib_context* c, FILE* fh, int* err)
{
    unsigned char marker = 0;
    short id             = 0;
    grib_file* file;
    *err = grib_read_uchar(fh, &marker);
    if (marker == NULL_MARKER)
        return NULL;
    if (marker != NOT_NULL_MARKER) {
        *err = GRIB_CORRUPTED_INDEX;
        return NULL;
    }

    file       = (grib_file*)grib_context_malloc(c, sizeof(grib_file));
    file->name = grib_read_string(c, fh, err);
    if (*err)
        return NULL;

    *err     = grib_read_short(fh, &id);
    file->id = id;
    if (*err)
        return NULL;

    file->next = grib_read_files(c, fh, err);
    if (*err)
        return NULL;

    return file;
}

int grib_index_write(grib_index* index, const char* filename)
{
    int err = 0;
    FILE* fh;
    grib_file* files;
    const char* identifier = NULL;

    fh = fopen(filename, "w");
    if (!fh) {
        grib_context_log(index->context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to write in file %s", filename);
        perror(filename);
        return GRIB_IO_PROBLEM;
    }

    if (index->product_kind == PRODUCT_GRIB) identifier = "GRBIDX1";
    if (index->product_kind == PRODUCT_BUFR) identifier = "BFRIDX1";
    ECCODES_ASSERT(identifier);
    err = grib_write_identifier(fh, identifier);
    if (err) {
        grib_context_log(index->context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to write in file %s", filename);
        perror(filename);
        return err;
    }

    if (!index)
        return grib_write_null_marker(fh);

    err = grib_write_not_null_marker(fh);
    if (err)
        return err;

    /* See GRIB-32: Do not use the file pool */
    /* files=grib_file_pool_get_files(); */
    files = index->files;
    err   = grib_write_files(fh, files);
    if (err) {
        grib_context_log(index->context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to write in file %s", filename);
        perror(filename);
        return err;
    }

    err = grib_write_index_keys(fh, index->keys);
    if (err) {
        grib_context_log(index->context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to write in file %s", filename);
        perror(filename);
        return err;
    }

    err = grib_write_field_tree(fh, index->fields);
    if (err) {
        grib_context_log(index->context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to write in file %s", filename);
        perror(filename);
        return err;
    }

    if (fclose(fh) != 0) {
        grib_context_log(index->context, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to write in file %s", filename);
        perror(filename);
        return GRIB_IO_PROBLEM;
    }

    return err;
}

grib_index* grib_index_read(grib_context* c, const char* filename, int* err)
{
    grib_file *file, *f;
    grib_file** files;
    grib_index* index    = NULL;
    unsigned char marker = 0;
    char* identifier     = NULL;
    int max              = 0;
    FILE* fh             = NULL;
    ProductKind product_kind = PRODUCT_GRIB;

    if (!c)
        c = grib_context_get_default();

    fh = fopen(filename, "r");
    if (!fh) {
        grib_context_log(c, (GRIB_LOG_ERROR) | (GRIB_LOG_PERROR),
                         "Unable to read file %s", filename);
        perror(filename);
        *err = GRIB_IO_PROBLEM;
        return NULL;
    }

    identifier = grib_read_string(c, fh, err);
    if (!identifier) {
        fclose(fh);
        return NULL;
    }

    if (strcmp(identifier, "BFRIDX1")==0) product_kind = PRODUCT_BUFR;
    grib_context_free(c, identifier);

    *err = grib_read_uchar(fh, &marker);
    if (marker == NULL_MARKER) {
        fclose(fh);
        return NULL;
    }
    if (marker != NOT_NULL_MARKER) {
        *err = GRIB_CORRUPTED_INDEX;
        fclose(fh);
        return NULL;
    }

    file = grib_read_files(c, fh, err);
    if (*err)
        return NULL;

    f = file;
    while (f) {
        if (max < f->id)
            max = f->id;
        f = f->next;
    }

    files = (grib_file**)grib_context_malloc_clear(c, sizeof(grib_file) * (max + 1));

    f = file;
    while (f) {
        grib_file_open(f->name, "r", err);
        if (*err)
            return NULL;
        files[f->id] = grib_get_file(f->name, err); /* fetch from pool */
        f            = f->next;
    }

    while (file) {
        f    = file;
        file = file->next;
        grib_context_free(c, f->name);
        grib_context_free(c, f);
    }

    index          = (grib_index*)grib_context_malloc_clear(c, sizeof(grib_index));
    index->context = c;
    index->product_kind = product_kind;

    index->keys = grib_read_index_keys(c, fh, err);
    if (*err)
        return NULL;

    index_count   = 0;
    index->fields = grib_read_field_tree(c, fh, files, err);
    if (*err)
        return NULL;

    index->count = index_count;

    fclose(fh);
    grib_context_free(c, files);
    return index;
}

int grib_index_search_same(grib_index* index, grib_handle* h)
{
    int err        = 0;
    char buf[STRING_VALUE_LEN] = {0,};
    size_t buflen = STRING_VALUE_LEN;
    grib_index_key* keys;
    long lval   = 0;
    double dval = 0.0;
    grib_context* c = NULL;

    if (!index)
        return GRIB_NULL_INDEX;
    c = index->context;

    keys = index->keys;

    while (keys) {
        if (keys->type == GRIB_TYPE_UNDEFINED) {
            err = grib_get_native_type(h, keys->name, &(keys->type));
            if (err)
                keys->type = GRIB_TYPE_STRING;
        }
        buflen = STRING_VALUE_LEN;
        switch (keys->type) {
            case GRIB_TYPE_STRING:
                err = grib_get_string(h, keys->name, buf, &buflen);
                if (err == GRIB_NOT_FOUND)
                    snprintf(buf, sizeof(buf), GRIB_KEY_UNDEF);
                break;
            case GRIB_TYPE_LONG:
                err = grib_get_long(h, keys->name, &lval);
                if (err == GRIB_NOT_FOUND)
                    snprintf(buf, sizeof(buf), GRIB_KEY_UNDEF);
                else
                    snprintf(buf, sizeof(buf), "%ld", lval);
                break;
            case GRIB_TYPE_DOUBLE:
                err = grib_get_double(h, keys->name, &dval);
                if (err == GRIB_NOT_FOUND)
                    snprintf(buf, sizeof(buf), GRIB_KEY_UNDEF);
                else
                    snprintf(buf, sizeof(buf), "%g", dval);
                break;
            default:
                err = GRIB_WRONG_TYPE;
                return err;
        }
        if (err && err != GRIB_NOT_FOUND) {
            grib_context_log(c, GRIB_LOG_ERROR,
                             "Unable to create index. \"%s\": %s",
                             keys->name, grib_get_error_message(err));
            return err;
        }
        snprintf(keys->value, sizeof(buf), "%s", buf);
        keys = keys->next;
    }
    grib_index_rewind(index);
    return 0;
}

int grib_index_add_file(grib_index* index, const char* filename)
{
    int message_type = 0;
    if (index->product_kind == PRODUCT_GRIB) message_type = CODES_GRIB;
    else if (index->product_kind == PRODUCT_BUFR) message_type = CODES_BUFR;
    else return GRIB_INVALID_ARGUMENT;

    return codes_index_add_file_internal(index, filename, message_type);
}

static grib_handle* new_message_from_file(int message_type, grib_context* c, FILE* f, int* error)
{
    if (message_type == CODES_GRIB)
        return grib_new_from_file(c, f, 0, error); /* headers_only=0 */
    if (message_type == CODES_BUFR)
        return bufr_new_from_file(c, f, error);
    ECCODES_ASSERT(!"new_message_from_file: invalid message type");
    return NULL;
}

#define MAX_NUM_KEYS 40

static int codes_index_add_file_internal(grib_index* index, const char* filename, int message_type)
{
    double dval;
    size_t svallen;
    size_t message_count = 0;
    long length, lval;
    char buf[1024] = {0,};
    int err = 0;
    grib_file* indfile;

    grib_index_key* index_key = NULL;
    grib_handle* h            = NULL;
    grib_field* field;
    grib_field_tree* field_tree;
    grib_file* file = NULL;
    grib_context* c;
    bool warn_about_duplicates = true;

    if (!index)
        return GRIB_NULL_INDEX;
    c = index->context;

    file = grib_file_open(filename, "r", &err);

    if (!file || !file->handle)
        return err;

    if (!index->files) {
        grib_filesid++;
        index->files = grib_file_pool_create_clone(c, grib_filesid, file);
    }
    else {
        indfile = index->files;
        while (indfile) {
            if (!strcmp(indfile->name, file->name))
                return 0;
            indfile = indfile->next;
        }
        indfile = index->files;
        while (indfile->next)
            indfile = indfile->next;
        grib_filesid++;
        indfile->next = grib_file_pool_create_clone(c, grib_filesid, file);
    }

    fseeko(file->handle, 0, SEEK_SET);

    std::map<off_t, grib_handle*> map_of_offsets;
    while ((h = new_message_from_file(message_type, c, file->handle, &err)) != NULL) {
        grib_string_list* v = 0;
        index_key           = index->keys;
        field_tree          = index->fields;
        index_key->value[0] = 0;
        message_count++;

        {
            const char* set_keys_env_var = "ECCODES_INDEX_SET_KEYS";
            char* envsetkeys = getenv(set_keys_env_var);
            if (envsetkeys) {
                grib_values set_values[MAX_NUM_KEYS];
                int set_values_count = MAX_NUM_KEYS;
                std::string copy_of_env(envsetkeys); //parse_keyval_string changes envsetkeys!
                int error = parse_keyval_string(NULL, envsetkeys, 1, GRIB_TYPE_UNDEFINED,
                        set_values, &set_values_count);
                if (!error && set_values_count != 0) {
                    err = grib_set_values(h, set_values, set_values_count);
                    if (err) {
                        grib_context_log(c, GRIB_LOG_ERROR,
                                        "codes_index_add_file: Unable to set %s", copy_of_env.c_str());
                        return err;
                    }
                } else {
                    grib_context_log(c, GRIB_LOG_ERROR, "codes_index_add_file: Unable to parse %s (%s)",
                                     set_keys_env_var, grib_get_error_message(error));
                    return error;
                }
            }
        }

        if (index->product_kind == PRODUCT_BUFR && index->unpack_bufr) {
            err = grib_set_long(h, "unpack", 1);
            if (err) {
                grib_context_log(c, GRIB_LOG_ERROR, "Unable to unpack BUFR to create index. \"%s\": %s",
                                 index_key->name, grib_get_error_message(err));
                return err;
            }
        }

        while (index_key) {
            if (index_key->type == GRIB_TYPE_UNDEFINED) {
                err = grib_get_native_type(h, index_key->name, &(index_key->type));
                if (err)
                    index_key->type = GRIB_TYPE_STRING;
            }
            svallen = 1024;
            switch (index_key->type) {
                case GRIB_TYPE_STRING:
                    err = grib_get_string(h, index_key->name, buf, &svallen);
                    if (err == GRIB_NOT_FOUND)
                        snprintf(buf, sizeof(buf), GRIB_KEY_UNDEF);
                    break;
                case GRIB_TYPE_LONG:
                    err = grib_get_long(h, index_key->name, &lval);
                    if (err == GRIB_NOT_FOUND)
                        snprintf(buf, sizeof(buf), GRIB_KEY_UNDEF);
                    else
                        snprintf(buf, sizeof(buf), "%ld", lval);
                    break;
                case GRIB_TYPE_DOUBLE:
                    err = grib_get_double(h, index_key->name, &dval);
                    if (err == GRIB_NOT_FOUND)
                        snprintf(buf, sizeof(buf), GRIB_KEY_UNDEF);
                    else
                        snprintf(buf, sizeof(buf), "%g", dval);
                    break;
                default:
                    err = GRIB_WRONG_TYPE;
                    return err;
            }
            if (err && err != GRIB_NOT_FOUND) {
                grib_context_log(c, GRIB_LOG_ERROR, "Unable to create index. key=\"%s\" (message #%lu): %s",
                                 index_key->name, message_count, grib_get_error_message(err));
                return err;
            }

            if (!index_key->values->value) {
                index_key->values->value = grib_context_strdup(c, buf);
                index_key->values_count++;
            }
            else {
                v = index_key->values;
                while (v->next && strcmp(v->value, buf))
                    v = v->next;
                if (strcmp(v->value, buf)) {
                    index_key->values_count++;
                    if (v->next)
                        v = v->next;
                    v->next        = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));
                    v->next->value = grib_context_strdup(c, buf);
                }
            }

            if (!field_tree->value) {
                field_tree->value = grib_context_strdup(c, buf);
            }
            else {
                while (field_tree->next &&
                       (field_tree->value == NULL ||
                        strcmp(field_tree->value, buf)))
                    field_tree = field_tree->next;

                if (!field_tree->value || strcmp(field_tree->value, buf)) {
                    field_tree->next =
                        (grib_field_tree*)grib_context_malloc_clear(c,
                                                                    sizeof(grib_field_tree));
                    field_tree        = field_tree->next;
                    field_tree->value = grib_context_strdup(c, buf);
                }
            }

            if (index_key->next) {
                if (!field_tree->next_level) {
                    field_tree->next_level =
                        (grib_field_tree*)grib_context_malloc_clear(c, sizeof(grib_field_tree));
                }
                field_tree = field_tree->next_level;
            }
            index_key = index_key->next;
        }

        field       = (grib_field*)grib_context_malloc_clear(c, sizeof(grib_field));
        field->file = file;
        index->count++;
        field->offset = h->offset;
        if (warn_about_duplicates) {
            const bool offset_is_unique = map_of_offsets.insert( std::pair<off_t, grib_handle*>(h->offset, h) ).second;
            if (!offset_is_unique) {
                fprintf(stderr, "ECCODES WARNING :  File '%s': field offset %ld is not unique.\n", filename, (long)h->offset);
                long edition = 0;
                if (grib_get_long(h, "edition", &edition) == GRIB_SUCCESS && edition == 2) {
                    fprintf(stderr, "ECCODES WARNING :  This can happen if the file contains multi-field GRIB messages.\n");
                    fprintf(stderr, "ECCODES WARNING :  Indexing multi-field messages is not fully supported.\n");
                }
                warn_about_duplicates = false;
            }
        }

        err = grib_get_long(h, "totalLength", &length);
        if (err)
            return err;
        field->length = length;

        if (field_tree->field) {
            grib_field* pfield = field_tree->field;
            while (pfield->next)
                pfield = pfield->next;
            pfield->next = field;
        }
        else
            field_tree->field = field;

        grib_handle_delete(h);
    }/*foreach message*/

    grib_file_close(file->name, 0, &err);

    if (err)
        return err;
    index->rewind = 1;
    if (message_count == 0) {
        grib_context_log(c, GRIB_LOG_ERROR, "File %s contains no messages", filename);
        return GRIB_END_OF_FILE;
    }

    if (c->debug) {
        fprintf(stderr, "ECCODES DEBUG %s %s\n", __func__, filename);
        grib_index_dump(stderr, index, GRIB_DUMP_FLAG_TYPE);
    }
    return GRIB_SUCCESS;
}

// int grib_index_add_file(grib_index* index, const char* filename)
// {
//     double dval;
//     size_t svallen;
//     long length,lval;
//     char buf[1024]={0,};
//     int err=0;
//     grib_file* indfile;
//     grib_file* newfile;

//     grib_index_key* index_key=NULL;
//     grib_handle* h=NULL;
//     grib_field* field;
//     grib_field_tree* field_tree;
//     grib_file* file=NULL;
//     grib_context* c;

//     if (!index) return GRIB_NULL_INDEX;
//     c=index->context;

//     file=grib_file_open(filename,"r",&err);

//     if (!file || !file->handle) return err;

//     if (!index->files) {
//         grib_filesid++;
//         newfile=(grib_file*)grib_context_malloc_clear(c,sizeof(grib_file));
//         newfile->id=grib_filesid;
//         newfile->name=strdup(file->name);
//         newfile->handle = file->handle;
//         index->files=newfile;
//     } else {
//         indfile=index->files;
//         while(indfile) {
//             if (!strcmp(indfile->name,file->name)) return 0;
//             indfile=indfile->next;
//         }
//         indfile=index->files;
//         while(indfile->next) indfile=indfile->next;
//         grib_filesid++;
//         newfile=(grib_file*)grib_context_malloc_clear(c,sizeof(grib_file));
//         newfile->id=grib_filesid;
//         newfile->name=strdup(file->name);
//         newfile->handle = file->handle;
//         indfile->next=newfile;
//     }

//     fseeko(file->handle,0,SEEK_SET);

//     while ((h=grib_handle_new_from_file(c,file->handle,&err))!=NULL) {
//         grib_string_list* v=0;
//         index_key=index->keys;
//         field_tree=index->fields;
//         index_key->value[0]=0;

//         /* process only GRIB for the moment*/
//         svallen=1024;
//         grib_get_string(h,"identifier",buf,&svallen);
//         if (strcmp(buf,"GRIB")) {
//             grib_handle_delete(h);
//             return 0;
//         }

//         while (index_key) {
//             if (index_key->type==GRIB_TYPE_UNDEFINED) {
//                 err=grib_get_native_type(h,index_key->name,&(index_key->type));
//                 if (err) index_key->type=GRIB_TYPE_STRING;
//             }
//             svallen=1024;
//             switch (index_key->type) {
//             case GRIB_TYPE_STRING:
//                 err=grib_get_string(h,index_key->name,buf,&svallen);
//                 if (err==GRIB_NOT_FOUND) snprintf(buf,1024,GRIB_KEY_UNDEF);
//                 break;
//             case GRIB_TYPE_LONG:
//                 err=grib_get_long(h,index_key->name,&lval);
//                 if (err==GRIB_NOT_FOUND) snprintf(buf,1024,GRIB_KEY_UNDEF);
//                 else snprintf(buf,1024,"%ld",lval);
//                 break;
//             case GRIB_TYPE_DOUBLE:
//                 err=grib_get_double(h,index_key->name,&dval);
//                 if (err==GRIB_NOT_FOUND) snprintf(buf,1024,GRIB_KEY_UNDEF);
//                 else snprintf(buf,1024,"%g",dval);
//                 break;
//             default :
//                 err=GRIB_WRONG_TYPE;
//                 return err;
//             }
//             if (err && err != GRIB_NOT_FOUND) {
//                 grib_context_log(c,GRIB_LOG_ERROR,"Unable to create index. \"%s\": %s",index_key->name,grib_get_error_message(err));
//                 return err;
//             }

//             if (!index_key->values->value) {
//                 index_key->values->value=grib_context_strdup(c,buf);
//                 index_key->values_count++;
//             } else {
//                 v=index_key->values;
//                 while (v->next && strcmp(v->value,buf)) v=v->next;
//                 if (strcmp(v->value,buf)) {
//                     index_key->values_count++;
//                     if (v->next) v=v->next;
//                     v->next=(grib_string_list*)grib_context_malloc_clear(c,sizeof(grib_string_list));
//                     v->next->value=grib_context_strdup(c,buf);
//                 }
//             }

//             if (!field_tree->value) {
//                 field_tree->value=grib_context_strdup(c,buf);
//             } else {
//                 while (field_tree->next &&
//                         (field_tree->value==NULL ||
//                                 strcmp(field_tree->value,buf)))
//                     field_tree=field_tree->next;

//                 if (!field_tree->value || strcmp(field_tree->value,buf)){
//                     field_tree->next=
//                             (grib_field_tree*)grib_context_malloc_clear(c,
//                                     sizeof(grib_field_tree));
//                     field_tree=field_tree->next;
//                     field_tree->value=grib_context_strdup(c,buf);
//                 }
//             }

//             if (index_key->next) {
//                 if (!field_tree->next_level) {
//                     field_tree->next_level=
//                             (grib_field_tree*)grib_context_malloc_clear(c,sizeof(grib_field_tree));
//                 }
//                 field_tree=field_tree->next_level;
//             }
//             index_key=index_key->next;
//         }

//         field=(grib_field*)grib_context_malloc_clear(c,sizeof(grib_field));
//         field->file=file;
//         index->count++;
//         field->offset=h->offset;

//         err=grib_get_long(h,"totalLength",&length);
//         if (err) return err;
//         field->length=length;

//         if (field_tree->field) {
//             grib_field* pfield=field_tree->field;
//             while (pfield->next) pfield=pfield->next;
//             pfield->next=field;
//         } else
//             field_tree->field=field;

//         if (h) grib_handle_delete(h);

//     }

//     grib_file_close(file->name, 0, &err);

//     if (err) return err;
//     index->rewind=1;
//     return GRIB_SUCCESS;
// }


grib_index* grib_index_new_from_file(grib_context* c, const char* filename, const char* keys, int* err)
{
    grib_index* index = NULL;

    if (!c)
        c = grib_context_get_default();

    index = grib_index_new(c, keys, err);

    *err = grib_index_add_file(index, filename);
    if (*err) {
        grib_index_delete(index);
        return NULL;
    }

    return index;
}

int grib_index_get_size(const grib_index* index, const char* key, size_t* size)
{
    grib_index_key* k = index->keys;
    while (k && strcmp(k->name, key))
        k = k->next;
    if (!k)
        return GRIB_NOT_FOUND;
    *size = k->values_count;
    return 0;
}

int grib_index_get_string(const grib_index* index, const char* key, char** values, size_t* size)
{
    grib_index_key* k = index->keys;
    grib_string_list* kv;
    int i = 0;
    while (k && strcmp(k->name, key))
        k = k->next;
    if (!k)
        return GRIB_NOT_FOUND;
    if (k->values_count > *size)
        return GRIB_ARRAY_TOO_SMALL;
    kv = k->values;
    while (kv) {
        if (kv->value == NULL)
            return GRIB_IO_PROBLEM;
        values[i++] = grib_context_strdup(index->context, kv->value);
        kv          = kv->next;
    }
    *size = k->values_count;
    qsort(values, *size, sizeof(char*), &compare_string);

    return GRIB_SUCCESS;
}

int grib_index_get_long(const grib_index* index, const char* key, long* values, size_t* size)
{
    grib_index_key* k = index->keys;
    grib_string_list* kv;
    int i = 0;
    while (k && strcmp(k->name, key))
        k = k->next;
    if (!k)
        return GRIB_NOT_FOUND;
    if (k->type != GRIB_TYPE_LONG) {
        grib_context_log(index->context, GRIB_LOG_ERROR, "Unable to get index %s as long", key);
        return GRIB_WRONG_TYPE;
    }
    if (k->values_count > *size)
        return GRIB_ARRAY_TOO_SMALL;
    kv = k->values;
    while (kv) {
        if (strcmp(kv->value, GRIB_KEY_UNDEF))
            values[i++] = atol(kv->value);
        else
            values[i++] = UNDEF_LONG;
        kv = kv->next;
    }
    *size = k->values_count;
    qsort(values, *size, sizeof(long), &compare_long);

    return GRIB_SUCCESS;
}

int grib_index_get_double(const grib_index* index, const char* key, double* values, size_t* size)
{
    grib_index_key* k = index->keys;
    grib_string_list* kv;
    int i = 0;
    while (k && strcmp(k->name, key))
        k = k->next;
    if (!k)
        return GRIB_NOT_FOUND;
    if (k->type != GRIB_TYPE_DOUBLE) {
        grib_context_log(index->context, GRIB_LOG_ERROR, "Unable to get index %s as double", key);
        return GRIB_WRONG_TYPE;
    }
    if (k->values_count > *size)
        return GRIB_ARRAY_TOO_SMALL;
    kv = k->values;
    while (kv) {
        if (strcmp(kv->value, GRIB_KEY_UNDEF))
            values[i++] = atof(kv->value);
        else
            values[i++] = UNDEF_DOUBLE;

        kv = kv->next;
    }
    *size = k->values_count;
    qsort(values, *size, sizeof(double), &compare_double);

    return GRIB_SUCCESS;
}

int grib_index_select_long(grib_index* index, const char* skey, long value)
{
    grib_index_key* key = NULL;
    int err             = GRIB_NOT_FOUND;

    if (!index) {
        grib_context* c = grib_context_get_default();
        grib_context_log(c, GRIB_LOG_ERROR, "null index pointer");
        return GRIB_INTERNAL_ERROR;
    }
    index->orderby = 0;
    key            = index->keys;

    while (key) {
        if (!strcmp(key->name, skey)) {
            err = 0;
            break;
        }
        key = key->next;
    }

    if (err) {
        grib_context_log(index->context, GRIB_LOG_ERROR,
                         "key \"%s\" not found in index", skey);
        return err;
    }
    ECCODES_ASSERT(key);
    snprintf(key->value, sizeof(key->value), "%ld", value);
    grib_index_rewind(index);
    return 0;
}

int grib_index_select_double(grib_index* index, const char* skey, double value)
{
    grib_index_key* key = NULL;
    int err             = GRIB_NOT_FOUND;

    if (!index) {
        grib_context* c = grib_context_get_default();
        grib_context_log(c, GRIB_LOG_ERROR, "null index pointer");
        return GRIB_INTERNAL_ERROR;
    }
    index->orderby = 0;
    key            = index->keys;

    while (key) {
        if (!strcmp(key->name, skey)) {
            err = 0;
            break;
        }
        key = key->next;
    }

    if (err) {
        grib_context_log(index->context, GRIB_LOG_ERROR,
                         "key \"%s\" not found in index", skey);
        return err;
    }
    ECCODES_ASSERT(key);
    snprintf(key->value, sizeof(key->value), "%g", value);
    grib_index_rewind(index);
    return 0;
}

int grib_index_select_string(grib_index* index, const char* skey, const char* value)
{
    grib_index_key* key = NULL;
    int err             = GRIB_NOT_FOUND;

    if (!index) {
        grib_context* c = grib_context_get_default();
        grib_context_log(c, GRIB_LOG_ERROR, "null index pointer");
        return GRIB_INTERNAL_ERROR;
    }
    index->orderby = 0;
    key            = index->keys;

    while (key) {
        if (!strcmp(key->name, skey)) {
            err = 0;
            break;
        }
        key = key->next;
    }

    if (err) {
        grib_context_log(index->context, GRIB_LOG_ERROR,
                         "key \"%s\" not found in index", skey);
        return err;
    }
    ECCODES_ASSERT(key);
    snprintf(key->value, sizeof(key->value), "%s", value);
    grib_index_rewind(index);
    return 0;
}

grib_handle* codes_index_get_handle(grib_field* field, int message_type, int* err)
{
    grib_handle* h = NULL;
    typedef grib_handle* (*message_new_proc)(grib_context*, FILE*, int*);
    message_new_proc message_new = NULL;

    if (!field->file) {
        grib_context_log(grib_context_get_default(), GRIB_LOG_ERROR, "codes_index_get_handle: NULL file handle");
        *err = GRIB_INTERNAL_ERROR;
        return NULL;
    }

    grib_file_open(field->file->name, "r", err);

    if (*err != GRIB_SUCCESS)
        return NULL;
    switch (message_type) {
        case CODES_GRIB:
            message_new = codes_grib_handle_new_from_file;
            break;
        case CODES_BUFR:
            message_new = codes_bufr_handle_new_from_file;
            break;
        default:
            grib_context_log(grib_context_get_default(), GRIB_LOG_ERROR, "codes_index_get_handle: invalid message type");
            *err = GRIB_INTERNAL_ERROR;
            return NULL;
    }

    fseeko(field->file->handle, field->offset, SEEK_SET);
    h = message_new(0, field->file->handle, err);
    if (*err != GRIB_SUCCESS)
        return NULL;

    grib_file_close(field->file->name, 0, err);
    return h;
}

static int grib_index_execute(grib_index* index)
{
    grib_index_key* keys = NULL;
    grib_field_tree* fields;

    if (!index)
        return GRIB_INTERNAL_ERROR;
    keys = index->keys;

    fields        = index->fields;
    index->rewind = 0;

    while (keys) {
        char* value;
        if (keys->value[0])
            value = keys->value;
        else {
            grib_context_log(index->context, GRIB_LOG_ERROR,
                             "please select a value for index key \"%s\"",
                             keys->name);
            return GRIB_NOT_FOUND;
        }

        while (fields && strcmp(fields->value, value))
            fields = fields->next;
        if (fields && !strcmp(fields->value, value)) {
            if (fields->next_level) {
                keys   = keys->next;
                fields = fields->next_level;
            }
            else {
                index->current = index->fieldset;
                while (index->current->next)
                    index->current = index->current->next;
                index->current->field = fields->field;
                return 0;
            }
        }
        else
            return GRIB_END_OF_INDEX;
    }

    return 0;
}

static void grib_dump_key_values(FILE* fout, grib_string_list* values)
{
    grib_string_list* sl = values;
    int first            = 1; /* boolean for commas */
    fprintf(fout, "values = ");
    while (sl) {
        if (!first) {
            fprintf(fout, ", ");
        }
        fprintf(fout, "%s", sl->value);
        first = 0;
        sl    = sl->next;
    }
    fprintf(fout, "\n");
}

static void grib_dump_index_keys(FILE* fout, grib_index_key* keys, unsigned long flags)
{
    if (!keys)
        return;
    fprintf(fout, "key name = %s\n", keys->name);
    if ((flags & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(fout, "key type = %s\n", grib_get_type_name(keys->type));
    }
    grib_dump_key_values(fout, keys->values);
    grib_dump_index_keys(fout, keys->next, flags);
}

#ifdef INDEX_DUMPS
static void grib_dump_files(FILE* fout, grib_file* files)
{
    if (!files) return;
    fprintf(fout, "file = %s\n", files->name);
    fprintf(fout, "ID = %d\n", files->id);
    grib_dump_files(fout, files->next);
}
static void grib_dump_field(FILE* fout, grib_field* field)
{
    if (!field) return;
    fprintf(fout, "field name = %s\n", field->file->name);
    /*fprintf(fout, "field FID = %d\n", field->file->id);
     * fprintf(fout, "field offset = %ld\n", field->offset);
     * fprintf(fout, "field length = %ld\n", field->length);
     */

    grib_dump_field(fout, field->next);
}
static void grib_dump_field_tree(FILE* fout, grib_field_tree* tree)
{
    if(!tree) return;
    grib_dump_field(fout,tree->field);

    fprintf(fout, "tree value = %s\n", tree->value);

    grib_dump_field_tree(fout,tree->next_level);
    grib_dump_field_tree(fout,tree->next);
}
#endif

int grib_index_dump_file(FILE* fout, const char* filename, unsigned long flags)
{
    int err           = 0;
    grib_index* index = NULL;
    grib_context* c   = grib_context_get_default();
    FILE* fh          = NULL;

    ECCODES_ASSERT(fout);
    ECCODES_ASSERT(filename);
    index = grib_index_read(c, filename, &err);
    if (err)
        return err;

    /* To get the GRIB files referenced we have */
    /* to resort to low level reading of the index file! */
    fh = fopen(filename, "r");
    if (fh) {
        grib_file *file, *f;
        unsigned char marker = 0;
        char* identifier     = grib_read_string(c, fh, &err);
        if (err)
            return err;
        grib_context_free(c, identifier);
        err = grib_read_uchar(fh, &marker);
        if (err)
            return err;
        file = grib_read_files(c, fh, &err);
        if (err)
            return err;
        f = file;
        while (f) {
            grib_file* prev = f;
            fprintf(fout, "%s File: %s\n",
                    index->product_kind == PRODUCT_GRIB ? "GRIB" : "BUFR", f->name);
            grib_context_free(c, f->name);
            f = f->next;
            grib_context_free(c, prev);
        }
        fclose(fh);
    }

    grib_index_dump(fout, index, flags);
    grib_index_delete(index);

    return GRIB_SUCCESS;
}

void grib_index_dump(FILE* fout, grib_index* index, unsigned long flags)
{
    if (!index)
        return;
    ECCODES_ASSERT(fout);

    /* The grib_dump_files does not print anything as  */
    /* the index object does not store the file names! */
    /* grib_dump_files(fout, index->files); */

    fprintf(fout, "Index keys:\n");
    grib_dump_index_keys(fout, index->keys, flags);

    /*
     * fprintf(fout, "Index field tree:\n");
     * grib_dump_field_tree(fout, index->fields);
     */

    fprintf(fout, "Index count = %d\n", index->count);
}

char* grib_get_field_file(grib_index* index, off_t* offset)
{
    char* file = NULL;
    if (index && index->current && index->current->field) {
        file    = index->current->field->file->name;
        *offset = index->current->field->offset;
    }
    return file;
}

grib_handle* grib_handle_new_from_index(grib_index* index, int* err)
{
    ProductKind pkind = index->product_kind;
    if (pkind == PRODUCT_GRIB)
        return codes_new_from_index(index, CODES_GRIB, err);
    if (pkind == PRODUCT_BUFR)
        return codes_new_from_index(index, CODES_BUFR, err);
    return NULL;
}

grib_handle* codes_new_from_index(grib_index* index, int message_type, int* err)
{
    /*grib_index_key* keys;*/
    grib_field_list *fieldset, *next;
    grib_handle* h  = NULL;
    grib_context* c = NULL;
    *err = 0;

    if (!index)
        return NULL;
    c = index->context;
    if (!index->rewind) {
        // ECC-1764
        if (!index->current || !index->current->field) {
            *err = GRIB_END_OF_INDEX;
            return NULL;
        }

        if (index->current->field->next)
            index->current->field = index->current->field->next;
        else if (index->current->next)
            index->current = index->current->next;
        else {
            *err = GRIB_END_OF_INDEX;
            return NULL;
        }

        h = codes_index_get_handle(index->current->field, message_type, err);
        return h;
    }

    if (!index->fieldset) {
        index->fieldset = (grib_field_list*)grib_context_malloc_clear(index->context,
                                                                      sizeof(grib_field_list));
        if (!index->fieldset) {
            grib_context_log(index->context, GRIB_LOG_ERROR,
                             "Unable to allocate %zu bytes", sizeof(grib_field_list));
            return NULL;
        }
        index->current = index->fieldset;
    }
    else {
        fieldset = index->fieldset;
        while (fieldset->next) {
            next = fieldset->next;
            grib_context_free(c, fieldset);
            fieldset = next;
        }
        fieldset->field = NULL;
        fieldset->next  = NULL;
        index->fieldset = fieldset;
        index->current  = fieldset;
    }

    *err = GRIB_END_OF_INDEX;
    h    = NULL;
    /*keys=index->keys;*/

    if ((*err = grib_index_execute(index)) == GRIB_SUCCESS) {
        if (!index->fieldset) {
            *err = GRIB_END_OF_INDEX;
            return NULL;
        }
        index->current = index->fieldset;
        h              = codes_index_get_handle(index->current->field, message_type, err);
    }
    return h;
}

void grib_index_rewind(grib_index* index)
{
    index->rewind = 1;
}

// static grib_index_key* search_key(grib_index_key* keys, grib_index_key* to_search)
// {
//     if (!keys || !strcmp(keys->name, to_search->name))
//         return keys;
//     return search_key(keys->next, to_search);
// }

// int grib_index_search(grib_index* index, grib_index_key* keys)
// {
//     grib_index_key* ki = index->keys;
//     grib_index_key* ks = keys;

//     while (ks) {
//         ki = search_key(ki, ks);
//         if (!ki) {
//             ki = index->keys;
//             ki = search_key(ki, ks);
//         }
//         if (ki)
//             snprintf(ki->value, 1024, "%s", ks->value);
//         ks = ks->next;
//     }

//     grib_index_rewind(index);
//     return 0;
// }

int codes_index_set_product_kind(grib_index* index, ProductKind product_kind)
{
    if (!index)
        return GRIB_INVALID_ARGUMENT;

    if (product_kind == PRODUCT_GRIB || product_kind == PRODUCT_BUFR) {
        index->product_kind = product_kind;
    } else {
        return GRIB_INVALID_ARGUMENT;
    }
    return GRIB_SUCCESS;
}

int codes_index_set_unpack_bufr(grib_index* index, int unpack)
{
    if (!index)
        return GRIB_INVALID_ARGUMENT;
    if (index->product_kind != PRODUCT_BUFR)
        return GRIB_INVALID_ARGUMENT;
    index->unpack_bufr = unpack;
    return GRIB_SUCCESS;
}

/* Return 1 if the file is an index file. 0 otherwise */
int is_index_file(const char* filename)
{
    FILE* fh;
    char buf[8] = {0,};
    /* Only read the first 6 characters of identifier. Exclude version */
    const size_t numChars = 6;
    const char* id_grib   = "GRBIDX";
    const char* id_bufr   = "BFRIDX";
    int ret         = 0;
    size_t size     = 0;

    fh = fopen(filename, "r");
    if (!fh)
        return 0;

    size = fread(buf, 1, 1, fh);
    if (size != 1) {
        fclose(fh);
        return 0;
    }
    size = fread(buf, numChars, 1, fh);
    if (size != 1) {
        fclose(fh);
        return 0;
    }

    ret = (strcmp(buf, id_grib)==0 || strcmp(buf, id_bufr)==0);

    fclose(fh);

    return ret;
}
