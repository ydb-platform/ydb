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
#include <cstdio>

#define GRIB_MAX_OPENED_FILES 200

#if GRIB_PTHREADS
static pthread_once_t once    = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex1, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex1;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_filepool_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex1);
            once = 1;
        }
    }
}
#endif

static short next_id = 0;

static grib_file* grib_file_new(grib_context* c, const char* name, int* err);

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

static grib_file_pool file_pool = {
    0,                    /* grib_context* context;*/
    0,                    /* grib_file* first;*/
    0,                    /* grib_file* current; */
    0,                    /* size_t size;*/
    0,                    /* int number_of_opened_files;*/
    GRIB_MAX_OPENED_FILES /* int max_opened_files; */
};

void grib_file_pool_clean()
{
    grib_file *file, *next;

    if (!file_pool.first)
        return;

    file = file_pool.first;
    while (file) {
        next = file->next;
        grib_file_delete(file);
        file = next;
    }
}

// static void grib_file_pool_change_id()
// {
//     grib_file* file;
//     if (!file_pool.first)
//         return;
//     file = file_pool.first;
//     while (file) {
//         file->id += 1000;
//         file = file->next;
//     }
// }

// static grib_file* grib_read_file(grib_context* c, FILE* fh, int* err)
// {
//     short marker = 0;
//     short id     = 0;
//     grib_file* file;
//     *err = grib_read_short(fh, &marker);
//     if (!marker)
//         return NULL;
//     file         = (grib_file*)grib_context_malloc_clear(c, sizeof(grib_file));
//     file->buffer = 0;
//     file->name   = grib_read_string(c, fh, err);
//     if (*err)
//         return NULL;
//     *err     = grib_read_short(fh, &id);
//     file->id = id;
//     if (*err)
//         return NULL;
//     file->next = grib_read_file(c, fh, err);
//     if (*err)
//         return NULL;
//     return file;
// }

// static int grib_write_file(FILE* fh, grib_file* file)
// {
//     int err = 0;
//     if (!file)
//         return grib_write_null_marker(fh);
//     err = grib_write_not_null_marker(fh);
//     if (err)
//         return err;
//     err = grib_write_string(fh, file->name);
//     if (err)
//         return err;
//     err = grib_write_short(fh, (short)file->id);
//     if (err)
//         return err;
//     return grib_write_file(fh, file->next);
// }

// grib_file* grib_file_pool_get_files()
// {
//     return file_pool.first;
// }

// int grib_file_pool_read(grib_context* c, FILE* fh)
// {
//     int err      = 0;
//     short marker = 0;
//     grib_file* file;
//     if (!c) c = grib_context_get_default();
//     err = grib_read_short(fh, &marker);
//     if (!marker) {
//         grib_context_log(c, GRIB_LOG_ERROR,
//                          "Unable to find file information in index file");
//         return GRIB_INVALID_FILE;
//     }
//     grib_file_pool_change_id();
//     file = file_pool.first;
//     while (file->next)
//         file = file->next;
//     file->next = grib_read_file(c, fh, &err);
//     if (err)
//         return err;
//     return GRIB_SUCCESS;
// }

// int grib_file_pool_write(FILE* fh)
// {
//     int err = 0;
//     if (!file_pool.first)
//         return grib_write_null_marker(fh);
//     err = grib_write_not_null_marker(fh);
//     if (err)
//         return err;
//     return grib_write_file(fh, file_pool.first);
// }

grib_file* grib_file_open(const char* filename, const char* mode, int* err)
{
    grib_file *file = 0, *prev = 0;
    int same_mode = 0;
    int is_new    = 0;
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);

    if (!file_pool.context)
        file_pool.context = grib_context_get_default();

    if (file_pool.current && !grib_inline_strcmp(filename, file_pool.current->name)) {
        file = file_pool.current;
    }
    else {
        GRIB_MUTEX_LOCK(&mutex1);
        file = file_pool.first;
        while (file) {
            if (!grib_inline_strcmp(filename, file->name))
                break;
            prev = file;
            file = file->next;
        }
        if (!file) {
            is_new = 1;
            file   = grib_file_new(file_pool.context, filename, err);
            if (prev)
                prev->next = file;
            file_pool.current = file;
            if (!prev)
                file_pool.first = file;
            file_pool.size++;
        }
        GRIB_MUTEX_UNLOCK(&mutex1);
    }

    if (file->mode)
        same_mode = grib_inline_strcmp(mode, file->mode) ? 0 : 1;
    if (file->handle && same_mode) {
        *err = 0;
        return file;
    }

    GRIB_MUTEX_LOCK(&mutex1);
    if (!same_mode && file->handle) {
        fclose(file->handle);
    }

    if (!file->handle) {
        if (!is_new && *mode == 'w') {
            file->handle = fopen(file->name, "a");
        }
        else {
            file->handle = fopen(file->name, mode);
        }

        if (!file->handle) {
            grib_context_log(file->context, GRIB_LOG_PERROR, "%s: Cannot open file '%s'", __func__, file->name);
            *err = GRIB_IO_PROBLEM;
            GRIB_MUTEX_UNLOCK(&mutex1);
            return NULL;
        }
        if (file->mode) free(file->mode);
        file->mode = strdup(mode);
        if (file_pool.context->io_buffer_size) {
#ifdef POSIX_MEMALIGN
            if (posix_memalign((void**)&(file->buffer), sysconf(_SC_PAGESIZE), file_pool.context->io_buffer_size)) {
                grib_context_log(file->context, GRIB_LOG_FATAL, "posix_memalign unable to allocate io_buffer");
            }
#else
            file->buffer = (char*)malloc(file_pool.context->io_buffer_size);
            if (!file->buffer) {
                grib_context_log(file->context, GRIB_LOG_FATAL, "Unable to allocate io_buffer\n");
            }
#endif
            setvbuf(file->handle, file->buffer, _IOFBF, file_pool.context->io_buffer_size);
        }

        file_pool.number_of_opened_files++;
    }

    GRIB_MUTEX_UNLOCK(&mutex1);
    return file;
}

grib_file* grib_file_pool_create_clone(grib_context* c, short clone_id, grib_file* pool_file)
{
    if(pool_file)
    {
        grib_file* newfile          = (grib_file*)grib_context_malloc_clear(c, sizeof(grib_file));
        newfile->id                 = clone_id;
        newfile->name               = strdup(pool_file->name);
        newfile->handle             = pool_file->handle;
        newfile->pool_file          = pool_file;
        newfile->pool_file_refcount = 0;

        GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
        GRIB_MUTEX_LOCK(&mutex1);

        ++pool_file->pool_file_refcount;

        GRIB_MUTEX_UNLOCK(&mutex1);

        return newfile;
    }
    else
        return 0;
}

void grib_file_pool_delete_clone(grib_file* cloned_file)
{
    grib_file* pool_file = cloned_file->pool_file;
    if(pool_file)
    {
        GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
        GRIB_MUTEX_LOCK(&mutex1);
        if(pool_file->pool_file_refcount > 0)
        {
            --pool_file->pool_file_refcount;

            if (pool_file->pool_file_refcount == 0)
                grib_file_pool_delete_file(pool_file);
        }

        GRIB_MUTEX_UNLOCK(&mutex1);
    }

    grib_file_delete(cloned_file);
}

void grib_file_pool_delete_file(grib_file* file)
{
    grib_file* prev = NULL;
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex1);

    if (file == file_pool.first) {
        file_pool.first   = file->next;
        file_pool.current = file->next;
        file_pool.size--;
    }
    else {
        prev              = file_pool.first;
        file_pool.current = file_pool.first;
        while (prev) {
            if (prev->next == file)
                break;
            prev = prev->next;
        }
        DEBUG_ASSERT(prev);
        if (prev) {
            prev->next = file->next;
            file_pool.size--;
        }
    }

    if (file->handle) {
        fclose(file->handle);
        file->handle = NULL;
        file_pool.number_of_opened_files--;
    }
    grib_file_delete(file);
    GRIB_MUTEX_UNLOCK(&mutex1);
}

void grib_file_close(const char* filename, int force, int* err)
{
    grib_file* file = NULL;
    const grib_context* context = grib_context_get_default();

    /* Performance: keep the files open to avoid opening and closing files when writing the output. */
    /* So only call fclose() when too many files are open. */
    /* Also see ECC-411 */
    int do_close = (file_pool.number_of_opened_files > context->file_pool_max_opened_files);
    if (force == 1)
        do_close = 1; /* Can be overridden with the force argument */

    if (do_close) {
        /*printf("+++++++++++++ closing file %s (n=%d)\n",filename, file_pool.number_of_opened_files);*/
        GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
        GRIB_MUTEX_LOCK(&mutex1);
        file = grib_get_file(filename, err);
        if (file->handle) {
            if (fclose(file->handle) != 0) {
                *err = GRIB_IO_PROBLEM;
            }
            if (file->buffer) {
                free(file->buffer);
                file->buffer = 0;
            }
            file->handle = NULL;
            file_pool.number_of_opened_files--;
        }
        GRIB_MUTEX_UNLOCK(&mutex1);
    }
}

void grib_file_close_all(int* err)
{
    grib_file* file = NULL;
    if (!file_pool.first)
        return;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex1);

    file = file_pool.first;
    while (file) {
        if (file->handle) {
            if (fclose(file->handle) != 0) {
                *err = GRIB_IO_PROBLEM;
            }
            file->handle = NULL;
        }
        file = file->next;
    }

    GRIB_MUTEX_UNLOCK(&mutex1);
}

grib_file* grib_get_file(const char* filename, int* err)
{
    grib_file* file = NULL;

    if (!file_pool.current) {
        *err = GRIB_IO_PROBLEM;
        return NULL;
    }

    if (file_pool.current->name && !grib_inline_strcmp(filename, file_pool.current->name)) {
        return file_pool.current;
    }

    file = file_pool.first;
    while (file) {
        if (!grib_inline_strcmp(filename, file->name))
            break;
        file = file->next;
    }
    if (!file)
        file = grib_file_new(0, filename, err);

    return file;
}

// grib_file* grib_find_file(short id)
// {
//     grib_file* file = NULL;
//     if (file_pool.current->name && id == file_pool.current->id) {
//         return file_pool.current;
//     }
//     file = file_pool.first;
//     while (file) {
//         if (id == file->id)
//             break;
//         file = file->next;
//     }
//     return file;
// }

static grib_file* grib_file_new(grib_context* c, const char* name, int* err)
{
    if (!c) c = grib_context_get_default();

    grib_file* file = (grib_file*)grib_context_malloc_clear(c, sizeof(grib_file));
    if (!file) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate memory", __func__);
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);

    file->name = strdup(name);
    file->id   = next_id;

    GRIB_MUTEX_LOCK(&mutex1);
    next_id++;
    GRIB_MUTEX_UNLOCK(&mutex1);

    file->mode               = 0;
    file->handle             = 0;
    file->refcount           = 0;
    file->context            = c;
    file->next               = 0;
    file->pool_file          = 0;
    file->pool_file_refcount = 0;
    file->buffer             = 0;
    return file;
}

void grib_file_delete(grib_file* file)
{
    if (!file) return;

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex1);
    /* GRIB-803: cannot call fclose yet! Causes crash */
    /* TODO: Set handle to NULL in filepool too */

    //if (file->handle) {
    //    if (fclose(file->handle) != 0) {
    //        perror(file->name);
    //    }
    //}

    free(file->name); file->name = 0;
    free(file->mode); file->mode = 0;
    free(file->buffer); file->buffer = 0;
    grib_context_free(file->context, file);
    /* file = NULL; */
    GRIB_MUTEX_UNLOCK(&mutex1);
}

void grib_file_pool_print(const char* title, FILE* out)
{
    int i = 0;
    grib_file* file = file_pool.first;
    printf("%s: size=%zu, num_opened_files=%d\n", title, file_pool.size, file_pool.number_of_opened_files);
    while (file) {
        printf("%s:\tfile_pool entry %d = %s\n", title, i++, file->name);
        file = file->next;
    }
    printf("\n");
}
