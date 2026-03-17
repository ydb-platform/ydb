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
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#ifndef ECCODES_ON_WINDOWS
 #include <unistd.h>
#else
 #include <fcntl.h> /* Windows: for _O_BINARY */
#endif

#ifdef ENABLE_FLOATING_POINT_EXCEPTIONS
 #include <fenv.h>
 int feenableexcept(int excepts);
#endif

grib_string_list grib_file_not_found;

/* Windows always has a colon in pathnames e.g. C:\temp\file. So instead we use semi-colons as delimiter */
/* in order to have multiple definitions/samples directories */
#ifdef ECCODES_ON_WINDOWS
 #define ECC_PATH_DELIMITER_CHAR ';'
 #define ECC_PATH_DELIMITER_STR ";"
#else
 #define ECC_PATH_DELIMITER_CHAR ':'
 #define ECC_PATH_DELIMITER_STR ":"
#endif

#if GRIB_PTHREADS
static pthread_once_t once = PTHREAD_ONCE_INIT;

static pthread_mutex_t mutex_mem = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_c   = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex_c, &attr);
    pthread_mutex_init(&mutex_mem, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex_mem;
static omp_nest_lock_t mutex_c;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_context_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex_mem);
            omp_init_nest_lock(&mutex_c);
            once = 1;
        }
    }
}
#endif


#if MANAGE_MEM

#else

static void default_long_lasting_free(const grib_context* c, void* p)
{
    free(p);
}

static void* default_long_lasting_malloc(const grib_context* c, size_t size)
{
    void* ret;
    ret = malloc(size);
    if (!ret) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return ret;
}

static void default_buffer_free(const grib_context* c, void* p)
{
    free(p);
}

static void* default_buffer_malloc(const grib_context* c, size_t size)
{
    void* ret;
    ret = malloc(size);
    if (!ret) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return ret;
}

static void* default_buffer_realloc(const grib_context* c, void* p, size_t size)
{
    void* ret;
    ret = realloc(p, size);
    if (!ret) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return ret;
}

static void default_free(const grib_context* c, void* p)
{
    free(p);
}

static void* default_malloc(const grib_context* c, size_t size)
{
    void* ret;
    ret = malloc(size);
    if (!ret) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return ret;
}

static void* default_realloc(const grib_context* c, void* p, size_t size)
{
    void* ret;
    ret = realloc(p, size);
    if (!ret) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return ret;
}
#endif

static size_t default_read(const grib_context* c, void* ptr, size_t size, void* stream)
{
    return fread(ptr, 1, size, (FILE*)stream);
}

static off_t default_tell(const grib_context* c, void* stream)
{
    return ftello((FILE*)stream);
}

static off_t default_seek(const grib_context* c, off_t offset, int whence, void* stream)
{
    return fseeko((FILE*)stream, offset, whence);
}

static int default_feof(const grib_context* c, void* stream)
{
    return feof((FILE*)stream);
}

static size_t default_write(const grib_context* c, const void* ptr, size_t size, void* stream)
{
    return fwrite(ptr, 1, size, (FILE*)stream);
}

size_t grib_context_read(const grib_context* c, void* ptr, size_t size, void* stream)
{
    if (!c)
        c = grib_context_get_default();
    return c->read(c, ptr, size, stream);
}

off_t grib_context_tell(const grib_context* c, void* stream)
{
    if (!c)
        c = grib_context_get_default();
    return c->tell(c, stream);
}

int grib_context_seek(const grib_context* c, off_t offset, int whence, void* stream)
{
    if (!c)
        c = grib_context_get_default();
    return c->seek(c, offset, whence, stream);
}

// int grib_context_eof(const grib_context* c, void* stream)
// {
//     if (!c)
//         c = grib_context_get_default();
//     return c->eof(c, stream);
// }

// size_t grib_context_write(const grib_context* c, const void* ptr, size_t size, void* stream)
// {
//     if (!c)
//         c = grib_context_get_default();
//     return c->write(c, ptr, size, stream);
// }

static void default_log(const grib_context* c, int level, const char* mess)
{
    if (!c)
        c = grib_context_get_default();
    if (level == GRIB_LOG_ERROR) {
        fprintf(c->log_stream, "ECCODES ERROR   :  %s\n", mess);
        /*ECCODES_ASSERT(1==0);*/
    }
    if (level == GRIB_LOG_FATAL)
        fprintf(c->log_stream, "ECCODES ERROR   :  %s\n", mess);
    if (level == GRIB_LOG_DEBUG && c->debug > 0)
        fprintf(c->log_stream, "ECCODES DEBUG   :  %s\n", mess);
    if (level == GRIB_LOG_WARNING)
        fprintf(c->log_stream, "ECCODES WARNING :  %s\n", mess);
    if (level == GRIB_LOG_INFO)
        fprintf(c->log_stream, "ECCODES INFO    :  %s\n", mess);

    if (level == GRIB_LOG_FATAL) {
        ECCODES_ASSERT(0);
    }

    if (getenv("ECCODES_FAIL_IF_LOG_MESSAGE")) {
        long n = atol(getenv("ECCODES_FAIL_IF_LOG_MESSAGE"));
        if (n >= 1 && level == GRIB_LOG_ERROR)
            ECCODES_ASSERT(0);
        if (n >= 2 && level == GRIB_LOG_WARNING)
            ECCODES_ASSERT(0);
    }
}

static void default_print(const grib_context* c, void* descriptor, const char* mess)
{
    fprintf((FILE*)descriptor, "%s", mess);
}

void grib_context_set_print_proc(grib_context* c, grib_print_proc p)
{
    c = c ? c : grib_context_get_default();
    /* Set printing back to the default if p is NULL */
    c->print = (p ? p : &default_print);
}

void grib_context_set_data_quality_checks(grib_context* c, int val)
{
    c = c ? c : grib_context_get_default();
    // If val == 0, disable data quality checks
    // If val == 1, failure results in an error
    // If val == 2, failure results in a warning
    ECCODES_ASSERT(val == 0 || val == 1 || val == 2);

    c->grib_data_quality_checks = val;
}

void grib_context_set_debug(grib_context* c, int mode)
{
    c = c ? c : grib_context_get_default();
    c->debug = mode;
}

void grib_context_set_logging_proc(grib_context* c, grib_log_proc p)
{
    c = c ? c : grib_context_get_default();
    /* Set logging back to the default if p is NULL */
    c->output_log = (p ? p : &default_log);
}

long grib_get_api_version()
{
    return ECCODES_VERSION;
}

void grib_print_api_version(FILE* out)
{
    fprintf(out, "%d.%d.%d",
            ECCODES_MAJOR_VERSION,
            ECCODES_MINOR_VERSION,
            ECCODES_REVISION_VERSION);

    if (ECCODES_MAJOR_VERSION < 1) {
        fprintf(out, "%s", " PRE-RELEASE");
    }
}

const char* grib_get_package_name()
{
    return "ecCodes";
}

#define DEFAULT_FILE_POOL_MAX_OPENED_FILES 0

static grib_context default_grib_context = {
    0,               /* inited                     */
    0,               /* debug                      */
    0,               /* write_on_fail              */
    0,               /* no_abort                   */
    0,               /* io_buffer_size             */
    0,               /* no_big_group_split         */
    0,               /* no_spd                     */
    0,               /* keep_matrix                */
    0,               /* show_hour_stepunit         */
    0,               /* grib_definition_files_path */
    0,               /* grib_samples_path          */
    0,               /* grib_concept_path          */
    0,               /* grib_reader                */
    0,               /* user data                  */
    CODES_REAL_MODE8, /* real mode for fortran     */

#if MANAGE_MEM
    &grib_transient_free,    /* free_mem                   */
    &grib_transient_malloc,  /* alloc_mem                  */
    &grib_transient_realloc, /* realloc_mem                */

    &grib_permanent_free,   /* free_persistant_mem        */
    &grib_permanent_malloc, /* alloc_persistant_mem       */

    &grib_buffer_free,    /* buffer_free_mem            */
    &grib_buffer_malloc,  /* buffer_alloc_mem           */
    &grib_buffer_realloc, /* buffer_realloc_mem         */

#else

    &default_free,    /* free_mem                  */
    &default_malloc,  /* alloc_mem                 */
    &default_realloc, /* realloc_mem               */

    &default_long_lasting_free,   /* free_persistant_mem       */
    &default_long_lasting_malloc, /* alloc_persistant_mem      */

    &default_buffer_free,    /* free_buffer_mem           */
    &default_buffer_malloc,  /* alloc_buffer_mem          */
    &default_buffer_realloc, /* realloc_buffer_mem        */
#endif

    &default_read,  /* file read procedure        */
    &default_write, /* file write procedure       */
    &default_tell,  /* lfile tell procedure       */
    &default_seek,  /* lfile seek procedure       */
    &default_feof,  /* file feof procedure        */

    &default_log,   /* output_log                 */
    &default_print, /* print                      */
    0,              /* codetable                  */
    0,              /* smart_table                */
    0,              /* outfilename                */
    0,              /* multi_support_on           */
    0,              /* multi_support              */
    0,              /* grib_definition_files_dir  */
    0,              /* handle_file_count          */
    0,              /* handle_total_count         */
    0,              /* message_file_offset        */
    0,              /* no_fail_on_wrong_length    */
    0,              /* gts_header_on              */
    0,              /* gribex_mode_on             */
    0,              /* large_constant_fields      */
    0,              /* keys                       */
    0,              /* keys_count                 */
    0,              /* concepts_index             */
    0,              /* concepts_count             */
    {0,},           /* concepts                   */
    0,              /* hash_array_index           */
    0,              /* hash_array_count           */
    {0,},           /* hash_array                 */
    0,              /* def_files                  */
    0,              /* blocklist                  */
    0,              /* ieee_packing               */
    0,              /* bufrdc_mode                */
    0,              /* bufr_set_to_missing_if_out_of_range */
    0,              /* bufr_multi_element_constant_arrays */
    0,              /* grib_data_quality_checks   */
    0,              /* single_precision           */
    0,              /* eckit_geo             */
    0,              /* log_stream                 */
    0,              /* classes                    */
    0,              /* lists                      */
    0,              /* expanded_descriptors       */
    DEFAULT_FILE_POOL_MAX_OPENED_FILES /* file_pool_max_opened_files */
#if GRIB_PTHREADS
    ,
    PTHREAD_MUTEX_INITIALIZER /* mutex */
#endif
};

/* Hopefully big enough. Note: Definitions and samples path environment variables can contain SEVERAL colon-separated directories */
#define ECC_PATH_MAXLEN 8192

grib_context* grib_context_get_default()
{
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);

    if (!default_grib_context.inited) {
        const char* write_on_fail                       = NULL;
        const char* large_constant_fields               = NULL;
        const char* no_abort                            = NULL;
        const char* debug                               = NULL;
        const char* gribex                              = NULL;
        const char* ieee_packing                        = NULL;
        const char* io_buffer_size                      = NULL;
        const char* log_stream                          = NULL;
        const char* no_big_group_split                  = NULL;
        const char* no_spd                              = NULL;
        const char* keep_matrix                         = NULL;
        const char* show_hour_stepunit                  = NULL;
        const char* bufrdc_mode                         = NULL;
        const char* bufr_set_to_missing_if_out_of_range = NULL;
        const char* bufr_multi_element_constant_arrays  = NULL;
        const char* grib_data_quality_checks            = NULL;
        const char* single_precision                    = NULL;
        const char* eckit_geo                           = NULL;
        const char* file_pool_max_opened_files          = NULL;

#ifdef ENABLE_FLOATING_POINT_EXCEPTIONS
        feenableexcept(FE_ALL_EXCEPT & ~FE_INEXACT);
#endif

        bufrdc_mode                         = getenv("ECCODES_BUFRDC_MODE_ON");
        bufr_set_to_missing_if_out_of_range = getenv("ECCODES_BUFR_SET_TO_MISSING_IF_OUT_OF_RANGE");
        bufr_multi_element_constant_arrays  = getenv("ECCODES_BUFR_MULTI_ELEMENT_CONSTANT_ARRAYS");
        grib_data_quality_checks            = getenv("ECCODES_GRIB_DATA_QUALITY_CHECKS");
        single_precision                    = getenv("ECCODES_SINGLE_PRECISION");
        file_pool_max_opened_files          = getenv("ECCODES_FILE_POOL_MAX_OPENED_FILES");
        eckit_geo                           = getenv("ECCODES_ECKIT_GEO");
        // The following had an equivalent env. var in grib_api
        write_on_fail                       = codes_getenv("ECCODES_GRIB_WRITE_ON_FAIL");
        large_constant_fields               = codes_getenv("ECCODES_GRIB_LARGE_CONSTANT_FIELDS");
        no_abort                            = codes_getenv("ECCODES_NO_ABORT");
        debug                               = codes_getenv("ECCODES_DEBUG");
        gribex                              = codes_getenv("ECCODES_GRIBEX_MODE_ON");
        ieee_packing                        = codes_getenv("ECCODES_GRIB_IEEE_PACKING");
        io_buffer_size                      = codes_getenv("ECCODES_IO_BUFFER_SIZE");
        log_stream                          = codes_getenv("ECCODES_LOG_STREAM");
        no_big_group_split                  = codes_getenv("ECCODES_GRIB_NO_BIG_GROUP_SPLIT");
        no_spd                              = codes_getenv("ECCODES_GRIB_NO_SPD");
        keep_matrix                         = codes_getenv("ECCODES_GRIB_KEEP_MATRIX");
        show_hour_stepunit                  = codes_getenv("ECCODES_GRIB_HOURLY_STEPS_WITH_UNITS");

        /* On UNIX, when we read from a file we get exactly what is in the file on disk.
         * But on Windows a file can be opened in binary or text mode. In binary mode the system behaves exactly as in UNIX.
         */
#ifdef ECCODES_ON_WINDOWS
        _set_fmode(_O_BINARY);
#endif

        default_grib_context.inited = 1;
        default_grib_context.io_buffer_size = io_buffer_size ? atoi(io_buffer_size) : 0;
        default_grib_context.no_big_group_split = no_big_group_split ? atoi(no_big_group_split) : 0;
        default_grib_context.no_spd = no_spd ? atoi(no_spd) : 0;
        default_grib_context.keep_matrix = keep_matrix ? atoi(keep_matrix) : 1;
        default_grib_context.grib_hourly_steps_with_units = show_hour_stepunit ? atoi(show_hour_stepunit) : 0;
        default_grib_context.write_on_fail = write_on_fail ? atoi(write_on_fail) : 0;
        default_grib_context.no_abort = no_abort ? atoi(no_abort) : 0;
        default_grib_context.debug = debug ? atoi(debug) : 0;
        default_grib_context.gribex_mode_on = gribex ? atoi(gribex) : 0;
        default_grib_context.large_constant_fields = large_constant_fields ? atoi(large_constant_fields) : 0;
        default_grib_context.ieee_packing = ieee_packing ? atoi(ieee_packing) : 0;
        default_grib_context.grib_samples_path = codes_getenv("ECCODES_SAMPLES_PATH");
        default_grib_context.log_stream = stderr;
        if (!log_stream) {
            default_grib_context.log_stream = stderr;
        }
        else if (!strcmp(log_stream, "stderr")) {
            default_grib_context.log_stream = stderr;
        }
        else if (!strcmp(log_stream, "stdout")) {
            default_grib_context.log_stream = stdout;
        }

#ifdef ECCODES_SAMPLES_PATH
        if (!default_grib_context.grib_samples_path)
            default_grib_context.grib_samples_path = (char*)ECCODES_SAMPLES_PATH;
#endif

        default_grib_context.grib_definition_files_path = codes_getenv("ECCODES_DEFINITION_PATH");
#ifdef ECCODES_DEFINITION_PATH
        if (!default_grib_context.grib_definition_files_path) {
            default_grib_context.grib_definition_files_path = strdup(ECCODES_DEFINITION_PATH);
        }
        else {
            default_grib_context.grib_definition_files_path = strdup(default_grib_context.grib_definition_files_path);
        }
#endif

        /* GRIB-779: Special case for ECMWF testing. Not for external use! */
        /* Append the new path to our existing path */
        {
            const char* test_defs = codes_getenv("_ECCODES_ECMWF_TEST_DEFINITION_PATH");
            const char* test_samp = codes_getenv("_ECCODES_ECMWF_TEST_SAMPLES_PATH");
            if (test_defs) {
                char buffer[ECC_PATH_MAXLEN]= {0,};
                if (default_grib_context.grib_definition_files_path) {
                    strcpy(buffer, default_grib_context.grib_definition_files_path);
                    strcat(buffer, ":");
                }
                strcat(buffer, test_defs);
                free(default_grib_context.grib_definition_files_path);
                default_grib_context.grib_definition_files_path = strdup(buffer);
            }
            if (test_samp) {
                char buffer[ECC_PATH_MAXLEN]= {0,};
                if (default_grib_context.grib_samples_path) {
                    strcpy(buffer, default_grib_context.grib_samples_path);
                    strcat(buffer, ":");
                }
                strcat(buffer, test_samp);
                default_grib_context.grib_samples_path = strdup(buffer);
            }
        }

        /* Definitions path extra: Added at the head of (i.e. before) existing path */
        {
            const char* defs_extra = getenv("ECCODES_EXTRA_DEFINITION_PATH");
            if (defs_extra) {
                char buffer[ECC_PATH_MAXLEN]= {0,};
                snprintf(buffer, ECC_PATH_MAXLEN, "%s%c%s", defs_extra, ECC_PATH_DELIMITER_CHAR, default_grib_context.grib_definition_files_path);
                free(default_grib_context.grib_definition_files_path);
                default_grib_context.grib_definition_files_path = strdup(buffer);
            }
        }
#ifdef ECCODES_DEFINITION_PATH
        {
            /* ECC-1088 */
            if (strstr(default_grib_context.grib_definition_files_path, ECCODES_DEFINITION_PATH) == NULL) {
                char buffer[ECC_PATH_MAXLEN]= {0,};
                snprintf(buffer, ECC_PATH_MAXLEN, "%s%c%s", default_grib_context.grib_definition_files_path,
                             ECC_PATH_DELIMITER_CHAR, ECCODES_DEFINITION_PATH);
                free(default_grib_context.grib_definition_files_path);
                default_grib_context.grib_definition_files_path = strdup(buffer);
            }
        }
#endif

        /* Samples path extra: Added at the head of (i.e. before) existing path */
        {
            const char* samples_extra = getenv("ECCODES_EXTRA_SAMPLES_PATH");
            if (samples_extra) {
                char buffer[ECC_PATH_MAXLEN];
                snprintf(buffer, ECC_PATH_MAXLEN, "%s%c%s", samples_extra, ECC_PATH_DELIMITER_CHAR, default_grib_context.grib_samples_path);
                default_grib_context.grib_samples_path = strdup(buffer);
            }
        }
#ifdef ECCODES_SAMPLES_PATH
        {
            if (strstr(default_grib_context.grib_samples_path, ECCODES_SAMPLES_PATH) == NULL) {
                char buffer[ECC_PATH_MAXLEN];
                snprintf(buffer, ECC_PATH_MAXLEN, "%s%c%s", default_grib_context.grib_samples_path,
                             ECC_PATH_DELIMITER_CHAR, ECCODES_SAMPLES_PATH);
                default_grib_context.grib_samples_path = strdup(buffer);
            }
        }
#endif

        if (default_grib_context.debug) {
            fprintf(stderr, "ECCODES DEBUG ecCodes Version:  %s\n", ECCODES_VERSION_STR);
            fprintf(stderr, "ECCODES DEBUG Definitions path: %s\n", default_grib_context.grib_definition_files_path);
            fprintf(stderr, "ECCODES DEBUG Samples path:     %s\n", default_grib_context.grib_samples_path);
        }
        default_grib_context.keys_count = 0;
        default_grib_context.keys       = grib_hash_keys_new(&(default_grib_context), &(default_grib_context.keys_count));

        default_grib_context.concepts_index = grib_itrie_new(&(default_grib_context), &(default_grib_context.concepts_count));
        default_grib_context.hash_array_index = grib_itrie_new(&(default_grib_context), &(default_grib_context.hash_array_count));
        default_grib_context.def_files = grib_trie_new(&(default_grib_context));
        default_grib_context.lists = grib_trie_new(&(default_grib_context));
        default_grib_context.classes = grib_trie_new(&(default_grib_context));
        default_grib_context.bufrdc_mode = bufrdc_mode ? atoi(bufrdc_mode) : 0;
        default_grib_context.bufr_set_to_missing_if_out_of_range = bufr_set_to_missing_if_out_of_range ? atoi(bufr_set_to_missing_if_out_of_range) : 0;
        default_grib_context.bufr_multi_element_constant_arrays = bufr_multi_element_constant_arrays ? atoi(bufr_multi_element_constant_arrays) : 0;
        default_grib_context.grib_data_quality_checks = grib_data_quality_checks ? atoi(grib_data_quality_checks) : 0;
        default_grib_context.single_precision = single_precision ? atoi(single_precision) : 0;
        default_grib_context.eckit_geo = eckit_geo ? atoi(eckit_geo) : 0;
        default_grib_context.file_pool_max_opened_files = file_pool_max_opened_files ? atoi(file_pool_max_opened_files) : DEFAULT_FILE_POOL_MAX_OPENED_FILES;
    }

    GRIB_MUTEX_UNLOCK(&mutex_c);
    return &default_grib_context;
}

// Do we really need this?
// grib_context* grib_context_new(grib_context* parent)
// {
//     grib_context* c;
// #if GRIB_PTHREADS
//     pthread_mutexattr_t attr;
// #endif

//     if (!parent) parent=grib_context_get_default();

//     GRIB_MUTEX_INIT_ONCE(&once,&init_mutex);
//     GRIB_MUTEX_LOCK(&(parent->mutex));

//     c = (grib_context*)grib_context_malloc_clear_persistent(&default_grib_context,sizeof(grib_context));

//     c->inited              = default_grib_context.inited;
//     c->debug               = default_grib_context.debug;

//     c->real_mode           = default_grib_context.real_mode;

//     c->free_mem            = default_grib_context.free_mem;
//     c->alloc_mem           = default_grib_context.alloc_mem;

//     c->free_persistent_mem = default_grib_context.free_persistent_mem;
//     c->alloc_persistent_mem= default_grib_context.alloc_persistent_mem;

//     c->read                = default_grib_context.read;
//     c->write               = default_grib_context.write;
//     c->tell                = default_grib_context.tell;

//     c->output_log          = default_grib_context.output_log;
//     c->print               = default_grib_context.print    ;
//     c->user_data           = default_grib_context.user_data;
//     c->def_files           = default_grib_context.def_files;
//     c->lists               = default_grib_context.lists;

// #if GRIB_PTHREADS
//     pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
//     pthread_mutex_init(&mutex_c,&attr);
//     pthread_mutexattr_destroy(&attr);
// #endif

//     GRIB_MUTEX_UNLOCK(&(parent->mutex));
//     return c;
// }


/* GRIB-235: Resolve path to expand symbolic links etc */
/* Note: return value is allocated. Client has to free */
char* codes_resolve_path(grib_context* c, const char* path)
{
    char* result = NULL;
#if defined(ECCODES_HAVE_REALPATH)
    char resolved[ECC_PATH_MAXLEN + 1];
    if (!realpath(path, resolved)) {
        result = grib_context_strdup(c, path); /* Failed to resolve. Use original path */
    }
    else {
        result = grib_context_strdup(c, resolved);
    }
#else
    result = grib_context_strdup(c, path);
#endif

    return result;
}

static int init_definition_files_dir(grib_context* c)
{
    int err = 0;
    char path[ECC_PATH_MAXLEN];
    char* p                = NULL;
    char* lasts            = NULL;
    grib_string_list* next = NULL;

    if (!c)
        c = grib_context_get_default();

    if (c->grib_definition_files_dir)
        return 0;
    if (!c->grib_definition_files_path)
        return GRIB_NO_DEFINITIONS;

    /* Note: strtok_r modifies its first argument so we copy */
    strncpy(path, c->grib_definition_files_path, ECC_PATH_MAXLEN-1);
    path[ ECC_PATH_MAXLEN - 1 ] = '\0';

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);

    p = path;

    while (*p != ECC_PATH_DELIMITER_CHAR && *p != '\0')
        p++;

    if (*p != ECC_PATH_DELIMITER_CHAR) {
        /* No delimiter found so this is a single directory */
        c->grib_definition_files_dir        = (grib_string_list*)grib_context_malloc_clear_persistent(c, sizeof(grib_string_list));
        c->grib_definition_files_dir->value = codes_resolve_path(c, path);
    }
    else {
        /* Definitions path contains multiple directories */
        const char* dir = strtok_r(path, ECC_PATH_DELIMITER_STR, &lasts);

        while (dir != NULL) {
            if (next) {
                next->next = (grib_string_list*)grib_context_malloc_clear_persistent(c, sizeof(grib_string_list));
                next = next->next;
            }
            else {
                c->grib_definition_files_dir = (grib_string_list*)grib_context_malloc_clear_persistent(c, sizeof(grib_string_list));
                next = c->grib_definition_files_dir;
            }
            next->value = codes_resolve_path(c, dir);
            dir = strtok_r(NULL, ECC_PATH_DELIMITER_STR, &lasts);
        }
    }

    GRIB_MUTEX_UNLOCK(&mutex_c);

    return err;
}

char* grib_context_full_defs_path(grib_context* c, const char* basename)
{
    int err         = 0;
    char full[1024] = {0,};
    grib_string_list* dir      = NULL;
    grib_string_list* fullpath = 0;
    if (!c)
        c = grib_context_get_default();

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);

    if (*basename == '/' || *basename == '.') {
        return (char*)basename;
    }
    else {
        GRIB_MUTEX_LOCK(&mutex_c); /* See ECC-604 */
        fullpath = (grib_string_list*)grib_trie_get(c->def_files, basename);
        GRIB_MUTEX_UNLOCK(&mutex_c);
        if (fullpath != NULL) {
            return fullpath->value;
        }
        if (!c->grib_definition_files_dir) {
            err = init_definition_files_dir(c);
        }

        if (err != GRIB_SUCCESS) {
            grib_context_log(c, GRIB_LOG_ERROR,
                             "Unable to find definition files directory");
            return NULL;
        }

        dir = c->grib_definition_files_dir;

        while (dir) {
            snprintf(full, sizeof(full), "%s/%s", dir->value, basename);
            if (codes_access(full, F_OK) == 0) { /* 0 means file exists */
                fullpath = (grib_string_list*)grib_context_malloc_clear_persistent(c, sizeof(grib_string_list));
                ECCODES_ASSERT(fullpath);
                fullpath->value = grib_context_strdup(c, full);
                GRIB_MUTEX_LOCK(&mutex_c);
                grib_trie_insert(c->def_files, basename, fullpath);
                grib_context_log(c, GRIB_LOG_DEBUG, "Found def file %s", full);
                GRIB_MUTEX_UNLOCK(&mutex_c);
                return fullpath->value;
            } else {
                grib_context_log(c, GRIB_LOG_DEBUG, "Nonexistent def file %s", full);
            }
            dir = dir->next;
        }
    }

    GRIB_MUTEX_LOCK(&mutex_c);
    /* Store missing files so we don't check for them again and again */
    grib_trie_insert(c->def_files, basename, &grib_file_not_found);
    /*grib_context_log(c,GRIB_LOG_ERROR,"Def file \"%s\" not found",basename);*/
    GRIB_MUTEX_UNLOCK(&mutex_c);
    full[0] = 0;
    return NULL;
}

char* grib_samples_path(const grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    return c->grib_samples_path;
}
char* grib_definition_path(const grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    return c->grib_definition_files_path;
}

void grib_context_free(const grib_context* c, void* p)
{
    if (!c)
        c = grib_context_get_default();
    if (p)
        c->free_mem(c, p);
}

void grib_context_free_persistent(const grib_context* c, void* p)
{
    if (!c)
        c = grib_context_get_default();
    if (p)
        c->free_persistent_mem(c, p);
}

void grib_context_reset(grib_context* c)
{
    size_t i = 0;
    if (!c)
        c = grib_context_get_default();

    if (c->grib_reader) {
        grib_action_file* fr = c->grib_reader->first;
        grib_action_file* fn = fr;
        grib_action* a;

        while (fn) {
            fr = fn;
            fn = fn->next;

            a = fr->root;
            while (a) {
                grib_action* na = a->next_;
                delete a;
                a = na;
            }
            grib_context_free_persistent(c, fr->filename);
            grib_context_free_persistent(c, fr);
        }
        grib_context_free_persistent(c, c->grib_reader);
    }

    c->grib_reader = NULL;

    if (c->codetable)
        grib_codetable_delete(c);
    c->codetable = NULL;

    if (c->smart_table)
        grib_smart_table_delete(c);
    c->smart_table = NULL;

    if (c->grib_definition_files_dir) {
        grib_string_list* next = c->grib_definition_files_dir;
        grib_string_list* cur = NULL;
        while (next) {
            cur  = next;
            next = next->next;
            grib_context_free(c, cur->value);
            grib_context_free(c, cur);
        }
        c->grib_definition_files_dir = 0;
    }

    if (c->multi_support_on)
        grib_multi_support_reset(c);

    for (i=0; i < MAX_NUM_CONCEPTS; ++i) {
        grib_concept_value* cv = c->concepts[i];
        if (cv) {
            grib_trie_delete_container(cv->index);
        }
        while (cv) {
            grib_concept_value* n = cv->next;
            grib_concept_value_delete(c, cv);
            cv = n;
        }
    }
}

void grib_context_delete(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();

    grib_hash_keys_delete(c->keys);
    /* grib_trie_delete(c->def_files);  TODO:masn */

    grib_context_reset(c);

    if (c != &default_grib_context)
        grib_context_free_persistent(&default_grib_context, c);

    for(size_t i=0; i<MAX_NUM_HASH_ARRAY; ++i)
        c->hash_array[i] = NULL;
    c->hash_array_count = 0;
    grib_itrie_delete(c->hash_array_index);
    c->hash_array_index=0;
    grib_trie_delete_container(c->expanded_descriptors);
    c->expanded_descriptors=0;

    c->inited = 0;
}

void codes_bufr_multi_element_constant_arrays_on(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->bufr_multi_element_constant_arrays = 1;
}
void codes_bufr_multi_element_constant_arrays_off(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->bufr_multi_element_constant_arrays = 0;
}
/*int  codes_get_bufr_multi_element_constant_arrays(grib_context* c);*/


void grib_context_set_definitions_path(grib_context* c, const char* path)
{
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);

    c->grib_definition_files_path = strdup(path);
    grib_context_log(c, GRIB_LOG_DEBUG, "Definitions path changed to: %s", c->grib_definition_files_path);

    GRIB_MUTEX_UNLOCK(&mutex_c);
}
void grib_context_set_samples_path(grib_context* c, const char* path)
{
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);

    c->grib_samples_path = strdup(path);
    grib_context_log(c, GRIB_LOG_DEBUG, "Samples path changed to: %s", c->grib_samples_path);

    GRIB_MUTEX_UNLOCK(&mutex_c);
}

void* grib_context_malloc_persistent(const grib_context* c, size_t size)
{
    void* p = c->alloc_persistent_mem(c, size);
    if (!p) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return p;
}

char* grib_context_strdup_persistent(const grib_context* c, const char* s)
{
    char* dup = (char*)grib_context_malloc_persistent(c, (strlen(s) * sizeof(char)) + 1);
    if (dup)
        strcpy(dup, s);
    return dup;
}

void* grib_context_malloc_clear_persistent(const grib_context* c, size_t size)
{
    void* p = grib_context_malloc_persistent(c, size);
    if (p)
        memset(p, 0, size);
    return p;
}

void* grib_context_malloc(const grib_context* c, size_t size)
{
    void* p = NULL;
    if (!c)
        c = grib_context_get_default();
    if (size == 0)
        return p;
    else
        p = c->alloc_mem(c, size);
    if (!p) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
    }
    return p;
}

void* grib_context_realloc(const grib_context* c, void* p, size_t size)
{
    void* q;
    if (!c)
        c = grib_context_get_default();
    q = c->realloc_mem(c, p, size);
    if (!q) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
        return NULL;
    }
    return q;
}

char* grib_context_strdup(const grib_context* c, const char* s)
{
    char* dup = 0;
    if (s) {
        dup = (char*)grib_context_malloc(c, (strlen(s) * sizeof(char)) + 1);
        if (dup)
            strcpy(dup, s);
    }
    return dup;
}

void* grib_context_malloc_clear(const grib_context* c, size_t size)
{
    void* p = grib_context_malloc(c, size);
    if (p)
        memset(p, 0, size);
    return p;
}

void* grib_context_buffer_malloc(const grib_context* c, size_t size)
{
    void* p = NULL;
    if (!c)
        c = grib_context_get_default();
    if (size == 0)
        return p;
    else
        p = c->alloc_buffer_mem(c, size);
    if (!p) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
        return NULL;
    }
    return p;
}

void grib_context_buffer_free(const grib_context* c, void* p)
{
    if (!c)
        c = grib_context_get_default();
    if (p)
        c->free_buffer_mem(c, p);
}

void* grib_context_buffer_realloc(const grib_context* c, void* p, size_t size)
{
    void* q = c->realloc_buffer_mem(c, p, size);
    if (!q) {
        grib_context_log(c, GRIB_LOG_FATAL, "%s: error allocating %zu bytes", __func__, size);
        return NULL;
    }
    return q;
}

void* grib_context_buffer_malloc_clear(const grib_context* c, size_t size)
{
    void* p = grib_context_buffer_malloc(c, size);
    if (p)
        memset(p, 0, size);
    return p;
}

void grib_context_set_data_accessing_proc(grib_context* c, grib_data_read_proc read, grib_data_write_proc write, grib_data_tell_proc tell)
{
    c->read  = read;
    c->write = write;
    c->tell  = tell;
}

/* Logging procedure */
void grib_context_log(const grib_context* c, int level, const char* fmt, ...)
{
    /* Save some CPU */
    if ((level == GRIB_LOG_DEBUG && c->debug < 1) ||
        (level == GRIB_LOG_WARNING && c->debug < 2)) {
        return;
    }
    else {
        char msg[1024];
        va_list list;
        const int errsv = errno;

        va_start(list, fmt);
        vsnprintf(msg, sizeof(msg), fmt, list);
        va_end(list);

        if (level & GRIB_LOG_PERROR) {
            level = level & ~GRIB_LOG_PERROR;

            /* #if HAS_STRERROR */
            strcat(msg, " (");
            strcat(msg, strerror(errsv));
            strcat(msg, ")");

            // if (errsv > 0 && errsv < sys_nerr) {
            //     strcat(msg, " (");
            //     strcat(msg, sys_errlist[errsv]);
            //     strcat(msg, " )");
            // }
        }

        if (c->output_log)
            c->output_log(c, level, msg);
    }
}

/* Logging procedure */
void grib_context_print(const grib_context* c, void* descriptor, const char* fmt, ...)
{
    char msg[1024];
    va_list list;
    va_start(list, fmt);
    vsnprintf(msg, sizeof(msg), fmt, list);
    va_end(list);
    c->print(c, descriptor, msg);
}

int grib_context_get_handle_file_count(grib_context* c)
{
    int r = 0;
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);
    r = c->handle_file_count;
    GRIB_MUTEX_UNLOCK(&mutex_c);
    return r;
}
int grib_context_get_handle_total_count(grib_context* c)
{
    int r = 0;
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);
    r = c->handle_total_count;
    GRIB_MUTEX_UNLOCK(&mutex_c);
    return r;
}

void grib_context_set_handle_file_count(grib_context* c, int new_count)
{
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);
    c->handle_file_count = new_count;
    GRIB_MUTEX_UNLOCK(&mutex_c);
}
void grib_context_set_handle_total_count(grib_context* c, int new_count)
{
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);
    c->handle_total_count = new_count;
    GRIB_MUTEX_UNLOCK(&mutex_c);
}

void grib_context_increment_handle_file_count(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);
    c->handle_file_count++;
    GRIB_MUTEX_UNLOCK(&mutex_c);
}
void grib_context_increment_handle_total_count(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);
    c->handle_total_count++;
    GRIB_MUTEX_UNLOCK(&mutex_c);
}

bufr_descriptors_array* grib_context_expanded_descriptors_list_get(grib_context* c, const char* key, long* u, size_t size)
{
    bufr_descriptors_map_list* expandedUnexpandedMapList;
    size_t i                       = 0;
    int found                      = 0;
    bufr_descriptors_array* result = NULL;
    if (!c)
        c = grib_context_get_default();

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);

    if (!c->expanded_descriptors) {
        c->expanded_descriptors = (grib_trie*)grib_trie_new(c);
        result                  = NULL;
        goto the_end;
    }
    expandedUnexpandedMapList = (bufr_descriptors_map_list*)grib_trie_get(c->expanded_descriptors, key);
    found                     = 0;
    while (expandedUnexpandedMapList) {
        if (expandedUnexpandedMapList->unexpanded->n == size) {
            found = 1;
            for (i = 0; i < size; i++) {
                if (expandedUnexpandedMapList->unexpanded->v[i]->code != u[i]) {
                    found = 0;
                    break;
                }
            }
        }
        if (found) {
            result = expandedUnexpandedMapList->expanded;
            goto the_end;
        }
        expandedUnexpandedMapList = expandedUnexpandedMapList->next;
    }
the_end:
    GRIB_MUTEX_UNLOCK(&mutex_c);
    return result;
}

void grib_context_expanded_descriptors_list_push(grib_context* c,
                                                 const char* key, bufr_descriptors_array* expanded, bufr_descriptors_array* unexpanded)
{
    bufr_descriptors_map_list* descriptorsList    = NULL;
    bufr_descriptors_map_list* next               = NULL;
    bufr_descriptors_map_list* newdescriptorsList = NULL;
    if (!c)
        c = grib_context_get_default();

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex_c);

    newdescriptorsList             = (bufr_descriptors_map_list*)grib_context_malloc_clear(c, sizeof(bufr_descriptors_map_list));
    newdescriptorsList->expanded   = expanded;
    newdescriptorsList->unexpanded = unexpanded;

    descriptorsList = (bufr_descriptors_map_list*)grib_trie_get(c->expanded_descriptors, key);
    if (descriptorsList) {
        next = descriptorsList;
        while (next->next) {
            next = next->next;
        }
        next->next = newdescriptorsList;
    }
    else {
        grib_trie_insert(c->expanded_descriptors, key, newdescriptorsList);
    }
    GRIB_MUTEX_UNLOCK(&mutex_c);
}

static codes_assertion_failed_proc assertion = NULL;

void codes_set_codes_assertion_failed_proc(codes_assertion_failed_proc proc)
{
    assertion = proc;
}

void codes_assertion_failed(const char* message, const char* file, int line)
{
    /* Default behaviour is to abort
     * unless user has supplied his own assertion routine */
    if (assertion == NULL) {
        const grib_context* c = grib_context_get_default();
        fprintf(stderr, "ecCodes assertion failed: `%s' in %s:%d\n", message, file, line);
        if (!c->no_abort) {
            abort();
        }
    }
    else {
        char buffer[10240];
        snprintf(buffer, sizeof(buffer), "ecCodes assertion failed: `%s' in %s:%d", message, file, line);
        assertion(buffer);
    }
}

int grib_get_gribex_mode(const grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    return c->gribex_mode_on;
}
void grib_gribex_mode_on(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->gribex_mode_on = 1;
}
void grib_gribex_mode_off(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->gribex_mode_on = 0;
}

void grib_gts_header_on(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->gts_header_on = 1;
}
void grib_gts_header_off(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->gts_header_on = 0;
}

void grib_multi_support_on(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->multi_support_on = 1;
}
void grib_multi_support_off(grib_context* c)
{
    if (!c)
        c = grib_context_get_default();
    c->multi_support_on = 0;
}
