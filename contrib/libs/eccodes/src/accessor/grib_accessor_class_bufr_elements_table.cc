/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_bufr_elements_table.h"
#include "grib_scaling.h"

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
    GRIB_OMP_CRITICAL(lock_grib_accessor_bufr_elements_table_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex1);
            once = 1;
        }
    }
}
#endif

grib_accessor_bufr_elements_table_t _grib_accessor_bufr_elements_table{};
grib_accessor* grib_accessor_bufr_elements_table = &_grib_accessor_bufr_elements_table;

void grib_accessor_bufr_elements_table_t::init(const long len, grib_arguments* params)
{
    grib_accessor_gen_t::init(len, params);
    int n = 0;

    dictionary_ = params->get_string(grib_handle_of_accessor(this), n++);
    masterDir_  = params->get_name(grib_handle_of_accessor(this), n++);
    localDir_   = params->get_name(grib_handle_of_accessor(this), n++);

    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

grib_trie* grib_accessor_bufr_elements_table_t::load_bufr_elements_table(int* err)
{
    char* filename = NULL;
    char line[1024] = {0,};
    char masterDir[1024] = {0,};
    char localDir[1024] = {0,};
    char dictName[1024] = {0,};
    char masterRecomposed[1024] = {0,};  // e.g. bufr/tables/0/wmo/36/element.table
    char localRecomposed[1024] = {0,};   // e.g. bufr/tables/0/local/0/98/0/element.table
    char* localFilename   = 0;
    char** list           = 0;
    char** cached_list    = 0;
    size_t len            = 1024;
    grib_trie* dictionary = NULL;
    FILE* f               = NULL;
    grib_handle* h        = grib_handle_of_accessor(this);
    grib_context* c       = context_;

    *err = GRIB_SUCCESS;

    len = 1024;
    if (masterDir_ != NULL)
        grib_get_string(h, masterDir_, masterDir, &len);
    len = 1024;
    if (localDir_ != NULL)
        grib_get_string(h, localDir_, localDir, &len);

    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex1);

    if (*masterDir != 0) {
        char name[4096] = {0,};
        snprintf(name, 4096, "%s/%s", masterDir, dictionary_);
        grib_recompose_name(h, NULL, name, masterRecomposed, 0);
        filename = grib_context_full_defs_path(c, masterRecomposed);
    }
    else {
        filename = grib_context_full_defs_path(c, dictionary_);
    }

    if (*localDir != 0) {
        char localName[2048] = {0,};
        snprintf(localName, 2048, "%s/%s", localDir, dictionary_);
        grib_recompose_name(h, NULL, localName, localRecomposed, 0);
        localFilename = grib_context_full_defs_path(c, localRecomposed);
        snprintf(dictName, 1024, "%s:%s", localFilename, filename);
    }
    else {
        snprintf(dictName, 1024, "%s", filename);
    }

    if (!filename) {
        grib_context_log(c, GRIB_LOG_ERROR, "Unable to find definition file %s", dictionary_);
        if (strlen(masterRecomposed) > 0) grib_context_log(c, GRIB_LOG_DEBUG, "master path=%s", masterRecomposed);
        if (strlen(localRecomposed) > 0) grib_context_log(c, GRIB_LOG_DEBUG, "local path=%s", localRecomposed);
        *err       = GRIB_FILE_NOT_FOUND;
        dictionary = NULL;
        goto the_end;
    }

    dictionary = (grib_trie*)grib_trie_get(c->lists, dictName);
    if (dictionary) {
        /*grib_context_log(c,GRIB_LOG_DEBUG,"using dictionary %s from cache",a->dictionary_ );*/
        goto the_end;
    }
    else {
        grib_context_log(c, GRIB_LOG_DEBUG, "using dictionary %s from file %s", dictionary_, filename);
    }

    f = codes_fopen(filename, "r");
    if (!f) {
        *err       = GRIB_IO_PROBLEM;
        dictionary = NULL;
        goto the_end;
    }

    dictionary = grib_trie_new(c);

    while (fgets(line, sizeof(line) - 1, f)) {
        DEBUG_ASSERT(strlen(line) > 0);
        if (line[0] == '#') continue; /* Ignore first line with column titles */
        list = string_split(line, "|");
        grib_trie_insert(dictionary, list[0], list);
    }

    fclose(f);

    if (localFilename != 0) {
        f = codes_fopen(localFilename, "r");
        if (!f) {
            *err       = GRIB_IO_PROBLEM;
            dictionary = NULL;
            goto the_end;
        }

        while (fgets(line, sizeof(line) - 1, f)) {
            DEBUG_ASSERT(strlen(line) > 0);
            if (line[0] == '#') continue; /* Ignore first line with column titles */
            list = string_split(line, "|");
            /* Look for the descriptor code in the trie. It might be there from before */
            cached_list = (char**)grib_trie_get(dictionary, list[0]);
            if (cached_list) { /* If found, we are about to overwrite it. So free memory */
                int i;
                for (i = 0; cached_list[i] != NULL; ++i)
                    free(cached_list[i]);
                free(cached_list);
            }
            grib_trie_insert(dictionary, list[0], list);
        }

        fclose(f);
    }
    grib_trie_insert(c->lists, dictName, dictionary);

the_end:
    GRIB_MUTEX_UNLOCK(&mutex1);
    return dictionary;
}

static int convert_type(const char* stype)
{
    int ret = BUFR_DESCRIPTOR_TYPE_UNKNOWN;
    switch (stype[0]) {
        case 's':
            if (!strcmp(stype, "string"))
                ret = BUFR_DESCRIPTOR_TYPE_STRING;
            break;
        case 'l':
            if (!strcmp(stype, "long"))
                ret = BUFR_DESCRIPTOR_TYPE_LONG;
            break;
        case 'd':
            if (!strcmp(stype, "double"))
                ret = BUFR_DESCRIPTOR_TYPE_DOUBLE;
            break;
        case 't':
            if (!strcmp(stype, "table"))
                ret = BUFR_DESCRIPTOR_TYPE_TABLE;
            break;
        case 'f':
            if (!strcmp(stype, "flag"))
                ret = BUFR_DESCRIPTOR_TYPE_FLAG;
            break;
        default:
            ret = BUFR_DESCRIPTOR_TYPE_UNKNOWN;
    }

    return ret;
}

static long atol_fast(const char* input)
{
    if (strcmp(input, "0") == 0)
        return 0;
    return atol(input);
}

int grib_accessor_bufr_elements_table_t::bufr_get_from_table(bufr_descriptor* v)
{
    int ret              = 0;
    char** list          = 0;
    char code[7]         = { 0 };
    const size_t codeLen = sizeof(code);

    grib_trie* table = load_bufr_elements_table(&ret);
    if (ret)
        return ret;

    snprintf(code, codeLen, "%06ld", v->code);

    list = (char**)grib_trie_get(table, code);
    if (!list)
        return GRIB_NOT_FOUND;

#ifdef DEBUG
    {
        /* ECC-1137: check descriptor key name and unit lengths */
        const size_t maxlen_shortName = sizeof(v->shortName);
        const size_t maxlen_units     = sizeof(v->units);
        ECCODES_ASSERT(strlen(list[1]) < maxlen_shortName);
        ECCODES_ASSERT(strlen(list[4]) < maxlen_units);
    }
#endif

    strcpy(v->shortName, list[1]);
    v->type = convert_type(list[2]);
    /* v->name=grib_context_strdup(c,list[3]);  See ECC-489 */
    strcpy(v->units, list[4]);

    /* ECC-985: Scale and reference are often 0 so we can reduce calls to atol */
    v->scale  = atol_fast(list[5]);
    v->factor = codes_power<double>(-v->scale, 10);

    v->reference = atol_fast(list[6]);
    v->width     = atol(list[7]);

    return GRIB_SUCCESS;
}

int bufr_descriptor_is_marker(bufr_descriptor* d)
{
    int isMarker = 0;
    switch (d->code) {
        case 223255:
        case 224255:
        case 225255:
        case 232255:
            return 1;
    }
    if (d->F == 2 && d->X == 5)
        isMarker = 1;
    return isMarker;
}

bufr_descriptor* accessor_bufr_elements_table_get_descriptor(grib_accessor* a, int code, int* err)
{
    grib_accessor_bufr_elements_table_t* self = (grib_accessor_bufr_elements_table_t*)a;
    grib_context* c;
    bufr_descriptor* v = NULL;

    if (!a)
        return NULL;

    c = a->context_;
    DEBUG_ASSERT(c);
    v = (bufr_descriptor*)grib_context_malloc_clear(c, sizeof(bufr_descriptor));
    if (!v) {
        grib_context_log(c, GRIB_LOG_ERROR, "%s: Unable to allocate %zu bytes", __func__, sizeof(bufr_descriptor));
        *err = GRIB_OUT_OF_MEMORY;
        return NULL;
    }
    v->context = c;
    v->code    = code;
    v->F       = code / 100000;
    v->X       = (code - v->F * 100000) / 1000;
    v->Y       = (code - v->F * 100000) % 1000;

    switch (v->F) {
        case 0:
            *err = self->bufr_get_from_table(v);
            break;
        case 1:
            v->type = BUFR_DESCRIPTOR_TYPE_REPLICATION;
            break;
        case 2:
            v->type = BUFR_DESCRIPTOR_TYPE_OPERATOR;
            break;
        case 3:
            v->type = BUFR_DESCRIPTOR_TYPE_SEQUENCE;
            break;
    }

    return v;
}

int grib_accessor_bufr_elements_table_t::unpack_string(char* buffer, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_bufr_elements_table_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

long grib_accessor_bufr_elements_table_t::get_native_type()
{
    return GRIB_TYPE_STRING;
}

int grib_accessor_bufr_elements_table_t::unpack_long(long* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}

int grib_accessor_bufr_elements_table_t::unpack_double(double* val, size_t* len)
{
    return GRIB_NOT_IMPLEMENTED;
}
