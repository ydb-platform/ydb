/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "action_class_hash_array.h"

#if GRIB_PTHREADS
static pthread_once_t once   = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_action_class_hash_array_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex);
            once = 1;
        }
    }
}
#endif

grib_action* grib_action_create_hash_array(grib_context* context,
                                           const char* name,
                                           grib_hash_array_value* hash_array,
                                           const char* basename, const char* name_space, const char* defaultkey,
                                           const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail)
{
    return new eccodes::action::HashArray(context, name, hash_array, basename, name_space, defaultkey, masterDir, localDir, ecmfDir, flags, nofail);
}

namespace eccodes::action
{

HashArray::HashArray(grib_context* context,
                     const char* name,
                     grib_hash_array_value* hash_array,
                     const char* basename, const char* name_space, const char* defaultkey,
                     const char* masterDir, const char* localDir, const char* ecmfDir, int flags, int nofail) :
    Gen(context, name, "hash_array", 0, NULL, NULL, flags, name_space, NULL)
{
    class_name_ = "action_class_hash_array";
    basename_   = basename ? grib_context_strdup_persistent(context, basename) : nullptr;
    masterDir_  = masterDir ? grib_context_strdup_persistent(context, masterDir) : nullptr;
    localDir_   = localDir ? grib_context_strdup_persistent(context, localDir) : nullptr;
    ecmfDir_    = ecmfDir ? grib_context_strdup_persistent(context, ecmfDir) : nullptr;
    defaultkey_ = defaultkey ? grib_context_strdup_persistent(context, defaultkey) : nullptr;
    nofail_     = nofail;
    hash_array_ = hash_array;

    if (hash_array) {
        grib_context_log(context, GRIB_LOG_FATAL, "%s: 'hash_array_list' not implemented", __func__);
        // grib_hash_array_value* ha = hash_array;
        // grib_trie* index = grib_trie_new(context);
        // while (ha) {
        //     ha->index = index;
        //     grib_trie_insert_no_replace(index, ha->name, ha);
        //     ha = ha->next;
        // }
    }
}

HashArray::~HashArray()
{
    // This is currently unset. So assert that it is NULL
    const grib_hash_array_value* v = hash_array_;
    ECCODES_ASSERT(v == NULL);
    // if (v)
    //     grib_trie_delete(v->index);
    // while (v) {
    //     grib_hash_array_value* n = v->next;
    //     grib_hash_array_value_delete(context_, v);
    //     v = n;
    // }

    grib_context_free_persistent(context_, masterDir_);
    grib_context_free_persistent(context_, localDir_);
    grib_context_free_persistent(context_, ecmfDir_);
    grib_context_free_persistent(context_, basename_);
}

void HashArray::dump(FILE* f, int lvl)
{
    // for (int i = 0; i < lvl; i++)
    //     grib_context_print(act->context, f, "     ");
    // printf("hash_array(%s) { \n", act->name);
    // for (int i = 0; i < lvl; i++)
    //     grib_context_print(act->context, f, "     ");
    // printf("}\n");
}

grib_hash_array_value* HashArray::get_hash_array_impl(grib_handle* h)
{
    char buf[4096]       = {0, };
    char master[1024]    = {0, };
    char local[1024]     = {0, };
    char ecmf[1024]      = {0, };
    char masterDir[1024] = {0, };
    size_t lenMasterDir  = 1024;
    char localDir[1024]  = {0, };
    size_t lenLocalDir   = 1024;
    char ecmfDir[1024]   = {0, };
    size_t lenEcmfDir    = 1024;
    char key[4096]       = {0, };
    char* full           = 0;
    int id;

    grib_context* context    = context_;
    grib_hash_array_value* c = NULL;

    if (hash_array_ != NULL)
        return hash_array_;

    ECCODES_ASSERT(masterDir_);
    grib_get_string(h, masterDir_, masterDir, &lenMasterDir);

    snprintf(buf, 4096, "%s/%s", masterDir, basename_);

    int err = grib_recompose_name(h, NULL, buf, master, 1);
    if (err) {
        grib_context_log(context, GRIB_LOG_ERROR,
                         "unable to build name of directory %s", masterDir_);
        return NULL;
    }

    if (localDir_) {
        grib_get_string(h, localDir_, localDir, &lenLocalDir);
        snprintf(buf, 4096, "%s/%s", localDir, basename_);
        grib_recompose_name(h, NULL, buf, local, 1);
    }

    if (ecmfDir_) {
        grib_get_string(h, ecmfDir_, ecmfDir, &lenEcmfDir);
        snprintf(buf, 4096, "%s/%s", ecmfDir, basename_);
        grib_recompose_name(h, NULL, buf, ecmf, 1);
    }

    snprintf(key, 4096, "%s%s%s", master, local, ecmf);

    id = grib_itrie_get_id(h->context->hash_array_index, key);
    if ((c = h->context->hash_array[id]) != NULL)
        return c;

    if (*local && (full = grib_context_full_defs_path(context, local)) != NULL) {
        c = grib_parse_hash_array_file(context, full);
        grib_context_log(h->context, GRIB_LOG_DEBUG,
                         "Loading hash_array %s from %s", name_, full);
    }
    else if (*ecmf && (full = grib_context_full_defs_path(context, ecmf)) != NULL) {
        c = grib_parse_hash_array_file(context, full);
        grib_context_log(h->context, GRIB_LOG_DEBUG,
                         "Loading hash_array %s from %s", name_, full);
    }

    full = grib_context_full_defs_path(context, master);

    if (c) {
        if (!full) {
            grib_context_log(context, GRIB_LOG_ERROR,
                             "unable to find definition file %s in %s:%s:%s\nDefinition files path=\"%s\"",
                             basename_, master, ecmf, local, context->grib_definition_files_path);
            return NULL;
        }
        grib_hash_array_value* last = c;
        while (last->next)
            last = last->next;
        last->next = grib_parse_hash_array_file(context, full);
    }
    else if (full) {
        c = grib_parse_hash_array_file(context, full);
    }
    else {
        grib_context_log(context, GRIB_LOG_ERROR,
                         "unable to find definition file %s in %s:%s:%s\nDefinition files path=\"%s\"",
                         basename_, master, ecmf, local, context->grib_definition_files_path);
        return NULL;
    }
    full_path_ = full;

    grib_context_log(h->context, GRIB_LOG_DEBUG,
                     "Loading hash_array %s from %s", name_, full);

    h->context->hash_array[id] = c;
    if (c) {
        grib_trie* index = grib_trie_new(context);
        while (c) {
            c->index = index;
            grib_trie_insert_no_replace(index, c->name, c);
            c = c->next;
        }
    }

    return h->context->hash_array[id];
}

const char* HashArray::get_hash_array_full_path()
{
    return full_path_;
}


grib_hash_array_value* HashArray::get_hash_array(grib_handle* h)
{
    grib_hash_array_value* result = NULL;
    GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
    GRIB_MUTEX_LOCK(&mutex);

    result = get_hash_array_impl(h);

    GRIB_MUTEX_UNLOCK(&mutex);
    return result;
}

}  // namespace eccodes::action
