/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_dictionary.h"

grib_accessor_dictionary_t _grib_accessor_dictionary{};
grib_accessor* grib_accessor_dictionary = &_grib_accessor_dictionary;

void grib_accessor_dictionary_t::init(const long len, grib_arguments* params)
{
    grib_accessor_gen_t::init(len, params);
    int n = 0;

    dictionary_ = params->get_string(grib_handle_of_accessor(this), n++);
    key_        = params->get_name(grib_handle_of_accessor(this), n++);
    column_     = params->get_long(grib_handle_of_accessor(this), n++);
    masterDir_  = params->get_name(grib_handle_of_accessor(this), n++);
    localDir_   = params->get_name(grib_handle_of_accessor(this), n++);

    length_ = 0;
    flags_ |= GRIB_ACCESSOR_FLAG_READ_ONLY;
}

grib_trie* grib_accessor_dictionary_t::load_dictionary(int* err)
{
    char* filename  = NULL;
    char line[1024] = {0,};
    char key[1024] = {0,};
    char masterDir[1024] = {0,};
    char localDir[1024] = {0,};
    char dictName[1024] = {0,};
    char* localFilename   = 0;
    char* list            = 0;
    size_t len            = 1024;
    grib_trie* dictionary = NULL;
    FILE* f               = NULL;
    int i                 = 0;
    grib_handle* h        = grib_handle_of_accessor(this);
    grib_context* c       = context_;

    *err = GRIB_SUCCESS;

    len = 1024;
    if (masterDir_ != NULL)
        grib_get_string(h, masterDir_, masterDir, &len);
    len = 1024;
    if (localDir_ != NULL)
        grib_get_string(h, localDir_, localDir, &len);

    if (*masterDir != 0) {
        char name[2048] = {0,};
        char recomposed[2048] = {0,};
        snprintf(name, sizeof(name), "%s/%s", masterDir, dictionary_);
        grib_recompose_name(h, NULL, name, recomposed, 0);
        filename = grib_context_full_defs_path(c, recomposed);
    }
    else {
        filename = grib_context_full_defs_path(c, dictionary_);
    }

    if (*localDir != 0) {
        char localName[2048] = {0,};
        char localRecomposed[1024] = {0,};
        snprintf(localName, sizeof(localName), "%s/%s", localDir, dictionary_);
        grib_recompose_name(h, NULL, localName, localRecomposed, 0);
        localFilename = grib_context_full_defs_path(c, localRecomposed);
        snprintf(dictName, sizeof(dictName), "%s:%s", localFilename, filename);
    }
    else {
        snprintf(dictName, sizeof(dictName), "%s", filename);
    }

    if (!filename) {
        grib_context_log(c, GRIB_LOG_ERROR, "Unable to find def file %s", dictionary_);
        *err = GRIB_FILE_NOT_FOUND;
        return NULL;
    }
    else {
        grib_context_log(c, GRIB_LOG_DEBUG, "dictionary: found def file %s", filename);
    }
    dictionary = (grib_trie*)grib_trie_get(c->lists, dictName);
    if (dictionary) {
        grib_context_log(c, GRIB_LOG_DEBUG, "using dictionary %s from cache", dictionary_);
        return dictionary;
    }
    else {
        grib_context_log(c, GRIB_LOG_DEBUG, "using dictionary %s from file %s", dictionary_, filename);
    }

    f = codes_fopen(filename, "r");
    if (!f) {
        *err = GRIB_IO_PROBLEM;
        return NULL;
    }

    dictionary = grib_trie_new(c);

    while (fgets(line, sizeof(line) - 1, f)) {
        i = 0;
        while (line[i] != '|' && line[i] != 0) {
            key[i] = line[i];
            i++;
        }
        key[i] = 0;
        list   = (char*)grib_context_malloc_clear(c, strlen(line) + 1);
        memcpy(list, line, strlen(line));
        grib_trie_insert(dictionary, key, list);
    }

    fclose(f);

    if (localFilename != 0) {
        *err = GRIB_NOT_IMPLEMENTED;
        return NULL;
        // f = codes_fopen(localFilename, "r");
        // if (!f) {
        //     *err = GRIB_IO_PROBLEM;
        //     return NULL;
        // }

        //while (fgets(line, sizeof(line) - 1, f)) {
        //    i = 0;
        //    while (line[i] != '|' && line[i] != 0) {
        //        key[i] = line[i];
        //        i++;
        //    }
        //    key[i] = 0;
        //    list   = (char*)grib_context_malloc_clear(c, strlen(line) + 1);
        //    memcpy(list, line, strlen(line));
        //    grib_trie_insert(dictionary, key, list);
        //}

        //fclose(f);
    }
    grib_trie_insert(c->lists, filename, dictionary);
    return dictionary;
}

void grib_accessor_dictionary_t::dump(eccodes::Dumper* dumper)
{
    switch (get_native_type()) {
        case GRIB_TYPE_STRING:
            dumper->dump_string(this, NULL);
            break;
        case GRIB_TYPE_LONG:
            dumper->dump_long(this, NULL);
            break;
        case GRIB_TYPE_DOUBLE:
            dumper->dump_double(this, NULL);
            break;
    }
}

int grib_accessor_dictionary_t::unpack_string(char* buffer, size_t* len)
{
    int err        = GRIB_SUCCESS;
    char key[1024] = {0,};
    size_t size  = 1024;
    char* list   = NULL;
    char* start  = NULL;
    char* end    = NULL;
    size_t rsize = 0;
    int i        = 0;

    grib_trie* dictionary = load_dictionary(&err);
    if (err)
        return err;

    if ((err = grib_get_string_internal(grib_handle_of_accessor(this), key_, key, &size)) != GRIB_SUCCESS) {
        /* grib_trie_delete(dictionary); */
        return err;
    }

    list = (char*)grib_trie_get(dictionary, key);
    if (!list) {
        /* grib_trie_delete(dictionary); */
        return GRIB_NOT_FOUND;
    }

    end = list;
    for (i = 0; i <= column_; i++) {
        start = end;
        while (*end != '|' && *end != 0)
            end++;
        if (!*end) {
            break;
        }
        end++;
    }
    end--;
    rsize = end - start;
    if (*len < rsize) {
        /* grib_trie_delete(dictionary); */
        return GRIB_ARRAY_TOO_SMALL;
    }

    *len = rsize;
    if (buffer && start)
        memcpy(buffer, start, rsize);
    if (buffer)
        buffer[rsize] = 0;

    /* grib_trie_delete(dictionary); */

    return err;
}

int grib_accessor_dictionary_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

long grib_accessor_dictionary_t::get_native_type()
{
    int type = GRIB_TYPE_DOUBLE;
    if (flags_ & GRIB_ACCESSOR_FLAG_LONG_TYPE)
        type = GRIB_TYPE_LONG;
    if (flags_ & GRIB_ACCESSOR_FLAG_STRING_TYPE)
        type = GRIB_TYPE_STRING;
    return type;
}

int grib_accessor_dictionary_t::unpack_long(long* val, size_t* len)
{
    int err           = 0;
    char buffer[1024] = {0,};
    size_t size = 1024;

    err = unpack_string(buffer, &size);
    if (err)
        return err;

    *val = atol(buffer);
    *len = 1;

    return GRIB_SUCCESS;
}

int grib_accessor_dictionary_t::unpack_double(double* val, size_t* len)
{
    int err           = 0;
    char buffer[1024] = {0,};
    size_t size = 1024;

    err = unpack_string(buffer, &size);
    if (err)
        return err;

    *val = atof(buffer);
    *len = 1;

    return GRIB_SUCCESS;
}
