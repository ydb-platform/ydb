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

bufr_keys_iterator* codes_bufr_keys_iterator_new(grib_handle* h, unsigned long filter_flags)
{
    bufr_keys_iterator* ki = NULL;

    if (!h)
        return NULL;

    if (h->product_kind != PRODUCT_BUFR) {
        grib_context_log(h->context, GRIB_LOG_ERROR,
                         "Invalid keys iterator for message: please use codes_keys_iterator_new");
        return NULL;
    }

    ki = (bufr_keys_iterator*)grib_context_malloc_clear(h->context, sizeof(bufr_keys_iterator));
    if (!ki)
        return NULL;

    ki->filter_flags = filter_flags;
    ki->handle       = h;
    DEBUG_ASSERT(h->product_kind == PRODUCT_BUFR);
    ki->key_name            = NULL;
    ki->i_curr_attribute    = 0;
    ki->accessor_flags_only = GRIB_ACCESSOR_FLAG_DUMP;
    ki->accessor_flags_skip = GRIB_ACCESSOR_FLAG_HIDDEN; /*ECC-568*/

    ki->at_start = 1;
    ki->match    = 0;

    if (ki->seen == NULL)
        ki->seen = grib_trie_new(h->context);

    return ki;
}

bufr_keys_iterator* codes_bufr_data_section_keys_iterator_new(grib_handle* h)
{
    bufr_keys_iterator* ki = NULL;

    if (!h)
        return NULL;

    ki = (bufr_keys_iterator*)grib_context_malloc_clear(h->context, sizeof(bufr_keys_iterator));
    if (!ki)
        return NULL;

    ki->handle = h;
    DEBUG_ASSERT(h->product_kind == PRODUCT_BUFR);
    ki->i_curr_attribute    = 0;
    ki->accessor_flags_only = GRIB_ACCESSOR_FLAG_BUFR_DATA | GRIB_ACCESSOR_FLAG_DUMP;
    ki->accessor_flags_skip = GRIB_ACCESSOR_FLAG_HIDDEN | GRIB_ACCESSOR_FLAG_READ_ONLY;

    ki->at_start = 1;
    ki->match    = 0;

    if (ki->seen == NULL)
        ki->seen = grib_trie_new(h->context);

    return ki;
}

static void mark_seen(bufr_keys_iterator* ki, const char* name)
{
    int* r = (int*)grib_trie_get(ki->seen, name);

    if (r)
        (*r)++;
    else {
        r  = (int*)grib_context_malloc(ki->handle->context, sizeof(int));
        *r = 1;
        grib_trie_insert(ki->seen, name, (void*)r);
    }
}

int codes_bufr_keys_iterator_rewind(bufr_keys_iterator* ki)
{
    ki->at_start = 1;
    return GRIB_SUCCESS;
}

static int skip(bufr_keys_iterator* kiter)
{
    if (kiter->current->sub_section_)
        return 1;

    if (kiter->current->flags_ & kiter->accessor_flags_skip) {
        return 1;
    }

    if (kiter->accessor_flags_only == (kiter->current->flags_ & kiter->accessor_flags_only)) {
        mark_seen(kiter, kiter->current->name_);
        return 0;
    }
    else {
        return 1;
    }
}

static int next_attribute(bufr_keys_iterator* kiter)
{
    int* r = 0;
    int i_curr_attribute;
    if (!kiter->current)
        return 0;
    if (!kiter->attributes) {
        kiter->attributes       = kiter->current->attributes_;
        kiter->prefix           = 0;
        kiter->i_curr_attribute = 0;
    }
    i_curr_attribute = kiter->i_curr_attribute - 1;

    while (kiter->i_curr_attribute < MAX_ACCESSOR_ATTRIBUTES && kiter->attributes[kiter->i_curr_attribute]) {
        if ((kiter->attributes[kiter->i_curr_attribute]->flags_ & GRIB_ACCESSOR_FLAG_DUMP) != 0 && (kiter->attributes[kiter->i_curr_attribute]->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) == 0)
            break;
        kiter->i_curr_attribute++;
    }

    if (kiter->attributes[kiter->i_curr_attribute]) {
        if (!kiter->prefix) {
            const size_t prefixLenMax = strlen(kiter->current->name_) + 10;
            kiter->prefix = (char*)grib_context_malloc_clear(kiter->current->context_, prefixLenMax);
            r             = (int*)grib_trie_get(kiter->seen, kiter->current->name_);
            snprintf(kiter->prefix, prefixLenMax, "#%d#%s", *r, kiter->current->name_);
        }
        kiter->i_curr_attribute++;
        return 1;
    }
    else {
        char* prefix = 0;
        if (!kiter->prefix)
            return 0;
        if (!kiter->attributes[i_curr_attribute]) {
            grib_context_free(kiter->current->context_, kiter->prefix);
            kiter->prefix = 0;
            return 0;
        }
        prefix = (char*)grib_context_malloc_clear(kiter->current->context_, strlen(kiter->prefix) + strlen(kiter->attributes[i_curr_attribute]->name_) + 3);
        /*sprintf(prefix,"%s->%s",kiter->prefix,kiter->attributes[i_curr_attribute]->name);*/
        strcpy(prefix, kiter->prefix); /* strcpy and strcat here are much faster than sprintf */
        strcat(prefix, "->");
        strcat(prefix, kiter->attributes[i_curr_attribute]->name_);
        grib_context_free(kiter->current->context_, kiter->prefix);
        kiter->prefix           = prefix;
        kiter->attributes       = kiter->attributes[i_curr_attribute]->attributes_;
        kiter->i_curr_attribute = 0;
        return next_attribute(kiter);
    }
}

int codes_bufr_keys_iterator_next(bufr_keys_iterator* kiter)
{
    /* ECC-734: de-allocate last key name stored */
    grib_context_free(kiter->handle->context, kiter->key_name);
    kiter->key_name = NULL;

    if (kiter->at_start) {
        kiter->current          = kiter->handle->root->block->first;
        kiter->at_start         = 0;
        kiter->i_curr_attribute = 0;
        kiter->prefix           = 0;
        kiter->attributes       = 0;
    }
    else {
        if (next_attribute(kiter)) {
            return 1;
        }
        else {
            kiter->current    = kiter->current->next_accessor();
            kiter->attributes = 0;
            if (kiter->prefix) {
                grib_context_free(kiter->current->context_, kiter->prefix);
                kiter->prefix = 0;
            }
            kiter->i_curr_attribute = 0;
        }
    }

    while (kiter->current && skip(kiter))
        kiter->current = kiter->current->next_accessor();

    return kiter->current != NULL;
}

/* The return value is constructed so we allocate memory for it. */
/* We free in codes_bufr_keys_iterator_delete() */
char* codes_bufr_keys_iterator_get_name(const bufr_keys_iterator* ckiter)
{
    bufr_keys_iterator* kiter = (bufr_keys_iterator*)ckiter;
    int* r                    = 0;
    char* ret                 = 0;
    const grib_context* c     = kiter->handle->context;
    DEBUG_ASSERT(kiter->current);

    if (kiter->prefix) {
        int iattribute = kiter->i_curr_attribute - 1;
        ret            = (char*)grib_context_malloc_clear(c, strlen(kiter->prefix) + strlen(kiter->attributes[iattribute]->name_) + 10);
        /*sprintf(ret,"%s->%s",kiter->prefix,kiter->attributes[iattribute]->name);*/
        strcpy(ret, kiter->prefix); /* strcpy and strcat here are much faster than sprintf */
        strcat(ret, "->");
        strcat(ret, kiter->attributes[iattribute]->name_);
    }
    else {
        const size_t retMaxLen = strlen(kiter->current->name_) + 10;
        ret = (char*)grib_context_malloc_clear(c, retMaxLen);

        if (kiter->current->flags_ & GRIB_ACCESSOR_FLAG_BUFR_DATA) {
            r = (int*)grib_trie_get(kiter->seen, kiter->current->name_);
            snprintf(ret, retMaxLen, "#%d#%s", *r, kiter->current->name_);
        }
        else {
            strcpy(ret, kiter->current->name_);
        }
    }

    kiter->key_name = ret; /*store reference to last key name*/

    return ret;
}

grib_accessor* codes_bufr_keys_iterator_get_accessor(bufr_keys_iterator* kiter)
{
    return kiter->current;
}

int codes_bufr_keys_iterator_delete(bufr_keys_iterator* kiter)
{
    if (kiter) {
        const grib_context* c = kiter->handle->context;
        kiter->key_name = NULL;
        if (kiter->seen)
            grib_trie_delete(kiter->seen);
        grib_context_free(c, kiter);
    }
    return 0;
}
