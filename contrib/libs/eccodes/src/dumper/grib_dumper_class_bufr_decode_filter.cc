/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_bufr_decode_filter.h"
#include "grib_dumper_factory.h"
#include <cctype>

eccodes::dumper::BufrDecodeFilter _grib_dumper_bufr_decode_filter;
eccodes::Dumper *grib_dumper_bufr_decode_filter = &_grib_dumper_bufr_decode_filter;

namespace eccodes::dumper {

int BufrDecodeFilter::init()
{
    section_offset_ = 0;
    empty_          = 1;
    isLeaf_         = 0;
    isAttribute_    = 0;
    keys_           = (grib_string_list*)grib_context_malloc_clear(
        context_, sizeof(grib_string_list));

    return GRIB_SUCCESS;
}

int BufrDecodeFilter::destroy()
{
    grib_string_list* next = keys_;
    grib_string_list* cur  = NULL;
    while (next) {
        cur  = next;
        next = next->next;
        grib_context_free(context_, cur->value);
        grib_context_free(context_, cur);
    }
    return GRIB_SUCCESS;
}

void BufrDecodeFilter::dump_values(grib_accessor* a)
{
    double value = 0;
    size_t size  = 0;
    int err      = 0;
    int r;
    long count      = 0;
    grib_context* c = a->context_;
    grib_handle* h  = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = count;

    if (size <= 1) {
        err = a->unpack_double(&value, &size);
    }

    begin_ = 0;
    empty_ = 0;

    if (size > 1) {
        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
        else
            fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
    }
    else {
        r = compute_bufr_key_rank(h, keys_, a->name_);
        if (!grib_is_missing_double(a, value)) {
            if (r != 0)
                fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
            else
                fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
        }
    }

    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if (r != 0) {
            prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + 10));
            dofree = 1;
            snprintf(prefix, 1024, "#%d#%s", r, a->name_);
        }
        else
            prefix = (char*)a->name_;

        dump_attributes(a, prefix);
        if (dofree)
            grib_context_free(c, prefix);
        depth_ -= 2;
    }

    (void)err; /* TODO */
}

void BufrDecodeFilter::dump_values_attribute(grib_accessor* a, const char* prefix)
{
    double value    = 0;
    size_t size     = 0;
    int err         = 0;
    long count      = 0;
    grib_context* c = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = count;

    if (size <= 1) {
        err = a->unpack_double(&value, &size);
    }

    empty_ = 0;

    if (size > 1) {
        fprintf(out_, "print \"%s->%s = [%s->%s]\";\n", prefix, a->name_, prefix, a->name_);
    }
    else {
        if (!grib_is_missing_double(a, value)) {
            fprintf(out_, "print \"%s->%s = [%s->%s]\";\n", prefix, a->name_, prefix, a->name_);
        }
    }

    if (isLeaf_ == 0) {
        char* prefix1 = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + strlen(prefix) + 5));
        snprintf(prefix1, 1024, "%s->%s", prefix, a->name_);

        dump_attributes(a, prefix1);

        grib_context_free(c, prefix1);
        depth_ -= 2;
    }

    (void)err; /* TODO */
}

void BufrDecodeFilter::dump_long(grib_accessor* a, const char* comment)
{
    long value      = 0;
    size_t size     = 0;
    int err         = 0;
    int r           = 0;
    long count      = 0;
    grib_context* c = a->context_;
    grib_handle* h  = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    a->value_count(&count);
    size = count;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0) {
        if (isLeaf_ == 0) {
            char* prefix;
            int dofree = 0;

            r = compute_bufr_key_rank(h, keys_, a->name_);
            if (r != 0) {
                prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + 10));
                dofree = 1;
                snprintf(prefix, 1024, "#%d#%s", r, a->name_);
            }
            else
                prefix = (char*)a->name_;

            dump_attributes(a, prefix);
            if (dofree)
                grib_context_free(c, prefix);
            depth_ -= 2;
        }
        return;
    }

    if (size <= 1) {
        err = a->unpack_long(&value, &size);
    }

    begin_ = 0;
    empty_ = 0;

    if (size > 1) {
        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
        else
            fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
    }
    else {
        r = compute_bufr_key_rank(h, keys_, a->name_);
        if (!grib_is_missing_long(a, value)) {
            if (r != 0)
                fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
            else
                fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
        }
    }

    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if (r != 0) {
            prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + 10));
            dofree = 1;
            snprintf(prefix, 1024, "#%d#%s", r, a->name_);
        }
        else
            prefix = (char*)a->name_;

        dump_attributes(a, prefix);
        if (dofree)
            grib_context_free(c, prefix);
        depth_ -= 2;
    }
    (void)err; /* TODO */
}

void BufrDecodeFilter::dump_long_attribute(grib_accessor* a, const char* prefix)
{
    int err         = 0;
    grib_context* c = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    empty_ = 0;
    if (!codes_bufr_key_exclude_from_dump(prefix)) {
        fprintf(out_, "print \"%s->%s = [%s->%s]\";\n", prefix, a->name_, prefix, a->name_);
    }
    if (isLeaf_ == 0) {
        char* prefix1 = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + strlen(prefix) + 5));
        snprintf(prefix1, 1024, "%s->%s", prefix, a->name_);

        dump_attributes(a, prefix1);

        grib_context_free(c, prefix1);
        depth_ -= 2;
    }
    (void)err; /* TODO */
}

void BufrDecodeFilter::dump_bits(grib_accessor* a, const char* comment) {}

void BufrDecodeFilter::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;
    int r;
    grib_handle* h  = grib_handle_of_accessor(a);
    grib_context* c = h->context;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->unpack_double(&value, &size);
    begin_ = 0;
    empty_ = 0;

    r = compute_bufr_key_rank(h, keys_, a->name_);
    if (!grib_is_missing_double(a, value)) {
        if (r != 0)
            fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
        else
            fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
    }

    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if (r != 0) {
            prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + 10));
            dofree = 1;
            snprintf(prefix, 1024, "#%d#%s", r, a->name_);
        }
        else
            prefix = (char*)a->name_;

        dump_attributes(a, prefix);
        if (dofree)
            grib_context_free(c, prefix);
        depth_ -= 2;
    }
}

void BufrDecodeFilter::dump_string_array(grib_accessor* a,
                                         const char* comment)
{
    size_t size     = 0;
    grib_context* c = NULL;
    int err         = 0;
    long count      = 0;
    int r           = 0;
    grib_handle* h  = grib_handle_of_accessor(a);

    c = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = count;
    if (size == 1) {
        dump_string(a, comment);
        return;
    }

    begin_ = 0;

    if (isLeaf_ == 0) {
        depth_ += 2;
        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
        else
            fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
    }

    empty_ = 0;

    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if (r != 0) {
            prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + 10));
            dofree = 1;
            snprintf(prefix, 1024, "#%d#%s", r, a->name_);
        }
        else
            prefix = (char*)a->name_;

        dump_attributes(a, prefix);
        if (dofree)
            grib_context_free(c, prefix);
        depth_ -= 2;
    }

    (void)err; /* TODO */
}

#define MAX_STRING_SIZE 4096
void BufrDecodeFilter::dump_string(grib_accessor* a, const char* comment)
{
    char value[MAX_STRING_SIZE] = {0, }; /* See ECC-710 */
    size_t size     = MAX_STRING_SIZE;
    char* p         = NULL;
    grib_context* c = a->context_;
    int r = 0, err = 0;
    grib_handle* h = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    begin_ = 0;

    empty_ = 0;

    err = a->unpack_string(value, &size);
    p   = value;
    r   = compute_bufr_key_rank(h, keys_, a->name_);
    if (grib_is_missing_string(a, (unsigned char*)value, size)) {
        return;
    }

    while (*p) {
        if (!isprint(*p))
            *p = '.';
        p++;
    }

    if (isLeaf_ == 0) {
        depth_ += 2;
        if (r != 0)
            fprintf(out_, "print \"#%d#%s=[#%d#%s]\";\n", r, a->name_, r, a->name_);
        else
            fprintf(out_, "print \"%s=[%s]\";\n", a->name_, a->name_);
    }

    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if (r != 0) {
            prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + 10));
            dofree = 1;
            snprintf(prefix, 1024, "#%d#%s", r, a->name_);
        }
        else
            prefix = (char*)a->name_;

        dump_attributes(a, prefix);
        if (dofree)
            grib_context_free(c, prefix);
        depth_ -= 2;
    }

    (void)err; /* TODO */
}

void BufrDecodeFilter::dump_bytes(grib_accessor* a, const char* comment) {}

void BufrDecodeFilter::dump_label(grib_accessor* a, const char* comment) {}

static void _dump_long_array(grib_handle* h, FILE* f, const char* key)
{
    size_t size = 0;
    if (grib_get_size(h, key, &size) == GRIB_NOT_FOUND)
        return;
    if (size == 0)
        return;

    fprintf(f, "print \"%s=[%s]\";\n", key, key);
}

void BufrDecodeFilter::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    if (strcmp(a->name_, "BUFR") == 0 || strcmp(a->name_, "GRIB") == 0 ||
        strcmp(a->name_, "META") == 0) {
        grib_handle* h = grib_handle_of_accessor(a);
        depth_         = 2;
        begin_         = 1;
        empty_         = 1;
        depth_ += 2;
        _dump_long_array(h, out_, "dataPresentIndicator");
        _dump_long_array(h, out_, "delayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "shortDelayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "extendedDelayedDescriptorReplicationFactor");
        /* Do not show the inputOverriddenReferenceValues array. That's more for
         * ENCODING */
        /*_dump_long_array(h,out_,"inputOverriddenReferenceValues","inputOverriddenReferenceValues");*/
        grib_dump_accessors_block(this, block);
        depth_ -= 2;
    }
    else if (strcmp(a->name_, "groupNumber") == 0) {
        if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
            return;
        begin_ = 1;
        empty_ = 1;
        depth_ += 2;
        grib_dump_accessors_block(this, block);
        depth_ -= 2;
    }
    else {
        grib_dump_accessors_block(this, block);
    }
}

void BufrDecodeFilter::dump_attributes(grib_accessor* a, const char* prefix)
{
    int i = 0;
    unsigned long flags;
    while (i < MAX_ACCESSOR_ATTRIBUTES && a->attributes_[i]) {
        isAttribute_ = 1;
        if ((option_flags_ & GRIB_DUMP_FLAG_ALL_ATTRIBUTES) == 0 && (a->attributes_[i]->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0) {
            i++;
            continue;
        }
        isLeaf_ = a->attributes_[i]->attributes_[0] == NULL ? 1 : 0;
        /* fprintf(out_,","); */
        /* fprintf(out_,"\n%-*s",depth," "); */
        /* fprintf(out,"\"%s\" : ",a->attributes_[i]->name); */
        flags = a->attributes_[i]->flags_;
        a->attributes_[i]->flags_ |= GRIB_ACCESSOR_FLAG_DUMP;
        switch (a->attributes_[i]->get_native_type()) {
            case GRIB_TYPE_LONG:
                dump_long_attribute(a->attributes_[i], prefix);
                break;
            case GRIB_TYPE_DOUBLE:
                dump_values_attribute(a->attributes_[i], prefix);
                break;
            case GRIB_TYPE_STRING:
                break;
        }
        a->attributes_[i]->flags_ = flags;
        i++;
    }
    isLeaf_      = 0;
    isAttribute_ = 0;
}

}  // namespace eccodes::dumper
