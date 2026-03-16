/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_bufr_decode_python.h"
#include "grib_dumper_factory.h"
#include <cctype>

eccodes::dumper::BufrDecodePython _grib_dumper_bufr_decode_python;
eccodes::Dumper* grib_dumper_bufr_decode_python = &_grib_dumper_bufr_decode_python;

namespace eccodes::dumper
{

static int depth = 0;

int BufrDecodePython::init()
{
    grib_context* c = context_;
    section_offset_ = 0;
    empty_          = 1;
    count_          = 1;
    isLeaf_         = 0;
    isAttribute_    = 0;
    keys_           = (grib_string_list*)grib_context_malloc_clear(c, sizeof(grib_string_list));

    return GRIB_SUCCESS;
}

int BufrDecodePython::destroy()
{
    grib_string_list* next = keys_;
    grib_string_list* cur  = NULL;
    grib_context* c        = context_;
    while (next) {
        cur  = next;
        next = next->next;
        grib_context_free(c, cur->value);
        grib_context_free(c, cur);
    }
    return GRIB_SUCCESS;
}

static char* dval_to_string(const grib_context* c, double v)
{
    char* sval = (char*)grib_context_malloc_clear(c, sizeof(char) * 40);
    snprintf(sval, 1024, "%.18e", v);
    return sval;
}

void BufrDecodePython::dump_values(grib_accessor* a)
{
    double value    = 0;
    size_t size     = 0;
    int err         = 0;
    int r           = 0;
    long count      = 0;
    char* sval      = NULL;
    grib_context* c = a->context_;
    grib_handle* h  = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = count;

    if (size <= 1) {
        err = a->unpack_double(&value, &size);
    }

    empty_ = 0;

    if (size > 1) {
        depth -= 2;
        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "    dVals = codes_get_array(ibufr, '#%d#%s')\n", r, a->name_);
        else
            fprintf(out_, "    dVals = codes_get_array(ibufr, '%s')\n", a->name_);
    }
    else {
        r = compute_bufr_key_rank(h, keys_, a->name_);
        if (!grib_is_missing_double(a, value)) {
            sval = dval_to_string(c, value);
            if (r != 0)
                fprintf(out_, "    dVal = codes_get(ibufr, '#%d#%s')\n", r, a->name_);
            else
                fprintf(out_, "    dVal = codes_get(ibufr, '%s')\n", a->name_);

            grib_context_free(c, sval);
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
        depth -= 2;
    }

    (void)err; /* TODO */
}

void BufrDecodePython::dump_values_attribute(grib_accessor* a, const char* prefix)
{
    double value = 0;
    size_t size  = 0;
    int err      = 0;
    long count   = 0;
    char* sval;
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
        depth -= 2;
        fprintf(out_, "    dVals = codes_get_array(ibufr, '%s->%s')\n", prefix, a->name_);
    }
    else {
        /* int r=compute_bufr_key_rank(h,keys_,a->name_); */
        if (!grib_is_missing_double(a, value)) {
            sval = dval_to_string(c, value);
            fprintf(out_, "    dVal = codes_get(ibufr, '%s->%s')\n", prefix, a->name_);

            grib_context_free(c, sval);
        }
    }

    if (isLeaf_ == 0) {
        char* prefix1;

        prefix1 = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + strlen(prefix) + 5));
        snprintf(prefix1, 1024, "%s->%s", prefix, a->name_);

        dump_attributes(a, prefix1);

        grib_context_free(c, prefix1);
        depth -= 2;
    }

    (void)err; /* TODO */
}

void BufrDecodePython::dump_long(grib_accessor* a, const char* comment)
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
            depth -= 2;
        }
        return;
    }

    if (size <= 1) {
        err = a->unpack_long(&value, &size);
    }

    empty_ = 0;

    if (size > 1) {
        depth -= 2;
        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "    iValues = codes_get_array(ibufr, '#%d#%s')\n", r, a->name_);
        else
            fprintf(out_, "    iValues = codes_get_array(ibufr, '%s')\n", a->name_);
    }
    else {
        r = compute_bufr_key_rank(h, keys_, a->name_);
        if (!grib_is_missing_long(a, value)) {
            if (r != 0)
                fprintf(out_, "    iVal = codes_get(ibufr, '#%d#%s')\n", r, a->name_);
            else
                fprintf(out_, "    iVal = codes_get(ibufr, '%s')\n", a->name_);
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
        depth -= 2;
    }
    (void)err; /* TODO */
}

void BufrDecodePython::dump_long_attribute(grib_accessor* a, const char* prefix)
{
    long value      = 0;
    size_t size     = 0;
    int err         = 0;
    long count      = 0;
    grib_context* c = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = count;

    if (size <= 1) {
        err = a->unpack_long(&value, &size);
    }

    empty_ = 0;

    if (size > 1) {
        depth -= 2;
        fprintf(out_, "    iVals = codes_get_array(ibufr, '%s->%s')\n", prefix, a->name_);
    }
    else {
        if (!codes_bufr_key_exclude_from_dump(prefix)) {
            if (!grib_is_missing_long(a, value)) {
                fprintf(out_, "    iVal = codes_get(ibufr, '%s->%s')\n", prefix, a->name_);
            }
        }
    }

    if (isLeaf_ == 0) {
        char* prefix1;

        prefix1 = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + strlen(prefix) + 5));
        snprintf(prefix1, 1024, "%s->%s", prefix, a->name_);

        dump_attributes(a, prefix1);

        grib_context_free(c, prefix1);
        depth -= 2;
    }
    (void)err; /* TODO */
}

void BufrDecodePython::dump_bits(grib_accessor* a, const char* comment)
{
}

void BufrDecodePython::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;
    int r;
    char* sval      = NULL;
    grib_handle* h  = grib_handle_of_accessor(a);
    grib_context* c = h->context;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->unpack_double(&value, &size);
    empty_ = 0;

    r = compute_bufr_key_rank(h, keys_, a->name_);
    if (!grib_is_missing_double(a, value)) {
        sval = dval_to_string(c, value);
        if (r != 0)
            fprintf(out_, "    dVal = codes_get(ibufr, '#%d#%s')\n", r, a->name_);
        else
            fprintf(out_, "    dVal = codes_get(ibufr, '%s')\n", a->name_);

        grib_context_free(c, sval);
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
        depth -= 2;
    }
}

void BufrDecodePython::dump_string_array(grib_accessor* a, const char* comment)
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

    empty_ = 0;

    if (isLeaf_ == 0) {
        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "    sVals = codes_get_string_array(ibufr, '#%d#%s')\n", r, a->name_);
        else
            fprintf(out_, "    sVals = codes_get_string_array(ibufr, '%s')\n", a->name_);
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
        depth -= 2;
    }

    (void)err; /* TODO */
}

#define MAX_STRING_SIZE 4096
void BufrDecodePython::dump_string(grib_accessor* a, const char* comment)
{
    char value[MAX_STRING_SIZE] = {0, }; /* See ECC-710 */
    char* p         = NULL;
    size_t size     = MAX_STRING_SIZE;
    grib_context* c = a->context_;
    int r = 0, err = 0;
    grib_handle* h = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

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
        depth += 2;
        if (r != 0)
            fprintf(out_, "    sVal = codes_get(ibufr, '#%d#%s')\n", r, a->name_);
        else
            fprintf(out_, "    sVal = codes_get(ibufr, '%s')\n", a->name_);
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
        depth -= 2;
    }

    (void)err; /* TODO */
}

void BufrDecodePython::dump_bytes(grib_accessor* a, const char* comment)
{
}

void BufrDecodePython::dump_label(grib_accessor* a, const char* comment)
{
}

static void _dump_long_array(grib_handle* h, FILE* f, const char* key)
{
    size_t size = 0;
    if (grib_get_size(h, key, &size) == GRIB_NOT_FOUND)
        return;
    if (size == 0)
        return;

    fprintf(f, "    iVals = codes_get_array(ibufr, '%s')\n", key);
}

void BufrDecodePython::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    if (strcmp(a->name_, "BUFR") == 0 ||
        strcmp(a->name_, "GRIB") == 0 ||
        strcmp(a->name_, "META") == 0) {
        grib_handle* h = grib_handle_of_accessor(a);
        depth          = 2;
        empty_         = 1;
        depth += 2;
        _dump_long_array(h, out_, "dataPresentIndicator");
        _dump_long_array(h, out_, "delayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "shortDelayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "extendedDelayedDescriptorReplicationFactor");
        /* Do not show the inputOverriddenReferenceValues array. That's more for ENCODING */
        /* _dump_long_array(h,out_,"inputOverriddenReferenceValues","inputOverriddenReferenceValues"); */
        grib_dump_accessors_block(this, block);
        depth -= 2;
    }
    else if (strcmp(a->name_, "groupNumber") == 0) {
        if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
            return;
        empty_ = 1;
        depth += 2;
        grib_dump_accessors_block(this, block);
        depth -= 2;
    }
    else {
        grib_dump_accessors_block(this, block);
    }
}

void BufrDecodePython::dump_attributes(grib_accessor* a, const char* prefix)
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
        flags   = a->attributes_[i]->flags_;
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

void BufrDecodePython::header(const grib_handle* h) const
{
    if (count_ < 2) {
        /* This is the first message being processed */
        fprintf(out_, "#  This program was automatically generated with bufr_dump -Dpython\n");
        fprintf(out_, "#  Using ecCodes version: ");
        grib_print_api_version(out_);
        fprintf(out_, "\n\n");
        fprintf(out_, "import traceback\n");
        fprintf(out_, "import sys\n");
        fprintf(out_, "from eccodes import *\n\n\n");
        fprintf(out_, "def bufr_decode(input_file):\n");
        fprintf(out_, "    f = open(input_file, 'rb')\n");
    }
    fprintf(out_, "    # Message number %ld\n    # -----------------\n", count_);
    fprintf(out_, "    print ('Decoding message number %ld')\n", count_);
    fprintf(out_, "    ibufr = codes_bufr_new_from_file(f)\n");
    fprintf(out_, "    codes_set(ibufr, 'unpack', 1)\n");
}

void BufrDecodePython::footer(const grib_handle* h) const
{
    fprintf(out_, "    codes_release(ibufr)\n");
}

}  // namespace eccodes::dumper
