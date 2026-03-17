/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_bufr_encode_fortran.h"
#include "grib_dumper_factory.h"
#include <cctype>

eccodes::dumper::BufrEncodeFortran _grib_dumper_bufr_encode_fortran;
eccodes::Dumper* grib_dumper_bufr_encode_fortran = &_grib_dumper_bufr_encode_fortran;

namespace eccodes::dumper
{

int BufrEncodeFortran::init()
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

int BufrEncodeFortran::destroy()
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

static char* lval_to_string(grib_context* c, long v)
{
    char* sval = (char*)grib_context_malloc_clear(c, sizeof(char) * 40);
    if (v == GRIB_MISSING_LONG)
        snprintf(sval, 1024, "CODES_MISSING_LONG");
    else
        snprintf(sval, 1024, "%ld", v);
    return sval;
}
static char* dval_to_string(grib_context* c, double v)
{
    char* sval = (char*)grib_context_malloc_clear(c, sizeof(char) * 40);
    if (v == GRIB_MISSING_DOUBLE) {
        snprintf(sval, 1024, "CODES_MISSING_DOUBLE");
    }
    else {
        char* p;
        snprintf(sval, 1024, "%.18e", v);
        p = sval;
        while (*p != 0) {
            if (*p == 'e')
                *p = 'd';
            p++;
        }
    }
    return sval;
}

/* Some lines can grow longer than Fortran compilers allow (=132). */
/* This is mainly due to long key names with attributes. */
/* The resturn value of this function must be freed by the caller */
static char* break_line(grib_context* c, const char* input)
{
    /* Break a long line using Fortran continuation characters */
    char* a_token    = NULL;
    char* lasts      = NULL;
    int first        = 1;
    const size_t len = strlen(input);
    /* Add a bit more for inserted newlines and continuation characters */
    char* result = (char*)grib_context_malloc_clear(c, sizeof(char) * len + 100);

    /* No need to alter input which is already too short or has newlines */
    if (len < 70 || strchr(input, '\n')) {
        strcpy(result, input);
        return result;
    }

    /* A Fortran multi-line string has two ampersands. E.g. */
    /* 'hello &
     * &world'  is the same as 'hello world'
     */
    a_token = strtok_r((char*)input, "->", &lasts);
    while (a_token) {
        if (first) {
            first = 0;
            strcat(result, a_token);
        }
        else {
            char tmp[256] = {0, };
            snprintf(tmp, sizeof(tmp), "->&\n    &%s", a_token);
            strcat(result, tmp);
        }
        a_token = strtok_r(NULL, "->", &lasts);
    }

    return result;
}

void BufrEncodeFortran::dump_values(grib_accessor* a)
{
    double value = 0;
    size_t size = 0, size2 = 0;
    double* values = NULL;
    int err        = 0;
    int i, r, icount;
    int cols   = 2;
    long count = 0;
    char* sval;
    grib_context* c = a->context_;
    grib_handle* h  = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = size2 = count;

    if (size > 1) {
        values = (double*)grib_context_malloc_clear(c, sizeof(double) * size);
        err    = a->unpack_double(values, &size2);
    }
    else {
        err = a->unpack_double(&value, &size2);
    }
    ECCODES_ASSERT(size2 == size);

    empty_ = 0;

    if (size > 1) {
        fprintf(out_, "  if(allocated(rvalues)) deallocate(rvalues)\n");
        fprintf(out_, "  allocate(rvalues(%lu))\n", (unsigned long)size);

        fprintf(out_, "  rvalues=(/");

        icount = 0;
        for (i = 0; i < size - 1; ++i) {
            if (icount > cols || i == 0) {
                fprintf(out_, "  &\n      ");
                icount = 0;
            }
            sval = dval_to_string(c, values[i]);
            fprintf(out_, "%s, ", sval);
            grib_context_free(c, sval);
            icount++;
        }
        if (icount > cols || i == 0) {
            fprintf(out_, "  &\n      ");
        }
        sval = dval_to_string(c, values[size - 1]);
        fprintf(out_, "%s", sval);
        grib_context_free(c, sval);

        depth_ -= 2;
        fprintf(out_, "/)\n");
        grib_context_free(c, values);

        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "  call codes_set(ibufr,'#%d#%s',rvalues)\n", r, a->name_);
        else
            fprintf(out_, "  call codes_set(ibufr,'%s',rvalues)\n", a->name_);
    }
    else {
        r    = compute_bufr_key_rank(h, keys_, a->name_);
        sval = dval_to_string(c, value);
        if (r != 0)
            fprintf(out_, "  call codes_set(ibufr,'#%d#%s',%s)\n", r, a->name_, sval);
        else
            fprintf(out_, "  call codes_set(ibufr,'%s',%s)\n", a->name_, sval);
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
        depth_ -= 2;
    }

    (void)err; /* TODO */
}

void BufrEncodeFortran::dump_values_attribute(grib_accessor* a, const char* prefix)
{
    double value = 0;
    size_t size = 0, size2 = 0;
    double* values = NULL;
    int err        = 0;
    int i, icount;
    int cols   = 2;
    long count = 0;
    char* sval;
    grib_context* c = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = size2 = count;

    if (size > 1) {
        values = (double*)grib_context_malloc_clear(c, sizeof(double) * size);
        err    = a->unpack_double(values, &size2);
    }
    else {
        err = a->unpack_double(&value, &size2);
    }
    ECCODES_ASSERT(size2 == size);

    empty_ = 0;

    if (size > 1) {
        fprintf(out_, "  if(allocated(rvalues)) deallocate(rvalues)\n");
        fprintf(out_, "  allocate(rvalues(%lu))\n", (unsigned long)size);

        fprintf(out_, "  rvalues=(/");

        icount = 0;
        for (i = 0; i < size - 1; ++i) {
            if (icount > cols || i == 0) {
                fprintf(out_, "  &\n      ");
                icount = 0;
            }
            sval = dval_to_string(c, values[i]);
            fprintf(out_, "%s, ", sval);
            grib_context_free(c, sval);
            icount++;
        }
        if (icount > cols || i == 0) {
            fprintf(out_, "  &\n      ");
        }
        sval = dval_to_string(c, values[size - 1]);
        fprintf(out_, "%s", sval);
        grib_context_free(c, sval);

        depth_ -= 2;
        fprintf(out_, "/)\n");
        grib_context_free(c, values);

        fprintf(out_, "  call codes_set(ibufr,'%s->%s' &\n,rvalues)\n", prefix, a->name_);
    }
    else {
        sval = dval_to_string(c, value);
        fprintf(out_, "  call codes_set(ibufr,'%s->%s' &\n,%s)\n", prefix, a->name_, sval);
        grib_context_free(c, sval);
    }

    if (isLeaf_ == 0) {
        char* prefix1;

        prefix1 = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + strlen(prefix) + 5));
        snprintf(prefix1, 1024, "%s->%s", prefix, a->name_);

        dump_attributes(a, prefix1);

        grib_context_free(c, prefix1);
        depth_ -= 2;
    }

    (void)err; /* TODO */
}

static int is_hidden(grib_accessor* a)
{
    return ((a->flags_ & GRIB_ACCESSOR_FLAG_HIDDEN) != 0);
}

void BufrEncodeFortran::dump_long(grib_accessor* a, const char* comment)
{
    long value  = 0;
    size_t size = 0, size2 = 0;
    long* values = NULL;
    int err      = 0;
    int i, r, icount;
    int cols                        = 4;
    long count                      = 0;
    char* sval                      = NULL;
    grib_context* c                 = a->context_;
    grib_handle* h                  = grib_handle_of_accessor(a);
    int doing_unexpandedDescriptors = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0) { /* key does not have the dump attribute */
        int skip = 1;
        /* See ECC-1107 */
        if (!is_hidden(a) && strcmp(a->name_, "messageLength") == 0) skip = 0;
        if (skip) return;
    }

    doing_unexpandedDescriptors = (strcmp(a->name_, "unexpandedDescriptors") == 0);
    a->value_count(&count);
    size = size2 = count;

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

    if (size > 1) {
        values = (long*)grib_context_malloc_clear(a->context_, sizeof(long) * size);
        err    = a->unpack_long(values, &size2);
    }
    else {
        err = a->unpack_long(&value, &size2);
    }
    ECCODES_ASSERT(size2 == size);

    empty_ = 0;

    if (size > 1) {
        fprintf(out_, "  if(allocated(ivalues)) deallocate(ivalues)\n");
        fprintf(out_, "  allocate(ivalues(%lu))\n", (unsigned long)size);

        fprintf(out_, "  ivalues=(/");
        icount = 0;
        for (i = 0; i < size - 1; i++) {
            if (icount > cols || i == 0) {
                fprintf(out_, "  &\n      ");
                icount = 0;
            }
            fprintf(out_, "%ld, ", values[i]);
            icount++;
        }
        if (icount > cols || i == 0) {
            fprintf(out_, "  &\n      ");
        }
        fprintf(out_, "%ld ", values[size - 1]);

        depth_ -= 2;
        fprintf(out_, "/)\n");
        grib_context_free(a->context_, values);

        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0) {
            fprintf(out_, "  call codes_set(ibufr,'#%d#%s',ivalues)\n", r, a->name_);
        }
        else {
            if (doing_unexpandedDescriptors) {
                fprintf(out_, "\n  ! Create the structure of the data section\n");
                /* fprintf(out_,"  call codes_set(ibufr,'skipExtraKeyAttributes',1)\n"); */
            }
            fprintf(out_, "  call codes_set(ibufr,'%s',ivalues)\n", a->name_);
            if (doing_unexpandedDescriptors)
                fprintf(out_, "\n");
        }
    }
    else {
        r    = compute_bufr_key_rank(h, keys_, a->name_);
        sval = lval_to_string(c, value);
        if (r != 0) {
            fprintf(out_, "  call codes_set(ibufr,'#%d#%s',", r, a->name_);
        }
        else {
            if (doing_unexpandedDescriptors) {
                fprintf(out_, "\n  ! Create the structure of the data section\n");
                /* fprintf(out_,"  call codes_set(ibufr,'skipExtraKeyAttributes',1)\n"); */
            }
            fprintf(out_, "  call codes_set(ibufr,'%s',", a->name_);
        }

        fprintf(out_, "%s)\n", sval);
        grib_context_free(c, sval);
        if (doing_unexpandedDescriptors)
            fprintf(out_, "\n");
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

void BufrEncodeFortran::dump_long_attribute(grib_accessor* a, const char* prefix)
{
    long value  = 0;
    size_t size = 0, size2 = 0;
    long* values = NULL;
    int err      = 0;
    int i, icount;
    int cols        = 4;
    long count      = 0;
    char* pref      = NULL;
    grib_context* c = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = size2 = count;

    if (size > 1) {
        values = (long*)grib_context_malloc_clear(a->context_, sizeof(long) * size);
        err    = a->unpack_long(values, &size2);
    }
    else {
        err = a->unpack_long(&value, &size2);
    }
    ECCODES_ASSERT(size2 == size);

    empty_ = 0;

    /* Fortran standard specifies the maximum length of a free-form source line is 132 characters */
    /* Break long prefix string into multiple lines to avoid compiler error */
    pref = break_line(c, prefix);

    if (size > 1) {
        fprintf(out_, "  if(allocated(ivalues)) deallocate(ivalues)\n");
        fprintf(out_, "  allocate(ivalues(%lu))\n", (unsigned long)size);

        fprintf(out_, "  ivalues=(/");
        icount = 0;
        for (i = 0; i < size - 1; i++) {
            if (icount > cols || i == 0) {
                fprintf(out_, "  &\n      ");
                icount = 0;
            }
            fprintf(out_, "%ld, ", values[i]);
            icount++;
        }
        if (icount > cols || i == 0) {
            fprintf(out_, "  &\n      ");
        }
        fprintf(out_, "%ld ", values[size - 1]);

        depth_ -= 2;
        fprintf(out_, "/)\n");
        grib_context_free(a->context_, values);

        fprintf(out_, "  call codes_set(ibufr,'%s->%s' &\n,ivalues)\n", pref, a->name_);
    }
    else {
        if (!codes_bufr_key_exclude_from_dump(prefix)) {
            char* sval = lval_to_string(c, value);
            fprintf(out_, "  call codes_set(ibufr,'%s->%s'&\n,", pref, a->name_);
            fprintf(out_, "%s)\n", sval);
            grib_context_free(c, sval);
        }
    }

    if (isLeaf_ == 0) {
        char* prefix1;

        prefix1 = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(a->name_) + strlen(pref) + 5));
        snprintf(prefix1, 1024, "%s->%s", pref, a->name_);

        dump_attributes(a, prefix1);

        grib_context_free(c, prefix1);
        depth_ -= 2;
    }
    grib_context_free(c, pref);
    (void)err; /* TODO */
}

void BufrEncodeFortran::dump_bits(grib_accessor* a, const char* comment)
{
}

void BufrEncodeFortran::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;
    int r;
    char* sval;
    grib_handle* h  = grib_handle_of_accessor(a);
    grib_context* c = h->context;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->unpack_double(&value, &size);
    empty_ = 0;

    r = compute_bufr_key_rank(h, keys_, a->name_);

    sval = dval_to_string(c, value);
    if (r != 0)
        fprintf(out_, "  call codes_set(ibufr,'#%d#%s',%s)\n", r, a->name_, sval);
    else
        fprintf(out_, "  call codes_set(ibufr,'%s',%s)\n", a->name_, sval);
    grib_context_free(c, sval);

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

void BufrEncodeFortran::dump_string_array(grib_accessor* a, const char* comment)
{
    char** values;
    size_t size = 0, i = 0;
    grib_context* c = a->context_;
    int err         = 0;
    long count      = 0;
    int r           = 0;
    grib_handle* h  = grib_handle_of_accessor(a);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    a->value_count(&count);
    size = count;
    if (size == 1) {
        dump_string(a, comment);
        return;
    }

    fprintf(out_, "  if(allocated(svalues)) deallocate(svalues)\n");
    fprintf(out_, "  allocate(svalues(%lu))\n", (unsigned long)size);

    fprintf(out_, "  svalues=(/");

    empty_ = 0;

    values = (char**)grib_context_malloc_clear(c, size * sizeof(char*));
    if (!values) {
        grib_context_log(c, GRIB_LOG_ERROR, "unable to allocate %zu bytes", size);
        return;
    }

    err = a->unpack_string_array(values, &size);

    for (i = 0; i < size - 1; i++) {
        fprintf(out_, "    \"%s\", &\n", values[i]);
    }
    fprintf(out_, "    \"%s\" /)\n", values[size - 1]);

    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if ((r = compute_bufr_key_rank(h, keys_, a->name_)) != 0)
            fprintf(out_, "  call codes_set_string_array(ibufr,'#%d#%s',svalues)\n", r, a->name_);
        else
            fprintf(out_, "  call codes_set_string_array(ibufr,'%s',svalues)\n", a->name_);

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
    for (i = 0; i < size; ++i)
        grib_context_free(c, values[i]);
    grib_context_free(c, values);
    (void)err; /* TODO */
}

void BufrEncodeFortran::dump_string(grib_accessor* a, const char* comment)
{
    char* value     = NULL;
    char* p         = NULL;
    size_t size     = 0;
    grib_context* c = a->context_;
    int r;
    int err              = 0;
    grib_handle* h       = grib_handle_of_accessor(a);
    const char* acc_name = a->name_;

    grib_get_string_length_acc(a, &size);
    if (size == 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0 || (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        return;

    value = (char*)grib_context_malloc_clear(c, size);
    if (!value) {
        grib_context_log(c, GRIB_LOG_ERROR, "unable to allocate %zu bytes", size);
        return;
    }

    empty_ = 0;

    err = a->unpack_string(value, &size);
    p   = value;
    r   = compute_bufr_key_rank(h, keys_, acc_name);
    if (grib_is_missing_string(a, (unsigned char*)value, size)) {
        strcpy(value, ""); /* Empty string means MISSING string */
    }

    while (*p) {
        if (!isprint(*p))
            *p = '?';
        p++;
    }

    if (isLeaf_ == 0) {
        depth_ += 2;
        if (r != 0)
            fprintf(out_, "  call codes_set(ibufr,'#%d#%s',", r, acc_name);
        else
            fprintf(out_, "  call codes_set(ibufr,'%s',", acc_name);
    }
    fprintf(out_, "\'%s\')\n", value);


    if (isLeaf_ == 0) {
        char* prefix;
        int dofree = 0;

        if (r != 0) {
            prefix = (char*)grib_context_malloc_clear(c, sizeof(char) * (strlen(acc_name) + 10));
            dofree = 1;
            snprintf(prefix, 1024, "#%d#%s", r, acc_name);
        }
        else
            prefix = (char*)acc_name;

        dump_attributes(a, prefix);
        if (dofree)
            grib_context_free(c, prefix);
        depth_ -= 2;
    }

    grib_context_free(c, value);
    (void)err; /* TODO */
}

void BufrEncodeFortran::dump_bytes(grib_accessor* a, const char* comment)
{
}

void BufrEncodeFortran::dump_label(grib_accessor* a, const char* comment)
{
}

static void _dump_long_array(grib_handle* h, FILE* f, const char* key, const char* print_key)
{
    long* val;
    size_t size = 0, i;
    int cols = 9, icount = 0;

    if (grib_get_size(h, key, &size) == GRIB_NOT_FOUND)
        return;
    if (size == 0)
        return;

    fprintf(f, "  if(allocated(ivalues)) deallocate(ivalues)\n");
    fprintf(f, "  allocate(ivalues(%lu))\n", (unsigned long)size);

    fprintf(f, "  ivalues=(/ ");

    val = (long*)grib_context_malloc_clear(h->context, sizeof(long) * size);
    grib_get_long_array(h, key, val, &size);
    for (i = 0; i < size - 1; i++) {
        if (icount > cols || i == 0) {
            fprintf(f, "  &\n      ");
            icount = 0;
        }
        fprintf(f, "%ld, ", val[i]);
        icount++;
    }
    if (icount > cols) {
        fprintf(f, "  &\n      ");
    }
    fprintf(f, "%ld /)\n", val[size - 1]);

    grib_context_free(h->context, val);
    fprintf(f, "  call codes_set(ibufr,'%s',ivalues)\n", print_key);
}

void BufrEncodeFortran::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    if (strcmp(a->name_, "BUFR") == 0 ||
        strcmp(a->name_, "GRIB") == 0 ||
        strcmp(a->name_, "META") == 0) {
        grib_handle* h = grib_handle_of_accessor(a);
        depth_         = 2;
        empty_         = 1;
        depth_ += 2;
        _dump_long_array(h, out_, "dataPresentIndicator", "inputDataPresentIndicator");
        _dump_long_array(h, out_, "delayedDescriptorReplicationFactor", "inputDelayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "shortDelayedDescriptorReplicationFactor", "inputShortDelayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "extendedDelayedDescriptorReplicationFactor", "inputExtendedDelayedDescriptorReplicationFactor");
        _dump_long_array(h, out_, "inputOverriddenReferenceValues", "inputOverriddenReferenceValues");
        grib_dump_accessors_block(this, block);
        depth_ -= 2;
    }
    else if (strcmp(a->name_, "groupNumber") == 0) {
        if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
            return;
        empty_ = 1;
        depth_ += 2;
        grib_dump_accessors_block(this, block);
        depth_ -= 2;
    }
    else {
        grib_dump_accessors_block(this, block);
    }
}

void BufrEncodeFortran::dump_attributes(grib_accessor* a, const char* prefix)
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

void BufrEncodeFortran::header(const grib_handle* h) const
{
    char sampleName[200] = { 0 };
    long localSectionPresent, edition, bufrHeaderCentre, isSatellite;

    grib_get_long(h, "localSectionPresent", &localSectionPresent);
    grib_get_long(h, "bufrHeaderCentre", &bufrHeaderCentre);
    grib_get_long(h, "edition", &edition);

    if (localSectionPresent && bufrHeaderCentre == 98) {
        grib_get_long(h, "isSatellite", &isSatellite);
        if (isSatellite)
            snprintf(sampleName, sizeof(sampleName), "BUFR%ld_local_satellite", edition);
        else
            snprintf(sampleName, sizeof(sampleName), "BUFR%ld_local", edition);
    }
    else {
        snprintf(sampleName, sizeof(sampleName), "BUFR%ld", edition);
    }

    if (count_ < 2) {
        fprintf(out_, "!  This program was automatically generated with bufr_dump -Efortran\n");
        fprintf(out_, "!  Using ecCodes version: ");
        grib_print_api_version(out_);
        fprintf(out_, "\n\n");
        fprintf(out_, "program bufr_encode\n");
        fprintf(out_, "  use eccodes\n");
        fprintf(out_, "  implicit none\n");
        fprintf(out_, "  integer                                       :: iret\n");
        fprintf(out_, "  integer                                       :: outfile\n");
        fprintf(out_, "  integer                                       :: ibufr\n");
        fprintf(out_, "  integer(kind=4), dimension(:), allocatable    :: ivalues\n");
        fprintf(out_, "  integer, parameter  :: max_strsize = 100\n");
        fprintf(out_, "  character(len=max_strsize) , dimension(:),allocatable   :: svalues\n");
        fprintf(out_, "  real(kind=8), dimension(:), allocatable       :: rvalues\n");
    }
    fprintf(out_, "  call codes_bufr_new_from_samples(ibufr,'%s',iret)\n", sampleName);
    fprintf(out_, "  if (iret/=CODES_SUCCESS) then\n");
    fprintf(out_, "    print *,'ERROR: Failed to create BUFR from %s'\n", sampleName);
    fprintf(out_, "    stop 1\n");
    fprintf(out_, "  endif\n");
}

void BufrEncodeFortran::footer(const grib_handle* h) const
{
    fprintf(out_, "\n  ! Encode the keys back in the data section\n");
    fprintf(out_, "  call codes_set(ibufr,'pack',1)\n\n");
    if (count_ == 1)
        fprintf(out_, "  call codes_open_file(outfile,'outfile.bufr','w')\n");
    else
        fprintf(out_, "  call codes_open_file(outfile,'outfile.bufr','a')\n");

    fprintf(out_, "  call codes_write(ibufr,outfile)\n");
    fprintf(out_, "  call codes_close_file(outfile)\n");
    fprintf(out_, "  call codes_release(ibufr)\n");
    if (count_ == 1)
        fprintf(out_, "  print *, \"Created output BUFR file 'outfile.bufr'\"\n");
    fprintf(out_, "  if(allocated(ivalues)) deallocate(ivalues)\n");
    fprintf(out_, "  if(allocated(rvalues)) deallocate(rvalues)\n");
    fprintf(out_, "  if(allocated(svalues)) deallocate(svalues)\n");
}

}  // namespace eccodes::dumper

