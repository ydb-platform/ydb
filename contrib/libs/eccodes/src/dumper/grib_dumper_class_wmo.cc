/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_wmo.h"
#include "grib_dumper_factory.h"
#include <cctype>


eccodes::dumper::Wmo _grib_dumper_wmo;
eccodes::Dumper* grib_dumper_wmo = &_grib_dumper_wmo;

namespace eccodes::dumper
{

static void print_hexadecimal(FILE* out, unsigned long flags, grib_accessor* a);

int Wmo::init()
{
    section_offset_ = 0;

    return GRIB_SUCCESS;
}

int Wmo::destroy()
{
    return GRIB_SUCCESS;
}

static void print_offset(FILE* out, long begin, long theEnd, int width = 10)
{
    char tmp[50];

    if (begin == theEnd)
        fprintf(out, "%-*ld", width, begin);
    else {
        snprintf(tmp, sizeof(tmp), "%ld-%ld", begin, theEnd);
        fprintf(out, "%-*s", width, tmp);
    }
}

void Wmo::aliases(grib_accessor* a)
{
    int i;

    if ((option_flags_ & GRIB_DUMP_FLAG_ALIASES) == 0)
        return;

    if (a->all_names_[1]) {
        const char* sep = "";
        fprintf(out_, " [");

        for (i = 1; i < MAX_ACCESSOR_NAMES; i++) {
            if (a->all_names_[i]) {
                if (a->all_name_spaces_[i])
                    fprintf(out_, "%s%s.%s", sep, a->all_name_spaces_[i], a->all_names_[i]);
                else
                    fprintf(out_, "%s%s", sep, a->all_names_[i]);
            }
            sep = ", ";
        }
        fprintf(out_, "]");
    }
}

void Wmo::dump_long(grib_accessor* a, const char* comment)
{
    long value   = 0;
    size_t size  = 0;
    long* values = NULL;
    int err = 0, i = 0;
    long count = 0;

    if (a->length_ == 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    a->value_count(&count);
    size = count;

    if (size > 1) {
        values = (long*)grib_context_malloc_clear(a->context_, sizeof(long) * size);
        err    = a->unpack_long(values, &size);
    }
    else {
        err = a->unpack_long(&value, &size);
    }

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0)
        return;

    set_begin_end(a);

    print_offset(out_, begin_, theEnd_);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, "%s (int) ", a->creator_->op_);

    if (size > 1) {
        int cols   = 19;
        int icount = 0;
        fprintf(out_, "%s = { \t", a->name_);
        if (values) {
            for (i = 0; i < size; i++) {
                if (icount > cols) {
                    fprintf(out_, "\n\t\t\t\t");
                    icount = 0;
                }
                fprintf(out_, "%ld ", values[i]);
                icount++;
            }
            fprintf(out_, "}\n");
            grib_context_free(a->context_, values);
        }
    }
    else {
        if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && a->is_missing_internal())
            fprintf(out_, "%s = MISSING", a->name_);
        else
            fprintf(out_, "%s = %ld", a->name_, value);

        print_hexadecimal(out_, option_flags_, a);

        if (comment)
            fprintf(out_, " [%s]", comment);
    }
    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_wmo::dump_long]", err, grib_get_error_message(err));

    aliases(a);


    fprintf(out_, "\n");
}

static int test_bit(long a, long b)
{
    return a & (1 << b);
}

void Wmo::dump_bits(grib_accessor* a, const char* comment)
{
    long value  = 0;
    size_t size = 1;
    int err     = 0;

    if (a->length_ == 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    err = a->unpack_long(&value, &size);
    set_begin_end(a);

    // for(i = 0; i < depth_ ; i++) fprintf(out_," ");
    print_offset(out_, begin_, theEnd_);
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, "%s (int) ", a->creator_->op_);

    fprintf(out_, "%s = %ld [", a->name_, value);

    for (long i = 0; i < (a->length_ * 8); i++) {
        if (test_bit(value, a->length_ * 8 - i - 1))
            fprintf(out_, "1");
        else
            fprintf(out_, "0");
    }

    if (comment) {
        // ECC-1186: Whole comment is too big, so pick the part that follows the ':' i.e. flag table file
        const char* p = strchr(comment, ':');
        if (p)
            fprintf(out_, " (%s) ]", p + 1);
        else
            fprintf(out_, "]");
    }
    else {
        fprintf(out_, "]");
    }

    if (err == 0)
        print_hexadecimal(out_, option_flags_, a);

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_wmo::dump_bits]", err, grib_get_error_message(err));

    aliases(a);
    fprintf(out_, "\n");
}

void Wmo::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;
    int err      = 0;

    if (a->length_ == 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    err = a->unpack_double(&value, &size);
    set_begin_end(a);

    // for(i = 0; i < depth_ ; i++) fprintf(out_," ");

    print_offset(out_, begin_, theEnd_);
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, "%s (double) ", a->creator_->op_);

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && a->is_missing_internal())
        fprintf(out_, "%s = MISSING", a->name_);
    else
        fprintf(out_, "%s = %g", a->name_, value);
    // if(comment) fprintf(out_," [%s]",comment);

    if (err == 0)
        print_hexadecimal(out_, option_flags_, a);

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_wmo::dump_double]", err, grib_get_error_message(err));
    aliases(a);
    fprintf(out_, "\n");
}

void Wmo::dump_string(grib_accessor* a, const char* comment)
{
    size_t size = 0;
    char* value = NULL;
    char* p     = NULL;
    int err     = 0;

    if (a->length_ == 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0) {
        return;
    }

    grib_get_string_length_acc(a, &size);
    value = (char*)grib_context_malloc_clear(a->context_, size);
    if (!value) {
        grib_context_log(a->context_, GRIB_LOG_ERROR, "unable to allocate %zu bytes", size);
        return;
    }
    err = a->unpack_string(value, &size);
    p   = value;

    set_begin_end(a);

    while (*p) {
        if (!isprint(*p))
            *p = '.';
        p++;
    }

    // for(i = 0; i < depth_ ; i++) fprintf(out_," ");
    print_offset(out_, begin_, theEnd_);
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, "%s (str) ", a->creator_->op_);

    fprintf(out_, "%s = %s", a->name_, value);

    if (err == 0)
        print_hexadecimal(out_, option_flags_, a);

    // if(comment) fprintf(out_," [%s]",comment);
    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_wmo::dump_string]", err, grib_get_error_message(err));
    aliases(a);
    fprintf(out_, "\n");
    grib_context_free(a->context_, value);
}

void Wmo::dump_bytes(grib_accessor* a, const char* comment)
{
    int i, k, err = 0;
    size_t more        = 0;
    size_t size        = a->length_;
    unsigned char* buf = (unsigned char*)grib_context_malloc(context_, size);

    if (a->length_ == 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    set_begin_end(a);

    // for(i = 0; i < depth_ ; i++) fprintf(out_," ");
    print_offset(out_, begin_, theEnd_);
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, "%s ", a->creator_->op_);

    fprintf(out_, "%s = %ld", a->name_, a->length_);
    aliases(a);
    fprintf(out_, " {");

    if (!buf) {
        if (size == 0)
            fprintf(out_, "}\n");
        else
            fprintf(out_, " *** ERR cannot malloc(%zu) }\n", size);
        return;
    }

    print_hexadecimal(out_, option_flags_, a);

    fprintf(out_, "\n");

    err = a->unpack_bytes(buf, &size);
    if (err) {
        grib_context_free(context_, buf);
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_wmo::dump_bytes]\n}", err, grib_get_error_message(err));
        return;
    }

    if (size > 100) {
        more = size - 100;
        size = 100;
    }

    k = 0;
    // if(size > 100) size = 100;
    while (k < size) {
        int j;
        for (i = 0; i < depth_ + 3; i++)
            fprintf(out_, " ");
        for (j = 0; j < 16 && k < size; j++, k++) {
            fprintf(out_, "%02x", buf[k]);
            if (k != size - 1)
                fprintf(out_, ", ");
        }
        fprintf(out_, "\n");
    }

    if (more) {
        for (i = 0; i < depth_ + 3; i++)
            fprintf(out_, " ");
        fprintf(out_, "... %lu more values\n", (unsigned long)more);
    }

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "} # %s %s \n", a->creator_->op_, a->name_);
    grib_context_free(context_, buf);
}

void Wmo::dump_values(grib_accessor* a)
{
    int k, err = 0;
    size_t more = 0;
    double* buf = NULL;
    size_t size = 0;
    long count  = 0;
    int is_char = 0;

    if (a->length_ == 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    a->value_count(&count);
    size = count;

    if (size == 1) {
        dump_double(a, NULL);
        return;
    }
    buf = (double*)grib_context_malloc(context_, size * sizeof(double));

    set_begin_end(a);

    // For the DIAG pseudo GRIBs. Key charValues uses 1-byte integers to represent a character
    if (a->flags_ & GRIB_ACCESSOR_FLAG_STRING_TYPE) {
        is_char = 1;
    }

    print_offset(out_, begin_, theEnd_, 12);  // ECC-1749
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        char type_name[32]     = "";
        const long native_type = a->get_native_type();
        if (native_type == GRIB_TYPE_LONG)
            strcpy(type_name, "(int)");
        else if (native_type == GRIB_TYPE_DOUBLE)
            strcpy(type_name, "(double)");
        else if (native_type == GRIB_TYPE_STRING)
            strcpy(type_name, "(str)");
        fprintf(out_, "%s %s ", a->creator_->op_, type_name);
    }

    fprintf(out_, "%s = (%ld,%ld)", a->name_, (long)size, a->length_);
    aliases(a);
    fprintf(out_, " {");

    if (!buf) {
        if (size == 0)
            fprintf(out_, "}\n");
        else
            fprintf(out_, " *** ERR cannot malloc(%zu) }\n", size);
        return;
    }

    fprintf(out_, "\n");

    err = a->unpack_double(buf, &size);

    if (err) {
        grib_context_free(context_, buf);
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_wmo::dump_values]\n}", err, grib_get_error_message(err));
        return;
    }

    if (size > 100) {
        more = size - 100;
        size = 100;
    }

    k = 0;
    while (k < size) {
        int j;
        for (j = 0; j < 8 && k < size; j++, k++) {
            if (is_char)
                fprintf(out_, "'%c'", (char)buf[k]);
            else
                fprintf(out_, "%.10e", buf[k]);
            if (k != size - 1)
                fprintf(out_, ", ");
        }
        fprintf(out_, "\n");
        // if (is_char)
        //     fprintf(out_, "%d '%c'\n", k, (char)buf[k]);
        // else
        //     fprintf(out_, "%d %g\n", k, buf[k]);
    }
    if (more) {
        // for(i = 0; i < depth_ + 3 ; i++) fprintf(out_," ");
        fprintf(out_, "... %lu more values\n", (unsigned long)more);
    }

    fprintf(out_, "} # %s %s \n", a->creator_->op_, a->name_);
    grib_context_free(context_, buf);
}

void Wmo::dump_label(grib_accessor* a, const char* comment)
{
    //  for(i = 0; i < depth_ ; i++) fprintf(out_," ");
    //  fprintf(out_,"----> %s %s %s\n",a->creator_->op, a->name_,comment?comment:"");
}

void Wmo::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    grib_section* s    = a->sub_section_;
    int is_wmo_section = 0;
    char* upper        = NULL;
    char tmp[512];
    char *p = NULL, *q = NULL;
    if (!strncmp(a->name_, "section", 7))
        is_wmo_section = 1;

    if (is_wmo_section) {
        upper = (char*)malloc(strlen(a->name_) + 1);
        ECCODES_ASSERT(upper);
        p = (char*)a->name_;
        q = upper;
        while (*p != '\0') {
            *q = toupper(*p);
            q++;
            p++;
        }
        *q = '\0';
        snprintf(tmp, sizeof(tmp), "%s ( length=%ld, padding=%ld )", upper, (long)s->length, (long)s->padding);
        fprintf(out_, "======================   %-35s   ======================\n", tmp);
        free(upper);
        section_offset_ = a->offset_;
    }
    else {
    }

    // printf("------------- section_offset = %ld\n",section_offset_);
    depth_ += 3;
    grib_dump_accessors_block(this, block);
    depth_ -= 3;

    // for(i = 0; i < depth_ ; i++) fprintf(out_," ");
    // fprintf(out_,"<===== %s %s\n",a->creator_->op, a->name_);
}

void Wmo::set_begin_end(grib_accessor* a)
{
    if ((option_flags_ & GRIB_DUMP_FLAG_OCTET) != 0) {
        begin_  = a->offset_ - section_offset_ + 1;
        theEnd_ = a->get_next_position_offset() - section_offset_;
    }
    else {
        begin_  = a->offset_;
        theEnd_ = a->get_next_position_offset();
    }
}

static void print_hexadecimal(FILE* out, unsigned long flags, grib_accessor* a)
{
    int i                = 0;
    unsigned long offset = 0;
    grib_handle* h       = grib_handle_of_accessor(a);
    if ((flags & GRIB_DUMP_FLAG_HEXADECIMAL) != 0 && a->length_ != 0) {
        fprintf(out, " (");
        offset = a->offset_;
        for (i = 0; i < a->length_; i++) {
            fprintf(out, " 0x%.2X", h->buffer->data[offset]);
            offset++;
        }
        fprintf(out, " )");
    }
}

void Wmo::dump_string_array(grib_accessor* a, const char* comment)
{
    char** values;
    size_t size = 0, i = 0;
    grib_context* c = NULL;
    int err         = 0;
    int tab         = 0;
    long count      = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    c = a->context_;
    a->value_count(&count);
    if (count == 0)
        return;
    size = count;
    if (size == 1) {
        dump_string(a, comment);
        return;
    }

    values = (char**)grib_context_malloc_clear(c, size * sizeof(char*));
    if (!values) {
        grib_context_log(c, GRIB_LOG_ERROR, "unable to allocate %zu bytes", size);
        return;
    }

    err = a->unpack_string_array(values, &size);

    // print_offset(out_,d,a);
    print_offset(out_, begin_, theEnd_);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(out_, "  ");
        fprintf(out_, "# type %s (str) \n", a->creator_->op_);
    }

    aliases(a);
    if (comment) {
        fprintf(out_, "  ");
        fprintf(out_, "# %s \n", comment);
    }
    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
        fprintf(out_, "  ");
        fprintf(out_, "#-READ ONLY- ");
        tab = 13;
    }
    else
        fprintf(out_, "  ");

    tab++;
    fprintf(out_, "%s = {\n", a->name_);
    for (i = 0; i < size; i++) {
        fprintf(out_, "%-*s\"%s\",\n", (int)(tab + strlen(a->name_) + 4), " ", values[i]);
    }
    fprintf(out_, "  }");

    if (err) {
        fprintf(out_, "  ");
        fprintf(out_, "# *** ERR=%d (%s)", err, grib_get_error_message(err));
    }

    fprintf(out_, "\n");
    for (i = 0; i < size; ++i)
        grib_context_free(c, values[i]);
    grib_context_free(c, values);
}

}  // namespace eccodes::dumper
