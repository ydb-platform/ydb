/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_default.h"
#include "grib_dumper_factory.h"
#include <cctype>

eccodes::dumper::Default _grib_dumper_default;
eccodes::Dumper* grib_dumper_default = &_grib_dumper_default;

namespace eccodes::dumper
{

int Default::init()
{
    section_offset_ = 0;

    return GRIB_SUCCESS;
}

int Default::destroy()
{
    return GRIB_SUCCESS;
}

void Default::aliases(grib_accessor* a)
{
    int i;

    if ((option_flags_ & GRIB_DUMP_FLAG_ALIASES) == 0)
        return;

    if (a->all_names_[1]) {
        const char* sep = "";
        fprintf(out_, "  ");
        fprintf(out_, "# ALIASES: ");

        for (i = 1; i < MAX_ACCESSOR_NAMES; i++) {
            if (a->all_names_[i]) {
                if (a->all_name_spaces_[i])
                    fprintf(out_, "%s%s.%s", sep, a->all_name_spaces_[i], a->all_names_[i]);
                else
                    fprintf(out_, "%s%s", sep, a->all_names_[i]);
            }
            sep = ", ";
        }
        fprintf(out_, "\n");
    }
}

void Default::dump_long(grib_accessor* a, const char* comment)
{
    long value  = 0;
    size_t size = 1, size2 = 0;
    long* values = NULL;
    int err      = 0;
    long count = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    a->value_count(&count);
    size = size2 = count;

    print_offset(out_, a);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(out_, "  ");
        fprintf(out_, "# type %s (int)\n", a->creator_->op_);
    }

    if (size > 1) {
        values = (long*)grib_context_malloc_clear(a->context_, sizeof(long) * size);
        err = a->unpack_long(values, &size2);
    }
    else {
        err = a->unpack_long(&value, &size2);
    }
    ECCODES_ASSERT(size2 == size);

    aliases(a);
    if (comment) {
        fprintf(out_, "  ");
        fprintf(out_, "# %s \n", comment);
    }

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
        fprintf(out_, "  ");
        fprintf(out_, "#-READ ONLY- ");
    }
    else
        fprintf(out_, "  ");

    if (size > 1) {
        int cols   = 19;
        int icount = 0;
        fprintf(out_, "%s = { \t", a->name_);
        for (int i = 0; i < size; i++) {
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
    else {
        if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && a->is_missing_internal())
            fprintf(out_, "%s = MISSING;", a->name_);
        else
            fprintf(out_, "%s = %ld;", a->name_, value);
    }

    if (err) {
        fprintf(out_, "  ");
        fprintf(out_, "# *** ERR=%d (%s) [grib_dumper_default::dump_long]", err, grib_get_error_message(err));
    }

    fprintf(out_, "\n");
}

static int test_bit(long a, long b)
{
    return a & (1 << b);
}

void Default::dump_bits(grib_accessor* a, const char* comment)
{
    long lvalue = 0;
    size_t size = 1;
    int err     = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    err = a->unpack_long(&lvalue, &size);

    print_offset(out_, a);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(out_, "  ");
        fprintf(out_, "# type %s \n", a->creator_->op_);
    }

    aliases(a);
    if (comment) {
        fprintf(out_, "  ");
        fprintf(out_, "# %s \n", comment);
    }

    fprintf(out_, "  ");
    fprintf(out_, "# flags: ");
    for (long i = 0; i < (a->length_ * 8); i++) {
        if (test_bit(lvalue, a->length_ * 8 - i - 1))
            fprintf(out_, "1");
        else
            fprintf(out_, "0");
    }
    fprintf(out_, "\n");

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
        fprintf(out_, "  ");
        fprintf(out_, "#-READ ONLY- ");
    }
    else {
        fprintf(out_, "  ");
    }

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && a->is_missing_internal())
        fprintf(out_, "%s = MISSING;", a->name_);
    else {
        fprintf(out_, "%s = %ld;", a->name_, lvalue);
    }

    if (err) {
        fprintf(out_, "  ");
        fprintf(out_, "# *** ERR=%d (%s) [grib_dumper_default::dump_bits]", err, grib_get_error_message(err));
    }

    fprintf(out_, "\n");
}

void Default::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;
    int err      = a->unpack_double(&value, &size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    print_offset(out_, a);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(out_, "  ");
        fprintf(out_, "# type %s (double)\n", a->creator_->op_);
    }

    aliases(a);
    if (comment) {
        fprintf(out_, "  ");
        fprintf(out_, "# %s \n", comment);
    }

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
        fprintf(out_, "  ");
        fprintf(out_, "#-READ ONLY- ");
    }
    else
        fprintf(out_, "  ");

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && a->is_missing_internal())
        fprintf(out_, "%s = MISSING;", a->name_);
    else
        fprintf(out_, "%s = %g;", a->name_, value);

    if (err) {
        fprintf(out_, "  ");
        fprintf(out_, "# *** ERR=%d (%s) [grib_dumper_default::dump_double]", err, grib_get_error_message(err));
    }

    fprintf(out_, "\n");
}

void Default::dump_string_array(grib_accessor* a, const char* comment)
{
    char** values;
    size_t size = 0, i = 0;
    grib_context* c = a->context_;
    int err         = 0;
    int tab         = 0;
    long count      = 0;

    a->value_count(&count);
    size = count;
    if (size == 1) {
        dump_string(a, comment);
        return;
    }

    values = (char**)grib_context_malloc_clear(c, size * sizeof(char*));
    if (!values) {
        grib_context_log(c, GRIB_LOG_ERROR, "Memory allocation error: %zu bytes", size);
        return;
    }

    err = a->unpack_string_array(values, &size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    print_offset(out_, a);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(out_, "  ");
        fprintf(out_, "# type %s (str)\n", a->creator_->op_);
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
    else {
        fprintf(out_, "  ");
    }

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
    grib_context_free(c, values);
}

void Default::dump_string(grib_accessor* a, const char* comment)
{
    char* value     = NULL;
    char* p         = NULL;
    size_t size     = 0;
    grib_context* c = a->context_;
    int err         = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0) {
        return;
    }

    grib_get_string_length_acc(a, &size);
    if (size == 0)
        return;

    value = (char*)grib_context_malloc_clear(c, size);
    if (!value) {
        grib_context_log(c, GRIB_LOG_ERROR, "Memory allocation error: %zu bytes", size);
        return;
    }

    err = a->unpack_string(value, &size);
    p   = value;

    while (*p) {
        if (!isprint(*p))
            *p = '.';
        p++;
    }

    print_offset(out_, a);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        fprintf(out_, "  ");
        fprintf(out_, "# type %s (str)\n", a->creator_->op_);
    }

    aliases(a);
    if (comment) {
        fprintf(out_, "  ");
        fprintf(out_, "# %s \n", comment);
    }

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
        fprintf(out_, "  ");
        fprintf(out_, "#-READ ONLY- ");
    }
    else
        fprintf(out_, "  ");

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) &&
        a->is_missing_internal())
        fprintf(out_, "%s = MISSING;", a->name_);
    else
        fprintf(out_, "%s = %s;", a->name_, value);

    if (err) {
        fprintf(out_, "  ");
        fprintf(out_, "# *** ERR=%d (%s) [grib_dumper_default::dump_string]", err, grib_get_error_message(err));
    }

    fprintf(out_, "\n");
    grib_context_free(c, value);
}

void Default::dump_bytes(grib_accessor* a, const char* comment)
{
//     int i,k,err =0;
//     size_t more = 0;
//     size_t size = a->length_;
//     unsigned char* buf = grib_context_malloc(context_,size);

//     if ( (a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
//         return;

//     if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
//         fprintf(out_,"-READ ONLY- ");

//     /*for(i = 0; i < depth_ ; i++) fprintf(out_," ");*/
//     /*print_offset(out_, theEnd_);*/
//     if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
//         fprintf(out_,"%s ",a->creator_->op);

//     fprintf(out_,"%s = %ld",a->name_,a->length_);
//     aliases(d,a);
//     fprintf(out_," {");

//     if(!buf)
//     {
//         if(size == 0)
//             fprintf(out_,"}\n");
//         else
//             fprintf(out_," *** ERR cannot malloc(%ld) }\n",(long)size);
//         return;
//     }

//     fprintf(out_,"\n");

//     err = a->unpack_bytes(buf,&size);
//     if(err){
//         grib_context_free(context_,buf);
//         fprintf(out_," *** ERR=%d (%s)  [grib_dumper_default::dump_bytes]\n}",err,grib_get_error_message(err));
//         return ;
//     }

//     if(size > 100) {
//         more = size - 100;
//         size = 100;
//     }

//     k = 0;
//     /* if(size > 100) size = 100;  */
//     while(k < size)
//     {
//         int j;
//         for(i = 0; i < depth_ + 3 ; i++) fprintf(out_," ");
//         for(j = 0; j < 16 && k < size; j++, k++)
//         {
//             fprintf(out_,"%02x",buf[k]);
//             if(k != size-1)
//                 fprintf(out_,", ");
//         }
//         fprintf(out_,"\n");
//     }

//     if(more)
//     {
//         for(i = 0; i < depth_ + 3 ; i++) fprintf(out_," ");
//         fprintf(out_,"... %lu more values\n", (unsigned long)more);
//     }

//     for(i = 0; i < depth_ ; i++) fprintf(out_," ");
//     fprintf(out_,"} # %s %s \n",a->creator_->op, a->name_);
//     grib_context_free(context_,buf);
}

void Default::dump_values(grib_accessor* a)
{
    int k, err = 0;
    size_t more = 0;
    double* buf = NULL;
    size_t size = 0;
    long count  = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    a->value_count(&count);
    size = count;
    if (size == 1) {
        dump_double(a, NULL);
        return;
    }

    buf = (double*)grib_context_malloc(context_, size * sizeof(double));

    print_offset(out_, a);

    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0) {
        char type_name[32]     = "";
        const long native_type = a->get_native_type();
        if (native_type == GRIB_TYPE_LONG)
            strcpy(type_name, "(int)");
        else if (native_type == GRIB_TYPE_DOUBLE)
            strcpy(type_name, "(double)");
        else if (native_type == GRIB_TYPE_STRING)
            strcpy(type_name, "(str)");
        fprintf(out_, "  ");
        fprintf(out_, "# type %s %s\n", a->creator_->op_, type_name);
    }

    aliases(a);

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) {
        fprintf(out_, "  ");
        fprintf(out_, "#-READ ONLY- ");
    }
    else
        fprintf(out_, "  ");

    fprintf(out_, "%s(%zu) = ", a->name_, size);
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
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_default::dump_values]\n}", err, grib_get_error_message(err));
        return;
    }

    if (!(option_flags_ & GRIB_DUMP_FLAG_ALL_DATA) && size > 100) {
        more = size - 100;
        size = 100;
    }

    k = 0;
    while (k < size) {
        int j;
        fprintf(out_, "  ");
        for (j = 0; j < 5 && k < size; j++, k++) {
            fprintf(out_, "%g", buf[k]);
            if (k != size - 1)
                fprintf(out_, ", ");
        }
        fprintf(out_, "\n");
    }
    if (more) {
        fprintf(out_, "  ");
        fprintf(out_, "... %lu more values\n", (unsigned long)more);
    }

    fprintf(out_, "  ");
    fprintf(out_, "} \n");
    grib_context_free(context_, buf);
}

void Default::dump_label(grib_accessor* a, const char* comment)
{
    /*grib_dumper_default *self = (grib_dumper_default*)d;

  for(i = 0; i < d->depth ; i++) fprintf(self->dumper.out," ");
  fprintf(self->dumper.out,"----> %s %s %s\n",a->creator_->op, a->name_,comment?comment:"");*/
}

void Default::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    int is_default_section = 0;
    char* upper            = NULL;
    char *p = NULL, *q = NULL;
    if (!strncmp(a->name_, "section", 7))
        is_default_section = 1;
    if (!strcmp(a->creator_->op_, "bufr_group")) {
        dump_long(a, NULL);
    }

    /*for(i = 0; i < depth_ ; i++) fprintf(out_," ");*/
    if (is_default_section) {
        /* char tmp[512]; */
        /* grib_section* s = a->sub_section; */
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

        /* snprintf(tmp, sizeof(tmp), "%s ( length=%ld, padding=%ld )", upper, (long)s->length, (long)s->padding); */
        /* fprintf(out_,"#==============   %-38s ==============\n",tmp); */
        free(upper);
        section_offset_ = a->offset_;
    }

    /*printf("------------- section_offset = %ld\n",section_offset_);*/
    depth_ += 3;
    grib_dump_accessors_block(this, block);
    depth_ -= 3;
    /*for(i = 0; i < depth_ ; i++) fprintf(out_," ");*/
    /*fprintf(out_,"<===== %s %s\n",a->creator_->op, a->name_);*/
}

void Default::print_offset(FILE* out, grib_accessor* a)
{
    int i, k;
    long offset;
    long theBegin = 0, theEnd = 0;
    size_t size = 0, more = 0;
    grib_handle* h = grib_handle_of_accessor(a);

    theBegin = a->offset_ - section_offset_ + 1;
    theEnd   = a->get_next_position_offset() - section_offset_;

    if ((option_flags_ & GRIB_DUMP_FLAG_HEXADECIMAL) != 0 && a->length_ != 0) {
        if (theBegin == theEnd) {
            fprintf(out_, "  ");
            fprintf(out, "# Octet: ");
            fprintf(out, "%ld", theBegin);
        }
        else {
            fprintf(out_, "  ");
            fprintf(out, "# Octets: ");
            fprintf(out, "%ld-%ld", theBegin, theEnd);
        }
        fprintf(out, "  = ");
        size = a->length_;

        if (!(option_flags_ & GRIB_DUMP_FLAG_ALL_DATA) && size > 112) {
            more = size - 112;
            size = 112;
        }

        k = 0;
        while (k < size) {
            offset = a->offset_;
            for (i = 0; i < 14 && k < size; i++, k++) {
                fprintf(out, " 0x%.2X", h->buffer->data[offset]);
                offset++;
            }
            if (k < size)
                fprintf(out_, "\n  #");
        }
        if (more) {
            fprintf(out_, "\n  #... %lu more values\n", (unsigned long)more);
        }
        fprintf(out_, "\n");
    }
}

}  // namespace eccodes::dumper
