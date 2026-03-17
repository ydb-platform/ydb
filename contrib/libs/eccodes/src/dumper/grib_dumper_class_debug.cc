/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_debug.h"
#include "grib_dumper_factory.h"
#include <cctype>

eccodes::dumper::Debug _grib_dumper_debug;
eccodes::Dumper* grib_dumper_debug = &_grib_dumper_debug;

namespace eccodes::dumper
{

int Debug::init()
{
    section_offset_ = 0;
    return GRIB_SUCCESS;
}

int Debug::destroy()
{
    return GRIB_SUCCESS;
}

void Debug::default_long_value(grib_accessor* a, long actualValue)
{
    grib_action* act = a->creator_;
    if (act->default_value_ == NULL)
        return;

    grib_handle* h = grib_handle_of_accessor(a);
    grib_expression* expression = act->default_value_->get_expression(h, 0);
    if (!expression)
        return;

    const int type = expression->native_type(h);
    if (type == GRIB_TYPE_LONG) {
        long defaultValue = 0;
        if (expression->evaluate_long(h, &defaultValue) == GRIB_SUCCESS && defaultValue != actualValue) {
            if (defaultValue == GRIB_MISSING_LONG)
                fprintf(out_, " (default=MISSING)");
            else
                fprintf(out_, " (default=%ld)", defaultValue);
        }
    }
}

// void Debug::default_string_value(grib_dumper* d, grib_accessor* a, const char* actualValue)
// {
//     grib_action* act = a->creator_;
//     if (act->default_value == NULL)
//         return;

//     grib_handle* h = grib_handle_of_accessor(a);
//     grib_expression* expression = grib_arguments_get_expression(h, act->default_value, 0);
//     if (!expression)
//         return;

//     const int type = grib_expression_native_type(h, expression);
//     DEBUG_ASSERT(type == GRIB_TYPE_STRING);
//     if (type == GRIB_TYPE_STRING) {
//         char tmp[1024] = {0,};
//         size_t s_len = sizeof(tmp);
//         int err = 0;
//         const char* p = grib_expression_evaluate_string(h, expression, tmp, &s_len, &err);
//         if (!err && !STR_EQUAL(p, actualValue)) {
//             fprintf(out_, " (default=%s)", p);
//         }
//     }
// }

void Debug::aliases(grib_accessor* a)
{
    int i;

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

void Debug::dump_long(grib_accessor* a, const char* comment)
{
    long value   = 0;
    size_t size  = 0;
    size_t more  = 0;
    long* values = NULL; /* array of long */
    long count   = 0;
    int err = 0, i = 0;

    if (a->length_ == 0 && (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 && (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0)
        return;

    a->value_count(&count);
    size = count;
    if (size > 1) {
        values = (long*)grib_context_malloc_clear(a->context_, sizeof(long) * size);
        err = a->unpack_long(values, &size);
    }
    else {
        err = a->unpack_long(&value, &size);
    }

    set_begin_end(a);

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");

    if (size > 1) {
        fprintf(out_, "%ld-%ld %s %s = {\n", begin_, theEnd_, a->creator_->op_, a->name_);
        if (values) {
            int k = 0;
            if (size > 100) {
                more = size - 100;
                size = 100;
            }
            while (k < size) {
                int j;
                for (i = 0; i < depth_ + 3; i++)
                    fprintf(out_, " ");
                for (j = 0; j < 8 && k < size; j++, k++) {
                    fprintf(out_, "%ld", values[k]);
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
            grib_context_free(a->context_, values);
        }
    }
    else {
        if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && a->is_missing_internal())
            fprintf(out_, "%ld-%ld %s %s = MISSING", begin_, theEnd_, a->creator_->op_, a->name_);
        else
            fprintf(out_, "%ld-%ld %s %s = %ld", begin_, theEnd_, a->creator_->op_, a->name_, value);
        if (comment)
            fprintf(out_, " [%s]", comment);
        if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
            fprintf(out_, " (%s)", grib_get_type_name(a->get_native_type()));
        if ((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0)
            fprintf(out_, " %s", "(can be missing)");
        if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
            fprintf(out_, " %s", "(read-only)");
    }
    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_debug::dump_long]", err, grib_get_error_message(err));
    aliases(a);
    default_long_value(a, value);

    fprintf(out_, "\n");
}

static int test_bit(long a, long b)
{
    return a & (1 << b);
}

void Debug::dump_bits(grib_accessor* a, const char* comment)
{
    if (a->length_ == 0 && (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    size_t size = 1;
    long value  = 0;
    int err     = a->unpack_long(&value, &size);
    set_begin_end(a);

    for (int i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "%ld-%ld %s %s = %ld [", begin_, theEnd_, a->creator_->op_, a->name_, value);
    for (long i = 0; i < (a->length_ * 8); i++) {
        if (test_bit(value, a->length_ * 8 - i - 1))
            fprintf(out_, "1");
        else
            fprintf(out_, "0");
    }

    if (comment)
        fprintf(out_, ":%s]", comment);
    else
        fprintf(out_, "]");

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_debug::dump_bits]", err, grib_get_error_message(err));
    aliases(a);
    fprintf(out_, "\n");
}

void Debug::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;
    int err      = a->unpack_double(&value, &size);
    int i;

    if (a->length_ == 0 && (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    set_begin_end(a);

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) &&
        a->is_missing_internal())
        fprintf(out_, "%ld-%ld %s %s = MISSING", begin_, theEnd_, a->creator_->op_, a->name_);
    else
        fprintf(out_, "%ld-%ld %s %s = %g", begin_, theEnd_, a->creator_->op_, a->name_, value);
    if (comment)
        fprintf(out_, " [%s]", comment);
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, " (%s)", grib_get_type_name(a->get_native_type()));
    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_debug::dump_double]", err, grib_get_error_message(err));
    aliases(a);
    fprintf(out_, "\n");
}

void Debug::dump_string(grib_accessor* a, const char* comment)
{
    int err = 0;
    int i;
    size_t size = 0;
    char* value = NULL;
    char* p     = NULL;

    if (a->length_ == 0 && (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    grib_get_string_length_acc(a, &size);
    if ((size < 2) && a->is_missing_internal()) {
        /* GRIB-302: transients and missing keys. Need to re-adjust the size */
        size = 10; /* big enough to hold the string "missing" */
    }

    value = (char*)grib_context_malloc_clear(a->context_, size);
    if (!value)
        return;
    err = a->unpack_string(value, &size);

    if (err)
        strcpy(value, "<error>");

    p = value;

    set_begin_end(a);

    while (*p) {
        if (!isprint(*p))
            *p = '.';
        p++;
    }

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "%ld-%ld %s %s = %s", begin_, theEnd_, a->creator_->op_, a->name_, value);
    if (comment)
        fprintf(out_, " [%s]", comment);
    if ((option_flags_ & GRIB_DUMP_FLAG_TYPE) != 0)
        fprintf(out_, " (%s)", grib_get_type_name(a->get_native_type()));

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_debug::dump_string]", err, grib_get_error_message(err));
    aliases(a);
    fprintf(out_, "\n");

    grib_context_free(a->context_, value);
}

void Debug::dump_string_array(grib_accessor* a, const char* comment)
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
    // print_offset(out_, begin_, theEnd_);

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

void Debug::dump_bytes(grib_accessor* a, const char* comment)
{
    int i, k, err = 0;
    size_t more        = 0;
    size_t size        = a->length_;
    unsigned char* buf = (unsigned char*)grib_context_malloc(context_, size);

    if (a->length_ == 0 && (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    set_begin_end(a);

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "%ld-%ld %s %s = %ld", begin_, theEnd_, a->creator_->op_, a->name_, a->length_);
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

    err = a->unpack_bytes(buf, &size);
    if (err) {
        grib_context_free(context_, buf);
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_debug::dump_bytes]\n}", err, grib_get_error_message(err));
        return;
    }

    if (size > 100) {
        more = size - 100;
        size = 100;
    }

    k = 0;
    /* if(size > 100) size = 100;  */
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

void Debug::dump_values(grib_accessor* a)
{
    int i, k, err = 0;
    size_t more = 0;
    double* buf = NULL;
    size_t size = 0;
    long count  = 0;

    if (a->length_ == 0 && (option_flags_ & GRIB_DUMP_FLAG_CODED) != 0)
        return;

    a->value_count(&count);
    size = count;
    if (size == 1) {
        dump_double(a, NULL);
        return;
    }
    buf = (double*)grib_context_malloc_clear(context_, size * sizeof(double));

    set_begin_end(a);

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "%ld-%ld %s %s = (%ld,%ld)", begin_, theEnd_, a->creator_->op_, a->name_, (long)size, a->length_);
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
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_debug::dump_values]\n}", err, grib_get_error_message(err));
        return;
    }

    if (size > 100) {
        more = size - 100;
        size = 100;
    }

    k = 0;
    while (k < size) {
        int j;
        for (i = 0; i < depth_ + 3; i++)
            fprintf(out_, " ");
        for (j = 0; j < 8 && k < size; j++, k++) {
            fprintf(out_, "%10g", buf[k]);
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

void Debug::dump_label(grib_accessor* a, const char* comment)
{
    int i;
    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "----> %s %s %s\n", a->creator_->op_, a->name_, comment ? comment : "");
}

void Debug::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    int i;
    /* grib_section* s = grib_get_sub_section(a); */
    grib_section* s = a->sub_section_;

    if (a->name_[0] == '_') {
        grib_dump_accessors_block(this, block);
        return;
    }

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "======> %s %s (%ld,%ld,%ld)\n", a->creator_->op_, a->name_, a->length_, (long)s->length, (long)s->padding);
    if (!strncmp(a->name_, "section", 7))
        section_offset_ = a->offset_;
    /*printf("------------- section_offset = %ld\n",section_offset_);*/
    depth_ += 3;
    grib_dump_accessors_block(this, block);
    depth_ -= 3;

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "<===== %s %s\n", a->creator_->op_, a->name_);
}

void Debug::set_begin_end(grib_accessor* a)
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

}  // namespace eccodes::dumper
