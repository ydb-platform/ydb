/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_json.h"
#include "grib_dumper_factory.h"
#include <cctype>


eccodes::dumper::Json _grib_dumper_json;
eccodes::Dumper* grib_dumper_json = &_grib_dumper_json;

namespace eccodes::dumper
{

int Json::init()
{
    section_offset_ = 0;
    empty_          = 1;
    isLeaf_         = 0;
    isAttribute_    = 0;

    return GRIB_SUCCESS;
}

int Json::destroy()
{
    return GRIB_SUCCESS;
}

void Json::dump_values(grib_accessor* a)
{
    double value = 0;
    size_t size = 1, size2 = 0;
    double* values = NULL;
    int err        = 0;
    int i;
    int cols             = 9;
    long count           = 0;
    double missing_value = GRIB_MISSING_DOUBLE;
    grib_handle* h       = NULL;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    h = grib_handle_of_accessor(a);
    a->value_count(&count);
    size = size2 = count;

    if (size > 1) {
        values = (double*)grib_context_malloc_clear(a->context_, sizeof(double) * size);
        err    = a->unpack_double(values, &size2);
    }
    else {
        err = a->unpack_double(&value, &size2);
    }
    ECCODES_ASSERT(size2 == size);
    (void)err; /* TODO */

    if (begin_ == 0 && empty_ == 0 && isAttribute_ == 0)
        fprintf(out_, ",");
    else
        begin_ = 0;

    empty_ = 0;

    if (isLeaf_ == 0) {
        fprintf(out_, "\n%-*s{\n", depth_, " ");
        depth_ += 2;
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "\"key\" : \"%s\",\n", a->name_);
    }

    err = grib_set_double(h, "missingValue", missing_value);
    if (size > 1) {
        int icount = 0;
        if (isLeaf_ == 0) {
            fprintf(out_, "%-*s", depth_, " ");
            fprintf(out_, "\"value\" :\n");
        }
        fprintf(out_, "%-*s[", depth_, " ");
        depth_ += 2;
        for (i = 0; i < size - 1; ++i) {
            if (icount > cols || i == 0) {
                fprintf(out_, "\n%-*s", depth_, " ");
                icount = 0;
            }
            if (values[i] == missing_value)
                fprintf(out_, "null, ");
            else
                fprintf(out_, "%g, ", values[i]);
            icount++;
        }
        if (icount > cols)
            fprintf(out_, "\n%-*s", depth_, " ");
        if (grib_is_missing_double(a, values[i]))
            fprintf(out_, "%s ", "null");
        else
            fprintf(out_, "%g ", values[i]);

        depth_ -= 2;
        fprintf(out_, "\n%-*s]", depth_, " ");
        /* if (a->attributes_[0]) fprintf(out_,","); */
        grib_context_free(a->context_, values);
    }
    else {
        if (isLeaf_ == 0) {
            fprintf(out_, "%-*s", depth_, " ");
            fprintf(out_, "\"value\" : ");
        }
        if (grib_is_missing_double(a, value))
            fprintf(out_, "null");
        else
            fprintf(out_, "%g", value);
    }

    if (isLeaf_ == 0) {
        dump_attributes(a);
        depth_ -= 2;
        fprintf(out_, "\n%-*s}", depth_, " ");
    }

    (void)err; /* TODO */
}

void Json::dump_long(grib_accessor* a, const char* comment)
{
    long value  = 0;
    size_t size = 1, size2 = 0;
    long* values = NULL;
    int err      = 0;
    int i;
    int cols   = 9;
    long count = 0;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
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

    if (begin_ == 0 && empty_ == 0 && isAttribute_ == 0)
        fprintf(out_, ",");
    else
        begin_ = 0;

    empty_ = 0;

    if (isLeaf_ == 0) {
        fprintf(out_, "\n%-*s{\n", depth_, " ");
        depth_ += 2;
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "\"key\" : \"%s\",\n", a->name_);
    }

    if (size > 1) {
        int doing_unexpandedDescriptors = 0;
        int icount                      = 0;
        if (isLeaf_ == 0) {
            fprintf(out_, "%-*s", depth_, " ");
            fprintf(out_, "\"value\" :\n");
        }
        fprintf(out_, "%-*s[", depth_, " ");
        /* See ECC-637: unfortunately json_xs says:
         *  malformed number (leading zero must not be followed by another digit
          if (strcmp(a->name_, "unexpandedDescriptors")==0)
            doing_unexpandedDescriptors = 1;
          */
        depth_ += 2;
        for (i = 0; i < size - 1; i++) {
            if (icount > cols || i == 0) {
                fprintf(out_, "\n%-*s", depth_, " ");
                icount = 0;
            }
            if (grib_is_missing_long(a, values[i])) {
                fprintf(out_, "null, ");
            }
            else {
                if (doing_unexpandedDescriptors)
                    fprintf(out_, "%06ld, ", values[i]);
                else
                    fprintf(out_, "%ld, ", values[i]);
            }
            icount++;
        }
        if (icount > cols)
            fprintf(out_, "\n%-*s", depth_, " ");
        if (doing_unexpandedDescriptors) {
            fprintf(out_, "%06ld ", values[i]);
        }
        else {
            if (grib_is_missing_long(a, values[i]))
                fprintf(out_, "%s", "null");
            else
                fprintf(out_, "%ld ", values[i]);
        }

        depth_ -= 2;
        fprintf(out_, "\n%-*s]", depth_, " ");
        /* if (a->attributes_[0]) fprintf(out_,","); */
        grib_context_free(a->context_, values);
    }
    else {
        if (isLeaf_ == 0) {
            fprintf(out_, "%-*s", depth_, " ");
            fprintf(out_, "\"value\" : ");
        }
        if (grib_is_missing_long(a, value))
            fprintf(out_, "null");
        else
            fprintf(out_, "%ld", value);
        /* if (a->attributes_[0]) fprintf(out_,","); */
    }

    if (isLeaf_ == 0) {
        dump_attributes(a);
        depth_ -= 2;
        fprintf(out_, "\n%-*s}", depth_, " ");
    }
    (void)err; /* TODO */
}

void Json::dump_bits(grib_accessor* a, const char* comment)
{
}

void Json::dump_double(grib_accessor* a, const char* comment)
{
    double value = 0;
    size_t size  = 1;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    a->unpack_double(&value, &size);

    if (begin_ == 0 && empty_ == 0 && isAttribute_ == 0)
        fprintf(out_, ",\n");
    else
        begin_ = 0;

    empty_ = 0;

    if (isLeaf_ == 0) {
        fprintf(out_, "%-*s{\n", depth_, " ");
        depth_ += 2;
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "\"key\" : \"%s\",\n", a->name_);

        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "\"value\" : ");
    }

    if (grib_is_missing_double(a, value))
        fprintf(out_, "null");
    else
        fprintf(out_, "%g", value);

    /* if (a->attributes_[0]) fprintf(out_,","); */

    if (isLeaf_ == 0) {
        dump_attributes(a);
        depth_ -= 2;
        fprintf(out_, "\n%-*s}", depth_, " ");
    }
}

void Json::dump_string_array(grib_accessor* a, const char* comment)
{
    char** values = NULL;
    size_t size = 0, i = 0;
    grib_context* c = NULL;
    int err         = 0;
    int is_missing  = 0;
    long count      = 0;
    c               = a->context_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
        return;

    a->value_count(&count);
    size = count;
    if (size == 1) {
        dump_string(a, comment);
        return;
    }

    if (begin_ == 0 && empty_ == 0 && isAttribute_ == 0)
        fprintf(out_, ",");
    else
        begin_ = 0;

    if (isLeaf_ == 0) {
        fprintf(out_, "\n%-*s{\n", depth_, " ");
        depth_ += 2;
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "\"key\" : \"%s\",\n", a->name_);
    }

    empty_ = 0;

    values = (char**)grib_context_malloc_clear(c, size * sizeof(char*));
    if (!values) {
        grib_context_log(c, GRIB_LOG_ERROR, "Memory allocation error: %zu bytes", size);
        return;
    }

    err = a->unpack_string_array(values, &size);

    if (isLeaf_ == 0) {
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "\"value\" : ");
    }
    fprintf(out_, "\n%-*s[", depth_, " ");
    depth_ += 2;
    for (i = 0; i < size - 1; i++) {
        is_missing = grib_is_missing_string(a, (unsigned char*)values[i], strlen(values[i]));
        if (is_missing)
            fprintf(out_, "%-*s%s,\n", depth_, " ", "null");
        else
            fprintf(out_, "%-*s\"%s\",\n", depth_, " ", values[i]);
    }
    is_missing = grib_is_missing_string(a, (unsigned char*)values[i], strlen(values[i]));
    if (is_missing)
        fprintf(out_, "%-*s%s", depth_, " ", "null");
    else
        fprintf(out_, "%-*s\"%s\"", depth_, " ", values[i]);

    depth_ -= 2;
    fprintf(out_, "\n%-*s]", depth_, " ");

    /* if (a->attributes_[0]) fprintf(out_,","); */

    if (isLeaf_ == 0) {
        dump_attributes(a);
        depth_ -= 2;
        fprintf(out_, "\n%-*s}", depth_, " ");
    }

    for (i = 0; i < size; i++) {
        grib_context_free(c, values[i]);
    }
    grib_context_free(c, values);
    (void)err; /* TODO */
}

#define MAX_STRING_SIZE 4096
void Json::dump_string(grib_accessor* a, const char* comment)
{
    char value[MAX_STRING_SIZE] = {0, }; /* See ECC-710 */
    size_t size          = MAX_STRING_SIZE;
    char* p              = NULL;
    int is_missing       = 0;
    int err              = 0;
    const char* acc_name = a->name_;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0) {
        return;
    }

    /* ECC-710: It is MUCH slower determining the string length here
     * than using a maximum size (and no need for malloc).
     * Specially for BUFR elements */
    /* err = grib_get_string_length_acc(a,&size);
     * if (size==0) return;
     * value=(char*)grib_context_malloc_clear(a->context_,size);
     * if (!value) {
     *   grib_context_log(a->context_,GRIB_LOG_ERROR,"Unable to allocate %zu bytes",size);
     *   return;
     * }
     */

    if (begin_ == 0 && empty_ == 0 && isAttribute_ == 0)
        fprintf(out_, ",");
    else
        begin_ = 0;

    empty_ = 0;

    err = a->unpack_string(value, &size);
    if (err) {
        snprintf(value, sizeof(value), " *** ERR=%d (%s) [dump_string on '%s']",
                 err, grib_get_error_message(err), a->name_);
    }
    else {
        ECCODES_ASSERT(size < MAX_STRING_SIZE);
    }
    p = value;
    if (grib_is_missing_string(a, (unsigned char*)value, size)) {
        is_missing = 1;
    }

    while (*p) {
        if (!isprint(*p))
            *p = '?';
        if (*p == '"')
            *p = '\''; /* ECC-1401 */
        p++;
    }

    if (isLeaf_ == 0) {
        fprintf(out_, "\n%-*s{", depth_, " ");
        depth_ += 2;
        fprintf(out_, "\n%-*s", depth_, " ");
        fprintf(out_, "\"key\" : \"%s\",", acc_name);
        fprintf(out_, "\n%-*s", depth_, " ");
        fprintf(out_, "\"value\" : ");
    }
    if (is_missing)
        fprintf(out_, "%s", "null");
    else
        fprintf(out_, "\"%s\"", value);

    /* if (a->attributes_[0]) fprintf(out_,","); */

    if (isLeaf_ == 0) {
        dump_attributes(a);
        depth_ -= 2;
        fprintf(out_, "\n%-*s}", depth_, " ");
    }

    /* grib_context_free(a->context_,value); */
    (void)err; /* TODO */
}

void Json::dump_bytes(grib_accessor* a, const char* comment)
{
}

void Json::dump_label(grib_accessor* a, const char* comment)
{
}

void Json::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    if (strcmp(a->name_, "BUFR") == 0 ||
        strcmp(a->name_, "GRIB") == 0 ||
        strcmp(a->name_, "META") == 0) {
        depth_ = 2;
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "[\n");
        begin_ = 1;
        empty_ = 1;
        depth_ += 2;
        grib_dump_accessors_block(this, block);
        depth_ -= 2;
        fprintf(out_, "\n]\n");
    }
    else if (strcmp(a->name_, "groupNumber") == 0) {
        if ((a->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0)
            return;
        if (!empty_)
            fprintf(out_, ",\n");
        fprintf(out_, "%-*s", depth_, " ");

        fprintf(out_, "[");

        fprintf(out_, "\n");
        /* fprintf(out_,"%-*s",depth," "); */
        begin_ = 1;
        empty_ = 1;
        depth_ += 2;
        grib_dump_accessors_block(this, block);
        depth_ -= 2;
        fprintf(out_, "\n");
        fprintf(out_, "%-*s", depth_, " ");
        fprintf(out_, "]");
    }
    else {
        grib_dump_accessors_block(this, block);
    }
}

void Json::dump_attributes(grib_accessor* a)
{
    int i     = 0;
    FILE* out = out_;
    unsigned long flags;
    while (i < MAX_ACCESSOR_ATTRIBUTES && a->attributes_[i]) {
        isAttribute_ = 1;
        if ((option_flags_ & GRIB_DUMP_FLAG_ALL_ATTRIBUTES) == 0 && (a->attributes_[i]->flags_ & GRIB_ACCESSOR_FLAG_DUMP) == 0) {
            i++;
            continue;
        }
        isLeaf_ = a->attributes_[i]->attributes_[0] == NULL ? 1 : 0;
        fprintf(out_, ",");
        fprintf(out_, "\n%-*s", depth_, " ");
        fprintf(out, "\"%s\" : ", a->attributes_[i]->name_);
        flags = a->attributes_[i]->flags_;
        a->attributes_[i]->flags_ |= GRIB_ACCESSOR_FLAG_DUMP;
        switch (a->attributes_[i]->get_native_type()) {
            case GRIB_TYPE_LONG:
                dump_long(a->attributes_[i], 0);
                break;
            case GRIB_TYPE_DOUBLE:
                dump_values(a->attributes_[i]);
                break;
            case GRIB_TYPE_STRING:
                dump_string_array(a->attributes_[i], 0);
                break;
        }
        a->attributes_[i]->flags_ = flags;
        i++;
    }
    isLeaf_      = 0;
    isAttribute_ = 0;
}

}  // namespace eccodes::dumper
