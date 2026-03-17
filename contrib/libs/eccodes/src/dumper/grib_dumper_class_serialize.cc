/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_serialize.h"
#include "grib_dumper_factory.h"
#include <cctype>


eccodes::dumper::Serialize _grib_dumper_serialize;
eccodes::Dumper* grib_dumper_serialize = &_grib_dumper_serialize;

namespace eccodes::dumper
{

int Serialize::init()
{
    format_ = (char*)arg_;
    return GRIB_SUCCESS;
}

int Serialize::destroy()
{
    return GRIB_SUCCESS;
}

void Serialize::dump_long(grib_accessor* a, const char* comment)
{
    long value  = 0;
    size_t size = 1;
    int err     = a->unpack_long(&value, &size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_HIDDEN) != 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0 &&
        (strcmp(a->class_name_, "lookup") != 0))
        return;

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && (value == GRIB_MISSING_LONG))
        fprintf(out_, "%s = MISSING", a->name_);
    else
        fprintf(out_, "%s = %ld", a->name_, value);

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0) &&
        (strcmp(a->class_name_, "lookup") != 0))
        fprintf(out_, " (read_only)");

    // if(comment) fprintf(out_," [%s]",comment);

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_serialize::dump_long]", err, grib_get_error_message(err));

    fprintf(out_, "\n");
}

// int Serialize::test_bit(long a, long b) {return a&(1<<b);}

void Serialize::dump_bits(grib_accessor* a, const char* comment)
{
    long value;
    size_t size = 1;
    int err     = a->unpack_long(&value, &size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_HIDDEN) != 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0)
        return;

    fprintf(out_, "%s = %ld ", a->name_, value);

    // fprintf(out_,"[");
    // for(i=0;i<(a->length_*8);i++) {
    //     if(test_bit(value,a->length_*8-i-1))
    //         fprintf(out_,"1");
    //     else
    //         fprintf(out_,"0");
    // }

    // if(comment)
    //     fprintf(out_,":%s]",comment);
    // else
    //     fprintf(out_,"]");

    if (err)
        fprintf(out_, " *** ERR=%d (%s)", err, grib_get_error_message(err));

    fprintf(out_, "\n");
}

void Serialize::dump_double(grib_accessor* a, const char* comment)
{
    double value;
    size_t size = 1;
    int err     = a->unpack_double(&value, &size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_HIDDEN) != 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0)
        return;

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && (value == GRIB_MISSING_DOUBLE))
        fprintf(out_, "%s = MISSING", a->name_);
    else
        fprintf(out_, "%s = %g", a->name_, value);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        fprintf(out_, " (read_only)");

    // if (comment) fprintf(out_," [%s]",comment);

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_serialize::dump_double]", err, grib_get_error_message(err));
    fprintf(out_, "\n");
}

void Serialize::dump_string(grib_accessor* a, const char* comment)
{
    char value[1024] = {0, };
    size_t size = sizeof(value);
    int err     = a->unpack_string(value, &size);
    int i;

    char* p = value;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_HIDDEN) != 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0)
        return;

    while (*p) {
        if (!isprint(*p))
            *p = '.';
        p++;
    }

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");

    fprintf(out_, "%s = %s", a->name_, value);
    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0)
        fprintf(out_, " (read_only)");

    // if(comment) fprintf(out_," [%s]",comment);

    if (err)
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_serialize::dump_string]", err, grib_get_error_message(err));
    fprintf(out_, "\n");
}

void Serialize::dump_bytes(grib_accessor* a, const char* comment)
{
    int i, k, err = 0;
    size_t more        = 0;
    size_t size        = a->length_;
    unsigned char* buf = (unsigned char*)grib_context_malloc(context_, size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_HIDDEN) != 0)
        return;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) != 0 &&
        (option_flags_ & GRIB_DUMP_FLAG_READ_ONLY) == 0)
        return;

    for (i = 0; i < depth_; i++)
        fprintf(out_, " ");
    fprintf(out_, "%s = (%ld) {", a->name_, a->length_);

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
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_serialize::dump_bytes]\n}", err, grib_get_error_message(err));
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

void Serialize::dump_values(grib_accessor* a)
{
    int k, err = 0;
    double* buf          = NULL;
    size_t last          = 0;
    int columns          = 4;
    char* values_format  = NULL;
    char* default_format = (char*)"%.16e";
    char* columns_str    = NULL;
    size_t len           = 0;
    char* pc             = NULL;
    char* pcf            = NULL;
    size_t size          = 0;
    long count           = 0;
    values_format        = default_format;

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY))
        return;

    a->value_count(&count);
    size = count;

    if (format_) {
        if (format_[0] == '\"')
            values_format = format_ + 1;
        else
            values_format = format_;
        last = strlen(values_format) - 1;
        if (values_format[last] == '\"')
            values_format[last] = '\0';
    }

    pc  = values_format;
    pcf = values_format;
    while (*pc != '\0' && *pc != '%')
        pc++;
    if (strlen(pc) > 1) {
        values_format = pc;
        len           = pc - pcf;
    }
    else {
        values_format = default_format;
        len           = 0;
    }

    if (len > 0) {
        columns_str = (char*)malloc((len + 1) * sizeof(char));
        ECCODES_ASSERT(columns_str);
        columns_str      = (char*)memcpy(columns_str, pcf, len);
        columns_str[len] = '\0';
        columns          = atoi(columns_str);
        free(columns_str);
    }

    if (size == 1) {
        dump_double(a, NULL);
        return;
    }

    if ((option_flags_ & GRIB_DUMP_FLAG_VALUES) == 0)
        return;

    buf = (double*)grib_context_malloc(context_, size * sizeof(double));

    fprintf(out_, "%s (%zu) {", a->name_, size);

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
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_serialize::dump_values]\n}", err, grib_get_error_message(err));
        return;
    }

    k = 0;
    while (k < size) {
        int j;
        for (j = 0; j < columns && k < size; j++, k++) {
            fprintf(out_, values_format, buf[k]);
            if (k != size - 1)
                fprintf(out_, ", ");
        }
        fprintf(out_, "\n");
    }
    fprintf(out_, "}\n");
    grib_context_free(context_, buf);
}

void Serialize::dump_label(grib_accessor* a, const char* comment)
{
    // int i;
    // for(i = 0; i < depth_ ; i++) fprintf(out_," ");
    // fprintf(out_,"----> %s %s %s\n",a->creator_->op, a->name_,comment?comment:"");
}

void Serialize::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    const char* secstr = "section";

    size_t len = strlen(secstr);

    if (a->name_[0] == '_') {
        grib_dump_accessors_block(this, block);
        return;
    }

    if (strncmp(secstr, a->name_, len) == 0)
        fprintf(out_, "#------ %s -------\n", a->name_);

    grib_dump_accessors_block(this, block);

    // fprintf(out_,"<------ %s %s\n",a->creator_->op, a->name_);
}

}  // namespace eccodes::dumper
