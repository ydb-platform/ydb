/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_dumper_class_grib_encode_C.h"
#include "grib_dumper_factory.h"


eccodes::dumper::GribEncodeC _grib_dumper_grib_encode_c;
eccodes::Dumper* grib_dumper_grib_encode_c = &_grib_dumper_grib_encode_c;

namespace eccodes::dumper {

int GribEncodeC::init()
{
    return GRIB_SUCCESS;
}

int GribEncodeC::destroy()
{
    return GRIB_SUCCESS;
}

static void pcomment(FILE* f, long value, const char* p)
{
    int cr = 0;
    fprintf(f, "\n    /* %ld = ", value);

    while (*p) {
        switch (*p) {
            case ';':
                fprintf(f, "\n    ");
                cr = 1;
                break;

            case ':':
                if (cr)
                    fprintf(f, "\n    See ");
                else
                    fprintf(f, ". See ");
                break;

            default:
                fputc(*p, f);
                break;
        }

        p++;
    }

    fprintf(f, " */\n");
}

void GribEncodeC::dump_long(grib_accessor* a, const char* comment)
{
    long value;
    size_t size = 1;
    int err     = a->unpack_long(&value, &size);

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY))
        return;

    if (comment)
        pcomment(out_, value, comment);

    if (((a->flags_ & GRIB_ACCESSOR_FLAG_CAN_BE_MISSING) != 0) && (value == GRIB_MISSING_LONG))
        fprintf(out_, "    GRIB_CHECK(grib_set_missing(h,\"%s\"),%d);\n", a->name_, 0);
    else
        fprintf(out_, "    GRIB_CHECK(grib_set_long(h,\"%s\",%ld),%d);\n", a->name_, value, 0);

    if (err)
        fprintf(out_, " /*  Error accessing %s (%s) */", a->name_, grib_get_error_message(err));

    if (comment)
        fprintf(out_, "\n");
}

static int test_bit(long a, long b)
{
    return a & (1 << b);
}


void GribEncodeC::dump_bits(grib_accessor* a, const char* comment)
{
    long value;
    size_t size = 1;
    int err     = a->unpack_long(&value, &size);
    int i;

    char buf[1024];

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
        return;

    if (a->length_ == 0)
        return;

    buf[0] = 0;

    for (i = 0; i < (a->length_ * 8); i++) {
        if (test_bit(value, a->length_ * 8 - i - 1))
            strcat(buf, "1");
        else
            strcat(buf, "0");
    }

    if (comment) {
        strcat(buf, ";");
        strcat(buf, comment);
    }

    pcomment(out_, value, buf);

    if (err)
        fprintf(out_, " /*  Error accessing %s (%s) */", a->name_, grib_get_error_message(err));
    else
        fprintf(out_, "    GRIB_CHECK(grib_set_long(h,\"%s\",%ld),%d);\n", a->name_, value, 0);

    fprintf(out_, "\n");
}

void GribEncodeC::dump_double(grib_accessor* a, const char* comment)
{
    double value;
    size_t size = 1;
    int err     = a->unpack_double(&value, &size);
    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
        return;

    if (a->length_ == 0)
        return;

    //if(comment) fprintf(out_,"/* %s */\n",comment);

    fprintf(out_, "    GRIB_CHECK(grib_set_double(h,\"%s\",%g),%d);\n", a->name_, value, 0);

    if (err)
        fprintf(out_, " /*  Error accessing %s (%s) */", a->name_, grib_get_error_message(err));
}

void GribEncodeC::dump_string(grib_accessor* a, const char* comment)
{
    char value[1024];
    size_t size = sizeof(value);
    int err     = a->unpack_string(value, &size);

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
        return;

    if (a->length_ == 0)
        return;

    if (comment)
        fprintf(out_, "/* %s */\n", comment);

    fprintf(out_, "    p    = \"%s\";\n", value);
    fprintf(out_, "    size = strlen(p);\n");
    fprintf(out_, "    GRIB_CHECK(grib_set_string(h,\"%s\",p,&size),%d);\n", a->name_, 0);

    if (err)
        fprintf(out_, " /*  Error accessing %s (%s) */", a->name_, grib_get_error_message(err));
}

void GribEncodeC::dump_bytes(grib_accessor* a, const char* comment)
{
    int err                         = 0;
    size_t size                     = a->length_;
    unsigned char* buf;

    if (a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY)
        return;

    if (size == 0)
        return;

    buf = (unsigned char*)grib_context_malloc(context_, size);

    if (!buf) {
        fprintf(out_, "/* %s: cannot malloc(%zu) */\n", a->name_, size);
        return;
    }

    err = a->unpack_bytes(buf, &size);
    if (err) {
        grib_context_free(context_, buf);
        fprintf(out_, " *** ERR=%d (%s) [grib_dumper_grib_encode_C::dump_bytes]\n}", err, grib_get_error_message(err));
        return;
    }

    // if(size > 100) {
    //     more = size - 100;
    //     size = 100;
    // }

    // k = 0;
    //  //if(size > 100) size = 100;
    // while(k < size)
    // {
    //     int j;
    //     for(i = 0; i < depth_ + 3 ; i++) fprintf(out_," ");
    //     for(j = 0; j < 16 && k < size; j++, k++)
    //     {
    //         fprintf(out_,"%02x",buf[k]);
    //         if(k != size-1)
    //             fprintf(out_,", ");
    //     }
    //     fprintf(out_,"\n");
    // }

    grib_context_free(context_, buf);
}

void GribEncodeC::dump_values(grib_accessor* a)
{
    int k, err = 0;
    double* buf = NULL;
    int type    = 0;
    char stype[10];
    size_t size = 0;
    long count  = 0;

    stype[0] = '\0';

    if ((a->flags_ & GRIB_ACCESSOR_FLAG_READ_ONLY) || ((a->flags_ & GRIB_ACCESSOR_FLAG_DATA) && (option_flags_ & GRIB_DUMP_FLAG_NO_DATA)))
        return;

    a->value_count(&count);
    size = count;

    if (size == 1) {
        dump_double(a, NULL);
        return;
    }

    type = a->get_native_type();
    switch (type) {
        case GRIB_TYPE_LONG:
            snprintf(stype, sizeof(stype), "%s", "long");
            break;
        case GRIB_TYPE_DOUBLE:
            snprintf(stype, sizeof(stype), "%s", "double");
            break;
        default:
            return;
    }

    buf = (double*)grib_context_malloc(context_, size * sizeof(double));
    if (!buf) {
        fprintf(out_, "/* %s: cannot malloc(%zu) */\n", a->name_, size);
        return;
    }

    err = a->unpack_double(buf, &size);

    if (err) {
        grib_context_free(context_, buf);
        fprintf(out_, " /*  Error accessing %s (%s) */", a->name_, grib_get_error_message(err));
        return;
    }

    fprintf(out_, "    size = %zu;\n", size);
    fprintf(out_, "    v%s    = (%s*)calloc(size,sizeof(%s));\n", stype, stype, stype);
    fprintf(out_, "    if(!v%s) {\n", stype);
    fprintf(out_, "        fprintf(stderr,\"failed to allocate %%zu bytes\\n\",size*sizeof(%s));\n", stype);
    fprintf(out_, "        exit(1);\n");
    fprintf(out_, "    }\n");


    fprintf(out_, "\n   ");
    k = 0;
    while (k < size) {
        fprintf(out_, " v%s[%4d] = %7g;", stype, k, buf[k]);
        k++;
        if (k % 4 == 0)
            fprintf(out_, "\n   ");
    }
    if (size % 4)
        fprintf(out_, "\n");
    fprintf(out_, "\n");
    fprintf(out_, "    GRIB_CHECK(grib_set_%s_array(h,\"%s\",v%s,size),%d);\n", stype, a->name_, stype, 0);
    fprintf(out_, "    free(v%s);\n", stype);

    grib_context_free(context_, buf);
}

void GribEncodeC::dump_label(grib_accessor* a, const char* comment)
{
    fprintf(out_, "\n    /* %s */\n\n", a->name_);
}

void GribEncodeC::dump_section(grib_accessor* a, grib_block_of_accessors* block)
{
    grib_dump_accessors_block(this, block);
}

void GribEncodeC::header(const grib_handle* h) const
{
    long edition                    = 0;
    int ret                         = 0;
    ret                             = grib_get_long(h, "editionNumber", &edition);
    if (ret != GRIB_SUCCESS) {
        grib_context_log(h->context, GRIB_LOG_ERROR, "Unable to get edition number.");
        ECCODES_ASSERT(0);
    }

    fprintf(out_,
            "#include <grib_api.h>\n"
            "\n"
            "/* This code was generated automatically */\n"
            "\n");

    fprintf(out_,
            "\n"
            "int main(int argc,const char** argv)\n"
            "{\n"
            "    grib_handle *h     = NULL;\n"
            "    size_t size        = 0;\n"
            "    double* vdouble    = NULL;\n"
            "    long* vlong        = NULL;\n"
            "    FILE* f            = NULL;\n"
            "    const char* p      = NULL;\n"
            "    const void* buffer = NULL;\n"
            "\n"
            "    if(argc != 2) {\n"
            "       fprintf(stderr,\"usage: %%s out\\n\",argv[0]);\n"
            "        exit(1);\n"
            "    }\n"
            "\n"
            "    h = grib_handle_new_from_samples(NULL,\"GRIB%ld\");\n"
            "    if(!h) {\n"
            "        fprintf(stderr,\"Cannot create grib handle\\n\");\n"
            "        exit(1);\n"
            "    }\n"
            "\n",
            (long)edition);
}

void GribEncodeC::footer(const grib_handle* h) const
{

    fprintf(out_,
            "/* Save the message */\n"
            "\n"
            "    f = fopen(argv[1],\"w\");\n"
            "    if(!f) {\n"
            "        perror(argv[1]);\n"
            "        exit(1);\n"
            "    }\n"
            "\n"
            "    GRIB_CHECK(grib_get_message(h,&buffer,&size),0);\n"
            "\n"
            "    if(fwrite(buffer,1,size,f) != size) {\n"
            "        perror(argv[1]);\n"
            "        exit(1);\n"
            "    }\n"
            "\n"
            "    if(fclose(f)) {\n"
            "        perror(argv[1]);\n"
            "        exit(1);\n"
            "    }\n"
            "\n"
            "    grib_handle_delete(h);\n"
            "    return 0;\n"
            "}\n");
}

} // namespace eccodes::dumper
