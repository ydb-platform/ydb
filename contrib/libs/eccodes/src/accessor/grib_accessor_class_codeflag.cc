/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_accessor_class_codeflag.h"

grib_accessor_codeflag_t _grib_accessor_codeflag{};
grib_accessor* grib_accessor_codeflag = &_grib_accessor_codeflag;

void grib_accessor_codeflag_t::init(const long len, grib_arguments* param)
{
    grib_accessor_unsigned_t::init(len, param);
    length_    = len;
    tablename_ = param->get_string(grib_handle_of_accessor(this), 0);
    ECCODES_ASSERT(length_ >= 0);
}

static int test_bit(long a, long b)
{
    DEBUG_ASSERT(b >= 0);
    return a & (1 << b);
}

int grib_accessor_codeflag_t::grib_get_codeflag(long code, char* codename)
{
    FILE* f                              = NULL;
    char fname[1024];
    char bval[50];
    char num[50];
    char* filename = 0;
    char line[1024];
    size_t i = 0;
    int j    = 0;
    int err  = 0;

    err = grib_recompose_name(grib_handle_of_accessor(this), NULL, tablename_, fname, 1);
    if (err) {
        strncpy(fname, tablename_, sizeof(fname) - 1);
        fname[sizeof(fname) - 1] = '\0';
    }

    if ((filename = grib_context_full_defs_path(context_, fname)) == NULL) {
        grib_context_log(context_, GRIB_LOG_WARNING, "Cannot open flag table %s", filename);
        strcpy(codename, "Cannot open flag table");
        return GRIB_FILE_NOT_FOUND;
    }

    f = codes_fopen(filename, "r");
    if (!f) {
        grib_context_log(context_, (GRIB_LOG_WARNING) | (GRIB_LOG_PERROR), "Cannot open flag table %s", filename);
        strcpy(codename, "Cannot open flag table");
        return GRIB_FILE_NOT_FOUND;
    }

    // strcpy(codename, tablename_ );
    // strcat(codename,": ");
    // j = strlen(codename);

    while (fgets(line, sizeof(line) - 1, f)) {
        sscanf(line, "%49s %49s", num, bval);

        if (num[0] != '#') {
            if ((test_bit(code, length_ * 8 - atol(num)) > 0) == atol(bval)) {
                size_t linelen = strlen(line);
                codename[j++]  = '(';
                codename[j++]  = num[0];
                codename[j++]  = '=';
                codename[j++]  = bval[0];
                codename[j++]  = ')';
                codename[j++]  = ' ';

                for (i = (strlen(num) + strlen(bval) + 2); i < linelen - 1; i++)
                    codename[j++] = line[i];
                if (line[i] != '\n')
                    codename[j++] = line[i];
                codename[j++] = ';';
            }
        }
    }

    if (j > 1 && codename[j - 1] == ';')
        j--;
    codename[j] = 0;

    strcat(codename, ":");
    strcat(codename, fname);

    fclose(f);
    return GRIB_SUCCESS;
}

int grib_accessor_codeflag_t::value_count(long* count)
{
    *count = 1;
    return 0;
}

void grib_accessor_codeflag_t::dump(eccodes::Dumper* dumper)
{
    long v              = 0;
    char flagname[1024] = {0,};
    char fname[1024] = {0,};

    size_t llen = 1;

    grib_recompose_name(grib_handle_of_accessor(this), NULL, tablename_, fname, 1);
    unpack_long(&v, &llen);
    grib_get_codeflag(v, flagname);

    dumper->dump_bits(this, flagname);
}
