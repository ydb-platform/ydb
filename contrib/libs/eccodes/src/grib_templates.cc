/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

//  This is a mechanism where we generate C code in grib_templates.h
//  from our GRIB sample files and then include the header so one
//  can instantiate samples without any disk access.
//  Note: This is now superseded by MEMFS
//
// typedef struct grib_templates {
//     const char*           name;
//     const unsigned char* data;
//     size_t               size;
// } grib_templates;

// grib_handle* grib_internal_sample(grib_context* c,const char* name)
// {
//     size_t i;
//     const size_t num_samples = sizeof(templates) / sizeof(templates[0]);
//     for(i = 0; i < num_samples; i++)
//         if(strcmp(name,templates[i].name) == 0)
//             return grib_handle_new_from_message_copy(c,templates[i].data,templates[i].size);
//     return NULL;
// }

// Windows always has a colon in pathnames e.g. C:\temp\file. It uses semi-colons as delimiter
#ifdef ECCODES_ON_WINDOWS
#define ECC_PATH_DELIMITER_CHAR ';'
#else
#define ECC_PATH_DELIMITER_CHAR ':'
#endif

// if product_kind is PRODUCT_ANY, the type of sample file is determined at runtime
static grib_handle* try_product_sample(grib_context* c, ProductKind product_kind, const char* dir, const char* name)
{
    char path[1024];
    grib_handle* g = NULL;
    int err        = 0;

    if (string_ends_with(name, ".tmpl"))
        snprintf(path, sizeof(path), "%s/%s", dir, name);
    else
        snprintf(path, sizeof(path), "%s/%s.tmpl", dir, name);

    if (c->debug) {
        fprintf(stderr, "ECCODES DEBUG try_product_sample product=%s, path='%s'\n", codes_get_product_name(product_kind), path);
    }

    if (codes_access(path, F_OK) == 0) { // 0 means file exists
        FILE* f = codes_fopen(path, "r");
        if (!f) {
            grib_context_log(c, GRIB_LOG_PERROR, "cannot open %s", path);
            return NULL;
        }
        if (product_kind == PRODUCT_ANY)
        {
            // Determine the product kind from sample file
            char* mesg   = NULL;
            size_t size  = 0;
            off_t offset = 0;
            mesg = (char*)wmo_read_any_from_file_malloc(f, 0, &size, &offset, &err);
            if (mesg && !err) {
                ECCODES_ASSERT(size > 4);
                if (strncmp(mesg, "GRIB", 4) == 0 || strncmp(mesg, "DIAG", 4) == 0 || strncmp(mesg, "BUDG", 4) == 0) {
                    product_kind = PRODUCT_GRIB;
                } else if (strncmp(mesg, "BUFR", 4) == 0) {
                    product_kind = PRODUCT_BUFR;
                } else {
                    grib_context_log(c, GRIB_LOG_ERROR, "Could not determine product kind");
                }
                grib_context_free(c, mesg);
                rewind(f);
            } else {
                grib_context_log(c, GRIB_LOG_ERROR, "Could not determine product kind");
            }
        }
        if (product_kind == PRODUCT_BUFR) {
            g = codes_bufr_handle_new_from_file(c, f, &err);
        } else {
            // Note: Pseudo GRIBs like DIAG and BUDG also come here
            DEBUG_ASSERT(product_kind == PRODUCT_GRIB);
            g = grib_handle_new_from_file(c, f, &err);
        }
        if (!g) {
            grib_context_log(c, GRIB_LOG_ERROR, "Cannot create handle from %s", path);
        }
        fclose(f);
    }

    return g;
}

static char* try_sample_path(grib_context* c, const char* dir, const char* name)
{
    // The ".tmpl" extension is historic. It should have been ".sample"
    char path[2048];
    if (string_ends_with(name, ".tmpl"))
        snprintf(path, sizeof(path), "%s/%s", dir, name);
    else
        snprintf(path, sizeof(path), "%s/%s.tmpl", dir, name);

    if (codes_access(path, F_OK) == 0) { // 0 means file exists
        return grib_context_strdup(c, path);
    }

    return NULL;
}

// External here means on disk
grib_handle* codes_external_sample(grib_context* c, ProductKind product_kind, const char* name)
{
    const char* base = c->grib_samples_path;
    char buffer[1024];
    char* p        = buffer;
    grib_handle* g = NULL;

    if (!base)
        return NULL;

    while (*base) {
        if (*base == ECC_PATH_DELIMITER_CHAR) {
            *p = 0;
            g  = try_product_sample(c, product_kind, buffer, name);
            if (g)
                return g;
            p = buffer;
            base++; //advance past delimiter
        }
        *p++ = *base++;
    }

    *p       = 0;
    g = try_product_sample(c, product_kind, buffer, name);
    return g;
}

char* get_external_sample_path(grib_context* c, const char* name)
{
    const char* base = c->grib_samples_path;
    char buffer[1024];
    char* p = buffer;
    char* g = NULL;

    if (!base)
        return NULL;

    while (*base) {
        if (*base == ECC_PATH_DELIMITER_CHAR) {
            *p = 0;
            g  = try_sample_path(c, buffer, name);
            if (g)
                return g;
            p = buffer;
            base++;
        }
        *p++ = *base++;
    }

    *p       = 0;
    return g = try_sample_path(c, buffer, name);
}
