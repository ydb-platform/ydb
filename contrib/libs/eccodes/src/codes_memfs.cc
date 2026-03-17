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

#ifdef HAVE_MEMFS
/* These two functions are implemented in the generated C file memfs_gen_final.c in the build area */
/* See the memfs.py Python generator */
int codes_memfs_exists(const char* path);
FILE* codes_memfs_open(const char* path);

FILE* codes_fopen(const char* name, const char* mode)
{
    FILE* f;

    if (strcmp(mode, "r") != 0) { /* Not reading */
        return fopen(name, mode);
    }

    f = codes_memfs_open(name); /* Load from memory */
    if (f) {
        return f;
    }

    return fopen(name, mode);
}

/* Returns 0 upon success */
int codes_access(const char* name, int mode)
{
    /* F_OK tests for the existence of the file  */
    if (mode != F_OK) {
        return access(name, mode);
    }

    if (codes_memfs_exists(name)) { /* Check memory */
        return 0;
    }

    return access(name, mode);
}

#else
/* No MEMFS */
FILE* codes_fopen(const char* name, const char* mode)
{
    return fopen(name, mode);
}

/* Returns 0 upon success */
int codes_access(const char* name, int mode)
{
    return access(name, mode);
}

#endif
