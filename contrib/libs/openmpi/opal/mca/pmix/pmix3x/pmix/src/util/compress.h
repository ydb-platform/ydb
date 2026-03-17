/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2016 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Compress/decompress long strings
 */

#ifndef PMIX_COMPRESS_H
#define PMIX_COMPRESS_H

#include <src/include/pmix_config.h>


BEGIN_C_DECLS

/* define a limit for storing raw strings */
#define PMIX_STRING_LIMIT  512

/* define a macro for quickly checking if a string exceeds the
 * compression limit */
#define PMIX_STRING_SIZE_CHECK(s) \
    (PMIX_STRING == (s)->type && NULL != (s)->data.string && PMIX_STRING_LIMIT < strlen((s)->data.string))

/**
 * Compress a string into a byte object using Zlib
 */
PMIX_EXPORT bool pmix_util_compress_string(char *instring,
                                           uint8_t **outbytes,
                                           size_t *nbytes);

/**
 * Decompress a byte object into a string using Zlib
 */
PMIX_EXPORT void pmix_util_uncompress_string(char **outstring,
                                             uint8_t *inbytes, size_t len);

END_C_DECLS

#endif /* PMIX_COMPRESS_H */
