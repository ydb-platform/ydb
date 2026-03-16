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
 * Copyright (c) 2015-2017 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Compress/decompress long data blocks
 */

#ifndef ORTE_COMPRESS_H
#define ORTE_COMPRESS_H

#include <orte_config.h>


BEGIN_C_DECLS

/* define a limit for compression */
#define ORTE_COMPRESS_LIMIT  4096

/**
 * Compress a string into a byte object using Zlib
 */
ORTE_DECLSPEC bool orte_util_compress_block(uint8_t *inbytes,
                                            size_t inlen,
                                            uint8_t **outbytes,
                                            size_t *olen);

/**
 * Decompress a byte object
 */
ORTE_DECLSPEC bool orte_util_uncompress_block(uint8_t **outbytes, size_t olen,
                                              uint8_t *inbytes, size_t len);

END_C_DECLS

#endif /* ORTE_COMPRESS_H */
