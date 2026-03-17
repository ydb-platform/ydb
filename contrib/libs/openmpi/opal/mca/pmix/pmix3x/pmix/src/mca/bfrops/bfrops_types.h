/* -*- C -*-
 *
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Buffer management types.
 */

#ifndef PMIX_MCA_BFROP_TYPES_H_
#define PMIX_MCA_BFROP_TYPES_H_

#include <src/include/pmix_config.h>


#include "src/class/pmix_object.h"
#include "src/class/pmix_pointer_array.h"
#include "src/class/pmix_list.h"
#include <pmix_common.h>

BEGIN_C_DECLS

/* define the results values for comparisons so we can change them in only one place */
typedef enum {
    PMIX_EQUAL = 0,
    PMIX_VALUE1_GREATER,
    PMIX_VALUE2_GREATER
} pmix_value_cmp_t;

/**
 * buffer type
 */
typedef uint8_t pmix_bfrop_buffer_type_t;
#define PMIX_BFROP_BUFFER_UNDEF         0x00
#define PMIX_BFROP_BUFFER_NON_DESC      0x01
#define PMIX_BFROP_BUFFER_FULLY_DESC    0x02

#define PMIX_BFROP_BUFFER_TYPE_HTON(h)
#define PMIX_BFROP_BUFFER_TYPE_NTOH(h)

/* internally used object for transferring data
 * to/from the server and for storing in the
 * hash tables */
typedef struct {
    pmix_list_item_t super;
    char *key;
    pmix_value_t *value;
} pmix_kval_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_kval_t);


/**
 * Structure for holding a buffer */
typedef struct {
    /** First member must be the object's parent */
    pmix_object_t parent;
    /** type of buffer */
    pmix_bfrop_buffer_type_t type;
    /** Start of my memory */
    char *base_ptr;
    /** Where the next data will be packed to (within the allocated
        memory starting at base_ptr) */
    char *pack_ptr;
    /** Where the next data will be unpacked from (within the
        allocated memory starting as base_ptr) */
    char *unpack_ptr;

    /** Number of bytes allocated (starting at base_ptr) */
    size_t bytes_allocated;
    /** Number of bytes used by the buffer (i.e., amount of data --
        including overhead -- packed in the buffer) */
    size_t bytes_used;
} pmix_buffer_t;
PMIX_EXPORT PMIX_CLASS_DECLARATION(pmix_buffer_t);

/* Convenience macro for loading a data blob into a pmix_buffer_t
 *
 * p - the pmix_peer_t of the process that provided the blob. This
 *     is needed so we can set the buffer type for later unpacking
 *
 * b - pointer to pmix_buffer_t
 *
 * d - pointer to the data blob
 *
 * s - number of bytes in the blob
 *
 * NOTE: the macro does NOT copy the data, but simply assigns
 * its address to the buffer. Accordingly, the macro will
 * set the provided data blob pointer to NULL and the size
 * to zero.
 */
#define PMIX_LOAD_BUFFER(p, b, d, s)                    \
    do {                                                \
        (b)->type = (p)->nptr->compat.type;             \
        (b)->base_ptr = (char*)(d);                     \
        (b)->bytes_used = (s);                          \
        (b)->bytes_allocated = (s);                     \
        (b)->pack_ptr = ((char*)(b)->base_ptr) + (s);   \
        (b)->unpack_ptr = (b)->base_ptr;                \
        (d) = NULL;                                     \
        (s) = 0;                                        \
    } while (0)

/* Convenience macro for extracting a pmix_buffer_t's payload
 * as a data blob
 *
 * b - pointer to the pmix_buffer_t
 *
 * d - char* pointer to the data blob
 *
 * s - number of bytes in the blob
 *
 * NOTE: the macro does NOT copy the data, but simply assigns
 * the address of the buffer's payload to the provided pointer.
 * Accordingly, the macro will set all pmix_buffer_t internal
 * tracking pointers to NULL and all counters to zero */
#define PMIX_UNLOAD_BUFFER(b, d, s)             \
    do {                                        \
        (d) = (char*)(b)->unpack_ptr;           \
        (s) = (b)->bytes_used;                  \
        (b)->base_ptr = NULL;                   \
        (b)->bytes_used = 0;                    \
        (b)->bytes_allocated = 0;               \
        (b)->pack_ptr = NULL;                   \
        (b)->unpack_ptr = NULL;                 \
    } while (0)

/* Convenience macro to check for empty buffer without
 * exposing the internals */
#define PMIX_BUFFER_IS_EMPTY(b)     \
    0 == (b)->bytes_used

END_C_DECLS

#endif /* PMIX_BFROP_TYPES_H */
