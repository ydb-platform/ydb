/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2010-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * shmem (shared memory backing facility) framework types, convenience macros,
 * etc.
 */

#ifndef OPAL_SHMEM_TYPES_H
#define OPAL_SHMEM_TYPES_H

#include "opal_config.h"

#include <stddef.h>
#include <string.h>

BEGIN_C_DECLS

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * ds_buf: pointer to opal_shmem_ds_t typedef'd struct
 */

/**
 * flag indicating the state (valid/invalid) of the shmem data structure
 * 0x0* - reserved for non-internal flags
 */
#define OPAL_SHMEM_DS_FLAGS_VALID 0x01

/**
 * 0x1* - reserved for internal flags. that is, flags that will NOT be
 * propagated via ds_copy during inter-process information sharing.
 */

/**
 * masks out internal flags
 */
#define OPAL_SHMEM_DS_FLAGS_INTERNAL_MASK 0x0F

/**
 * invalid id value
 */
#define OPAL_SHMEM_DS_ID_INVALID -1

/**
 * macro that sets all bits in flags to 0
 */
#define OPAL_SHMEM_DS_RESET_FLAGS(ds_buf)                                      \
do {                                                                           \
    (ds_buf)->flags = 0x00;                                                    \
} while (0)

/**
 * sets valid bit in flags to 1
 */
#define OPAL_SHMEM_DS_SET_VALID(ds_buf)                                        \
do {                                                                           \
    (ds_buf)->flags |= OPAL_SHMEM_DS_FLAGS_VALID;                              \
} while (0)

/**
 * sets valid bit in flags to 0
 */
#define OPAL_SHMEM_DS_INVALIDATE(ds_buf)                                       \
do {                                                                           \
    (ds_buf)->flags &= ~OPAL_SHMEM_DS_FLAGS_VALID;                             \
} while (0)

/**
 * evaluates to 1 if the valid bit in flags is set to 1.  evaluates to 0
 * otherwise.
 */
#define OPAL_SHMEM_DS_IS_VALID(ds_buf)                                         \
    ( (ds_buf)->flags & OPAL_SHMEM_DS_FLAGS_VALID )

typedef uint8_t opal_shmem_ds_flag_t;

/* shared memory segment header */
struct opal_shmem_seg_hdr_t {
    /* segment lock */
    opal_atomic_lock_t lock;
    /* pid of the segment creator */
    pid_t cpid;
};
typedef struct opal_shmem_seg_hdr_t opal_shmem_seg_hdr_t;

struct opal_shmem_ds_t {
    /* pid of the shared memory segment creator */
    pid_t seg_cpid;
    /* state flags */
    opal_shmem_ds_flag_t flags;
    /* ds id */
    int seg_id;
    /* size of shared memory segment */
    size_t seg_size;
    /* base address of shared memory segment */
    unsigned char *seg_base_addr;
    /* path to backing store -- last element so we can easily calculate the
     * "real" size of opal_shmem_ds_t. that is, the amount of the struct that
     * is actually being used. for example: if seg_name is something like:
     * "foo_baz" and OPAL_PATH_MAX is 4096, we want to know that only a very
     * limited amount of the seg_name buffer is actually being used.
     */
    char seg_name[OPAL_PATH_MAX];
};
typedef struct opal_shmem_ds_t opal_shmem_ds_t;

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * Simply returns the amount of used space. For use when sending the entire
 * opal_shmem_ds_t payload isn't viable -- due to the potential disparity
 * between the reserved buffer space and what is actually in use.
 */
static inline size_t
opal_shmem_sizeof_shmem_ds(const opal_shmem_ds_t *ds_bufp)
{
    char *name_base = NULL;
    size_t name_buf_offset = offsetof(opal_shmem_ds_t, seg_name);

    name_base = (char *)ds_bufp + name_buf_offset;

    return name_buf_offset + strlen(name_base) + 1;
}

END_C_DECLS

#endif /* OPAL_SHMEM_TYPES_H */
