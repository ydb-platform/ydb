/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _COMMON_SM_H_
#define _COMMON_SM_H_

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_list.h"
#include "opal/sys/atomic.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/mca/btl/base/base.h"
#include "opal/util/proc.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "common_sm_mpool.h"

BEGIN_C_DECLS

struct mca_mpool_base_module_t;

typedef struct mca_common_sm_seg_header_t {
    /* lock to control atomic access */
    opal_atomic_lock_t seg_lock;
    /* indicates whether or not the segment is ready for use */
    volatile int32_t seg_inited;
    /* number of local processes that are attached to the shared memory segment.
     * this is primarily used as a way of determining whether or not it is safe
     * to unlink the shared memory backing store. for example, once seg_att
     * is equal to the number of local processes, then we can safely unlink.
     */
    volatile size_t seg_num_procs_inited;
    /* offset to next available memory location available for allocation */
    size_t seg_offset;
    /* total size of the segment */
    size_t seg_size;
} mca_common_sm_seg_header_t;

typedef struct mca_common_sm_module_t {
    /* double link list element */
    opal_list_item_t super;
    /* pointer to header embedded in the shared memory segment */
    mca_common_sm_seg_header_t *module_seg;
    /* base address of the segment */
    unsigned char *module_seg_addr;
    /* base address of data segment */
    unsigned char *module_data_addr;
    /* shared memory backing facility object that encapsulates shmem info */
    opal_shmem_ds_t shmem_ds;
    /* memory pool interface to shared-memory region */
    mca_mpool_base_module_t *mpool;
} mca_common_sm_module_t;

OBJ_CLASS_DECLARATION(mca_common_sm_module_t);

/**
 * This routine reorders procs array to have all the local procs at the
 * beginning and returns the number of local procs through out_num_local_procs.
 * The proc with the lowest name is at the beginning of the reordered procs
 * array.
 *
 * @returnvalue OPAL_SUCCESS on success, something else, otherwise.
 */
OPAL_DECLSPEC extern int
mca_common_sm_local_proc_reorder(opal_proc_t **procs,
                                 size_t num_procs,
                                 size_t *out_num_local_procs);

/**
 *  This routine is used to create and attach to a shared memory segment
 *  (whether it's an mmaped file or a SYSV IPC segment).  It is assumed that
 *  the shared memory segment does not exist before this call.
 *
 *  @returnvalue pointer to control structure at head of shared memory segment.
 *  Returns NULL if an error occurred.
 */
OPAL_DECLSPEC extern mca_common_sm_module_t *
mca_common_sm_module_create_and_attach(size_t size,
                                       char *file_name,
                                       size_t size_ctl_structure,
                                       size_t data_seg_alignment);

/**
 *  This routine is used to attach to the shared memory segment associated with
 *  *seg_meta.  It is assumed that the shared memory segment associated with
 *  *seg_meta was already created with mca_common_sm_module_create.
 *
 *  @seg_meta points to shared memory information used for initialization and setup.
 *
 *  @returnvalue pointer to control structure at head of shared memory segment.
 *  Returns NULL if an error occurred.
 */
OPAL_DECLSPEC extern mca_common_sm_module_t *
mca_common_sm_module_attach(opal_shmem_ds_t *seg_meta,
                            size_t size_ctl_structure,
                            size_t data_seg_alignment);

/**
 * A thin wrapper around opal_shmem_unlink.
 *
 * @ modp points to an initialized mca_common_sm_module_t.
 *
 * @returnvalue OPAL_SUCCESS if the operation completed successfully,
 * OPAL_ERROR otherwise.
 */
OPAL_DECLSPEC extern int
mca_common_sm_module_unlink(mca_common_sm_module_t *modp);

/**
 * callback from the sm mpool
 */
OPAL_DECLSPEC extern void *mca_common_sm_seg_alloc (void *ctx, size_t *size);

/**
 * This function will release all local resources attached to the
 * shared memory segment. We assume that the operating system will
 * release the memory resources when the last process release it.
 *
 * @param mca_common_sm_module - instance that is shared between
 *                               components that use shared memory.
 *
 * @return OPAL_SUCCESS if everything was okay, otherwise return OPAL_ERROR.
 */

OPAL_DECLSPEC extern int
mca_common_sm_fini(mca_common_sm_module_t *mca_common_sm_module);

/**
 * instance that is shared between components that use shared memory.
 */
OPAL_DECLSPEC extern mca_common_sm_module_t *mca_common_sm_module;


END_C_DECLS

#endif /* _COMMON_SM_H_ */
