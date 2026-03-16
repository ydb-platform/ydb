/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2009 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/* ASSUMING local process homogeneity with respect to all utilized shared memory
 * facilities. that is, if one local process deems a particular shared memory
 * facility acceptable, then ALL local processes should be able to utilize that
 * facility. as it stands, this is an important point because one process
 * dictates to all other local processes which common sm component will be
 * selected based on its own, local run-time test.
 */

#include "opal_config.h"

#include "opal/align.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"
#include "opal/util/error.h"
#include "opal/mca/shmem/base/base.h"
#if OPAL_ENABLE_FT_CR == 1
#include "opal/runtime/opal_cr.h"
#endif
#include "common_sm.h"
#include "opal/constants.h"


OBJ_CLASS_INSTANCE(mca_common_sm_module_t,opal_list_item_t,
                   NULL, NULL);


/* ////////////////////////////////////////////////////////////////////////// */
/* static utility functions */
/* ////////////////////////////////////////////////////////////////////////// */

/* ////////////////////////////////////////////////////////////////////////// */
static mca_common_sm_module_t *
attach_and_init(opal_shmem_ds_t *shmem_bufp,
                size_t size,
                size_t size_ctl_structure,
                size_t data_seg_alignment,
                bool first_call)
{
    mca_common_sm_module_t *map = NULL;
    mca_common_sm_seg_header_t *seg = NULL;
    unsigned char *addr = NULL;

    /* attach to the specified segment. note that at this point, the contents of
     * *shmem_bufp have already been initialized via opal_shmem_segment_create.
     */
    if (NULL == (seg = (mca_common_sm_seg_header_t *)
                       opal_shmem_segment_attach(shmem_bufp))) {
        return NULL;
    }
    opal_atomic_rmb();

    if (NULL == (map = OBJ_NEW(mca_common_sm_module_t))) {
        OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);
        (void)opal_shmem_segment_detach(shmem_bufp);
        return NULL;
    }

    /* copy meta information into common sm module
     *                                     from ====> to                */
    if (OPAL_SUCCESS != opal_shmem_ds_copy(shmem_bufp, &map->shmem_ds)) {
        (void)opal_shmem_segment_detach(shmem_bufp);
        free(map);
        return NULL;
    }

    /* the first entry in the file is the control structure. the first
     * entry in the control structure is an mca_common_sm_seg_header_t
     * element.
     */
    map->module_seg = seg;

    addr = ((unsigned char *)seg) + size_ctl_structure;
    /* if we have a data segment (i.e., if 0 != data_seg_alignment),
     * then make it the first aligned address after the control
     * structure.  IF THIS HAPPENS, THIS IS A PROGRAMMING ERROR IN
     * OPEN MPI!
     */
    if (0 != data_seg_alignment) {
        addr = OPAL_ALIGN_PTR(addr, data_seg_alignment, unsigned char *);
        /* is addr past end of the shared memory segment? */
        if ((unsigned char *)seg + shmem_bufp->seg_size < addr) {
            opal_show_help("help-mpi-common-sm.txt", "mmap too small", 1,
                           opal_proc_local_get()->proc_hostname,
                           (unsigned long)shmem_bufp->seg_size,
                           (unsigned long)size_ctl_structure,
                           (unsigned long)data_seg_alignment);
            (void)opal_shmem_segment_detach(shmem_bufp);
            free(map);
            return NULL;
        }
    }

    map->module_data_addr = addr;
    map->module_seg_addr = (unsigned char *)seg;

    /* note that size is only used during the first call */
    if (first_call) {
        /* initialize some segment information */
        size_t mem_offset = map->module_data_addr -
                            (unsigned char *)map->module_seg;
        opal_atomic_lock_init(&map->module_seg->seg_lock, OPAL_ATOMIC_LOCK_UNLOCKED);
        map->module_seg->seg_inited = 0;
        map->module_seg->seg_num_procs_inited = 0;
        map->module_seg->seg_offset = mem_offset;
        map->module_seg->seg_size = size - mem_offset;
        opal_atomic_wmb();
    }

    /* increment the number of processes that are attached to the segment. */
    (void)opal_atomic_add_fetch_size_t(&map->module_seg->seg_num_procs_inited, 1);

    /* commit the changes before we return */
    opal_atomic_wmb();

    return map;
}

/* ////////////////////////////////////////////////////////////////////////// */
/* api implementation */
/* ////////////////////////////////////////////////////////////////////////// */

/* ////////////////////////////////////////////////////////////////////////// */
mca_common_sm_module_t *
mca_common_sm_module_create_and_attach(size_t size,
                                       char *file_name,
                                       size_t size_ctl_structure,
                                       size_t data_seg_alignment)
{
    mca_common_sm_module_t *map = NULL;
    opal_shmem_ds_t *seg_meta = NULL;

    if (NULL == (seg_meta = calloc(1, sizeof(*seg_meta)))) {
        /* out of resources */
        return NULL;
    }
    if (OPAL_SUCCESS == opal_shmem_segment_create(seg_meta, file_name, size)) {
        map = attach_and_init(seg_meta, size, size_ctl_structure,
                              data_seg_alignment, true);
    }
    /* at this point, seg_meta has been copied to the newly created
     * shared memory segment, so we can free it */
    if (seg_meta) {
        free(seg_meta);
    }

    return map;
}

/* ////////////////////////////////////////////////////////////////////////// */
/**
 * @return a pointer to the mca_common_sm_module_t associated with seg_meta if
 * everything was okay, otherwise returns NULL.
 */
mca_common_sm_module_t *
mca_common_sm_module_attach(opal_shmem_ds_t *seg_meta,
                            size_t size_ctl_structure,
                            size_t data_seg_alignment)
{
    /* notice that size is 0 here. it really doesn't matter because size WILL
     * NOT be used because this is an attach (first_call is false). */
    return attach_and_init(seg_meta, 0, size_ctl_structure,
                           data_seg_alignment, false);
}

/* ////////////////////////////////////////////////////////////////////////// */
int
mca_common_sm_module_unlink(mca_common_sm_module_t *modp)
{
    if (NULL == modp) {
        return OPAL_ERROR;
    }
    if (OPAL_SUCCESS != opal_shmem_unlink(&modp->shmem_ds)) {
        return OPAL_ERROR;
    }
    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
int
mca_common_sm_local_proc_reorder(opal_proc_t **procs,
                                 size_t num_procs,
                                 size_t *out_num_local_procs)
{
    size_t num_local_procs = 0;
    bool found_lowest = false;
    opal_proc_t *temp_proc = NULL;
    size_t p;

    if (NULL == out_num_local_procs || NULL == procs) {
        return OPAL_ERR_BAD_PARAM;
    }
    /* o reorder procs array to have all the local procs at the beginning.
     * o look for the local proc with the lowest name.
     * o determine the number of local procs.
     * o ensure that procs[0] is the lowest named process.
     */
    for (p = 0; p < num_procs; ++p) {
        if (OPAL_PROC_ON_LOCAL_NODE(procs[p]->proc_flags)) {
            /* if we don't have a lowest, save the first one */
            if (!found_lowest) {
                procs[0] = procs[p];
                found_lowest = true;
            }
            else {
                /* save this proc */
                procs[num_local_procs] = procs[p];
                /* if we have a new lowest, swap it with position 0
                 * so that procs[0] is always the lowest named proc */
                if( 0 > opal_compare_proc(procs[p]->proc_name, procs[0]->proc_name) ) {
                    temp_proc = procs[0];
                    procs[0] = procs[p];
                    procs[num_local_procs] = temp_proc;
                }
            }
            /* regardless of the comparisons above, we found
             * another proc on the local node, so increment
             */
            ++num_local_procs;
        }
    }
    *out_num_local_procs = num_local_procs;

    return OPAL_SUCCESS;
}

/* ////////////////////////////////////////////////////////////////////////// */
/**
 *  allocate memory from a previously allocated shared memory
 *  block.
 *
 *  @param size size of request, in bytes (IN)
 *
 *  @retval addr virtual address
 */
void *mca_common_sm_seg_alloc (void *ctx, size_t *size)
{
    mca_common_sm_module_t *sm_module = (mca_common_sm_module_t *) ctx;
    mca_common_sm_seg_header_t *seg = sm_module->module_seg;
    void *addr;

    opal_atomic_lock(&seg->seg_lock);
    if (seg->seg_offset + *size > seg->seg_size) {
        addr = NULL;
    }
    else {
        size_t fixup;

        /* add base address to segment offset */
        addr = sm_module->module_data_addr + seg->seg_offset;
        seg->seg_offset += *size;

        /* fix up seg_offset so next allocation is aligned on a
         * sizeof(long) boundry.  Do it here so that we don't have to
         * check before checking remaining size in buffer
         */
        if ((fixup = (seg->seg_offset & (sizeof(long) - 1))) > 0) {
            seg->seg_offset += sizeof(long) - fixup;
        }
    }

    opal_atomic_unlock(&seg->seg_lock);
    return addr;
}

/* ////////////////////////////////////////////////////////////////////////// */
int
mca_common_sm_fini(mca_common_sm_module_t *mca_common_sm_module)
{
    int rc = OPAL_SUCCESS;

    if (NULL != mca_common_sm_module->module_seg) {
        if (OPAL_SUCCESS !=
            opal_shmem_segment_detach(&mca_common_sm_module->shmem_ds)) {
            rc = OPAL_ERROR;
        }
    }
    return rc;
}
