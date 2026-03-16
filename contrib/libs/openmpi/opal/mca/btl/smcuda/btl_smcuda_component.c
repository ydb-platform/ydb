/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2011 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2009-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2011-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "opal_config.h"
#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif  /* HAVE_FCNTL_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif  /* HAVE_SYS_MMAN_H */
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>  /* for mkfifo */
#endif  /* HAVE_SYS_STAT_H */

#include "opal/mca/shmem/base/base.h"
#include "opal/mca/shmem/shmem.h"
#include "opal/util/bit_ops.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"

#include "opal/mca/mpool/base/base.h"
#include "opal/mca/common/sm/common_sm.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "opal/runtime/opal_params.h"

#if OPAL_CUDA_SUPPORT
#include "opal/mca/common/cuda/common_cuda.h"
#endif /* OPAL_CUDA_SUPPORT */
#if OPAL_ENABLE_FT_CR    == 1
#include "opal/runtime/opal_cr.h"
#endif

#include "btl_smcuda.h"
#include "btl_smcuda_frag.h"
#include "btl_smcuda_fifo.h"

static int mca_btl_smcuda_component_open(void);
static int mca_btl_smcuda_component_close(void);
static int smcuda_register(void);
static mca_btl_base_module_t** mca_btl_smcuda_component_init(
    int *num_btls,
    bool enable_progress_threads,
    bool enable_mpi_threads
);

typedef enum {
    MCA_BTL_SM_RNDV_MOD_SM = 0,
    MCA_BTL_SM_RNDV_MOD_MPOOL
} mca_btl_sm_rndv_module_type_t;

/*
 * Shared Memory (SM) component instance.
 */
mca_btl_smcuda_component_t mca_btl_smcuda_component = {
    .super = {
        /* First, the mca_base_component_t struct containing meta information
          about the component itself */
        .btl_version = {
            MCA_BTL_DEFAULT_VERSION("smcuda"),
            .mca_open_component = mca_btl_smcuda_component_open,
            .mca_close_component = mca_btl_smcuda_component_close,
            .mca_register_component_params = smcuda_register,
        },
        .btl_data = {
            /* The component is checkpoint ready */
            .param_field = MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .btl_init = mca_btl_smcuda_component_init,
        .btl_progress = mca_btl_smcuda_component_progress,
    }  /* end super */
};


/*
 * utility routines for parameter registration
 */

static inline int mca_btl_smcuda_param_register_int(
    const char* param_name,
    int default_value,
    int level,
    int *storage)
{
    *storage = default_value;
    (void) mca_base_component_var_register (&mca_btl_smcuda_component.super.btl_version,
                                            param_name, NULL, MCA_BASE_VAR_TYPE_INT,
                                            NULL, 0, 0, level,
                                            MCA_BASE_VAR_SCOPE_READONLY, storage);
    return *storage;
}

static inline unsigned int mca_btl_smcuda_param_register_uint(
    const char* param_name,
    unsigned int default_value,
    int level,
    unsigned int *storage)
{
    *storage = default_value;
    (void) mca_base_component_var_register (&mca_btl_smcuda_component.super.btl_version,
                                            param_name, NULL, MCA_BASE_VAR_TYPE_UNSIGNED_INT,
                                            NULL, 0, 0, level,
                                            MCA_BASE_VAR_SCOPE_READONLY, storage);
    return *storage;
}

static int mca_btl_smcuda_component_verify(void) {

    return mca_btl_base_param_verify(&mca_btl_smcuda.super);
}

static int smcuda_register(void)
{
    /* register SM component parameters */
    mca_btl_smcuda_component.mpool_min_size = 134217728;
    (void) mca_base_component_var_register(&mca_btl_smcuda_component.super.btl_version, "min_size",
                                           "Minimum size of the common/sm mpool shared memory file",
                                           MCA_BASE_VAR_TYPE_UNSIGNED_LONG, NULL, 0, 0,
                                           OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_smcuda_component.mpool_min_size);

    mca_btl_smcuda_param_register_int("free_list_num", 8, OPAL_INFO_LVL_5, &mca_btl_smcuda_component.sm_free_list_num);
    mca_btl_smcuda_param_register_int("free_list_max", -1, OPAL_INFO_LVL_5, &mca_btl_smcuda_component.sm_free_list_max);
    mca_btl_smcuda_param_register_int("free_list_inc", 64, OPAL_INFO_LVL_5, &mca_btl_smcuda_component.sm_free_list_inc);
    mca_btl_smcuda_param_register_int("max_procs", -1, OPAL_INFO_LVL_5, &mca_btl_smcuda_component.sm_max_procs);
    /* there is no practical use for the mpool name parameter since mpool resources differ
       between components */
    mca_btl_smcuda_component.sm_mpool_name = "sm";
    mca_btl_smcuda_param_register_uint("fifo_size", 4096, OPAL_INFO_LVL_4, &mca_btl_smcuda_component.fifo_size);
    mca_btl_smcuda_param_register_int("num_fifos", 1, OPAL_INFO_LVL_4, &mca_btl_smcuda_component.nfifos);

    mca_btl_smcuda_param_register_uint("fifo_lazy_free", 120, OPAL_INFO_LVL_5, &mca_btl_smcuda_component.fifo_lazy_free);

    /* default number of extra procs to allow for future growth */
    mca_btl_smcuda_param_register_int("sm_extra_procs", 0, OPAL_INFO_LVL_9, &mca_btl_smcuda_component.sm_extra_procs);

    mca_btl_smcuda_component.allocator = "bucket";
    (void) mca_base_component_var_register (&mca_btl_smcuda_component.super.btl_version, "allocator",
                                            "Name of allocator component to use for btl/smcuda allocations",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_LOCAL, &mca_btl_smcuda_component.allocator);

#if OPAL_CUDA_SUPPORT
    /* Lower priority when CUDA support is not requested */
    if (opal_cuda_support) {
        mca_btl_smcuda.super.btl_exclusivity = MCA_BTL_EXCLUSIVITY_HIGH+1;
    } else {
        mca_btl_smcuda.super.btl_exclusivity = MCA_BTL_EXCLUSIVITY_LOW;
    }
    mca_btl_smcuda_param_register_int("use_cuda_ipc", 1, OPAL_INFO_LVL_4, &mca_btl_smcuda_component.use_cuda_ipc);
    mca_btl_smcuda_param_register_int("use_cuda_ipc_same_gpu", 1, OPAL_INFO_LVL_4,&mca_btl_smcuda_component.use_cuda_ipc_same_gpu);
    mca_btl_smcuda_param_register_int("cuda_ipc_verbose", 0, OPAL_INFO_LVL_4, &mca_btl_smcuda_component.cuda_ipc_verbose);
    mca_btl_smcuda_component.cuda_ipc_output = opal_output_open(NULL);
    opal_output_set_verbosity(mca_btl_smcuda_component.cuda_ipc_output, mca_btl_smcuda_component.cuda_ipc_verbose);
#else /* OPAL_CUDA_SUPPORT */
    mca_btl_smcuda.super.btl_exclusivity = MCA_BTL_EXCLUSIVITY_LOW;
#endif /* OPAL_CUDA_SUPPORT */
    mca_btl_smcuda.super.btl_eager_limit = 4*1024;
    mca_btl_smcuda.super.btl_rndv_eager_limit = 4*1024;
    mca_btl_smcuda.super.btl_max_send_size = 32*1024;
    mca_btl_smcuda.super.btl_rdma_pipeline_send_length = 64*1024;
    mca_btl_smcuda.super.btl_rdma_pipeline_frag_size = 64*1024;
    mca_btl_smcuda.super.btl_min_rdma_pipeline_size = 64*1024;
    mca_btl_smcuda.super.btl_flags = MCA_BTL_FLAGS_SEND;
    mca_btl_smcuda.super.btl_registration_handle_size = sizeof (mca_btl_base_registration_handle_t);
    mca_btl_smcuda.super.btl_bandwidth = 9000;  /* Mbs */
    mca_btl_smcuda.super.btl_latency   = 1;     /* Microsecs */

    /* Call the BTL based to register its MCA params */
    mca_btl_base_param_register(&mca_btl_smcuda_component.super.btl_version,
                                &mca_btl_smcuda.super);
#if OPAL_CUDA_SUPPORT
    /* If user has not set the value, then set to the defalt */
    if (0 == mca_btl_smcuda.super.btl_cuda_max_send_size) {
        mca_btl_smcuda.super.btl_cuda_max_send_size = 128*1024;
    }
    /* If user has not set the value, then set to magic number which will be converted to the minimum
     * size needed to fit the PML header (see pml_ob1.c) */
    if (0 == mca_btl_smcuda.super.btl_cuda_eager_limit) {
        mca_btl_smcuda.super.btl_cuda_eager_limit = SIZE_MAX; /* magic number */
    }
    mca_common_cuda_register_mca_variables();
#endif /* OPAL_CUDA_SUPPORT */
    return mca_btl_smcuda_component_verify();
}

/*
 *  Called by MCA framework to open the component, registers
 *  component parameters.
 */

static int mca_btl_smcuda_component_open(void)
{
    if (OPAL_SUCCESS != mca_btl_smcuda_component_verify()) {
        return OPAL_ERROR;
    }

    mca_btl_smcuda_component.sm_max_btls = 1;

    /* make sure the number of fifos is a power of 2 */
    mca_btl_smcuda_component.nfifos = opal_next_poweroftwo_inclusive (mca_btl_smcuda_component.nfifos);

    /* make sure that queue size and lazy free parameter are compatible */
    if (mca_btl_smcuda_component.fifo_lazy_free >= (mca_btl_smcuda_component.fifo_size >> 1) )
        mca_btl_smcuda_component.fifo_lazy_free  = (mca_btl_smcuda_component.fifo_size >> 1);
    if (mca_btl_smcuda_component.fifo_lazy_free <= 0)
        mca_btl_smcuda_component.fifo_lazy_free  = 1;

    mca_btl_smcuda_component.max_frag_size = mca_btl_smcuda.super.btl_max_send_size;
    mca_btl_smcuda_component.eager_limit = mca_btl_smcuda.super.btl_eager_limit;

#if OPAL_CUDA_SUPPORT
    /* Possibly adjust max_frag_size if the cuda size is bigger */
    if (mca_btl_smcuda.super.btl_cuda_max_send_size > mca_btl_smcuda.super.btl_max_send_size) {
        mca_btl_smcuda_component.max_frag_size = mca_btl_smcuda.super.btl_cuda_max_send_size;
    }
    opal_output_verbose(10, opal_btl_base_framework.framework_output,
                        "btl: smcuda: cuda_max_send_size=%d, max_send_size=%d, max_frag_size=%d",
                        (int)mca_btl_smcuda.super.btl_cuda_max_send_size, (int)mca_btl_smcuda.super.btl_max_send_size,
                        (int)mca_btl_smcuda_component.max_frag_size);
#endif /* OPAL_CUDA_SUPPORT */

    /* initialize objects */
    OBJ_CONSTRUCT(&mca_btl_smcuda_component.sm_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&mca_btl_smcuda_component.sm_frags_eager, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_smcuda_component.sm_frags_max, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_smcuda_component.sm_frags_user, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_smcuda_component.pending_send_fl, opal_free_list_t);
    return OPAL_SUCCESS;
}


/*
 * component cleanup - sanity checking of queue lengths
 */

static int mca_btl_smcuda_component_close(void)
{
    int return_value = OPAL_SUCCESS;


    OBJ_DESTRUCT(&mca_btl_smcuda_component.sm_lock);
    /**
     * We don't have to destroy the fragment lists. They are allocated
     * directly into the mmapped file, they will auto-magically disappear
     * when the file get unmapped.
     */
    /*OBJ_DESTRUCT(&mca_btl_smcuda_component.sm_frags_eager);*/
    /*OBJ_DESTRUCT(&mca_btl_smcuda_component.sm_frags_max);*/

    /* unmap the shared memory control structure */
    if(mca_btl_smcuda_component.sm_seg != NULL) {
        return_value = mca_common_sm_fini( mca_btl_smcuda_component.sm_seg );
        if( OPAL_SUCCESS != return_value ) {
            return_value = OPAL_ERROR;
            opal_output(0," mca_common_sm_fini failed\n");
            goto CLEANUP;
        }

        /* unlink file, so that it will be deleted when all references
         * to it are gone - no error checking, since we want all procs
         * to call this, so that in an abnormal termination scenario,
         * this file will still get cleaned up */
#if OPAL_ENABLE_FT_CR    == 1
        /* Only unlink the file if we are *not* restarting
         * If we are restarting the file will be unlinked at a later time.
         */
        if(OPAL_CR_STATUS_RESTART_PRE  != opal_cr_checkpointing_state &&
           OPAL_CR_STATUS_RESTART_POST != opal_cr_checkpointing_state ) {
            unlink(mca_btl_smcuda_component.sm_seg->shmem_ds.seg_name);
        }
#else
        unlink(mca_btl_smcuda_component.sm_seg->shmem_ds.seg_name);
#endif
        OBJ_RELEASE(mca_btl_smcuda_component.sm_seg);
    }

#if OPAL_ENABLE_PROGRESS_THREADS == 1
    /* close/cleanup fifo create for event notification */
    if(mca_btl_smcuda_component.sm_fifo_fd > 0) {
        /* write a done message down the pipe */
        unsigned char cmd = DONE;
        if( write(mca_btl_smcuda_component.sm_fifo_fd,&cmd,sizeof(cmd)) !=
                sizeof(cmd)){
            opal_output(0, "mca_btl_smcuda_component_close: write fifo failed: errno=%d\n",
                    errno);
        }
        opal_thread_join(&mca_btl_smcuda_component.sm_fifo_thread, NULL);
        close(mca_btl_smcuda_component.sm_fifo_fd);
        unlink(mca_btl_smcuda_component.sm_fifo_path);
    }
#endif

CLEANUP:

#if OPAL_CUDA_SUPPORT
    mca_common_cuda_fini();
#endif /* OPAL_CUDA_SUPPORT */

    /* return */
    return return_value;
}

/*
 * Returns the number of processes on the node.
 */
static inline int
get_num_local_procs(void)
{
    /* num_local_peers does not include us in
     * its calculation, so adjust for that */
    return (int)(1 + opal_process_info.num_local_peers);
}

static void
calc_sm_max_procs(int n)
{
    /* see if need to allocate space for extra procs */
    if (0 > mca_btl_smcuda_component.sm_max_procs) {
        /* no limit */
        if (0 <= mca_btl_smcuda_component.sm_extra_procs) {
            /* limit */
            mca_btl_smcuda_component.sm_max_procs =
                n + mca_btl_smcuda_component.sm_extra_procs;
        } else {
            /* no limit */
            mca_btl_smcuda_component.sm_max_procs = 2 * n;
        }
    }
}

static int
create_and_attach(mca_btl_smcuda_component_t *comp_ptr,
                  size_t size,
                  char *file_name,
                  size_t size_ctl_structure,
                  size_t data_seg_alignment,
                  mca_common_sm_module_t **out_modp)

{
    if (NULL == (*out_modp =
        mca_common_sm_module_create_and_attach(size, file_name,
                                               size_ctl_structure,
                                               data_seg_alignment))) {
        opal_output(0, "create_and_attach: unable to create shared memory "
                    "BTL coordinating strucure :: size %lu \n",
                    (unsigned long)size);
        return OPAL_ERROR;
    }
    return OPAL_SUCCESS;
}

static int
get_mpool_res_size(int32_t max_procs,
                   size_t *out_res_size)
{
    size_t size = 0;

    *out_res_size = 0;
    /* determine how much memory to create */
    /*
     * This heuristic formula mostly says that we request memory for:
     * - nfifos FIFOs, each comprising:
     *   . a sm_fifo_t structure
     *   . many pointers (fifo_size of them per FIFO)
     * - eager fragments (2*n of them, allocated in sm_free_list_inc chunks)
     * - max fragments (sm_free_list_num of them)
     *
     * On top of all that, we sprinkle in some number of
     * "opal_cache_line_size" additions to account for some
     * padding and edge effects that may lie in the allocator.
     */
    size = FIFO_MAP_NUM(max_procs) *
           (sizeof(sm_fifo_t) + sizeof(void *) *
            mca_btl_smcuda_component.fifo_size + 4 * opal_cache_line_size) +
           (2 * max_procs + mca_btl_smcuda_component.sm_free_list_inc) *
           (mca_btl_smcuda_component.eager_limit + 2 * opal_cache_line_size) +
           mca_btl_smcuda_component.sm_free_list_num *
           (mca_btl_smcuda_component.max_frag_size + 2 * opal_cache_line_size);

    /* add something for the control structure */
    size += sizeof(mca_common_sm_module_t);

    /* before we multiply by max_procs, make sure the result won't overflow */
    /* Stick that little pad in, particularly since we'll eventually
     * need a little extra space.  E.g., in mca_mpool_sm_init() in
     * mpool_sm_component.c when sizeof(mca_common_sm_module_t) is
     * added.
     */
    if (((double)size) * max_procs > LONG_MAX - 4096) {
        return OPAL_ERR_VALUE_OUT_OF_BOUNDS;
    }
    size *= (size_t)max_procs;
    *out_res_size = size;
    return OPAL_SUCCESS;
}


/* Generates all the unique paths for the shared-memory segments that this BTL
 * needs along with other file paths used to share "connection information". */
static int
set_uniq_paths_for_init_rndv(mca_btl_smcuda_component_t *comp_ptr)
{
    int rc = OPAL_ERR_OUT_OF_RESOURCE;

    /* NOTE: don't forget to free these after init */
    comp_ptr->sm_mpool_ctl_file_name = NULL;
    comp_ptr->sm_mpool_rndv_file_name = NULL;
    comp_ptr->sm_ctl_file_name = NULL;
    comp_ptr->sm_rndv_file_name = NULL;

    if (asprintf(&comp_ptr->sm_mpool_ctl_file_name,
                 "%s"OPAL_PATH_SEP"shared_mem_cuda_pool.%s",
                 opal_process_info.job_session_dir,
                 opal_process_info.nodename) < 0) {
        /* rc set */
        goto out;
    }
    if (asprintf(&comp_ptr->sm_mpool_rndv_file_name,
                 "%s"OPAL_PATH_SEP"shared_mem_cuda_pool_rndv.%s",
                 opal_process_info.job_session_dir,
                 opal_process_info.nodename) < 0) {
        /* rc set */
        goto out;
    }
    if (asprintf(&comp_ptr->sm_ctl_file_name,
                 "%s"OPAL_PATH_SEP"shared_mem_cuda_btl_module.%s",
                 opal_process_info.job_session_dir,
                 opal_process_info.nodename) < 0) {
        /* rc set */
        goto out;
    }
    if (asprintf(&comp_ptr->sm_rndv_file_name,
                 "%s"OPAL_PATH_SEP"shared_mem_cuda_btl_rndv.%s",
                 opal_process_info.job_session_dir,
                 opal_process_info.nodename) < 0) {
        /* rc set */
        goto out;
    }
    /* all is well */
    rc = OPAL_SUCCESS;

out:
    if (OPAL_SUCCESS != rc) {
        if (comp_ptr->sm_mpool_ctl_file_name) {
            free(comp_ptr->sm_mpool_ctl_file_name);
        }
        if (comp_ptr->sm_mpool_rndv_file_name) {
            free(comp_ptr->sm_mpool_rndv_file_name);
        }
        if (comp_ptr->sm_ctl_file_name) {
            free(comp_ptr->sm_ctl_file_name);
        }
        if (comp_ptr->sm_rndv_file_name) {
            free(comp_ptr->sm_rndv_file_name);
        }
    }
    return rc;
}

static int
create_rndv_file(mca_btl_smcuda_component_t *comp_ptr,
                  mca_btl_sm_rndv_module_type_t type)
{
    size_t size = 0;
    int rc = OPAL_SUCCESS;
    int fd = -1;
    char *fname = NULL;
    /* used as a temporary store so we can extract shmem_ds info */
    mca_common_sm_module_t *tmp_modp = NULL;

    if (MCA_BTL_SM_RNDV_MOD_MPOOL == type) {
        /* get the segment size for the sm mpool. */
        if (OPAL_SUCCESS != (rc = get_mpool_res_size(comp_ptr->sm_max_procs,
                                                     &size))) {
            /* rc is already set */
            goto out;
        }

        /* update size if less than required minimum */
        if (size < mca_btl_smcuda_component.mpool_min_size) {
            size = mca_btl_smcuda_component.mpool_min_size;
        }

        /* we only need the shmem_ds info at this point. initilization will be
         * completed in the mpool module code. the idea is that we just need this
         * info so we can populate the rndv file (or modex when we have it). */
        if (OPAL_SUCCESS != (rc =
            create_and_attach(comp_ptr, size, comp_ptr->sm_mpool_ctl_file_name,
                              sizeof(mca_common_sm_module_t), 8, &tmp_modp))) {
            /* rc is set */
            goto out;
        }
        fname = comp_ptr->sm_mpool_rndv_file_name;
    }
    else if (MCA_BTL_SM_RNDV_MOD_SM == type) {
        /* calculate the segment size. */
        size = sizeof(mca_common_sm_seg_header_t) +
               comp_ptr->sm_max_procs *
               (sizeof(sm_fifo_t *) +
                sizeof(char *) + sizeof(uint16_t)) +
               opal_cache_line_size;

        if (OPAL_SUCCESS != (rc =
            create_and_attach(comp_ptr, size, comp_ptr->sm_ctl_file_name,
                              sizeof(mca_common_sm_seg_header_t),
                              opal_cache_line_size, &comp_ptr->sm_seg))) {
            /* rc is set */
            goto out;
        }
        fname = comp_ptr->sm_rndv_file_name;
        tmp_modp = comp_ptr->sm_seg;
    }
    else {
        return OPAL_ERR_BAD_PARAM;
    }

    /* at this point, we have all the info we need to populate the rendezvous
     * file containing all the meta info required for attach. */

    /* now just write the contents of tmp_modp->shmem_ds to the full
     * sizeof(opal_shmem_ds_t), so we know where the mpool_res_size starts. */
    if (-1 == (fd = open(fname, O_CREAT | O_RDWR, 0600))) {
        int err = errno;
        opal_show_help("help-mpi-btl-smcuda.txt", "sys call fail", true,
                       "open(2)", strerror(err), err);
        rc = OPAL_ERR_IN_ERRNO;
        goto out;
    }
    if ((ssize_t)sizeof(opal_shmem_ds_t) != write(fd, &(tmp_modp->shmem_ds),
                                                  sizeof(opal_shmem_ds_t))) {
        int err = errno;
        opal_show_help("help-mpi-btl-smcuda.txt", "sys call fail", true,
                       "write(2)", strerror(err), err);
        rc = OPAL_ERR_IN_ERRNO;
        goto out;
    }
    if (MCA_BTL_SM_RNDV_MOD_MPOOL == type) {
        if ((ssize_t)sizeof(size) != write(fd, &size, sizeof(size))) {
            int err = errno;
            opal_show_help("help-mpi-btl-smcuda.txt", "sys call fail", true,
                           "write(2)", strerror(err), err);
            rc = OPAL_ERR_IN_ERRNO;
            goto out;
        }
        /* only do this for the mpool case */
        OBJ_RELEASE(tmp_modp);
    }

out:
    if (-1 != fd) {
        (void)close(fd);
    }
    return rc;
}

/*
 * Creates information required for the sm modex and modex sends it.
 */
static int
backing_store_init(mca_btl_smcuda_component_t *comp_ptr,
                   uint32_t local_rank)
{
    int rc = OPAL_SUCCESS;

    if (OPAL_SUCCESS != (rc = set_uniq_paths_for_init_rndv(comp_ptr))) {
        goto out;
    }
    /* only let the lowest rank setup the metadata */
    if (0 == local_rank) {
        /* === sm mpool === */
        if (OPAL_SUCCESS != (rc =
            create_rndv_file(comp_ptr, MCA_BTL_SM_RNDV_MOD_MPOOL))) {
            goto out;
        }
        /* === sm === */
        if (OPAL_SUCCESS != (rc =
            create_rndv_file(comp_ptr, MCA_BTL_SM_RNDV_MOD_SM))) {
            goto out;
        }
    }

out:
    return rc;
}

#if OPAL_CUDA_SUPPORT

/**
 * Send a CUDA IPC ACK or NOTREADY message back to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 * @param peer (IN)     If ready, then send ACK
 */
static void mca_btl_smcuda_send_cuda_ipc_ack(struct mca_btl_base_module_t* btl,
                                             struct mca_btl_base_endpoint_t* endpoint, int ready)
{
    mca_btl_smcuda_frag_t* frag;
    ctrlhdr_t ctrlhdr;
    int rc;

    if ( mca_btl_smcuda_component.num_outstanding_frags * 2 > (int) mca_btl_smcuda_component.fifo_size ) {
        mca_btl_smcuda_component_progress();
    }

    /* allocate a fragment, giving up if we can't get one */
    MCA_BTL_SMCUDA_FRAG_ALLOC_EAGER(frag);
    if( OPAL_UNLIKELY(NULL == frag) ) {
        endpoint->ipcstate = IPC_BAD;
        return;
    }

    if (ready) {
        ctrlhdr.ctag = IPC_ACK;
    } else {
        ctrlhdr.ctag = IPC_NOTREADY;
    }

    /* Fill in fragment fields. */
    frag->hdr->tag = MCA_BTL_TAG_SMCUDA;
    frag->base.des_flags = MCA_BTL_DES_FLAGS_BTL_OWNERSHIP;
    frag->endpoint = endpoint;
    memcpy(frag->segment.seg_addr.pval, &ctrlhdr, sizeof(struct ctrlhdr_st));

    /* write the fragment pointer to the FIFO */
    /*
     * Note that we don't care what the FIFO-write return code is.  Even if
     * the return code indicates failure, the write has still "completed" from
     * our point of view:  it has been posted to a "pending send" queue.
     */
    OPAL_THREAD_ADD_FETCH32(&mca_btl_smcuda_component.num_outstanding_frags, +1);

    MCA_BTL_SMCUDA_FIFO_WRITE(endpoint, endpoint->my_smp_rank,
                              endpoint->peer_smp_rank, (void *) VIRTUAL2RELATIVE(frag->hdr), false, true, rc);

    /* Set state now that we have sent message */
    if (ready) {
        endpoint->ipcstate = IPC_ACKED;
    } else {
        endpoint->ipcstate = IPC_INIT;
    }

    return;

}
/* This function is utilized to set up CUDA IPC support within the smcuda
 * BTL.  It handles smcuda specific control messages that are triggered
 * when GPU memory transfers are initiated. */
static void btl_smcuda_control(mca_btl_base_module_t* btl,
                               mca_btl_base_tag_t tag,
                               mca_btl_base_descriptor_t* des, void* cbdata)
{
    int mydevnum, ipcaccess, res;
    ctrlhdr_t ctrlhdr;
    opal_proc_t *ep_proc;
    struct mca_btl_base_endpoint_t *endpoint;
    mca_btl_smcuda_t *smcuda_btl = (mca_btl_smcuda_t *)btl;
    mca_btl_smcuda_frag_t *frag = (mca_btl_smcuda_frag_t *)des;
    mca_btl_base_segment_t* segments = des->des_segments;

    /* Use the rank of the peer that sent the data to get to the endpoint
     * structure.  This is needed for PML callback. */
    endpoint = mca_btl_smcuda_component.sm_peers[frag->hdr->my_smp_rank];
    ep_proc = endpoint->proc_opal;

    /* Copy out control message payload to examine it */
    memcpy(&ctrlhdr, segments->seg_addr.pval, sizeof(struct ctrlhdr_st));

    /* Handle an incoming CUDA IPC control message. */
    switch (ctrlhdr.ctag) {
    case IPC_REQ:
        /* Initial request to set up IPC.  If the state of IPC
         * initialization is IPC_INIT, then check on the peer to peer
         * access and act accordingly.  If we are in the IPC_SENT
         * state, then this means both sides are trying to set up the
         * connection.  If my smp rank is higher then check and act
         * accordingly.  Otherwise, drop the request and let the other
         * side continue the handshake. */
        OPAL_THREAD_LOCK(&endpoint->endpoint_lock);
        if ((IPC_INIT == endpoint->ipcstate) ||
            ((IPC_SENT == endpoint->ipcstate) && (endpoint->my_smp_rank > endpoint->peer_smp_rank))) {
            endpoint->ipcstate = IPC_ACKING; /* Move into new state to prevent any new connection attempts */
            OPAL_THREAD_UNLOCK(&endpoint->endpoint_lock);

            /* If not yet CUDA ready, send a NOTREADY message back. */
            if (!mca_common_cuda_enabled) {
                opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                    "Sending CUDA IPC NOTREADY: myrank=%d, peerrank=%d",
                                    mca_btl_smcuda_component.my_smp_rank,
                                    endpoint->peer_smp_rank);
                mca_btl_smcuda_send_cuda_ipc_ack(btl, endpoint, 0);
                return;
            }

            /* Get my current device.  If this fails, move this endpoint state into
             * bad state.  No need to send a reply.  */
            res = mca_common_cuda_get_device(&mydevnum);
            if (0 != res) {
                endpoint->ipcstate = IPC_BAD;
                return;
            }

            /* Check for IPC support between devices. If they are the
             * same device and use_cuda_ipc_same_gpu is 1 (default),
             * then assume CUDA IPC is possible.  This could be a
             * device running in DEFAULT mode or running under MPS.
             * Otherwise, check peer acces to determine CUDA IPC
             * support.  If the CUDA API call fails, then just move
             * endpoint into bad state.  No need to send a reply. */
            if (mydevnum == ctrlhdr.cudev) {
                if (mca_btl_smcuda_component.use_cuda_ipc_same_gpu) {
                    ipcaccess = 1;
                } else {
                    opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                        "Analyzed CUDA IPC request: myrank=%d, mydev=%d, peerrank=%d, "
                                        "peerdev=%d --> Access is disabled by btl_smcuda_use_cuda_ipc_same_gpu",
                                        endpoint->my_smp_rank, mydevnum, endpoint->peer_smp_rank,
                                        ctrlhdr.cudev);
                    endpoint->ipcstate = IPC_BAD;
                    return;
                }
            } else {
                res = mca_common_cuda_device_can_access_peer(&ipcaccess, mydevnum, ctrlhdr.cudev);
                if (0 != res) {
                    opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                        "Analyzed CUDA IPC request: myrank=%d, mydev=%d, peerrank=%d, "
                                        "peerdev=%d --> Access is disabled because peer check failed with err=%d",
                                        endpoint->my_smp_rank, mydevnum, endpoint->peer_smp_rank,
                                        ctrlhdr.cudev, res);
                    endpoint->ipcstate = IPC_BAD;
                    return;
                }
            }

            assert(endpoint->peer_smp_rank == frag->hdr->my_smp_rank);
            opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                "Analyzed CUDA IPC request: myrank=%d, mydev=%d, peerrank=%d, "
                                "peerdev=%d --> ACCESS=%d",
                                endpoint->my_smp_rank, mydevnum, endpoint->peer_smp_rank,
                                ctrlhdr.cudev, ipcaccess);

            if (0 == ipcaccess) {
                /* No CUDA IPC support */
                opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                    "Not sending CUDA IPC ACK, no P2P support");
                endpoint->ipcstate = IPC_BAD;
            } else {
                /* CUDA IPC works */
                smcuda_btl->error_cb(&smcuda_btl->super, MCA_BTL_ERROR_FLAGS_ADD_CUDA_IPC,
                                     ep_proc, (char *)&mca_btl_smcuda_component.cuda_ipc_output);
                opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                    "Sending CUDA IPC ACK:  myrank=%d, mydev=%d, peerrank=%d, peerdev=%d",
                                    endpoint->my_smp_rank, mydevnum, endpoint->peer_smp_rank,
                                    ctrlhdr.cudev);
                mca_btl_smcuda_send_cuda_ipc_ack(btl, endpoint, 1);
            }
        } else {
            OPAL_THREAD_UNLOCK(&endpoint->endpoint_lock);
            opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                                "Not sending CUDA IPC ACK because request already initiated");
        }
        break;

    case IPC_ACK:
        opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                            "Received CUDA IPC ACK, notifying PML: myrank=%d, peerrank=%d",
                            endpoint->my_smp_rank, endpoint->peer_smp_rank);

        smcuda_btl->error_cb(&smcuda_btl->super, MCA_BTL_ERROR_FLAGS_ADD_CUDA_IPC,
                             ep_proc, (char *)&mca_btl_smcuda_component.cuda_ipc_output);
        assert(endpoint->ipcstate == IPC_SENT);
        endpoint->ipcstate = IPC_ACKED;
        break;

    case IPC_NOTREADY:
        /* The remote side is not ready.  Reset state to initialized so next
         * send call will try again to set up connection. */
        opal_output_verbose(10, mca_btl_smcuda_component.cuda_ipc_output,
                            "Received CUDA IPC NOTREADY, reset state to allow another attempt: "
                            "myrank=%d, peerrank=%d",
                            endpoint->my_smp_rank, endpoint->peer_smp_rank);
        OPAL_THREAD_LOCK(&endpoint->endpoint_lock);
        if (IPC_SENT == endpoint->ipcstate) {
            endpoint->ipcstate = IPC_INIT;
        }
        OPAL_THREAD_UNLOCK(&endpoint->endpoint_lock);
        break;

    default:
        opal_output(0, "Received UNKNOWN CUDA IPC control message. This should not happen.");
    }
}

#endif /* OPAL_CUDA_SUPPORT */

/*
 *  SM component initialization
 */
static mca_btl_base_module_t **
mca_btl_smcuda_component_init(int *num_btls,
                          bool enable_progress_threads,
                          bool enable_mpi_threads)
{
    int num_local_procs = 0;
    mca_btl_base_module_t **btls = NULL;
    uint32_t my_local_rank = UINT32_MAX;

    *num_btls = 0;
    /* lookup/create shared memory pool only when used */
    mca_btl_smcuda_component.sm_mpool = NULL;
    mca_btl_smcuda_component.sm_mpool_base = NULL;

#if OPAL_CUDA_SUPPORT
    mca_common_cuda_stage_one_init();
#endif /* OPAL_CUDA_SUPPORT */

    /* if no session directory was created, then we cannot be used */
    if (NULL == opal_process_info.job_session_dir) {
    /* SKG - this isn't true anymore. Some backing facilities don't require a
     * file-backed store. Extend shmem to provide this info one day. Especially
     * when we use a proper modex for init. */
        return NULL;
    }
    /* if we don't have locality information, then we cannot be used because we
     * need to know who the respective node ranks for initialization. note the
     * use of my_local_rank here. we use this instead of my_node_rank because in
     * the spawn case we need to designate a metadata creator rank within the
     * set of processes that are initializing the btl, and my_local_rank seems
     * to provide that for us. */
    if (UINT32_MAX ==
        (my_local_rank = opal_process_info.my_local_rank)) {
        opal_show_help("help-mpi-btl-smcuda.txt", "no locality", true);
        return NULL;
    }
    /* no use trying to use sm with less than two procs, so just bail. */
    if ((num_local_procs = get_num_local_procs()) < 2) {
        return NULL;
    }
    /* calculate max procs so we can figure out how large to make the
     * shared-memory segment. this routine sets component sm_max_procs. */
    calc_sm_max_procs(num_local_procs);

    /* This is where the modex will live some day. For now, just have local rank
     * 0 create a rendezvous file containing the backing store info, so the
     * other local procs can read from it during add_procs. The rest will just
     * stash the known paths for use later in init. */
    if (OPAL_SUCCESS != backing_store_init(&mca_btl_smcuda_component,
                                           my_local_rank)) {
        return NULL;
    }

#if OPAL_ENABLE_PROGRESS_THREADS == 1
    /* create a named pipe to receive events  */
    sprintf( mca_btl_smcuda_component.sm_fifo_path,
             "%s"OPAL_PATH_SEP"sm_fifo.%lu", opal_process_info.job_session_dir,
             (unsigned long)OPAL_PROC_MY_NAME->vpid );
    if(mkfifo(mca_btl_smcuda_component.sm_fifo_path, 0660) < 0) {
        opal_output(0, "mca_btl_smcuda_component_init: mkfifo failed with errno=%d\n",errno);
        return NULL;
    }
    mca_btl_smcuda_component.sm_fifo_fd = open(mca_btl_smcuda_component.sm_fifo_path,
                                           O_RDWR);
    if(mca_btl_smcuda_component.sm_fifo_fd < 0) {
        opal_output(0, "mca_btl_smcuda_component_init: "
                   "open(%s) failed with errno=%d\n",
                    mca_btl_smcuda_component.sm_fifo_path, errno);
        return NULL;
    }

    OBJ_CONSTRUCT(&mca_btl_smcuda_component.sm_fifo_thread, opal_thread_t);
    mca_btl_smcuda_component.sm_fifo_thread.t_run =
        (opal_thread_fn_t)mca_btl_smcuda_component_event_thread;
    opal_thread_start(&mca_btl_smcuda_component.sm_fifo_thread);
#endif

    mca_btl_smcuda_component.sm_btls =
        (mca_btl_smcuda_t **)malloc(mca_btl_smcuda_component.sm_max_btls *
                                sizeof(mca_btl_smcuda_t *));
    if (NULL == mca_btl_smcuda_component.sm_btls) {
        return NULL;
    }

    /* allocate the Shared Memory BTL */
    *num_btls = 1;
    btls = (mca_btl_base_module_t**)malloc(sizeof(mca_btl_base_module_t*));
    if (NULL == btls) {
        return NULL;
    }

    /* get pointer to the btls */
    btls[0] = (mca_btl_base_module_t*)(&(mca_btl_smcuda));
    mca_btl_smcuda_component.sm_btls[0] = (mca_btl_smcuda_t*)(&(mca_btl_smcuda));

    /* initialize some BTL data */
    /* start with no SM procs */
    mca_btl_smcuda_component.num_smp_procs = 0;
    mca_btl_smcuda_component.my_smp_rank   = -1;  /* not defined */
    mca_btl_smcuda_component.sm_num_btls   = 1;
    /* set flag indicating btl not inited */
    mca_btl_smcuda.btl_inited = false;

#if OPAL_CUDA_SUPPORT
    /* Assume CUDA GET works. */
    mca_btl_smcuda.super.btl_get = mca_btl_smcuda_get_cuda;
    /* Register a smcuda control function to help setup IPC support */
    mca_btl_base_active_message_trigger[MCA_BTL_TAG_SMCUDA].cbfunc = btl_smcuda_control;
    mca_btl_base_active_message_trigger[MCA_BTL_TAG_SMCUDA].cbdata = NULL;
#endif /* OPAL_CUDA_SUPPORT */

    return btls;

}


/*
 *  SM component progress.
 */

#if OPAL_ENABLE_PROGRESS_THREADS == 1
void mca_btl_smcuda_component_event_thread(opal_object_t* thread)
{
    while(1) {
        unsigned char cmd;
        if(read(mca_btl_smcuda_component.sm_fifo_fd, &cmd, sizeof(cmd)) != sizeof(cmd)) {
            /* error condition */
            return;
        }
        if( DONE == cmd ){
            /* return when done message received */
            return;
        }
        mca_btl_smcuda_component_progress();
    }
}
#endif

void btl_smcuda_process_pending_sends(struct mca_btl_base_endpoint_t *ep)
{
    btl_smcuda_pending_send_item_t *si;
    int rc;

    while ( 0 < opal_list_get_size(&ep->pending_sends) ) {
        /* Note that we access the size of ep->pending_sends unlocked
           as it doesn't really matter if the result is wrong as
           opal_list_remove_first is called with a lock and we handle it
           not finding an item to process */
        OPAL_THREAD_LOCK(&ep->endpoint_lock);
        si = (btl_smcuda_pending_send_item_t*)opal_list_remove_first(&ep->pending_sends);
        OPAL_THREAD_UNLOCK(&ep->endpoint_lock);

        if(NULL == si) return; /* Another thread got in before us. Thats ok. */

        OPAL_THREAD_ADD_FETCH32(&mca_btl_smcuda_component.num_pending_sends, -1);

        MCA_BTL_SMCUDA_FIFO_WRITE(ep, ep->my_smp_rank, ep->peer_smp_rank, si->data,
                          true, false, rc);

        opal_free_list_return (&mca_btl_smcuda_component.pending_send_fl, (opal_free_list_item_t*)si);

        if ( OPAL_SUCCESS != rc )
            return;
    }
}

int mca_btl_smcuda_component_progress(void)
{
    /* local variables */
    mca_btl_base_segment_t seg;
    mca_btl_smcuda_frag_t *frag;
    mca_btl_smcuda_frag_t Frag;
    sm_fifo_t *fifo = NULL;
    mca_btl_smcuda_hdr_t *hdr;
    int my_smp_rank = mca_btl_smcuda_component.my_smp_rank;
    int peer_smp_rank, j, rc = 0, nevents = 0;

    /* first, deal with any pending sends */
    /* This check should be fast since we only need to check one variable. */
    if ( 0 < mca_btl_smcuda_component.num_pending_sends ) {

        /* perform a loop to find the endpoints that have pending sends */
        /* This can take a while longer if there are many endpoints to check. */
        for ( peer_smp_rank = 0; peer_smp_rank < mca_btl_smcuda_component.num_smp_procs; peer_smp_rank++) {
            struct mca_btl_base_endpoint_t* endpoint;
            if ( peer_smp_rank == my_smp_rank )
                continue;
            endpoint = mca_btl_smcuda_component.sm_peers[peer_smp_rank];
            if ( 0 < opal_list_get_size(&endpoint->pending_sends) )
                btl_smcuda_process_pending_sends(endpoint);
        }
    }

    /* poll each fifo */
    for(j = 0; j < FIFO_MAP_NUM(mca_btl_smcuda_component.num_smp_procs); j++) {
        fifo = &(mca_btl_smcuda_component.fifo[my_smp_rank][j]);
      recheck_peer:
        /* aquire thread lock */
        if(opal_using_threads()) {
            opal_atomic_lock(&(fifo->tail_lock));
        }

        hdr = (mca_btl_smcuda_hdr_t *)sm_fifo_read(fifo);

        /* release thread lock */
        if(opal_using_threads()) {
            opal_atomic_unlock(&(fifo->tail_lock));
        }

        if(SM_FIFO_FREE == hdr) {
            continue;
        }

        nevents++;
        /* dispatch fragment by type */
        switch(((uintptr_t)hdr) & MCA_BTL_SMCUDA_FRAG_TYPE_MASK) {
            case MCA_BTL_SMCUDA_FRAG_SEND:
            {
                mca_btl_active_message_callback_t* reg;
                /* change the address from address relative to the shared
                 * memory address, to a true virtual address */
                hdr = (mca_btl_smcuda_hdr_t *) RELATIVE2VIRTUAL(hdr);
                peer_smp_rank = hdr->my_smp_rank;
#if OPAL_ENABLE_DEBUG
                if ( FIFO_MAP(peer_smp_rank) != j ) {
                    opal_output(0, "mca_btl_smcuda_component_progress: "
                                "rank %d got %d on FIFO %d, but this sender should send to FIFO %d\n",
                                my_smp_rank, peer_smp_rank, j, FIFO_MAP(peer_smp_rank));
                }
#endif
                /* recv upcall */
                reg = mca_btl_base_active_message_trigger + hdr->tag;
                seg.seg_addr.pval = ((char *)hdr) + sizeof(mca_btl_smcuda_hdr_t);
                seg.seg_len = hdr->len;
                Frag.base.des_segment_count = 1;
                Frag.base.des_segments = &seg;
#if OPAL_CUDA_SUPPORT
                Frag.hdr = hdr;  /* needed for peer rank in control messages */
#endif /* OPAL_CUDA_SUPPORT */
                reg->cbfunc(&mca_btl_smcuda.super, hdr->tag, &(Frag.base),
                            reg->cbdata);
                /* return the fragment */
                MCA_BTL_SMCUDA_FIFO_WRITE(
                        mca_btl_smcuda_component.sm_peers[peer_smp_rank],
                        my_smp_rank, peer_smp_rank, hdr->frag, false, true, rc);
                break;
            }
        case MCA_BTL_SMCUDA_FRAG_ACK:
            {
                int status = (uintptr_t)hdr & MCA_BTL_SMCUDA_FRAG_STATUS_MASK;
                int btl_ownership;
                struct mca_btl_base_endpoint_t* endpoint;

                frag = (mca_btl_smcuda_frag_t *)((char*)((uintptr_t)hdr &
                                                     (~(MCA_BTL_SMCUDA_FRAG_TYPE_MASK |
                                                        MCA_BTL_SMCUDA_FRAG_STATUS_MASK))));

                endpoint = frag->endpoint;
                btl_ownership = (frag->base.des_flags & MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
                if( MCA_BTL_DES_SEND_ALWAYS_CALLBACK & frag->base.des_flags ) {
                    /* completion callback */
                    frag->base.des_cbfunc(&mca_btl_smcuda.super, frag->endpoint,
                                          &frag->base, status?OPAL_ERROR:OPAL_SUCCESS);
                }
                if( btl_ownership ) {
                    MCA_BTL_SMCUDA_FRAG_RETURN(frag);
                }
                OPAL_THREAD_ADD_FETCH32(&mca_btl_smcuda_component.num_outstanding_frags, -1);
                if ( 0 < opal_list_get_size(&endpoint->pending_sends) ) {
                    btl_smcuda_process_pending_sends(endpoint);
                }
                goto recheck_peer;
            }
            default:
                /* unknown */
                /*
                 * This code path should presumably never be called.
                 * It's unclear if it should exist or, if so, how it should be written.
                 * If we want to return it to the sending process,
                 * we have to figure out who the sender is.
                 * It seems we need to subtract the mask bits.
                 * Then, hopefully this is an sm header that has an smp_rank field.
                 * Presumably that means the received header was relative.
                 * Or, maybe this code should just be removed.
                 */
                opal_output(0, "mca_btl_smcuda_component_progress read an unknown type of header");
                hdr = (mca_btl_smcuda_hdr_t *) RELATIVE2VIRTUAL(hdr);
                peer_smp_rank = hdr->my_smp_rank;
                hdr = (mca_btl_smcuda_hdr_t*)((uintptr_t)hdr->frag |
                        MCA_BTL_SMCUDA_FRAG_STATUS_MASK);
                MCA_BTL_SMCUDA_FIFO_WRITE(
                        mca_btl_smcuda_component.sm_peers[peer_smp_rank],
                        my_smp_rank, peer_smp_rank, hdr, false, true, rc);
                break;
        }
    }
    (void)rc; /* this is safe to ignore as the message is requeued till success */

#if OPAL_CUDA_SUPPORT
    /* Check to see if there are any outstanding CUDA events that have
     * completed.  If so, issue the PML callbacks on the fragments.
     */
    while (1 == progress_one_cuda_ipc_event((mca_btl_base_descriptor_t **)&frag)) {
        mca_btl_base_rdma_completion_fn_t cbfunc = (mca_btl_base_rdma_completion_fn_t) frag->base.des_cbfunc;

        cbfunc (&mca_btl_smcuda.super, frag->endpoint, frag->segment.seg_addr.pval,
                frag->local_handle, frag->base.des_context, frag->base.des_cbdata,
                OPAL_SUCCESS);

        if(frag->registration != NULL) {
            frag->endpoint->rcache->rcache_deregister (frag->endpoint->rcache,
                                                       (mca_rcache_base_registration_t*)frag->registration);
            frag->registration = NULL;
            MCA_BTL_SMCUDA_FRAG_RETURN(frag);
        }
        nevents++;
    }
#endif /* OPAL_CUDA_SUPPORT */
    return nevents;
}
