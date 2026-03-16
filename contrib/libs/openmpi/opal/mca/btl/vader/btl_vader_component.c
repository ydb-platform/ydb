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
 * Copyright (c) 2010-2018 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2019 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2018      Amazon.com, Inc. or its affiliates.  All Rights reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "opal_config.h"

#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/threads/mutex.h"
#include "opal/mca/btl/base/btl_base_error.h"

#include "btl_vader.h"
#include "btl_vader_frag.h"
#include "btl_vader_fifo.h"
#include "btl_vader_fbox.h"
#include "btl_vader_xpmem.h"

#include <sys/mman.h>
#include <fcntl.h>

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

/* NTH: OS X does not define MAP_ANONYMOUS */
#if !defined(MAP_ANONYMOUS)
#define MAP_ANONYMOUS MAP_ANON
#endif

static int mca_btl_vader_component_progress (void);
static int mca_btl_vader_component_open(void);
static int mca_btl_vader_component_close(void);
static int mca_btl_vader_component_register(void);
static mca_btl_base_module_t** mca_btl_vader_component_init(int *num_btls,
                                                            bool enable_progress_threads,
                                                            bool enable_mpi_threads);

/* This enumeration is in order of preference */
static mca_base_var_enum_value_t single_copy_mechanisms[] = {
#if OPAL_BTL_VADER_HAVE_XPMEM
    {.value = MCA_BTL_VADER_XPMEM, .string = "xpmem"},
#endif
#if OPAL_BTL_VADER_HAVE_CMA
    {.value = MCA_BTL_VADER_CMA, .string = "cma"},
#endif
#if OPAL_BTL_VADER_HAVE_KNEM
    {.value = MCA_BTL_VADER_KNEM, .string = "knem"},
#endif
    {.value = MCA_BTL_VADER_EMUL, .string = "emulated"},
    {.value = MCA_BTL_VADER_NONE, .string = "none"},
    {.value = 0, .string = NULL}
};

/*
 * Shared Memory (VADER) component instance.
 */
mca_btl_vader_component_t mca_btl_vader_component = {
    .super = {
        /* First, the mca_base_component_t struct containing meta information
           about the component itself */
        .btl_version = {
            MCA_BTL_DEFAULT_VERSION("vader"),
            .mca_open_component = mca_btl_vader_component_open,
            .mca_close_component = mca_btl_vader_component_close,
            .mca_register_component_params = mca_btl_vader_component_register,
        },
        .btl_data = {
            /* The component is checkpoint ready */
            .param_field = MCA_BASE_METADATA_PARAM_CHECKPOINT
        },

        .btl_init = mca_btl_vader_component_init,
        .btl_progress = mca_btl_vader_component_progress,
    }  /* end super */
};

static int mca_btl_vader_component_register (void)
{
    mca_base_var_enum_t *new_enum;

    (void) mca_base_var_group_component_register(&mca_btl_vader_component.super.btl_version,
                                                 "Enhanced shared memory byte transport later");

    /* register VADER component variables */
    mca_btl_vader_component.vader_free_list_num = 8;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "free_list_num", "Initial number of fragments "
                                           "to allocate for shared memory communication.",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.vader_free_list_num);
    mca_btl_vader_component.vader_free_list_max = 512;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "free_list_max", "Maximum number of fragments "
                                           "to allocate for shared memory communication.",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.vader_free_list_max);
    mca_btl_vader_component.vader_free_list_inc = 64;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "free_list_inc", "Number of fragments to create "
                                           "on each allocation.", MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.vader_free_list_inc);

    mca_btl_vader_component.memcpy_limit = 524288;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "memcpy_limit", "Message size to switch from using "
                                           "memove to memcpy. The relative speed of these two "
                                           "routines can vary by size.", MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.memcpy_limit);
#if OPAL_BTL_VADER_HAVE_XPMEM
    mca_btl_vader_component.log_attach_align = 21;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "log_align", "Log base 2 of the alignment to use for xpmem "
                                           "segments (default: 21, minimum: 12, maximum: 25)",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.log_attach_align);
#endif

#if OPAL_BTL_VADER_HAVE_XPMEM && 64 == MCA_BTL_VADER_BITNESS
    mca_btl_vader_component.segment_size = 1 << 24;
#else
    mca_btl_vader_component.segment_size = 1 << 22;
#endif
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "segment_size", "Maximum size of all shared "
#if OPAL_BTL_VADER_HAVE_XPMEM && 64 == MCA_BTL_VADER_BITNESS
                                           "memory buffers (default: 16M)",
#else
                                           "memory buffers (default: 4M)",
#endif
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.segment_size);

    mca_btl_vader_component.max_inline_send = 256;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "max_inline_send", "Maximum size to transfer "
                                           "using copy-in copy-out semantics",
                                           MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_LOCAL,
                                           &mca_btl_vader_component.max_inline_send);

    mca_btl_vader_component.fbox_threshold = 16;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "fbox_threshold", "Number of sends required "
                                           "before an eager send buffer is setup for a peer "
                                           "(default: 16)", MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL,
                                           0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_LOCAL, &mca_btl_vader_component.fbox_threshold);

    mca_btl_vader_component.fbox_max = 32;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "fbox_max", "Maximum number of eager send buffers "
                                           "to allocate (default: 32)", MCA_BASE_VAR_TYPE_UNSIGNED_INT,
                                           NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_5,
                                           MCA_BASE_VAR_SCOPE_LOCAL, &mca_btl_vader_component.fbox_max);

    mca_btl_vader_component.fbox_size = 4096;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "fbox_size", "Size of per-peer fast transfer buffers (default: 4k)",
                                           MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_5, MCA_BASE_VAR_SCOPE_LOCAL, &mca_btl_vader_component.fbox_size);

    (void) mca_base_var_enum_create ("btl_vader_single_copy_mechanisms", single_copy_mechanisms, &new_enum);

    /* Default to the best available mechanism (see the enumerator for ordering) */
    mca_btl_vader_component.single_copy_mechanism = single_copy_mechanisms[0].value;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version,
                                           "single_copy_mechanism", "Single copy mechanism to use (defaults to best available)",
                                           MCA_BASE_VAR_TYPE_INT, new_enum, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                           OPAL_INFO_LVL_3, MCA_BASE_VAR_SCOPE_GROUP, &mca_btl_vader_component.single_copy_mechanism);
    OBJ_RELEASE(new_enum);

    if (0 == access ("/dev/shm", W_OK)) {
        mca_btl_vader_component.backing_directory = "/dev/shm";
    } else {
        mca_btl_vader_component.backing_directory = opal_process_info.job_session_dir;
    }
    (void) mca_base_component_var_register (&mca_btl_vader_component.super.btl_version, "backing_directory",
                                            "Directory to place backing files for shared memory communication. "
                                            "This directory should be on a local filesystem such as /tmp or "
                                            "/dev/shm (default: (linux) /dev/shm, (others) session directory)",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_3,
                                            MCA_BASE_VAR_SCOPE_READONLY, &mca_btl_vader_component.backing_directory);


#if OPAL_BTL_VADER_HAVE_KNEM
    /* Currently disabling DMA mode by default; it's not clear that this is useful in all applications and architectures. */
    mca_btl_vader_component.knem_dma_min = 0;
    (void) mca_base_component_var_register(&mca_btl_vader_component.super.btl_version, "knem_dma_min",
                                           "Minimum message size (in bytes) to use the knem DMA mode; "
                                           "ignored if knem does not support DMA mode (0 = do not use the "
                                           "knem DMA mode, default: 0)", MCA_BASE_VAR_TYPE_UNSIGNED_INT,
                                           NULL, 0, 0, OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_vader_component.knem_dma_min);
#endif

    mca_btl_vader.super.btl_exclusivity               = MCA_BTL_EXCLUSIVITY_HIGH;

    if (MCA_BTL_VADER_XPMEM == mca_btl_vader_component.single_copy_mechanism) {
        mca_btl_vader.super.btl_eager_limit               = 32 * 1024;
        mca_btl_vader.super.btl_rndv_eager_limit          = mca_btl_vader.super.btl_eager_limit;
        mca_btl_vader.super.btl_max_send_size             = mca_btl_vader.super.btl_eager_limit;
        mca_btl_vader.super.btl_min_rdma_pipeline_size    = INT_MAX;
    } else {
        mca_btl_vader.super.btl_eager_limit               = 4 * 1024;
        mca_btl_vader.super.btl_rndv_eager_limit          = 32 * 1024;
        mca_btl_vader.super.btl_max_send_size             = 32 * 1024;
        mca_btl_vader.super.btl_min_rdma_pipeline_size    = INT_MAX;
    }

    mca_btl_vader.super.btl_rdma_pipeline_send_length = mca_btl_vader.super.btl_eager_limit;
    mca_btl_vader.super.btl_rdma_pipeline_frag_size   = mca_btl_vader.super.btl_eager_limit;

#if OPAL_HAVE_ATOMIC_MATH_64
    mca_btl_vader.super.btl_flags = MCA_BTL_FLAGS_SEND_INPLACE | MCA_BTL_FLAGS_SEND | MCA_BTL_FLAGS_RDMA |
        MCA_BTL_FLAGS_ATOMIC_OPS | MCA_BTL_FLAGS_ATOMIC_FOPS;

    mca_btl_vader.super.btl_atomic_flags = MCA_BTL_ATOMIC_SUPPORTS_ADD | MCA_BTL_ATOMIC_SUPPORTS_AND |
        MCA_BTL_ATOMIC_SUPPORTS_OR | MCA_BTL_ATOMIC_SUPPORTS_XOR | MCA_BTL_ATOMIC_SUPPORTS_CSWAP |
        MCA_BTL_ATOMIC_SUPPORTS_GLOB | MCA_BTL_ATOMIC_SUPPORTS_SWAP;
#if OPAL_HAVE_ATOMIC_MATH_32
    mca_btl_vader.super.btl_atomic_flags |= MCA_BTL_ATOMIC_SUPPORTS_32BIT;
#endif /* OPAL_HAVE_ATOMIC_MATH_32 */

#if OPAL_HAVE_ATOMIC_MIN_64
    mca_btl_vader.super.btl_atomic_flags |= MCA_BTL_ATOMIC_SUPPORTS_MIN;
#endif /* OPAL_HAVE_ATOMIC_MIN_64 */

#if OPAL_HAVE_ATOMIC_MAX_64
    mca_btl_vader.super.btl_atomic_flags |= MCA_BTL_ATOMIC_SUPPORTS_MAX;
#endif /* OPAL_HAVE_ATOMIC_MAX_64 */

#else
    mca_btl_vader.super.btl_flags = MCA_BTL_FLAGS_SEND_INPLACE | MCA_BTL_FLAGS_SEND | MCA_BTL_FLAGS_RDMA;
#endif /* OPAL_HAVE_ATOMIC_MATH_64 */

    if (MCA_BTL_VADER_NONE != mca_btl_vader_component.single_copy_mechanism) {
        /* True single copy mechanisms should provide better bandwidth */
        mca_btl_vader.super.btl_bandwidth = 40000; /* Mbs */
    } else {
        mca_btl_vader.super.btl_bandwidth = 10000; /* Mbs */
    }

    mca_btl_vader.super.btl_get = mca_btl_vader_get_sc_emu;
    mca_btl_vader.super.btl_put = mca_btl_vader_put_sc_emu;
    mca_btl_vader.super.btl_atomic_op = mca_btl_vader_emu_aop;
    mca_btl_vader.super.btl_atomic_fop = mca_btl_vader_emu_afop;
    mca_btl_vader.super.btl_atomic_cswap = mca_btl_vader_emu_acswap;

    mca_btl_vader.super.btl_latency   = 1;     /* Microsecs */

    /* Call the BTL based to register its MCA params */
    mca_btl_base_param_register(&mca_btl_vader_component.super.btl_version,
                                &mca_btl_vader.super);

    return OPAL_SUCCESS;
}

/*
 *  Called by MCA framework to open the component, registers
 *  component parameters.
 */

static int mca_btl_vader_component_open(void)
{
    /* initialize objects */
    OBJ_CONSTRUCT(&mca_btl_vader_component.vader_frags_eager, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_vader_component.vader_frags_user, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_vader_component.vader_frags_max_send, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_vader_component.vader_fboxes, opal_free_list_t);
    OBJ_CONSTRUCT(&mca_btl_vader_component.lock, opal_mutex_t);
    OBJ_CONSTRUCT(&mca_btl_vader_component.pending_endpoints, opal_list_t);
    OBJ_CONSTRUCT(&mca_btl_vader_component.pending_fragments, opal_list_t);
#if OPAL_BTL_VADER_HAVE_KNEM
    mca_btl_vader.knem_fd = -1;
#endif

    return OPAL_SUCCESS;
}


/*
 * component cleanup - sanity checking of queue lengths
 */

static int mca_btl_vader_component_close(void)
{
    OBJ_DESTRUCT(&mca_btl_vader_component.vader_frags_eager);
    OBJ_DESTRUCT(&mca_btl_vader_component.vader_frags_user);
    OBJ_DESTRUCT(&mca_btl_vader_component.vader_frags_max_send);
    OBJ_DESTRUCT(&mca_btl_vader_component.vader_fboxes);
    OBJ_DESTRUCT(&mca_btl_vader_component.lock);
    OBJ_DESTRUCT(&mca_btl_vader_component.pending_endpoints);
    OBJ_DESTRUCT(&mca_btl_vader_component.pending_fragments);

    if (MCA_BTL_VADER_XPMEM == mca_btl_vader_component.single_copy_mechanism &&
        NULL != mca_btl_vader_component.my_segment) {
        munmap (mca_btl_vader_component.my_segment, mca_btl_vader_component.segment_size);
    }

    mca_btl_vader_component.my_segment = NULL;

#if OPAL_BTL_VADER_HAVE_KNEM
    mca_btl_vader_knem_fini ();
#endif

    if (mca_btl_vader_component.mpool) {
        mca_btl_vader_component.mpool->mpool_finalize (mca_btl_vader_component.mpool);
        mca_btl_vader_component.mpool = NULL;
    }

    return OPAL_SUCCESS;
}

static int mca_btl_base_vader_modex_send (void)
{
    union vader_modex_t modex;
    int modex_size, rc;

#if OPAL_BTL_VADER_HAVE_XPMEM
    if (MCA_BTL_VADER_XPMEM == mca_btl_vader_component.single_copy_mechanism) {
        modex.xpmem.seg_id = mca_btl_vader_component.my_seg_id;
        modex.xpmem.segment_base = mca_btl_vader_component.my_segment;

        modex_size = sizeof (modex.xpmem);
    } else {
#endif
        modex_size = opal_shmem_sizeof_shmem_ds (&mca_btl_vader_component.seg_ds);
        memmove (&modex.seg_ds, &mca_btl_vader_component.seg_ds, modex_size);

#if OPAL_BTL_VADER_HAVE_XPMEM
    }
#endif

    OPAL_MODEX_SEND(rc, OPAL_PMIX_LOCAL,
                    &mca_btl_vader_component.super.btl_version, &modex, modex_size);

    return rc;
}

static void mca_btl_vader_select_next_single_copy_mechanism (void)
{
    for (int i = 0 ; single_copy_mechanisms[i].value != MCA_BTL_VADER_NONE ; ++i) {
        if (single_copy_mechanisms[i].value == mca_btl_vader_component.single_copy_mechanism) {
            mca_btl_vader_component.single_copy_mechanism = single_copy_mechanisms[i+1].value;
            return;
        }
    }
}

static void mca_btl_vader_check_single_copy (void)
{
    int initial_mechanism = mca_btl_vader_component.single_copy_mechanism;

    /* single-copy emulation is always used to support AMO's right now */
    mca_btl_vader_sc_emu_init ();

#if OPAL_BTL_VADER_HAVE_XPMEM
    if (MCA_BTL_VADER_XPMEM == mca_btl_vader_component.single_copy_mechanism) {
        /* try to create an xpmem segment for the entire address space */
        int rc = mca_btl_vader_xpmem_init ();
        if (OPAL_SUCCESS != rc) {
            if (MCA_BTL_VADER_XPMEM == initial_mechanism) {
                opal_show_help("help-btl-vader.txt", "xpmem-make-failed",
                               true, opal_process_info.nodename, errno,
                               strerror(errno));
            }

            mca_btl_vader_select_next_single_copy_mechanism ();
        }
    }
#endif

#if OPAL_BTL_VADER_HAVE_CMA
    if (MCA_BTL_VADER_CMA == mca_btl_vader_component.single_copy_mechanism) {
        /* Check if we have the proper permissions for CMA */
        char buffer = '0';
        bool cma_happy = false;
        int fd;

        /* check system setting for current ptrace scope */
        fd = open ("/proc/sys/kernel/yama/ptrace_scope", O_RDONLY);
        if (0 <= fd) {
            read (fd, &buffer, 1);
            close (fd);
        }

        /* ptrace scope 0 will allow an attach from any of the process owner's
         * processes. ptrace scope 1 limits attachers to the process tree
         * starting at the parent of this process. */
        if ('0' != buffer) {
#if defined PR_SET_PTRACER
            /* try setting the ptrace scope to allow attach */
            int ret = prctl (PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
            if (0 == ret) {
                cma_happy = true;
            }
#endif
        } else {
            cma_happy = true;
        }

        if (!cma_happy) {
            mca_btl_vader_select_next_single_copy_mechanism ();

            if (MCA_BTL_VADER_CMA == initial_mechanism) {
                opal_show_help("help-btl-vader.txt", "cma-permission-denied",
                               true, opal_process_info.nodename);
            }
        } else {
            /* ptrace_scope will allow CMA */
            mca_btl_vader.super.btl_get = mca_btl_vader_get_cma;
            mca_btl_vader.super.btl_put = mca_btl_vader_put_cma;
        }
    }
#endif

#if OPAL_BTL_VADER_HAVE_KNEM
    if (MCA_BTL_VADER_KNEM == mca_btl_vader_component.single_copy_mechanism) {
        /* mca_btl_vader_knem_init will set the appropriate get/put functions */
        int rc = mca_btl_vader_knem_init ();
        if (OPAL_SUCCESS != rc) {
            if (MCA_BTL_VADER_KNEM == initial_mechanism) {
                opal_show_help("help-btl-vader.txt", "knem requested but not available",
                               true, opal_process_info.nodename);
            }

            /* disable single copy */
            mca_btl_vader_select_next_single_copy_mechanism ();
        }
    }
#endif

    if (MCA_BTL_VADER_NONE == mca_btl_vader_component.single_copy_mechanism) {
        mca_btl_vader.super.btl_flags &= ~MCA_BTL_FLAGS_RDMA;
        mca_btl_vader.super.btl_get = NULL;
        mca_btl_vader.super.btl_put = NULL;
    }

    if (MCA_BTL_VADER_EMUL == mca_btl_vader_component.single_copy_mechanism) {
        /* limit to the maximum fragment size */
        mca_btl_vader.super.btl_put_limit = mca_btl_vader.super.btl_max_send_size - sizeof (mca_btl_vader_sc_emu_hdr_t);
        mca_btl_vader.super.btl_get_limit = mca_btl_vader.super.btl_max_send_size - sizeof (mca_btl_vader_sc_emu_hdr_t);
    }
}

/*
 *  VADER component initialization
 */
static mca_btl_base_module_t **mca_btl_vader_component_init (int *num_btls,
                                                             bool enable_progress_threads,
                                                             bool enable_mpi_threads)
{
    mca_btl_vader_component_t *component = &mca_btl_vader_component;
    mca_btl_base_module_t **btls = NULL;
    int rc;

    *num_btls = 0;

    /* disable if there are no local peers */
    if (0 == MCA_BTL_VADER_NUM_LOCAL_PEERS) {
        BTL_VERBOSE(("No peers to communicate with. Disabling vader."));
        return NULL;
    }

#if OPAL_BTL_VADER_HAVE_XPMEM
    /* limit segment alignment to be between 4k and 16M */
    if (component->log_attach_align < 12) {
        component->log_attach_align = 12;
    } else if (component->log_attach_align > 25) {
        component->log_attach_align = 25;
    }
#endif

    btls = (mca_btl_base_module_t **) calloc (1, sizeof (mca_btl_base_module_t *));
    if (NULL == btls) {
        return NULL;
    }

    /* ensure a sane segment size */
    if (component->segment_size < (2 << 20)) {
        component->segment_size = (2 << 20);
    }

    component->fbox_size = (component->fbox_size + MCA_BTL_VADER_FBOX_ALIGNMENT_MASK) & ~MCA_BTL_VADER_FBOX_ALIGNMENT_MASK;

    if (component->segment_size > (1ul << MCA_BTL_VADER_OFFSET_BITS)) {
        component->segment_size = 2ul << MCA_BTL_VADER_OFFSET_BITS;
    }

    /* no fast boxes allocated initially */
    component->num_fbox_in_endpoints = 0;

    mca_btl_vader_check_single_copy ();

    if (MCA_BTL_VADER_XPMEM != mca_btl_vader_component.single_copy_mechanism) {
        const char *base_dir = opal_process_info.proc_session_dir;
        char *sm_file;

        rc = asprintf(&sm_file, "%s" OPAL_PATH_SEP "vader_segment.%s.%x.%d", mca_btl_vader_component.backing_directory,
                      opal_process_info.nodename, OPAL_PROC_MY_NAME.jobid, MCA_BTL_VADER_LOCAL_RANK);
        if (0 > rc) {
            free (btls);
            return NULL;
        }
        if (NULL != opal_pmix.register_cleanup) {
            opal_pmix.register_cleanup (sm_file, false, false, false);
        }

        rc = opal_shmem_segment_create (&component->seg_ds, sm_file, component->segment_size);
        free (sm_file);
        if (OPAL_SUCCESS != rc) {
            BTL_VERBOSE(("Could not create shared memory segment"));
            free (btls);
            return NULL;
        }

        component->my_segment = opal_shmem_segment_attach (&component->seg_ds);
        if (NULL == component->my_segment) {
            BTL_VERBOSE(("Could not attach to just created shared memory segment"));
            goto failed;
        }
    } else {
        /* when using xpmem it is safe to use an anonymous segment */
        component->my_segment = mmap (NULL, component->segment_size, PROT_READ |
                                      PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
        if ((void *)-1 == component->my_segment) {
            BTL_VERBOSE(("Could not create anonymous memory segment"));
            free (btls);
            return NULL;
        }
    }

    /* initialize my fifo */
    vader_fifo_init ((struct vader_fifo_t *) component->my_segment);

    rc = mca_btl_base_vader_modex_send ();
    if (OPAL_SUCCESS != rc) {
        BTL_VERBOSE(("Error sending modex"));
        goto failed;
    }

    *num_btls = 1;

    /* get pointer to the btls */
    btls[0] = (mca_btl_base_module_t *) &mca_btl_vader;

    /* set flag indicating btl not inited */
    mca_btl_vader.btl_inited = false;

    return btls;
failed:
#if OPAL_BTL_VADER_HAVE_XPMEM
    if (MCA_BTL_VADER_XPMEM == mca_btl_vader_component.single_copy_mechanism) {
        munmap (component->my_segment, component->segment_size);
    } else
#endif
        opal_shmem_unlink (&component->seg_ds);

    if (btls) {
        free (btls);
    }

    return NULL;
}

void mca_btl_vader_poll_handle_frag (mca_btl_vader_hdr_t *hdr, struct mca_btl_base_endpoint_t *endpoint)
{
    mca_btl_base_segment_t segments[2];
    mca_btl_base_descriptor_t frag = {.des_segments = segments, .des_segment_count = 1};
    const mca_btl_active_message_callback_t *reg;

    if (hdr->flags & MCA_BTL_VADER_FLAG_COMPLETE) {
        mca_btl_vader_frag_complete (hdr->frag);
        return;
    }

    reg = mca_btl_base_active_message_trigger + hdr->tag;
    segments[0].seg_addr.pval = (void *) (hdr + 1);
    segments[0].seg_len       = hdr->len;

    if (hdr->flags & MCA_BTL_VADER_FLAG_SINGLE_COPY) {
        mca_rcache_base_registration_t *xpmem_reg;

        xpmem_reg = vader_get_registation (endpoint, hdr->sc_iov.iov_base,
                                           hdr->sc_iov.iov_len, 0,
                                           &segments[1].seg_addr.pval);
        assert (NULL != xpmem_reg);

        segments[1].seg_len = hdr->sc_iov.iov_len;
        frag.des_segment_count = 2;

        /* recv upcall */
        reg->cbfunc(&mca_btl_vader.super, hdr->tag, &frag, reg->cbdata);
        vader_return_registration (xpmem_reg, endpoint);
    } else {
        reg->cbfunc(&mca_btl_vader.super, hdr->tag, &frag, reg->cbdata);
    }

    if (OPAL_UNLIKELY(MCA_BTL_VADER_FLAG_SETUP_FBOX & hdr->flags)) {
        mca_btl_vader_endpoint_setup_fbox_recv (endpoint, relative2virtual(hdr->fbox_base));
        mca_btl_vader_component.fbox_in_endpoints[mca_btl_vader_component.num_fbox_in_endpoints++] = endpoint;
    }

    hdr->flags = MCA_BTL_VADER_FLAG_COMPLETE;
    vader_fifo_write_back (hdr, endpoint);
}

static int mca_btl_vader_poll_fifo (void)
{
    struct mca_btl_base_endpoint_t *endpoint;
    mca_btl_vader_hdr_t *hdr;

    /* poll the fifo until it is empty or a limit has been hit (8 is arbitrary) */
    for (int fifo_count = 0 ; fifo_count < 31 ; ++fifo_count) {
        hdr = vader_fifo_read (mca_btl_vader_component.my_fifo, &endpoint);
        if (NULL == hdr) {
            return fifo_count;
        }

        mca_btl_vader_poll_handle_frag (hdr, endpoint);
    }

    return 1;
}

/**
 * Progress pending messages on an endpoint
 *
 * @param ep (IN)       Vader BTL endpoint
 *
 * This is called with the component lock held so the component lock does
 * not need to be aquired before modifying the pending_endpoints list.
 */
static void mca_btl_vader_progress_waiting (mca_btl_base_endpoint_t *ep)
{
    mca_btl_vader_frag_t *frag, *next;
    int ret = 1;

    if (OPAL_UNLIKELY(NULL == ep)) {
        return;
    }

    OPAL_THREAD_LOCK(&ep->pending_frags_lock);
    OPAL_LIST_FOREACH_SAFE(frag, next, &ep->pending_frags, mca_btl_vader_frag_t) {
        ret = vader_fifo_write_ep (frag->hdr, ep);
        if (!ret) {
            OPAL_THREAD_UNLOCK(&ep->pending_frags_lock);
            return;
        }

        (void) opal_list_remove_first (&ep->pending_frags);
    }

    ep->waiting = false;
    opal_list_remove_item (&mca_btl_vader_component.pending_endpoints, &ep->super);

    OPAL_THREAD_UNLOCK(&ep->pending_frags_lock);
}

/**
 * Progress pending messages on all waiting endpoints
 *
 * @param ep (IN)       Vader BTL endpoint
 */
static void mca_btl_vader_progress_endpoints (void)
{
    mca_btl_base_endpoint_t *ep, *next;
    int count;

    count = opal_list_get_size (&mca_btl_vader_component.pending_endpoints);
    if (OPAL_LIKELY(0 == count)) {
        return;
    }

    OPAL_THREAD_LOCK(&mca_btl_vader_component.lock);
    OPAL_LIST_FOREACH_SAFE(ep, next, &mca_btl_vader_component.pending_endpoints, mca_btl_base_endpoint_t) {
        mca_btl_vader_progress_waiting (ep);
    }
    OPAL_THREAD_UNLOCK(&mca_btl_vader_component.lock);
}

static int mca_btl_vader_component_progress (void)
{
    static int32_t lock = 0;
    int count = 0;

    if (opal_using_threads()) {
        if (opal_atomic_swap_32 (&lock, 1)) {
            return 0;
        }
    }

    /* check for messages in fast boxes */
    if (mca_btl_vader_component.num_fbox_in_endpoints) {
        count = mca_btl_vader_check_fboxes ();
    }

    mca_btl_vader_progress_endpoints ();

    if (VADER_FIFO_FREE == mca_btl_vader_component.my_fifo->fifo_head) {
        lock = 0;
        return count;
    }

    count += mca_btl_vader_poll_fifo ();
    opal_atomic_mb ();
    lock = 0;

    return count;
}
