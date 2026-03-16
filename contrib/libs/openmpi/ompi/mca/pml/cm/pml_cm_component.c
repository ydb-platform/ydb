/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2006-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "pml_cm.h"
#include "opal/mca/event/event.h"
#include "ompi/mca/mtl/mtl.h"
#include "ompi/mca/mtl/base/base.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"

#include "pml_cm_sendreq.h"
#include "pml_cm_recvreq.h"
#include "pml_cm_component.h"

static int mca_pml_cm_component_register(void);
static int mca_pml_cm_component_open(void);
static int mca_pml_cm_component_close(void);
static mca_pml_base_module_t* mca_pml_cm_component_init( int* priority,
                            bool enable_progress_threads, bool enable_mpi_threads);
static int mca_pml_cm_component_fini(void);

mca_pml_base_component_2_0_0_t mca_pml_cm_component = {
    /* First, the mca_base_component_t struct containing meta
     * information about the component itself */

    .pmlm_version = {
        MCA_PML_BASE_VERSION_2_0_0,

        .mca_component_name = "cm",
        MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                              OMPI_RELEASE_VERSION),
        .mca_open_component = mca_pml_cm_component_open,
        .mca_close_component = mca_pml_cm_component_close,
        .mca_register_component_params = mca_pml_cm_component_register,
    },
    .pmlm_data = {
        /* This component is not checkpoint ready */
        MCA_BASE_METADATA_PARAM_NONE
    },

    .pmlm_init = mca_pml_cm_component_init,
    .pmlm_finalize = mca_pml_cm_component_fini,
};

/* Array of send completion callback - one per send type
 * These are called internally by the library when the send
 * is completed from its perspective.
 */
void (*send_completion_callbacks[MCA_PML_BASE_SEND_SIZE])
    (struct mca_mtl_request_t *mtl_request) =
  { mca_pml_cm_send_request_completion,
    mca_pml_cm_send_request_completion,
    mca_pml_cm_send_request_completion,
    mca_pml_cm_send_request_completion,
    mca_pml_cm_send_request_completion } ;

static int
mca_pml_cm_component_register(void)
{

    ompi_pml_cm.free_list_num = 4;
    (void) mca_base_component_var_register(&mca_pml_cm_component.pmlm_version, "free_list_num",
                                           "Initial size of request free lists",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &ompi_pml_cm.free_list_num);

    ompi_pml_cm.free_list_max = -1;
    (void) mca_base_component_var_register(&mca_pml_cm_component.pmlm_version, "free_list_max",
                                           "Maximum size of request free lists",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &ompi_pml_cm.free_list_max);

    ompi_pml_cm.free_list_inc = 64;
    (void) mca_base_component_var_register(&mca_pml_cm_component.pmlm_version, "free_list_inc",
                                           "Number of elements to add when growing request free lists",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &ompi_pml_cm.free_list_inc);

    return OPAL_SUCCESS;
}

static int
mca_pml_cm_component_open(void)
{
    int ret;

    ret = mca_base_framework_open(&ompi_mtl_base_framework, 0);
    if (OMPI_SUCCESS == ret) {
      /* If no MTL components initialized CM component can be unloaded */
      if (0 == opal_list_get_size(&ompi_mtl_base_framework.framework_components)) {
	ret = OPAL_ERR_NOT_AVAILABLE;
      }
    }

    return ret;
}


static int
mca_pml_cm_component_close(void)
{
    return mca_base_framework_close(&ompi_mtl_base_framework);
}


static mca_pml_base_module_t*
mca_pml_cm_component_init(int* priority,
                          bool enable_progress_threads,
                          bool enable_mpi_threads)
{
    int ret;

    *priority = -1;

    opal_output_verbose( 10, 0,
                         "in cm pml priority is %d\n", *priority);
    /* find a useable MTL */
    ret = ompi_mtl_base_select(enable_progress_threads, enable_mpi_threads, priority);
    if (OMPI_SUCCESS != ret) {
        return NULL;
    }

    if (ompi_mtl->mtl_flags & MCA_MTL_BASE_FLAG_REQUIRE_WORLD) {
        ompi_pml_cm.super.pml_flags |= MCA_PML_BASE_FLAG_REQUIRE_WORLD;
    }

    /* update our tag / context id max values based on MTL
       information */
    ompi_pml_cm.super.pml_max_contextid = ompi_mtl->mtl_max_contextid;
    ompi_pml_cm.super.pml_max_tag = ompi_mtl->mtl_max_tag;

    return &ompi_pml_cm.super;
}


static int
mca_pml_cm_component_fini(void)
{
    if (NULL != ompi_mtl) {
        return OMPI_MTL_CALL(finalize(ompi_mtl));
    }

    return OMPI_SUCCESS;
}

