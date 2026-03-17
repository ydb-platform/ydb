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
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/base/pml_base_request.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/mca/pml/base/base.h"

#include "pml_cm.h"
#include "pml_cm_sendreq.h"
#include "pml_cm_recvreq.h"

ompi_pml_cm_t ompi_pml_cm = {
    {
        mca_pml_cm_add_procs,
        mca_pml_cm_del_procs,
        mca_pml_cm_enable,
        NULL, /* No progress function. The MTL register their own */
        mca_pml_cm_add_comm,
        mca_pml_cm_del_comm,
        mca_pml_cm_irecv_init,
        mca_pml_cm_irecv,
        mca_pml_cm_recv,
        mca_pml_cm_isend_init,
        mca_pml_cm_isend,
        mca_pml_cm_send,
        mca_pml_cm_iprobe,
        mca_pml_cm_probe,
        mca_pml_cm_start,
        mca_pml_cm_improbe,
        mca_pml_cm_mprobe,
        mca_pml_cm_imrecv,
        mca_pml_cm_mrecv,
        mca_pml_cm_dump,
        NULL,
        0,
        0
    }
};


int
mca_pml_cm_enable(bool enable)
{
    /* BWB - FIX ME - need to have this actually do something,
       maybe? */
    opal_free_list_init (&mca_pml_base_send_requests,
                         sizeof(mca_pml_cm_hvy_send_request_t) + ompi_mtl->mtl_request_size,
                         opal_cache_line_size,
                         OBJ_CLASS(mca_pml_cm_hvy_send_request_t),
                         0,opal_cache_line_size,
                         ompi_pml_cm.free_list_num,
                         ompi_pml_cm.free_list_max,
                         ompi_pml_cm.free_list_inc,
                         NULL, 0, NULL, NULL, NULL);

    opal_free_list_init (&mca_pml_base_recv_requests,
                         sizeof(mca_pml_cm_hvy_recv_request_t) + ompi_mtl->mtl_request_size,
                         opal_cache_line_size,
                         OBJ_CLASS(mca_pml_cm_hvy_recv_request_t),
                         0,opal_cache_line_size,
                         ompi_pml_cm.free_list_num,
                         ompi_pml_cm.free_list_max,
                         ompi_pml_cm.free_list_inc,
                         NULL, 0, NULL, NULL, NULL);

    return OMPI_SUCCESS;
}


int
mca_pml_cm_add_comm(ompi_communicator_t* comm)
{
    /* should never happen, but it was, so check */
    if (comm->c_contextid > ompi_pml_cm.super.pml_max_contextid) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* initialize per-communicator data. MTLs may override this. */
    comm->c_pml_comm = NULL;

    /* notify the MTL about the added communicator */
    return OMPI_MTL_CALL(add_comm(ompi_mtl, comm));
}


int
mca_pml_cm_del_comm(ompi_communicator_t* comm)
{
    /* notify the MTL about the deleted communicator */
    return OMPI_MTL_CALL(del_comm(ompi_mtl, comm));
}


int
mca_pml_cm_add_procs(struct ompi_proc_t** procs, size_t nprocs)
{
    int ret;

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    for (size_t i = 0 ; i < nprocs ; ++i) {
        if (procs[i]->super.proc_arch != ompi_proc_local()->super.proc_arch) {
            return OMPI_ERR_NOT_SUPPORTED;
        }
    }
#endif

    /* make sure remote procs are using the same PML as us */
    if (OMPI_SUCCESS != (ret = mca_pml_base_pml_check_selected("cm",
                                                              procs,
                                                              nprocs))) {
        return ret;
    }

    ret = OMPI_MTL_CALL(add_procs(ompi_mtl, nprocs, procs));
    return ret;
}


int
mca_pml_cm_del_procs(struct ompi_proc_t** procs, size_t nprocs)
{
    int ret;

    ret = OMPI_MTL_CALL(del_procs(ompi_mtl, nprocs, procs));
    return ret;
}


/* print any available useful information from this communicator */
int
mca_pml_cm_dump(struct ompi_communicator_t* comm, int verbose)
{
    return OMPI_ERR_NOT_IMPLEMENTED;
}
