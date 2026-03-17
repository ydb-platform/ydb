/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/sys/atomic.h"
#include "ompi/mca/bml/bml.h"



static void mca_bml_base_endpoint_construct(mca_bml_base_endpoint_t* ep)
{
    ep->btl_pipeline_send_length = 0;
    ep->btl_send_limit = 0;

    OBJ_CONSTRUCT(&ep->btl_eager, mca_bml_base_btl_array_t);
    OBJ_CONSTRUCT(&ep->btl_send,  mca_bml_base_btl_array_t);
    OBJ_CONSTRUCT(&ep->btl_rdma,  mca_bml_base_btl_array_t);
}


static void mca_bml_base_endpoint_destruct(mca_bml_base_endpoint_t* ep)
{
    OBJ_DESTRUCT(&ep->btl_eager);
    OBJ_DESTRUCT(&ep->btl_send);
    OBJ_DESTRUCT(&ep->btl_rdma);
}


OBJ_CLASS_INSTANCE(
    mca_bml_base_endpoint_t,
    opal_object_t,
    mca_bml_base_endpoint_construct,
    mca_bml_base_endpoint_destruct
);

