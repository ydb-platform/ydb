/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/*%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/

#include "ompi_config.h"
#include "ompi/types.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"


static void mca_pml_base_recv_request_construct(mca_pml_base_recv_request_t*);
static void mca_pml_base_recv_request_destruct(mca_pml_base_recv_request_t*);


OBJ_CLASS_INSTANCE(
    mca_pml_base_recv_request_t,
    mca_pml_base_request_t,
    mca_pml_base_recv_request_construct,
    mca_pml_base_recv_request_destruct
);


static void mca_pml_base_recv_request_construct(mca_pml_base_recv_request_t* request)
{
    /* no need to reinit for every recv -- never changes */
    request->req_base.req_type = MCA_PML_REQUEST_RECV;
    OBJ_CONSTRUCT(&request->req_base.req_convertor, opal_convertor_t);
}


static void mca_pml_base_recv_request_destruct(mca_pml_base_recv_request_t* request)
{
    /* For each request the convertor get cleaned after each message
     * (in the base _FINI macro). Therefore, as the convertor is a static object
     * we don't have to call OBJ_DESTRUCT here.
     */
}

