/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_PML_BASE_REQUEST_H
#define MCA_PML_BASE_REQUEST_H

#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "ompi/communicator/communicator.h"
#include "ompi/request/request.h"
#include "opal/datatype/opal_convertor.h"

BEGIN_C_DECLS

/**
 * External list for the requests. They are declared as lists of
 * the basic request type, which will allow all PML to overload
 * the list. Beware these free lists have to be initialized
 * directly by the PML who win the PML election.
 */
OMPI_DECLSPEC extern opal_free_list_t mca_pml_base_send_requests;
OMPI_DECLSPEC extern opal_free_list_t mca_pml_base_recv_requests;

/**
 * Type of request.
 */
/*
 * The following include pulls in shared typedefs with debugger plugins.
 * For more information on why we do this see the Notice to developers
 * comment at the top of the ompi_msgq_dll.c file.
 */
#include "pml_base_request_dbg.h"


/**
 *  Base type for PML P2P requests
 */
struct mca_pml_base_request_t {

/* START: These fields have to match the definition of the mca_pml_cm_request_t */
    ompi_request_t req_ompi;              /**< base request */
    volatile int32_t req_pml_complete;    /**< flag indicating if the pt-2-pt layer is done with this request */
    volatile int32_t req_free_called;     /**< flag indicating if the user has freed this request */
    mca_pml_base_request_type_t req_type; /**< MPI request type - used for test */
    struct ompi_communicator_t *req_comm; /**< communicator pointer */
    struct ompi_datatype_t *req_datatype; /**< pointer to data type */
    opal_convertor_t req_convertor;       /**< always need the convertor */
/* END: These field have to match the definition of the mca_pml_cm_request_t */

    void *req_addr;                       /**< pointer to application buffer */
    size_t req_count;                     /**< count of user datatype elements */
    int32_t req_peer;                     /**< peer process - rank w/in this communicator */
    int32_t req_tag;                      /**< user defined tag */
    struct ompi_proc_t* req_proc;         /**< peer process */
    uint64_t req_sequence;                /**< sequence number for MPI pt-2-pt ordering */
};
typedef struct mca_pml_base_request_t mca_pml_base_request_t;

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_pml_base_request_t);

END_C_DECLS

#endif

