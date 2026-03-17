/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PML_CM_REQUEST_H
#define PML_CM_REQUEST_H

#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/mtl/mtl.h"

/**
 * Type of request.
 */
typedef enum {
    MCA_PML_CM_REQUEST_SEND_HEAVY,
    MCA_PML_CM_REQUEST_SEND_THIN,
    MCA_PML_CM_REQUEST_RECV_HEAVY,
    MCA_PML_CM_REQUEST_RECV_THIN,
    MCA_PML_CM_REQUEST_NULL
} mca_pml_cm_request_type_t;

/**
 *  Base type for PML CM P2P requests
 */
struct mca_pml_cm_request_t {

/* START: These fields have to match the definition of the mca_pml_base_request_t */
    ompi_request_t req_ompi;              /**< base request */
    volatile int32_t req_pml_complete;    /**< flag indicating if the pt-2-pt layer is done with this request */
    volatile int32_t req_free_called;     /**< flag indicating if the user has freed this request */
    mca_pml_cm_request_type_t req_pml_type;
    struct ompi_communicator_t *req_comm; /**< communicator pointer */
    struct ompi_datatype_t *req_datatype; /**< pointer to data type */
    opal_convertor_t req_convertor;       /**< convertor that describes the memory layout */
/* END: These fields have to match the definition of the mca_pml_base_request_t */
};
typedef struct mca_pml_cm_request_t mca_pml_cm_request_t;
OBJ_CLASS_DECLARATION(mca_pml_cm_request_t);

/*
 * Avoid CUDA convertor inits only for contiguous memory and if indicated by
 * the MTL. For non-contiguous memory, do not skip CUDA convertor init phases.
 */
#if OPAL_CUDA_SUPPORT
#define MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count)            \
    {                                                                           \
        if (opal_datatype_is_contiguous_memory_layout(&datatype->super, count)  \
            && (ompi_mtl->mtl_flags & MCA_MTL_BASE_FLAG_CUDA_INIT_DISABLE)) {   \
            flags |= CONVERTOR_SKIP_CUDA_INIT;                                  \
        }                                                                       \
    }
#else
#define MCA_PML_CM_SWITCH_CUDA_CONVERTOR_OFF(flags, datatype, count)
#endif

#endif
