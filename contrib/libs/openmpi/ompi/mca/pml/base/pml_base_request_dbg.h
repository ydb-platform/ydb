/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef MCA_PML_BASE_REQUEST_DBG_H
#define MCA_PML_BASE_REQUEST_DBG_H

/*
 * This file contains definitions used by both OMPI and debugger plugins.
 * For more information on why we do this see the Notice to developers
 * comment at the top of the ompi_msgq_dll.c file.
 */

/**
 * Type of request.
 */
typedef enum {
    MCA_PML_REQUEST_NULL,
    MCA_PML_REQUEST_SEND,
    MCA_PML_REQUEST_RECV,
    MCA_PML_REQUEST_IPROBE,
    MCA_PML_REQUEST_PROBE,
    MCA_PML_REQUEST_IMPROBE,
    MCA_PML_REQUEST_MPROBE
} mca_pml_base_request_type_t;

#endif /* MCA_PML_BASE_REQUEST_DBG_H */
