/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_PML_BASE_BSEND_H_
#define _MCA_PML_BASE_BSEND_H_

#include "ompi_config.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

OMPI_DECLSPEC int mca_pml_base_bsend_init(bool enable_mpi_threads);
OMPI_DECLSPEC int mca_pml_base_bsend_fini(void);

int mca_pml_base_bsend_attach(void* addr, int size);
int mca_pml_base_bsend_detach(void* addr, int* size);

OMPI_DECLSPEC int mca_pml_base_bsend_request_alloc(ompi_request_t*);
OMPI_DECLSPEC int mca_pml_base_bsend_request_start(ompi_request_t*);
OMPI_DECLSPEC int mca_pml_base_bsend_request_fini(ompi_request_t*);
OMPI_DECLSPEC void*  mca_pml_base_bsend_request_alloc_buf( size_t length );
OMPI_DECLSPEC int mca_pml_base_bsend_request_free(void* addr);

extern unsigned char   *mca_pml_bsend_userbase;   /* user provided buffer base */
extern unsigned char   *mca_pml_bsend_base;       /* adjusted base of user buffer */
extern unsigned char   *mca_pml_bsend_addr;       /* current offset into user buffer */

END_C_DECLS

#endif

