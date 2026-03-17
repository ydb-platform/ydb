/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 *  @file
 */

#ifndef MCA_PML_OB1_RDMA_H
#define MCA_PML_OB1_RDMA_H

struct mca_bml_base_endpoint_t;

/*
 * Of the set of available btls that support RDMA,
 * find those that already have registrations - or
 * register if required (for leave_pinned option)
 */
size_t mca_pml_ob1_rdma_btls(struct mca_bml_base_endpoint_t* endpoint,
    unsigned char* base, size_t size, struct mca_pml_ob1_com_btl_t* btls);

/* Choose RDMA BTLs to use for sending of a request by pipeline protocol.
 * Calculate number of bytes to send through each BTL according to available
 * bandwidth */
size_t mca_pml_ob1_rdma_pipeline_btls(struct mca_bml_base_endpoint_t* endpoint,
                size_t size, mca_pml_ob1_com_btl_t* rdma_btls);

size_t mca_pml_ob1_rdma_pipeline_btls_count (mca_bml_base_endpoint_t* bml_endpoint);

#endif

