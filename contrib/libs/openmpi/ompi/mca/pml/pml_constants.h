/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_PML_CONSTANTS_H
#define MCA_PML_CONSTANTS_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "mpi.h" /* needed for MPI_ANY_TAG */

typedef enum {
    MCA_PML_BASE_SEND_SYNCHRONOUS,
    MCA_PML_BASE_SEND_COMPLETE,
    MCA_PML_BASE_SEND_BUFFERED,
    MCA_PML_BASE_SEND_READY,
    MCA_PML_BASE_SEND_STANDARD,
    MCA_PML_BASE_SEND_SIZE
} mca_pml_base_send_mode_t;


#define OMPI_ANY_TAG    MPI_ANY_TAG
#define OMPI_ANY_SOURCE MPI_ANY_SOURCE
#define OMPI_PROC_NULL  MPI_PROC_NULL

struct ompi_proc_t;

#endif /* #define MCA_PML_CONSTANTS_H */
