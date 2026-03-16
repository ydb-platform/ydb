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
 * Copyright (c) 2008-2014 University of Houston. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_MCA_FBTL_H
#define OMPI_MCA_FBTL_H

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/request/request.h"
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

BEGIN_C_DECLS

struct ompio_file_t;
struct mca_ompio_request_t;

/*
 * Macro for use in components that are of type coll
 */
#define MCA_FBTL_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("fbtl", 2, 0, 0)

/*
 * The file byte transfer layer (fbtl) framework provides the abstraction
 * for individual blocking and non-blocking read and write operations.
 * The functionality provided by the interfaces in this module
 * can be used to implement the corresponding operations in MPI I/O.
 * Note however, that the interfaces are not a one-to-one mapping
 * of the MPI individual read and write operations, since the fbtl framework
 * avoids using derived MPI datatypes. The step mapping/unrolling the MPI
 * derived data types into a vector of (offset into file, memory address, length)
 * is done in the OMPIO module of the IO framework.
 *
 * These are the component function prototypes. These function pointers
 * go into the component structure. These functions (query() and finalize()
 * are called during fbtl_base_select(). Each component is query() ied
 * and subsequently, all the unselected components are finalize() 'ed
 * so that any *stuff* they did during query() can be undone. By
 * similar logic, finalize() is also called on the component which
 * was selected when the communicator is being destroyed.
 *
 * So, to sum it up, every component carries 4 functions:
 * 1. open() - called during MPI_INIT
 * 2. close() - called during MPI_FINALIZE
 * 3. query() - called to select a particular component
 * 4. finalize() - called when actions taken during query have
 *                 to be undone
 */

/*
 * **************** component struct *******************************
 * *********** These functions go in the component struct **********
 * **************** component struct *******************************
 */

typedef int (*mca_fbtl_base_component_init_query_1_0_0_fn_t)
    (bool enable_progress_threads,
     bool enable_mpi_threads);

typedef struct mca_fbtl_base_module_1_0_0_t *
(*mca_fbtl_base_component_file_query_1_0_0_fn_t) (struct ompio_file_t *file,
                                                  int *priority);

typedef int (*mca_fbtl_base_component_file_unquery_1_0_0_fn_t)
    (struct ompio_file_t *file);

/*
 * ****************** component struct ******************************
 * Structure for fbtl v2.0.0 components.This is chained to MCA v2.0.0
 * ****************** component struct ******************************
 */
struct mca_fbtl_base_component_2_0_0_t {
    mca_base_component_t fbtlm_version;
    mca_base_component_data_t fbtlm_data;

    mca_fbtl_base_component_init_query_1_0_0_fn_t fbtlm_init_query;
    mca_fbtl_base_component_file_query_1_0_0_fn_t fbtlm_file_query;
    mca_fbtl_base_component_file_unquery_1_0_0_fn_t fbtlm_file_unquery;
};
typedef struct mca_fbtl_base_component_2_0_0_t mca_fbtl_base_component_2_0_0_t;
typedef struct mca_fbtl_base_component_2_0_0_t mca_fbtl_base_component_t;

/*
 * ***********************************************************************
 * ************************  Interface function definitions **************
 * These are the typedefbtl for the function pointers to various fbtl
 * backend functions which will be used by the various fbtl components
 * ***********************************************************************
 */

typedef int (*mca_fbtl_base_module_init_1_0_0_fn_t)
    (struct ompio_file_t *file);

typedef int (*mca_fbtl_base_module_finalize_1_0_0_fn_t)
    (struct ompio_file_t *file);


typedef ssize_t (*mca_fbtl_base_module_preadv_fn_t)
    (struct ompio_file_t *file );
typedef ssize_t (*mca_fbtl_base_module_pwritev_fn_t)
    (struct ompio_file_t *file );
typedef ssize_t (*mca_fbtl_base_module_ipreadv_fn_t)
    (struct ompio_file_t *file,
     ompi_request_t *request);
typedef ssize_t (*mca_fbtl_base_module_ipwritev_fn_t)
    (struct ompio_file_t *file,
     ompi_request_t *request);
typedef bool (*mca_fbtl_base_module_progress_fn_t)
    ( struct mca_ompio_request_t *request);

typedef void (*mca_fbtl_base_module_request_free_fn_t)
    ( struct mca_ompio_request_t *request);
/*
 * ***********************************************************************
 * ***************************  module structure *************************
 * ***********************************************************************
 */
struct mca_fbtl_base_module_1_0_0_t {
    /*
     * Per-file initialization function. This is called only
     * on the module which is selected. The finalize corresponding to
     * this function is present on the component struct above
     */
    mca_fbtl_base_module_init_1_0_0_fn_t fbtl_module_init;
    mca_fbtl_base_module_finalize_1_0_0_fn_t fbtl_module_finalize;

    /* FBTL function pointers */
    mca_fbtl_base_module_preadv_fn_t        fbtl_preadv;
    mca_fbtl_base_module_ipreadv_fn_t       fbtl_ipreadv;
    mca_fbtl_base_module_pwritev_fn_t       fbtl_pwritev;
    mca_fbtl_base_module_ipwritev_fn_t      fbtl_ipwritev;
    mca_fbtl_base_module_progress_fn_t      fbtl_progress;
    mca_fbtl_base_module_request_free_fn_t  fbtl_request_free;
};
typedef struct mca_fbtl_base_module_1_0_0_t mca_fbtl_base_module_1_0_0_t;
typedef mca_fbtl_base_module_1_0_0_t mca_fbtl_base_module_t;

END_C_DECLS

#endif /* OMPI_MCA_FBTL_H */
