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
 * Copyright (c) 2008-2015 University of Houston. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_MCA_FCOLL_H
#define OMPI_MCA_FCOLL_H

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

struct ompio_file_t;
struct mca_fcoll_request_t;

/*
 * Macro for use in components that are of type coll
 */
#define MCA_FCOLL_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("fcoll", 2, 0, 0)

/*
 * This framework provides the abstraction for the collective file
 * read and write operations of MPI I/O. The interfaces include
 * blocking collective operations using the individual file pointer,
 * blocking collective operations using explicit offsets and
 * the split collective operations defined in MPI/O for the same.
 *
 * These are the component function prototypes. These function pointers
 * go into the component structure. These functions (query() and finalize()
 * are called during fcoll_base_select(). Each component is query() ied
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

typedef int (*mca_fcoll_base_component_init_query_1_0_0_fn_t)
    (bool enable_progress_threads,
     bool enable_mpi_threads);

typedef struct mca_fcoll_base_module_1_0_0_t *
(*mca_fcoll_base_component_file_query_1_0_0_fn_t) (struct ompio_file_t *file,
                                                   int *priority);

typedef int (*mca_fcoll_base_component_file_unquery_1_0_0_fn_t)
    (struct ompio_file_t *file);

/*
 * ****************** component struct ******************************
 * Structure for fcoll v2.0.0 components.This is chained to MCA v2.0.0
 * ****************** component struct ******************************
 */
struct mca_fcoll_base_component_2_0_0_t {
    mca_base_component_t fcollm_version;
    mca_base_component_data_t fcollm_data;

    mca_fcoll_base_component_init_query_1_0_0_fn_t fcollm_init_query;
    mca_fcoll_base_component_file_query_1_0_0_fn_t fcollm_file_query;
    mca_fcoll_base_component_file_unquery_1_0_0_fn_t fcollm_file_unquery;
};
typedef struct mca_fcoll_base_component_2_0_0_t mca_fcoll_base_component_2_0_0_t;
typedef struct mca_fcoll_base_component_2_0_0_t mca_fcoll_base_component_t;

/*
 * ***********************************************************************
 * ************************  Interface function definitions **************
 * These are the typedefcoll for the function pointers to various fcoll
 * backend functions which will be used by the various fcoll components
 * ***********************************************************************
 */

typedef int (*mca_fcoll_base_module_init_1_0_0_fn_t)
(struct ompio_file_t *file);

typedef int (*mca_fcoll_base_module_finalize_1_0_0_fn_t)
(struct ompio_file_t *file);

typedef int (*mca_fcoll_base_module_file_read_all_fn_t)
(struct ompio_file_t *fh,
 void *buf,
 int count,
 struct ompi_datatype_t *datatype,
 ompi_status_public_t *status);

typedef int (*mca_fcoll_base_module_file_iread_all_fn_t)
(struct ompio_file_t *fh,
 void *buf,
 int count,
 struct ompi_datatype_t *datatype,
 ompi_request_t **request);

typedef int (*mca_fcoll_base_module_file_write_all_fn_t)
(struct ompio_file_t *fh,
 const void *buf,
 int count,
 struct ompi_datatype_t *datatype,
 ompi_status_public_t *status);

typedef int (*mca_fcoll_base_module_file_iwrite_all_fn_t)
(struct ompio_file_t *fh,
 const void *buf,
 int count,
 struct ompi_datatype_t *datatype,
 ompi_request_t **request);

typedef bool (*mca_fcoll_base_module_progress_fn_t)
( struct mca_fcoll_request_t *request);

typedef void (*mca_fcoll_base_module_request_free_fn_t)
( struct mca_fcoll_request_t *request);

/*
 * ***********************************************************************
 * ***************************  module structure *************************
 * ***********************************************************************
 */
struct mca_fcoll_base_module_1_0_0_t {
    /*
     * Per-file initialization function. This is called only
     * on the module which is selected. The finalize corresponding to
     * this function is present on the component struct above
     */
    mca_fcoll_base_module_init_1_0_0_fn_t fcoll_module_init;
    mca_fcoll_base_module_finalize_1_0_0_fn_t fcoll_module_finalize;

    /* FCOLL function pointers */
    mca_fcoll_base_module_file_read_all_fn_t           fcoll_file_read_all;
    mca_fcoll_base_module_file_iread_all_fn_t          fcoll_file_iread_all;
    mca_fcoll_base_module_file_write_all_fn_t          fcoll_file_write_all;
    mca_fcoll_base_module_file_iwrite_all_fn_t         fcoll_file_iwrite_all;
    mca_fcoll_base_module_progress_fn_t                fcoll_progress;
    mca_fcoll_base_module_request_free_fn_t            fcoll_request_free;

};
typedef struct mca_fcoll_base_module_1_0_0_t mca_fcoll_base_module_1_0_0_t;
typedef mca_fcoll_base_module_1_0_0_t mca_fcoll_base_module_t;

END_C_DECLS

#endif /* OMPI_MCA_FCOLL_H */
