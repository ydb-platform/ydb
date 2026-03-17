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
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_MCA_FS_H
#define OMPI_MCA_FS_H

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/info/info.h"

BEGIN_C_DECLS

struct ompio_file_t;

/*
 * Macro for use in components that are of type coll
 */
#define MCA_FS_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("fs", 2, 0, 0)

/*
 * This framework provides the abstraction for file management operations
 * of the MPI I/O chapter in MPI-2. The operations defined by this
 * framework are mostly collective in nature.
 *
 * These are the component function prototypes. These function pointers
 * go into the component structure. These functions (query() and finalize()
 * are called during fs_base_select(). Each component is query() ied
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

typedef int (*mca_fs_base_component_init_query_1_0_0_fn_t)
    (bool enable_progress_threads,
     bool enable_mpi_threads);

typedef struct mca_fs_base_module_1_0_0_t *
(*mca_fs_base_component_file_query_1_0_0_fn_t) (struct ompio_file_t *file,
                                                int *priority);

typedef int (*mca_fs_base_component_file_unquery_1_0_0_fn_t)
    (struct ompio_file_t *file);

/*
 * ****************** component struct ******************************
 * Structure for fs v2.0.0 components.This is chained to MCA v2.0.0
 * ****************** component struct ******************************
 */
struct mca_fs_base_component_2_0_0_t {
    mca_base_component_t fsm_version;
    mca_base_component_data_t fsm_data;

    mca_fs_base_component_init_query_1_0_0_fn_t fsm_init_query;
    mca_fs_base_component_file_query_1_0_0_fn_t fsm_file_query;
    mca_fs_base_component_file_unquery_1_0_0_fn_t fsm_file_unquery;
};
typedef struct mca_fs_base_component_2_0_0_t mca_fs_base_component_2_0_0_t;
typedef struct mca_fs_base_component_2_0_0_t mca_fs_base_component_t;

/*
 * ***********************************************************************
 * ************************  Interface function definitions **************
 * These are the typedefs for the function pointers to various fs
 * backend functions which will be used by the various fs components
 * ***********************************************************************
 */

typedef int (*mca_fs_base_module_init_1_0_0_fn_t)
(struct ompio_file_t *file);

typedef int (*mca_fs_base_module_finalize_1_0_0_fn_t)
(struct ompio_file_t *file);

typedef int (*mca_fs_base_module_file_open_fn_t)(
    struct ompi_communicator_t *comm, const char *filename, int amode,
    struct opal_info_t *info, struct ompio_file_t *fh);
typedef int (*mca_fs_base_module_file_close_fn_t)(struct ompio_file_t *fh);
typedef int (*mca_fs_base_module_file_delete_fn_t)(
    char *filename, struct opal_info_t *info);
typedef int (*mca_fs_base_module_file_set_size_fn_t)
    (struct ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE size);
typedef int (*mca_fs_base_module_file_get_size_fn_t)
    (struct ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE *size);
typedef int (*mca_fs_base_module_file_sync_fn_t)
    (struct ompio_file_t *fh);

/*
 * ***********************************************************************
 * ***************************  module structure *************************
 * ***********************************************************************
 */
struct mca_fs_base_module_1_0_0_t {
    /*
     * Per-file initialization function. This is called only
     * on the module which is selected. The finalize corresponding to
     * this function is present on the component struct above
     */
    mca_fs_base_module_init_1_0_0_fn_t fs_module_init;
    mca_fs_base_module_finalize_1_0_0_fn_t fs_module_finalize;

    /* FS function pointers */
    mca_fs_base_module_file_open_fn_t        fs_file_open;
    mca_fs_base_module_file_close_fn_t       fs_file_close;
    mca_fs_base_module_file_delete_fn_t      fs_file_delete;
    mca_fs_base_module_file_set_size_fn_t    fs_file_set_size;
    mca_fs_base_module_file_get_size_fn_t    fs_file_get_size;
    mca_fs_base_module_file_sync_fn_t        fs_file_sync;
};
typedef struct mca_fs_base_module_1_0_0_t mca_fs_base_module_1_0_0_t;
typedef mca_fs_base_module_1_0_0_t mca_fs_base_module_t;

END_C_DECLS

#endif /* OMPI_MCA_FS_H */
