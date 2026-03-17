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
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
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

#ifndef OMPI_MCA_SHAREDFP_H
#define OMPI_MCA_SHAREDFP_H

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/request/request.h"
#include "ompi/info/info.h"


BEGIN_C_DECLS

struct ompio_file_t;
struct ompi_file_t;
/*
 * Macro for use in components that are of type coll
 */
#define MCA_SHAREDFP_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("sharedfp", 2, 0, 0)

/*
 * This framework abstracts out operations of the shared filepointer
 * in MPI I/O. It is initialized by the OMPIO module whenever a file is
 * opened.
 *
 * These are the component function prototypes. These function pointers
 * go into the component structure. These functions (query() and finalize()
 * are called during sharedfp_base_select(). Each component is query() ied
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
 * In addition, two functions, namely updating the value of a shared
 * file pointer and moving the shared file pointer (seek) have to be provided
 * by every module.
 */

/*
 * **************** component struct *******************************
 * *********** These functions go in the component struct **********
 * **************** component struct *******************************
 */

typedef int (*mca_sharedfp_base_component_init_query_1_0_0_fn_t)
(bool enable_progress_threads,
 bool enable_mpi_threads);

typedef struct mca_sharedfp_base_module_1_0_0_t *
        (*mca_sharedfp_base_component_file_query_1_0_0_fn_t)
        (struct ompio_file_t *file, int *priority);

typedef int (*mca_sharedfp_base_component_file_unquery_1_0_0_fn_t)
        (struct ompio_file_t *file);

/*
 * ****************** component struct ******************************
 * Structure for sharedfp v2.0.0 components.This is chained to MCA v2.0.0
 * ****************** component struct ******************************
 */

struct mca_sharedfp_base_component_2_0_0_t {
    mca_base_component_t sharedfpm_version;
    mca_base_component_data_t sharedfpm_data;

    mca_sharedfp_base_component_init_query_1_0_0_fn_t sharedfpm_init_query;
    mca_sharedfp_base_component_file_query_1_0_0_fn_t sharedfpm_file_query;
    mca_sharedfp_base_component_file_unquery_1_0_0_fn_t sharedfpm_file_unquery;
};
typedef struct mca_sharedfp_base_component_2_0_0_t mca_sharedfp_base_component_2_0_0_t;
typedef struct mca_sharedfp_base_component_2_0_0_t mca_sharedfp_base_component_t;

/*
 * ***********************************************************************
 * ************************  Interface function definitions **************
 * These are the typedesharedfp for the function pointers to various sharedfp
 * backend functions which will be used by the various sharedfp components
 * ***********************************************************************
 */

typedef int (*mca_sharedfp_base_module_init_1_0_0_fn_t)
(struct ompio_file_t *file);

typedef int (*mca_sharedfp_base_module_finalize_1_0_0_fn_t)
(struct ompio_file_t *file);

/* SHAREDFP function definitions */
/* IMPORTANT: Update here when adding sharedfp component interface functions*/
typedef int (*mca_sharedfp_base_module_seek_fn_t)(
        struct ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset, int whence);
typedef int (*mca_sharedfp_base_module_get_position_fn_t)(
        struct ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE * offset);
typedef int (*mca_sharedfp_base_module_write_fn_t)(
        struct ompio_file_t *fh,
        const void *buf,
        int count,
        struct ompi_datatype_t *datatype,
        ompi_status_public_t *status);
typedef int (*mca_sharedfp_base_module_write_ordered_fn_t)(
        struct ompio_file_t *fh,
        const void *buf,
        int count,
        struct ompi_datatype_t *datatype,
        ompi_status_public_t *status);
typedef int (*mca_sharedfp_base_module_write_ordered_begin_fn_t)(
        struct ompio_file_t *fh,
        const void *buf,
        int count,
        struct ompi_datatype_t *datatype);
typedef int (*mca_sharedfp_base_module_write_ordered_end_fn_t)(
        struct ompio_file_t *fh,
        const void *buf,
        ompi_status_public_t *status);
typedef int (*mca_sharedfp_base_module_iwrite_fn_t)(
        struct ompio_file_t *fh,
        const void *buf,
        int count,
        struct ompi_datatype_t *datatype,
        ompi_request_t ** request);
typedef int (*mca_sharedfp_base_module_read_fn_t)(
        struct ompio_file_t *fh,
        void *buf,
        int count,
        struct ompi_datatype_t *datatype,
        ompi_status_public_t *status);
typedef int (*mca_sharedfp_base_module_read_ordered_fn_t)(
        struct ompio_file_t *fh,
        void *buf,
        int count,
        struct ompi_datatype_t *datatype,
        ompi_status_public_t *status);
typedef int (*mca_sharedfp_base_module_iread_fn_t)(
        struct ompio_file_t *fh,
        void *buf,
        int count,
        struct ompi_datatype_t *datatype,
        ompi_request_t ** request);
typedef int (*mca_sharedfp_base_module_read_ordered_begin_fn_t)(
        struct ompio_file_t *fh,
        void *buf,
        int count,
        struct ompi_datatype_t *datatype);
typedef int (*mca_sharedfp_base_module_read_ordered_end_fn_t)(
        struct ompio_file_t *fh,
        void *buf,
        ompi_status_public_t *status);
typedef int (*mca_sharedfp_base_module_file_open_fn_t)(
        struct ompi_communicator_t *comm, const char *filename, int amode,
        struct opal_info_t *info, struct ompio_file_t *fh);
typedef int (*mca_sharedfp_base_module_file_close_fn_t)(struct ompio_file_t *fh);


/*
 * ***********************************************************************
 * ***************************  module structure *************************
 * ***********************************************************************
 */
struct mca_sharedfp_base_module_1_0_0_t {
    /*
     * Per-file initialization function. This is called only
     * on the module which is selected. The finalize corresponding to
     * this function is present on the component struct above
     */
    mca_sharedfp_base_module_init_1_0_0_fn_t sharedfp_module_init;
    mca_sharedfp_base_module_finalize_1_0_0_fn_t sharedfp_module_finalize;

    /* SHAREDFP function pointers */
    /* IMPORTANT: Update here when adding sharedfp component interface functions*/
    mca_sharedfp_base_module_seek_fn_t        sharedfp_seek;
    mca_sharedfp_base_module_get_position_fn_t sharedfp_get_position;
    mca_sharedfp_base_module_read_fn_t        sharedfp_read;
    mca_sharedfp_base_module_read_ordered_fn_t        sharedfp_read_ordered;
    mca_sharedfp_base_module_read_ordered_begin_fn_t       sharedfp_read_ordered_begin;
    mca_sharedfp_base_module_read_ordered_end_fn_t       sharedfp_read_ordered_end;
    mca_sharedfp_base_module_iread_fn_t       sharedfp_iread;
    mca_sharedfp_base_module_write_fn_t       sharedfp_write;
    mca_sharedfp_base_module_write_ordered_fn_t       sharedfp_write_ordered;
    mca_sharedfp_base_module_write_ordered_begin_fn_t       sharedfp_write_ordered_begin;
    mca_sharedfp_base_module_write_ordered_end_fn_t       sharedfp_write_ordered_end;
    mca_sharedfp_base_module_iwrite_fn_t       sharedfp_iwrite;
    mca_sharedfp_base_module_file_open_fn_t   sharedfp_file_open;
    mca_sharedfp_base_module_file_close_fn_t  sharedfp_file_close;
};
typedef struct mca_sharedfp_base_module_1_0_0_t mca_sharedfp_base_module_1_0_0_t;
typedef mca_sharedfp_base_module_1_0_0_t mca_sharedfp_base_module_t;


/* This structure keeps all of the data needed by a sharedfp module.
 * This structure is assigned to the ompio file handle's
 * 'f_sharedfp_data' attribute during the call to the file_open function.
 */
struct mca_sharedfp_base_data_t{
    /* attributes that will be used by all of the sharedfp components */
    OMPI_MPI_OFFSET_TYPE global_offset;

    /* attributes that are specific to a component are
     * combined into a structure that is assigned to this attribute */
    void   *selected_module_data;
};

/**************************************/

END_C_DECLS

#endif /* OMPI_MCA_SHAREDFP_H */
