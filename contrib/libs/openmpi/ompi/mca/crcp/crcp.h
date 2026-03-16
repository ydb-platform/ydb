/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Checkpoint/Restart Coordination Protocol (CRCP) Interface
 *
 */

#ifndef MCA_CRCP_H
#define MCA_CRCP_H

#include "ompi_config.h"

#include "opal/class/opal_object.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/base.h"
#include "opal/class/opal_free_list.h"

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"


BEGIN_C_DECLS

/**
 * Module initialization function.
 * Returns OMPI_SUCCESS
 */
typedef int (*ompi_crcp_base_module_init_fn_t)
     (void);

/**
 * Module finalization function.
 * Returns OMPI_SUCCESS
 */
typedef int (*ompi_crcp_base_module_finalize_fn_t)
     (void);


/************************
 * MPI Quiesce Interface
 ************************/
/**
 * MPI_Quiesce_start component interface
 */
typedef int (*ompi_crcp_base_quiesce_start_fn_t)
    (MPI_Info *info);

/**
 * MPI_Quiesce_end component interface
 */
typedef int (*ompi_crcp_base_quiesce_end_fn_t)
    (MPI_Info *info);


/************************
 * PML Wrapper hooks
 * PML Wrapper is the CRCPW PML component
 ************************/
/**
 * To allow us to work before and after a PML command
 */
enum ompi_crcp_base_pml_states_t {
    OMPI_CRCP_PML_PRE,
    OMPI_CRCP_PML_POST,
    OMPI_CRCP_PML_SKIP,
    OMPI_CRCP_PML_DONE
};
typedef enum ompi_crcp_base_pml_states_t ompi_crcp_base_pml_states_t;

struct ompi_crcp_base_pml_state_t {
    opal_free_list_item_t super;
    ompi_crcp_base_pml_states_t state;
    int error_code;
    mca_pml_base_component_t  *wrapped_pml_component;
    mca_pml_base_module_t     *wrapped_pml_module;
};
typedef struct ompi_crcp_base_pml_state_t ompi_crcp_base_pml_state_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_crcp_base_pml_state_t);

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_enable_fn_t)
     (bool enable, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_add_comm_fn_t)
     ( struct ompi_communicator_t* comm , ompi_crcp_base_pml_state_t*);
typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_del_comm_fn_t)
     ( struct ompi_communicator_t* comm , ompi_crcp_base_pml_state_t*);

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_add_procs_fn_t)
     ( struct ompi_proc_t **procs, size_t nprocs , ompi_crcp_base_pml_state_t*);
typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_del_procs_fn_t)
     ( struct ompi_proc_t **procs, size_t nprocs , ompi_crcp_base_pml_state_t*);

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_progress_fn_t)
     (ompi_crcp_base_pml_state_t*);

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_iprobe_fn_t)
     (int dst, int tag, struct ompi_communicator_t* comm, int *matched,
      ompi_status_public_t* status, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_probe_fn_t)
     ( int dst, int tag, struct ompi_communicator_t* comm,
       ompi_status_public_t* status, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_isend_init_fn_t)
     ( void *buf, size_t count, ompi_datatype_t *datatype, int dst, int tag,
       mca_pml_base_send_mode_t mode, struct ompi_communicator_t* comm,
       struct ompi_request_t **request, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_isend_fn_t)
     ( void *buf, size_t count, ompi_datatype_t *datatype, int dst, int tag,
       mca_pml_base_send_mode_t mode, struct ompi_communicator_t* comm,
       struct ompi_request_t **request, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_send_fn_t)
     ( void *buf, size_t count, ompi_datatype_t *datatype, int dst, int tag,
       mca_pml_base_send_mode_t mode, struct ompi_communicator_t* comm,
       ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_irecv_init_fn_t)
     ( void *buf, size_t count, ompi_datatype_t *datatype, int src, int tag,
       struct ompi_communicator_t* comm,  struct ompi_request_t **request,
       ompi_crcp_base_pml_state_t*);

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_irecv_fn_t)
     ( void *buf, size_t count, ompi_datatype_t *datatype, int src, int tag,
       struct ompi_communicator_t* comm, struct ompi_request_t **request,
       ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_recv_fn_t)
     (  void *buf, size_t count, ompi_datatype_t *datatype, int src, int tag,
        struct ompi_communicator_t* comm,  ompi_status_public_t* status,
        ompi_crcp_base_pml_state_t*);

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_dump_fn_t)
     ( struct ompi_communicator_t* comm, int verbose, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_start_fn_t)
     ( size_t count, ompi_request_t** requests, ompi_crcp_base_pml_state_t* );

typedef ompi_crcp_base_pml_state_t* (*ompi_crcp_base_pml_ft_event_fn_t)
     (int state, ompi_crcp_base_pml_state_t*);

/* Request Interface */
typedef int (*ompi_crcp_base_request_complete_fn_t)
     (struct ompi_request_t *request);

/************************
 * BTL Wrapper hooks
 * JJH: Wrapper BTL not currently implemented.
 ************************/
/**
 * To allow us to work before and after a BTL command
 */
enum ompi_crcp_base_btl_states_t {
    OMPI_CRCP_BTL_PRE,
    OMPI_CRCP_BTL_POST,
    OMPI_CRCP_BTL_SKIP,
    OMPI_CRCP_BTL_DONE
};
typedef enum ompi_crcp_base_btl_states_t ompi_crcp_base_btl_states_t;

struct ompi_crcp_base_btl_state_t {
    opal_free_list_item_t super;
    ompi_crcp_base_btl_states_t state;
    int error_code;
    mca_btl_base_descriptor_t* des;
    mca_btl_base_component_t  *wrapped_btl_component;
    mca_btl_base_module_t     *wrapped_btl_module;
};
typedef struct ompi_crcp_base_btl_state_t ompi_crcp_base_btl_state_t;
OBJ_CLASS_DECLARATION(ompi_crcp_base_btl_state_t);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_add_procs_fn_t)
     ( struct mca_btl_base_module_t* btl,
       size_t nprocs,
       struct ompi_proc_t** procs,
       struct mca_btl_base_endpoint_t** endpoints,
       struct opal_bitmap_t* reachable,
       ompi_crcp_base_btl_state_t* );

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_del_procs_fn_t)
     ( struct mca_btl_base_module_t* btl,
       size_t nprocs,
       struct ompi_proc_t** procs,
       struct mca_btl_base_endpoint_t**,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_register_fn_t)
     ( struct mca_btl_base_module_t* btl,
       mca_btl_base_tag_t tag,
       mca_btl_base_module_recv_cb_fn_t cbfunc,
       void* cbdata,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_finalize_fn_t)
     ( struct mca_btl_base_module_t* btl,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_alloc_fn_t)
     ( struct mca_btl_base_module_t* btl,
       size_t size,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_free_fn_t)
     ( struct mca_btl_base_module_t* btl,
       mca_btl_base_descriptor_t* descriptor,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_prepare_fn_t)
     ( struct mca_btl_base_module_t* btl,
       struct mca_btl_base_endpoint_t* endpoint,
       mca_rcache_base_registration_t* registration,
       struct opal_convertor_t* convertor,
       size_t reserve,
       size_t* size,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_send_fn_t)
     ( struct mca_btl_base_module_t* btl,
       struct mca_btl_base_endpoint_t* endpoint,
       struct mca_btl_base_descriptor_t* descriptor,
       mca_btl_base_tag_t tag,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_put_fn_t)
     ( struct mca_btl_base_module_t* btl,
       struct mca_btl_base_endpoint_t* endpoint,
       struct mca_btl_base_descriptor_t* descriptor,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_get_fn_t)
     ( struct mca_btl_base_module_t* btl,
       struct mca_btl_base_endpoint_t* endpoint,
       struct mca_btl_base_descriptor_t* descriptor,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_dump_fn_t)
     ( struct mca_btl_base_module_t* btl,
       struct mca_btl_base_endpoint_t* endpoint,
       int verbose,
       ompi_crcp_base_btl_state_t*);

typedef ompi_crcp_base_btl_state_t* (*mca_crcp_base_btl_module_ft_event_fn_t)
     (int state,
      ompi_crcp_base_btl_state_t*);


/**
 * Structure for CRCP components.
 */
struct ompi_crcp_base_component_2_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;

    /** Verbosity Level */
    int verbose;
    /** Output Handle for opal_output */
    int output_handle;
    /** Default Priority */
    int priority;

};
typedef struct ompi_crcp_base_component_2_0_0_t ompi_crcp_base_component_2_0_0_t;
typedef struct ompi_crcp_base_component_2_0_0_t ompi_crcp_base_component_t;

/**
 * Structure for CRCP modules
 */
struct ompi_crcp_base_module_1_0_0_t {
    /** Initialization Function */
    ompi_crcp_base_module_init_fn_t           crcp_init;
    /** Finalization Function */
    ompi_crcp_base_module_finalize_fn_t       crcp_finalize;

    /**< MPI_Quiesce Interface Functions ******************/
    ompi_crcp_base_quiesce_start_fn_t         quiesce_start;
    ompi_crcp_base_quiesce_end_fn_t           quiesce_end;

    /**< PML Wrapper Functions ****************************/
    ompi_crcp_base_pml_enable_fn_t            pml_enable;

    ompi_crcp_base_pml_add_comm_fn_t          pml_add_comm;
    ompi_crcp_base_pml_del_comm_fn_t          pml_del_comm;

    ompi_crcp_base_pml_add_procs_fn_t         pml_add_procs;
    ompi_crcp_base_pml_del_procs_fn_t         pml_del_procs;

    ompi_crcp_base_pml_progress_fn_t          pml_progress;

    ompi_crcp_base_pml_iprobe_fn_t            pml_iprobe;
    ompi_crcp_base_pml_probe_fn_t             pml_probe;

    ompi_crcp_base_pml_isend_init_fn_t        pml_isend_init;
    ompi_crcp_base_pml_isend_fn_t             pml_isend;
    ompi_crcp_base_pml_send_fn_t              pml_send;

    ompi_crcp_base_pml_irecv_init_fn_t        pml_irecv_init;
    ompi_crcp_base_pml_irecv_fn_t             pml_irecv;
    ompi_crcp_base_pml_recv_fn_t              pml_recv;

    ompi_crcp_base_pml_dump_fn_t              pml_dump;
    ompi_crcp_base_pml_start_fn_t             pml_start;

    ompi_crcp_base_pml_ft_event_fn_t          pml_ft_event;

    /**< Request complete Function ****************************/
    ompi_crcp_base_request_complete_fn_t      request_complete;

    /**< BTL Wrapper Functions ****************************/
    mca_crcp_base_btl_module_add_procs_fn_t   btl_add_procs;
    mca_crcp_base_btl_module_del_procs_fn_t   btl_del_procs;

    mca_crcp_base_btl_module_register_fn_t    btl_register;
    mca_crcp_base_btl_module_finalize_fn_t    btl_finalize;

    mca_crcp_base_btl_module_alloc_fn_t       btl_alloc;
    mca_crcp_base_btl_module_free_fn_t        btl_free;

    mca_crcp_base_btl_module_prepare_fn_t     btl_prepare_src;
    mca_crcp_base_btl_module_prepare_fn_t     btl_prepare_dst;

    mca_crcp_base_btl_module_send_fn_t        btl_send;
    mca_crcp_base_btl_module_put_fn_t         btl_put;
    mca_crcp_base_btl_module_get_fn_t         btl_get;

    mca_crcp_base_btl_module_dump_fn_t        btl_dump;

    mca_crcp_base_btl_module_ft_event_fn_t    btl_ft_event;
};
typedef struct ompi_crcp_base_module_1_0_0_t ompi_crcp_base_module_1_0_0_t;
typedef struct ompi_crcp_base_module_1_0_0_t ompi_crcp_base_module_t;

OMPI_DECLSPEC extern ompi_crcp_base_module_t ompi_crcp;

/**
 * Macro for use in components that are of type CRCP
 */
#define OMPI_CRCP_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("crcp", 2, 0, 0)

/**
 * Macro to call the CRCP Request Complete function
 */
#if OPAL_ENABLE_FT_CR == 1
#define OMPI_CRCP_REQUEST_COMPLETE(req)      \
  if( NULL != ompi_crcp.request_complete) {  \
    ompi_crcp.request_complete(req);         \
  }
#else
#define OMPI_CRCP_REQUEST_COMPLETE(req) ;
#endif

END_C_DECLS

#endif /* OMPI_CRCP_H */
