/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef __INCLUDE_VPROTOCOL_H_
#define __INCLUDE_VPROTOCOL_H_

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/request/request.h"

BEGIN_C_DECLS

/* PML_V->PROTOCOL Called by MCA_PML_V framework to initialize the component.
 *
 * @param priority (OUT) Relative priority or ranking used by MCA to
 * select a component.
 *
 * @param enable_progress_threads (IN) Whether this component is
 * allowed to run a hidden/progress thread or not.
 *
 * @param enable_mpi_threads (IN) Whether support for multiple MPI
 * threads is enabled or not (i.e., MPI_THREAD_MULTIPLE), which
 * indicates whether multiple threads may invoke this component
 * simultaneously or not.
 */
typedef struct mca_vprotocol_base_module_2_0_0_t *
    (*mca_vprotocol_base_component_init_fn_t)(int *priority,
                                              bool enable_progress_threads,
                                              bool enable_mpi_threads);

/* Release any resource allocated in init
 */
typedef int (*mca_vprotocol_base_component_finalize_fn_t)(void);


/* The MCA type for class instance
 */
typedef struct mca_vprotocol_base_component_2_0_0_t {
    mca_base_component_t pmlm_version;
    mca_base_component_data_t pmlm_data;
    mca_vprotocol_base_component_init_fn_t pmlm_init;
    mca_vprotocol_base_component_finalize_fn_t pmlm_finalize;
} mca_vprotocol_base_component_2_0_0_t;
typedef mca_vprotocol_base_component_2_0_0_t mca_vprotocol_base_component_t;

/* The base module of the component
 */
typedef struct mca_vprotocol_base_module_2_0_0_t
{
    /* PML module stuff */
    mca_pml_base_module_add_procs_fn_t      add_procs;
    mca_pml_base_module_del_procs_fn_t      del_procs;
    mca_pml_base_module_enable_fn_t         enable;
    mca_pml_base_module_progress_fn_t       progress;
    mca_pml_base_module_add_comm_fn_t       add_comm;
    mca_pml_base_module_del_comm_fn_t       del_comm;
    mca_pml_base_module_irecv_init_fn_t     irecv_init;
    mca_pml_base_module_irecv_fn_t          irecv;
    mca_pml_base_module_recv_fn_t           recv;
    mca_pml_base_module_isend_init_fn_t     isend_init;
    mca_pml_base_module_isend_fn_t          isend;
    mca_pml_base_module_send_fn_t           send;
    mca_pml_base_module_iprobe_fn_t         iprobe;
    mca_pml_base_module_probe_fn_t          probe;
    mca_pml_base_module_start_fn_t          start;
    mca_pml_base_module_dump_fn_t           dump;
    /* Request wait/test stuff */
    ompi_request_test_fn_t                  test;
    ompi_request_test_any_fn_t              test_any;
    ompi_request_test_all_fn_t              test_all;
    ompi_request_test_some_fn_t             test_some;
    ompi_request_wait_fn_t                  wait;
    ompi_request_wait_any_fn_t              wait_any;
    ompi_request_wait_all_fn_t              wait_all;
    ompi_request_wait_some_fn_t             wait_some;

    /* Custom requests classes to add extra data at end of pml requests */
    opal_class_t *                            req_recv_class;
    opal_class_t *                            req_send_class;
} mca_vprotocol_base_module_2_0_0_t;
typedef mca_vprotocol_base_module_2_0_0_t mca_vprotocol_base_module_t;

END_C_DECLS

/* silently include the pml_v.h as every file including vprotocol.h will also
 * need it
 */
#include "ompi/mca/pml/v/pml_v.h"
#include "ompi/mca/vprotocol/base/base.h"

#endif /* __INCLUDE_VPROTOCOL_H_ */
