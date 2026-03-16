/*
 * Copyright (c) 2004-2007 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include "vprotocol_pessimist.h"

mca_vprotocol_pessimist_module_t mca_vprotocol_pessimist =
{
  {
    /* mca_pml_base_module_add_procs_fn_t     */ NULL,
    /* mca_pml_base_module_del_procs_fn_t     */ NULL,
    /* mca_pml_base_module_enable_fn_f        */ mca_vprotocol_pessimist_enable,
#ifdef SB_USE_PROGRESS_METHOD
    /* mca_pml_base_module_progress_fn_t      */ mca_vprotocol_pessimist_progress,
#else
    /* mca_pml_base_module_progress_fn_t      */ NULL,
#endif
    /* mca_pml_base_module_add_comm_fn_t      */ NULL,
    /* mca_pml_base_module_del_comm_fn_t      */ NULL,
    /* mca_pml_base_module_irecv_init_fn_t    */ NULL,
    /* mca_pml_base_module_irecv_fn_t         */ mca_vprotocol_pessimist_irecv,
    /* mca_pml_base_module_recv_fn_t          */ mca_vprotocol_pessimist_recv,
    /* mca_pml_base_module_isend_init_fn_t    */ NULL,
    /* mca_pml_base_module_isend_fn_t         */ mca_vprotocol_pessimist_isend,
    /* mca_pml_base_module_send_fn_t          */ mca_vprotocol_pessimist_send,
    /* mca_pml_base_module_iprobe_fn_t        */ mca_vprotocol_pessimist_iprobe,
    /* mca_pml_base_module_probe_fn_t         */ mca_vprotocol_pessimist_probe,
    /* mca_pml_base_module_start_fn_t         */ mca_vprotocol_pessimist_start,
    /* mca_pml_base_module_dump_fn_t          */ mca_vprotocol_pessimist_dump,

    /* ompi_request_test_fn_t                 */ mca_vprotocol_pessimist_test,
    /* ompi_request_testany_fn_t              */ mca_vprotocol_pessimist_test_any,
    /* ompi_request_testall_fn_t              */ mca_vprotocol_pessimist_test_all,
    /* ompi_request_testsome_fn_t             */ mca_vprotocol_pessimist_test_some,
    /* ompi_request_wait_fn_t                 */ NULL,
    /* ompi_request_waitany_fn_t              */ mca_vprotocol_pessimist_wait_any,
    /* ompi_request_waitall_fn_t              */ NULL,
    /* ompi_request_waitsome_fn_t             */ mca_vprotocol_pessimist_wait_some,

    /* opal_class_t *                         */ OBJ_CLASS(mca_vprotocol_pessimist_recv_request_t),
    /* opal_class_t *                         */ OBJ_CLASS(mca_vprotocol_pessimist_send_request_t),
  },
};

int mca_vprotocol_pessimist_dump(struct ompi_communicator_t* comm, int verbose)
{
  V_OUTPUT_VERBOSE(verbose, "vprotocol_pessimist: dump for comm %d", comm->c_contextid);
  return mca_pml_v.host_pml.pml_dump(comm, verbose);
}
