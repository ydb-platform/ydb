/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#ifndef MCA_BTL_BASE_H
#define MCA_BTL_BASE_H

#include "opal_config.h"
#include "opal/class/opal_list.h"
#include "opal/mca/mca.h"
#include "opal/mca/btl/btl.h"

BEGIN_C_DECLS

struct mca_btl_base_selected_module_t {
  opal_list_item_t super;
  mca_btl_base_component_t *btl_component;
  mca_btl_base_module_t *btl_module;
};
typedef struct mca_btl_base_selected_module_t mca_btl_base_selected_module_t;


/* holds the recv call back function to be called by the btl on
 * a receive.
 */
struct mca_btl_base_recv_reg_t {
    mca_btl_base_module_recv_cb_fn_t cbfunc;
    void* cbdata;
};
typedef struct mca_btl_base_recv_reg_t mca_btl_base_recv_reg_t;


OPAL_DECLSPEC OBJ_CLASS_DECLARATION(mca_btl_base_selected_module_t);

/*
 * Global functions for MCA: overall BTL open and close
 */

OPAL_DECLSPEC  int mca_btl_base_select(bool enable_progress_threads, bool enable_mpi_threads);
OPAL_DECLSPEC  void mca_btl_base_dump(
    struct mca_btl_base_module_t*,
    struct mca_btl_base_endpoint_t*,
    int verbose);
OPAL_DECLSPEC  int mca_btl_base_param_register(mca_base_component_t *version,
        mca_btl_base_module_t *module);
OPAL_DECLSPEC  int mca_btl_base_param_verify(mca_btl_base_module_t *module);

/*
 * Globals
 */
extern char* mca_btl_base_include;
extern char* mca_btl_base_exclude;
extern int mca_btl_base_warn_component_unused;
extern int mca_btl_base_already_opened;
OPAL_DECLSPEC extern opal_list_t mca_btl_base_modules_initialized;
OPAL_DECLSPEC extern bool mca_btl_base_thread_multiple_override;

OPAL_DECLSPEC extern mca_base_framework_t opal_btl_base_framework;

extern mca_base_var_enum_flag_t *mca_btl_base_flag_enum;
extern mca_base_var_enum_flag_t *mca_btl_base_atomic_enum;

END_C_DECLS

#endif /* MCA_BTL_BASE_H */
