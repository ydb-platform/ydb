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
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 */

#ifndef MCA_GRPCOMM_BASE_H
#define MCA_GRPCOMM_BASE_H

/*
 * includes
 */
#include "orte_config.h"

#include "opal/class/opal_list.h"
#include "opal/class/opal_hash_table.h"
#include "opal/dss/dss_types.h"
#include "orte/mca/mca.h"
#include "opal/mca/hwloc/hwloc-internal.h"

#include "orte/mca/odls/odls_types.h"
#include "orte/mca/rml/rml_types.h"
#include "orte/mca/grpcomm/grpcomm.h"


/*
 * Global functions for MCA overall collective open and close
 */
BEGIN_C_DECLS

/*
 * MCA framework
 */
ORTE_DECLSPEC extern mca_base_framework_t orte_grpcomm_base_framework;
/*
 * Select an available component.
 */
ORTE_DECLSPEC int orte_grpcomm_base_select(void);

/*
 * globals that might be needed
 */
typedef struct {
    opal_list_item_t super;
    int pri;
    orte_grpcomm_base_module_t *module;
    mca_base_component_t *component;
} orte_grpcomm_base_active_t;
OBJ_CLASS_DECLARATION(orte_grpcomm_base_active_t);

typedef struct {
    opal_list_t actives;
    opal_list_t ongoing;
    opal_hash_table_t sig_table;
    char *transports;
} orte_grpcomm_base_t;

ORTE_DECLSPEC extern orte_grpcomm_base_t orte_grpcomm_base;

/* Public API stubs */
ORTE_DECLSPEC int orte_grpcomm_API_xcast(orte_grpcomm_signature_t *sig,
                                         orte_rml_tag_t tag,
                                         opal_buffer_t *buf);

ORTE_DECLSPEC int orte_grpcomm_API_allgather(orte_grpcomm_signature_t *sig,
                                             opal_buffer_t *buf,
                                             orte_grpcomm_cbfunc_t cbfunc,
                                             void *cbdata);

ORTE_DECLSPEC orte_grpcomm_coll_t* orte_grpcomm_base_get_tracker(orte_grpcomm_signature_t *sig, bool create);
ORTE_DECLSPEC void orte_grpcomm_base_mark_distance_recv(orte_grpcomm_coll_t *coll, uint32_t distance);
ORTE_DECLSPEC unsigned int orte_grpcomm_base_check_distance_recv(orte_grpcomm_coll_t *coll, uint32_t distance);

END_C_DECLS
#endif
