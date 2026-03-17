/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2014-2016 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_RML_OOB_RML_OOB_H
#define MCA_RML_OOB_RML_OOB_H

#include "orte_config.h"

#include "opal/dss/dss_types.h"
#include "opal/mca/event/event.h"

#include "orte/mca/oob/oob.h"

#include "orte/mca/rml/base/base.h"

BEGIN_C_DECLS

typedef struct {
    orte_rml_base_module_t  api;
    opal_list_t             queued_routing_messages;
    opal_event_t            *timer_event;
    struct timeval          timeout;
    char                    *routed; // name of routed module to be used
} orte_rml_oob_module_t;

ORTE_MODULE_DECLSPEC extern orte_rml_component_t mca_rml_oob_component;

void orte_rml_oob_fini(struct orte_rml_base_module_t *mod);

int orte_rml_oob_send_nb(struct orte_rml_base_module_t *mod,
                         orte_process_name_t* peer,
                         struct iovec* msg,
                         int count,
                         orte_rml_tag_t tag,
                         orte_rml_callback_fn_t cbfunc,
                         void* cbdata);

int orte_rml_oob_send_buffer_nb(struct orte_rml_base_module_t *mod,
                                orte_process_name_t* peer,
                                opal_buffer_t* buffer,
                                orte_rml_tag_t tag,
                                orte_rml_buffer_callback_fn_t cbfunc,
                                void* cbdata);

int orte_rml_oob_ping(struct orte_rml_base_module_t *mod,
                      const char* uri,
                      const struct timeval* tv);

END_C_DECLS

#endif
