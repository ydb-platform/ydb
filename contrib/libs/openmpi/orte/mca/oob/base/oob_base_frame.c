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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include "opal/class/opal_bitmap.h"
#include "orte/mca/mca.h"
#include "opal/runtime/opal_progress_threads.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/mca/rml/base/base.h"
#include "orte/mca/oob/base/base.h"

#if OPAL_ENABLE_FT_CR == 1
#include "orte/mca/state/state.h"
#endif

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/oob/base/static-components.h"

/*
 * Global variables
 */
orte_oob_base_t orte_oob_base = {0};

static int orte_oob_base_register(mca_base_register_flag_t flags)
{
    orte_oob_base.num_threads = 0;
    (void)mca_base_var_register("orte", "oob", "base", "num_progress_threads",
                                "Number of independent progress OOB messages for each interface",
                                MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY,
                                &orte_oob_base.num_threads);

#if OPAL_ENABLE_TIMING
    /* Detailed timing setup */
    orte_oob_base.timing = false;
    (void) mca_base_var_register ("orte", "oob", "base", "timing",
                                  "Enable OOB timings",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_oob_base.timing);
#endif
    return ORTE_SUCCESS;
}

static int orte_oob_base_close(void)
{
    mca_oob_base_component_t *component;
    mca_base_component_list_item_t *cli;
    opal_object_t *value;
    uint64_t key;

    /* shutdown all active transports */
    while (NULL != (cli = (mca_base_component_list_item_t *) opal_list_remove_first (&orte_oob_base.actives))) {
        component = (mca_oob_base_component_t*)cli->cli_component;
        if (NULL != component->shutdown) {
            component->shutdown();
        }
        OBJ_RELEASE(cli);
    }

    if (!ORTE_PROC_IS_APP && !ORTE_PROC_IS_TOOL) {
        opal_progress_thread_finalize("OOB-BASE");
    }

    /* destruct our internal lists */
    OBJ_DESTRUCT(&orte_oob_base.actives);

    /* release all peers from the hash table */
    OPAL_HASH_TABLE_FOREACH(key, uint64, value, &orte_oob_base.peers) {
        if (NULL != value) {
            OBJ_RELEASE(value);
        }
    }

    OBJ_DESTRUCT(&orte_oob_base.peers);

    return mca_base_framework_components_close(&orte_oob_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components,
 * or the one that was specifically requested via a MCA parameter.
 */
static int orte_oob_base_open(mca_base_open_flag_t flags)
{
    /* setup globals */
    orte_oob_base.max_uri_length = -1;
    OBJ_CONSTRUCT(&orte_oob_base.peers, opal_hash_table_t);
    opal_hash_table_init(&orte_oob_base.peers, 128);
    OBJ_CONSTRUCT(&orte_oob_base.actives, opal_list_t);

    if (ORTE_PROC_IS_APP || ORTE_PROC_IS_TOOL) {
        orte_oob_base.ev_base = orte_event_base;
    } else {
        orte_oob_base.ev_base = opal_progress_thread_init("OOB-BASE");
    }


#if OPAL_ENABLE_FT_CR == 1
    /* register the FT events callback */
    orte_state.add_job_state(ORTE_JOB_STATE_FT_CHECKPOINT, orte_oob_base_ft_event, ORTE_ERROR_PRI);
    orte_state.add_job_state(ORTE_JOB_STATE_FT_CONTINUE, orte_oob_base_ft_event, ORTE_ERROR_PRI);
    orte_state.add_job_state(ORTE_JOB_STATE_FT_RESTART, orte_oob_base_ft_event, ORTE_ERROR_PRI);
#endif

     /* Open up all available components */
    return mca_base_framework_components_open(&orte_oob_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, oob, "Out-of-Band Messaging Subsystem",
                           orte_oob_base_register, orte_oob_base_open, orte_oob_base_close,
                           mca_oob_base_static_components, 0);


OBJ_CLASS_INSTANCE(orte_oob_send_t,
                   opal_object_t,
                   NULL, NULL);

static void pr_cons(orte_oob_base_peer_t *ptr)
{
    ptr->component = NULL;
    OBJ_CONSTRUCT(&ptr->addressable, opal_bitmap_t);
    opal_bitmap_init(&ptr->addressable, 8);
}
static void pr_des(orte_oob_base_peer_t *ptr)
{
    OBJ_DESTRUCT(&ptr->addressable);
}
OBJ_CLASS_INSTANCE(orte_oob_base_peer_t,
                   opal_object_t,
                   pr_cons, pr_des);
