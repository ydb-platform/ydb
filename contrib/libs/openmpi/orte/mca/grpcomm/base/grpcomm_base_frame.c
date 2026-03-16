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
 * Copyright (c) 2011-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"

#include "orte/mca/grpcomm/base/base.h"


/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/grpcomm/base/static-components.h"

/*
 * Global variables
 */
orte_grpcomm_base_t orte_grpcomm_base = {{{0}}};

orte_grpcomm_API_module_t orte_grpcomm = {
    orte_grpcomm_API_xcast,
    orte_grpcomm_API_allgather
};

static bool recv_issued = false;

static int orte_grpcomm_base_close(void)
{
    orte_grpcomm_base_active_t *active;
    void *key;
    size_t size;
    uint32_t *seq_number;

    if (recv_issued) {
        orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_XCAST);
        recv_issued = false;
    }

    /* Close the active modules */
    OPAL_LIST_FOREACH(active, &orte_grpcomm_base.actives, orte_grpcomm_base_active_t) {
        if (NULL != active->module->finalize) {
            active->module->finalize();
        }
    }
    OPAL_LIST_DESTRUCT(&orte_grpcomm_base.actives);
    OPAL_LIST_DESTRUCT(&orte_grpcomm_base.ongoing);
    for (void *_nptr=NULL;                                   \
         OPAL_SUCCESS == opal_hash_table_get_next_key_ptr(&orte_grpcomm_base.sig_table, &key, &size, (void **)&seq_number, _nptr, &_nptr);) {
        free(seq_number);
    }
    OBJ_DESTRUCT(&orte_grpcomm_base.sig_table);

    return mca_base_framework_components_close(&orte_grpcomm_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_grpcomm_base_open(mca_base_open_flag_t flags)
{
    OBJ_CONSTRUCT(&orte_grpcomm_base.actives, opal_list_t);
    OBJ_CONSTRUCT(&orte_grpcomm_base.ongoing, opal_list_t);
    OBJ_CONSTRUCT(&orte_grpcomm_base.sig_table, opal_hash_table_t);
    opal_hash_table_init(&orte_grpcomm_base.sig_table, 128);

    return mca_base_framework_components_open(&orte_grpcomm_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, grpcomm, "GRPCOMM", NULL,
                           orte_grpcomm_base_open,
                           orte_grpcomm_base_close,
                           mca_grpcomm_base_static_components, 0);

OBJ_CLASS_INSTANCE(orte_grpcomm_base_active_t,
                   opal_list_item_t,
                   NULL, NULL);

static void scon(orte_grpcomm_signature_t *p)
{
    p->signature = NULL;
    p->sz = 0;
}
static void sdes(orte_grpcomm_signature_t *p)
{
    if (NULL != p->signature) {
        free(p->signature);
    }
}
OBJ_CLASS_INSTANCE(orte_grpcomm_signature_t,
                   opal_object_t,
                   scon, sdes);

static void ccon(orte_grpcomm_coll_t *p)
{
    p->sig = NULL;
    OBJ_CONSTRUCT(&p->bucket, opal_buffer_t);
    OBJ_CONSTRUCT(&p->distance_mask_recv, opal_bitmap_t);
    p->dmns = NULL;
    p->ndmns = 0;
    p->nexpected = 0;
    p->nreported = 0;
    p->cbfunc = NULL;
    p->cbdata = NULL;
    p->buffers = NULL;
}
static void cdes(orte_grpcomm_coll_t *p)
{
    if (NULL != p->sig) {
        OBJ_RELEASE(p->sig);
    }
    OBJ_DESTRUCT(&p->bucket);
    OBJ_DESTRUCT(&p->distance_mask_recv);
    free(p->dmns);
    free(p->buffers);
}
OBJ_CLASS_INSTANCE(orte_grpcomm_coll_t,
                   opal_list_item_t,
                   ccon, cdes);
