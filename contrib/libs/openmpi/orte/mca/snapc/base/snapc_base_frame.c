/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */

/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2008 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/opal_environ.h"
#include "opal/util/output.h"

#include "orte/constants.h"
#include "orte/mca/sstore/sstore.h"
#include "orte/mca/sstore/base/base.h"

#include "orte/mca/snapc/snapc.h"
#include "orte/mca/snapc/base/base.h"

#include "orte/mca/snapc/base/static-components.h"

/*
 * Globals
 */
bool orte_snapc_base_is_tool = false;
orte_snapc_base_module_t orte_snapc = {
    NULL, /* snapc_init      */
    NULL, /* snapc_finalize  */
    NULL, /* setup_job       */
    NULL  /* release_job     */
};

orte_snapc_coord_type_t orte_snapc_coord_type = ORTE_SNAPC_UNASSIGN_TYPE;

bool orte_snapc_base_store_only_one_seq = false;
bool   orte_snapc_base_has_recovered = false;

static int orte_snapc_base_register(mca_base_register_flag_t flags)
{
    /*
     * Reuse sequence numbers
     * This will create a directory and always use seq 0 for all checkpoints
     * This *should* also enforce a 2-phase commit protocol
     */
    orte_snapc_base_store_only_one_seq = false;
    (void) mca_base_var_register("orte", "snapc", "base", "only_one_seq",
                                 "Only store the most recent checkpoint sequence. [Default = disabled]",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &orte_snapc_base_store_only_one_seq);

    return ORTE_SUCCESS;
}

static int orte_snapc_base_close(void)
{
    /* Close the selected component */
    if( NULL != orte_snapc.snapc_finalize ) {
        orte_snapc.snapc_finalize();
    }

    return mca_base_framework_components_close(&orte_snapc_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components,
 * or the one that was specifically requested via a MCA parameter.
 */
static int orte_snapc_base_open(mca_base_open_flag_t flags)
{
    /* Init the sequence (interval) number */
    orte_snapc_base_snapshot_seq_number = 0;

    /* Open up all available components */
    return mca_base_framework_components_open(&orte_snapc_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, snapc, "ORTE Snapc", orte_snapc_base_register,
                           orte_snapc_base_open, orte_snapc_base_close,
                           mca_snapc_base_static_components, 0);

