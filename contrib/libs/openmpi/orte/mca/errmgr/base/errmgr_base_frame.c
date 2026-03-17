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
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

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

#include "orte/util/show_help.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"

#include "orte/mca/errmgr/base/static-components.h"

/*
 * Globals
 */
orte_errmgr_base_t orte_errmgr_base = {{{0}}};

/* Public module provides a wrapper around previous functions */
orte_errmgr_base_module_t orte_errmgr_default_fns = {
    .init = NULL, /* init     */
    .finalize = NULL, /* finalize */
    .logfn = orte_errmgr_base_log,
    .abort = orte_errmgr_base_abort,
    .abort_peers = orte_errmgr_base_abort_peers
};
/* NOTE: ABSOLUTELY MUST initialize this
 * struct to include the log function as it
 * gets called even if the errmgr hasn't been
 * opened yet due to error
 */
orte_errmgr_base_module_t orte_errmgr = {
    .logfn = orte_errmgr_base_log
};

static int orte_errmgr_base_close(void)
{
    /* Close selected component */
    if (NULL != orte_errmgr.finalize) {
        orte_errmgr.finalize();
    }

    /* always leave a default set of fn pointers */
    orte_errmgr = orte_errmgr_default_fns;

    /* destruct the callback list */
    OPAL_LIST_DESTRUCT(&orte_errmgr_base.error_cbacks);

    return mca_base_framework_components_close(&orte_errmgr_base_framework, NULL);
}

/**
 *  * Function for finding and opening either all MCA components, or the one
 *   * that was specifically requested via a MCA parameter.
 *    */
static int orte_errmgr_base_open(mca_base_open_flag_t flags)
{
    /* load the default fns */
    orte_errmgr = orte_errmgr_default_fns;

    /* initialize the error callback list */
    OBJ_CONSTRUCT(&orte_errmgr_base.error_cbacks, opal_list_t);

    /* Open up all available components */
    return mca_base_framework_components_open(&orte_errmgr_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, errmgr, "ORTE Error Manager", NULL,
                           orte_errmgr_base_open, orte_errmgr_base_close,
                           mca_errmgr_base_static_components, 0);
