/*
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
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

#include "orte/mca/mca.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/mca/regx/base/base.h"
/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/regx/base/static-components.h"

/*
 * Global variables
 */
orte_regx_base_module_t orte_regx = {0};

static int orte_regx_base_close(void)
{
    /* give the selected module a chance to finalize */
    if (NULL != orte_regx.finalize) {
        orte_regx.finalize();
    }
    return mca_base_framework_components_close(&orte_regx_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_regx_base_open(mca_base_open_flag_t flags)
{
    int rc;

    /* Open up all available components */
    rc = mca_base_framework_components_open(&orte_regx_base_framework, flags);

    /* All done */
    return rc;
}

MCA_BASE_FRAMEWORK_DECLARE(orte, regx, "ORTE Regx Subsystem", NULL,
                           orte_regx_base_open, orte_regx_base_close,
                           mca_regx_base_static_components, 0);

/* OBJECT INSTANTIATIONS */
static void nrcon(orte_nidmap_regex_t *p)
{
    p->ctx = 0;
    p->nprocs = -1;
    p->cnt = 0;
}
OBJ_CLASS_INSTANCE(orte_nidmap_regex_t,
                   opal_list_item_t,
                   nrcon, NULL);
