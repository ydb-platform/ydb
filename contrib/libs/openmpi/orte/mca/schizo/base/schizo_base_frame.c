/*
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
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

#include "orte/mca/schizo/base/base.h"
/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/schizo/base/static-components.h"

/*
 * Global variables
 */
orte_schizo_base_t orte_schizo_base = {{{0}}};
orte_schizo_base_module_t orte_schizo = {
    .define_cli = orte_schizo_base_define_cli,
    .parse_cli = orte_schizo_base_parse_cli,
    .parse_env = orte_schizo_base_parse_env,
    .setup_app = orte_schizo_base_setup_app,
    .setup_fork = orte_schizo_base_setup_fork,
    .setup_child = orte_schizo_base_setup_child,
    .check_launch_environment = orte_schizo_base_check_launch_environment,
    .get_remaining_time = orte_schizo_base_get_remaining_time,
    .finalize = orte_schizo_base_finalize
};

static char *personalities = NULL;

static int orte_schizo_base_register(mca_base_register_flag_t flags)
{
    /* pickup any defined personalities */
    personalities = NULL;
    mca_base_var_register("orte", "schizo", "base", "personalities",
                          "Comma-separated list of personalities",
                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                          OPAL_INFO_LVL_9,
                          MCA_BASE_VAR_SCOPE_READONLY,
                          &personalities);
    return ORTE_SUCCESS;
}

static int orte_schizo_base_close(void)
{
    /* cleanup globals */
    OPAL_LIST_DESTRUCT(&orte_schizo_base.active_modules);
    if (NULL != orte_schizo_base.personalities) {
        opal_argv_free(orte_schizo_base.personalities);
    }

    return mca_base_framework_components_close(&orte_schizo_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_schizo_base_open(mca_base_open_flag_t flags)
{
    int rc;

    /* init the globals */
    OBJ_CONSTRUCT(&orte_schizo_base.active_modules, opal_list_t);
    orte_schizo_base.personalities = NULL;
    if (NULL != personalities) {
        orte_schizo_base.personalities = opal_argv_split(personalities, ',');
    }

    /* Open up all available components */
    rc = mca_base_framework_components_open(&orte_schizo_base_framework, flags);

    /* All done */
    return rc;
}

MCA_BASE_FRAMEWORK_DECLARE(orte, schizo, "ORTE Schizo Subsystem",
                           orte_schizo_base_register,
                           orte_schizo_base_open, orte_schizo_base_close,
                           mca_schizo_base_static_components, 0);

OBJ_CLASS_INSTANCE(orte_schizo_base_active_module_t,
                   opal_list_item_t,
                   NULL, NULL);
