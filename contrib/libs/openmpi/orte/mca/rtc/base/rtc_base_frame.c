/*
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
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
#include "opal/class/opal_list.h"
#include "opal/mca/base/base.h"

#include "orte/mca/rtc/base/base.h"
/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "orte/mca/rtc/base/static-components.h"

/*
 * Global variables
 */
orte_rtc_API_module_t orte_rtc = {
    orte_rtc_base_assign,
    orte_rtc_base_set,
    orte_rtc_base_get_avail_vals
};
orte_rtc_base_t orte_rtc_base = {{{0}}};

static int orte_rtc_base_close(void)
{
    opal_list_item_t *item;

    /* cleanup globals */
    while (NULL != (item = opal_list_remove_first(&orte_rtc_base.actives))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&orte_rtc_base.actives);

    return mca_base_framework_components_close(&orte_rtc_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_rtc_base_open(mca_base_open_flag_t flags)
{
    /* init the globals */
    OBJ_CONSTRUCT(&orte_rtc_base.actives, opal_list_t);

    /* Open up all available components */
    return mca_base_framework_components_open(&orte_rtc_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, rtc, "ORTE Mapping Subsystem",
                           NULL, orte_rtc_base_open, orte_rtc_base_close,
                           mca_rtc_base_static_components, 0);

static void mdes(orte_rtc_base_selected_module_t *active)
{
    if (NULL != active->module->finalize) {
        active->module->finalize();
    }
}
OBJ_CLASS_INSTANCE(orte_rtc_base_selected_module_t,
                   opal_list_item_t,
                   NULL, mdes);

static void rcon(orte_rtc_resource_t *p)
{
    p->component = NULL;
    p->category = NULL;
    OBJ_CONSTRUCT(&p->control, opal_value_t);
}
static void rdes(orte_rtc_resource_t *p)
{
    if (NULL != p->component) {
        free(p->component);
    }
    if (NULL != p->category) {
        free(p->category);
    }
    OBJ_DESTRUCT(&p->control);
}
OBJ_CLASS_INSTANCE(orte_rtc_resource_t,
                   opal_list_item_t,
                   rcon, rdes);
