/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"

#include "rtc_hwloc.h"

/*
 * Local functions
 */

static int rtc_hwloc_query(mca_base_module_t **module, int *priority);
static int rtc_hwloc_register(void);

static int my_priority;

orte_rtc_hwloc_component_t mca_rtc_hwloc_component = {
    .super = {
        .base_version = {
            ORTE_RTC_BASE_VERSION_1_0_0,

            .mca_component_name = "hwloc",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                  ORTE_RELEASE_VERSION),
            .mca_query_component = rtc_hwloc_query,
            .mca_register_component_params = rtc_hwloc_register,
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    },
    .kind = VM_HOLE_BIGGEST
};

static char *biggest = "biggest";
static char *vmhole;

static int rtc_hwloc_register(void)
{
    mca_base_component_t *c = &mca_rtc_hwloc_component.super.base_version;

    /* set as the default */
    my_priority = 70;
    (void) mca_base_component_var_register (c, "priority", "Priority of the HWLOC rtc component",
                                            MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &my_priority);

    mca_rtc_hwloc_component.kind = VM_HOLE_BIGGEST;
    vmhole = biggest;
    (void) mca_base_component_var_register(c, "vmhole",
                                           "Kind of VM hole to identify - none, begin, biggest, libs, heap, stack (default=biggest)",
                                            MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &vmhole);
    if (0 == strcasecmp(vmhole, "none")) {
        mca_rtc_hwloc_component.kind = VM_HOLE_NONE;
    } else if (0 == strcasecmp(vmhole, "begin")) {
        mca_rtc_hwloc_component.kind = VM_HOLE_BEGIN;
    } else if (0 == strcasecmp(vmhole, "biggest")) {
        mca_rtc_hwloc_component.kind = VM_HOLE_BIGGEST;
    } else if (0 == strcasecmp(vmhole, "libs")) {
        mca_rtc_hwloc_component.kind = VM_HOLE_IN_LIBS;
    } else if (0 == strcasecmp(vmhole, "heap")) {
        mca_rtc_hwloc_component.kind = VM_HOLE_AFTER_HEAP;
    } else if (0 == strcasecmp(vmhole, "stack")) {
        mca_rtc_hwloc_component.kind = VM_HOLE_BEFORE_STACK;
    } else {
        opal_output(0, "INVALID VM HOLE TYPE");
        return ORTE_ERROR;
    }

    return ORTE_SUCCESS;
}


static int rtc_hwloc_query(mca_base_module_t **module, int *priority)
{
    /* Only run on the HNP */

    *priority = my_priority;
    *module = (mca_base_module_t *)&orte_rtc_hwloc_module;

    return ORTE_SUCCESS;
}
