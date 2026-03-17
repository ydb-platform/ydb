/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include "pmix_common.h"
#include "src/util/output.h"

#include "pmix_mca_base_framework.h"
#include "pmix_mca_base_var.h"
#include "src/mca/base/base.h"

bool pmix_mca_base_framework_is_registered (struct pmix_mca_base_framework_t *framework)
{
    return !!(framework->framework_flags & PMIX_MCA_BASE_FRAMEWORK_FLAG_REGISTERED);
}

bool pmix_mca_base_framework_is_open (struct pmix_mca_base_framework_t *framework)
{
    return !!(framework->framework_flags & PMIX_MCA_BASE_FRAMEWORK_FLAG_OPEN);
}

static void framework_open_output (struct pmix_mca_base_framework_t *framework)
{
    if (0 < framework->framework_verbose) {
        if (-1 == framework->framework_output) {
            framework->framework_output = pmix_output_open (NULL);
        }
        pmix_output_set_verbosity(framework->framework_output,
                                  framework->framework_verbose);
    } else if (-1 != framework->framework_output) {
        pmix_output_close (framework->framework_output);
        framework->framework_output = -1;
    }
}

static void framework_close_output (struct pmix_mca_base_framework_t *framework)
{
    if (-1 != framework->framework_output) {
        pmix_output_close (framework->framework_output);
        framework->framework_output = -1;
    }
}

int pmix_mca_base_framework_register (struct pmix_mca_base_framework_t *framework,
                                 pmix_mca_base_register_flag_t flags)
{
    char *desc;
    int ret;

    assert (NULL != framework);

    framework->framework_refcnt++;

    if (pmix_mca_base_framework_is_registered (framework)) {
        return PMIX_SUCCESS;
    }

    PMIX_CONSTRUCT(&framework->framework_components, pmix_list_t);
    PMIX_CONSTRUCT(&framework->framework_failed_components, pmix_list_t);

    if (framework->framework_flags & PMIX_MCA_BASE_FRAMEWORK_FLAG_NO_DSO) {
        flags |= PMIX_MCA_BASE_REGISTER_STATIC_ONLY;
    }

    if (!(PMIX_MCA_BASE_FRAMEWORK_FLAG_NOREGISTER & framework->framework_flags)) {
        /* register this framework with the MCA variable system */
        ret = pmix_mca_base_var_group_register (framework->framework_project,
                                           framework->framework_name,
                                           NULL, framework->framework_description);
        if (0 > ret) {
            return ret;
        }

        ret = asprintf (&desc, "Default selection set of components for the %s framework (<none>"
                        " means use all components that can be found)", framework->framework_name);
        if (0 > ret) {
            return PMIX_ERR_OUT_OF_RESOURCE;
        }

        ret = pmix_mca_base_var_register (framework->framework_project, framework->framework_name,
                                     NULL, NULL, desc, PMIX_MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                     PMIX_MCA_BASE_VAR_FLAG_SETTABLE, PMIX_INFO_LVL_2,
                                     PMIX_MCA_BASE_VAR_SCOPE_ALL_EQ, &framework->framework_selection);
        free (desc);
        if (0 > ret) {
            return ret;
        }

        /* register a verbosity variable for this framework */
        ret = asprintf (&desc, "Verbosity level for the %s framework (default: 0)",
                        framework->framework_name);
        if (0 > ret) {
            return PMIX_ERR_OUT_OF_RESOURCE;
        }

        framework->framework_verbose = PMIX_MCA_BASE_VERBOSE_ERROR;
        ret = pmix_mca_base_framework_var_register (framework, "verbose", desc,
                                               PMIX_MCA_BASE_VAR_TYPE_INT,
                                               &pmix_mca_base_var_enum_verbose, 0,
                                               PMIX_MCA_BASE_VAR_FLAG_SETTABLE,
                                               PMIX_INFO_LVL_8,
                                               PMIX_MCA_BASE_VAR_SCOPE_LOCAL,
                                               &framework->framework_verbose);
        free(desc);
        if (0 > ret) {
            return ret;
        }

        /* check the initial verbosity and open the output if necessary. we
           will recheck this on open */
        framework_open_output (framework);

        /* register framework variables */
        if (NULL != framework->framework_register) {
            ret = framework->framework_register (flags);
            if (PMIX_SUCCESS != ret) {
                return ret;
            }
        }

        /* register components variables */
        ret = pmix_mca_base_framework_components_register (framework, flags);
        if (PMIX_SUCCESS != ret) {
            return ret;
        }
    }

    framework->framework_flags |= PMIX_MCA_BASE_FRAMEWORK_FLAG_REGISTERED;

    /* framework did not provide a register function */
    return PMIX_SUCCESS;
}

int pmix_mca_base_framework_open (struct pmix_mca_base_framework_t *framework,
                             pmix_mca_base_open_flag_t flags) {
    int ret;

    assert (NULL != framework);

    /* register this framework before opening it */
    ret = pmix_mca_base_framework_register (framework, PMIX_MCA_BASE_REGISTER_DEFAULT);
    if (PMIX_SUCCESS != ret) {
        return ret;
    }

    /* check if this framework is already open */
    if (pmix_mca_base_framework_is_open (framework)) {
        return PMIX_SUCCESS;
    }

    if (PMIX_MCA_BASE_FRAMEWORK_FLAG_NOREGISTER & framework->framework_flags) {
        flags |= PMIX_MCA_BASE_OPEN_FIND_COMPONENTS;

        if (PMIX_MCA_BASE_FRAMEWORK_FLAG_NO_DSO & framework->framework_flags) {
            flags |= PMIX_MCA_BASE_OPEN_STATIC_ONLY;
        }
    }

    /* lock all of this frameworks's variables */
    ret = pmix_mca_base_var_group_find (framework->framework_project,
                                   framework->framework_name,
                                   NULL);
    pmix_mca_base_var_group_set_var_flag (ret, PMIX_MCA_BASE_VAR_FLAG_SETTABLE, false);

    /* check the verbosity level and open (or close) the output */
    framework_open_output (framework);

    if (NULL != framework->framework_open) {
        ret = framework->framework_open (flags);
    } else {
        ret = pmix_mca_base_framework_components_open (framework, flags);
    }

    if (PMIX_SUCCESS != ret) {
        framework->framework_refcnt--;
    } else {
        framework->framework_flags |= PMIX_MCA_BASE_FRAMEWORK_FLAG_OPEN;
    }

    return ret;
}

int pmix_mca_base_framework_close (struct pmix_mca_base_framework_t *framework) {
    bool is_open = pmix_mca_base_framework_is_open (framework);
    bool is_registered = pmix_mca_base_framework_is_registered (framework);
    int ret, group_id;

    assert (NULL != framework);

    if (!(is_open || is_registered)) {
        return PMIX_SUCCESS;
    }

    assert (framework->framework_refcnt);
    if (--framework->framework_refcnt) {
        return PMIX_SUCCESS;
    }

    /* find and deregister all component groups and variables */
    group_id = pmix_mca_base_var_group_find (framework->framework_project,
                                        framework->framework_name, NULL);
    if (0 <= group_id) {
        (void) pmix_mca_base_var_group_deregister (group_id);
    }

    /* close the framework and all of its components */
    if (is_open) {
        if (NULL != framework->framework_close) {
            ret = framework->framework_close ();
        } else {
            ret = pmix_mca_base_framework_components_close (framework, NULL);
        }

        if (PMIX_SUCCESS != ret) {
            return ret;
        }
    } else {
        pmix_list_item_t *item;
        while (NULL != (item = pmix_list_remove_first (&framework->framework_components))) {
            pmix_mca_base_component_list_item_t *cli;
            cli = (pmix_mca_base_component_list_item_t*) item;
            pmix_mca_base_component_unload(cli->cli_component,
                                           framework->framework_output);
            PMIX_RELEASE(item);
        }
        ret = PMIX_SUCCESS;
    }

    framework->framework_flags &= ~(PMIX_MCA_BASE_FRAMEWORK_FLAG_REGISTERED | PMIX_MCA_BASE_FRAMEWORK_FLAG_OPEN);

    PMIX_DESTRUCT(&framework->framework_components);
    PMIX_LIST_DESTRUCT(&framework->framework_failed_components);

    framework_close_output (framework);

    return ret;
}
