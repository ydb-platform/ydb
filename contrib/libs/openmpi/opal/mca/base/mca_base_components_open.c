/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Hochschule Esslingen.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "opal/class/opal_list.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/constants.h"
#if OPAL_ENABLE_FT_CR == 1
#include "opal/runtime/opal_params.h"
#endif

/*
 * Local functions
 */
static int open_components(mca_base_framework_t *framework);

struct mca_base_dummy_framework_list_item_t {
    opal_list_item_t super;
    mca_base_framework_t framework;
};

/**
 * Function for finding and opening either all MCA components, or the
 * one that was specifically requested via a MCA parameter.
 */
int mca_base_framework_components_open (mca_base_framework_t *framework,
                                        mca_base_open_flag_t flags)
{
    /* Open flags are not used at this time. Suppress compiler warning. */
    if (flags & MCA_BASE_OPEN_FIND_COMPONENTS) {
        bool open_dso_components = !(flags & MCA_BASE_OPEN_STATIC_ONLY);
        /* Find and load requested components */
        int ret = mca_base_component_find(NULL, framework, false, open_dso_components);
        if (OPAL_SUCCESS != ret) {
            return ret;
        }
    }

    /* Open all registered components */
    return open_components (framework);
}

/*
 * Traverse the entire list of found components (a list of
 * mca_base_component_t instances).  If the requested_component_names
 * array is empty, or the name of each component in the list of found
 * components is in the requested_components_array, try to open it.
 * If it opens, add it to the components_available list.
 */
static int open_components(mca_base_framework_t *framework)
{
    opal_list_t *components = &framework->framework_components;
    uint32_t open_only_flags = MCA_BASE_METADATA_PARAM_NONE;
    int output_id = framework->framework_output;
    mca_base_component_list_item_t *cli, *next;
    int ret;

    /*
     * Pre-process the list with parameter constraints
     * e.g., If requested to select only CR enabled components
     *       then only make available those components.
     *
     * JJH Note: Currently checkpoint/restart is the only user of this
     *           functionality. If other component constraint options are
     *           added, then this logic can be used for all contraint
     *           options.
     *
     * NTH: Logic moved to mca_base_components_filter.
     */
#if (OPAL_ENABLE_FT == 1) && (OPAL_ENABLE_FT_CR == 1)
    if (opal_base_distill_checkpoint_ready) {
        open_only_flags |= MCA_BASE_METADATA_PARAM_CHECKPOINT;
    }
#endif  /* (OPAL_ENABLE_FT == 1) && (OPAL_ENABLE_FT_CR == 1) */

    /* If mca_base_framework_register_components was called with the MCA_BASE_COMPONENTS_ALL flag
       we need to trim down and close any extra components we do not want open */
    ret = mca_base_components_filter (framework, open_only_flags);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    /* Announce */
    opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id, "mca: base: components_open: opening %s components",
                         framework->framework_name);

    /* Traverse the list of components */
    OPAL_LIST_FOREACH_SAFE(cli, next, components, mca_base_component_list_item_t) {
        const mca_base_component_t *component = cli->cli_component;

        opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                             "mca: base: components_open: found loaded component %s",
                             component->mca_component_name);

	if (NULL != component->mca_open_component) {
	    /* Call open if register didn't call it already */
            ret = component->mca_open_component();

            if (OPAL_SUCCESS == ret) {
                opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                                     "mca: base: components_open: "
                                     "component %s open function successful",
                                     component->mca_component_name);
            } else {
		if (OPAL_ERR_NOT_AVAILABLE != ret) {
		    /* If the component returns OPAL_ERR_NOT_AVAILABLE,
		       it's a cue to "silently ignore me" -- it's not a
		       failure, it's just a way for the component to say
		       "nope!".

		       Otherwise, however, display an error.  We may end
		       up displaying this twice, but it may go to separate
		       streams.  So better to be redundant than to not
		       display the error in the stream where it was
		       expected. */

		    if (mca_base_component_show_load_errors) {
			opal_output_verbose (MCA_BASE_VERBOSE_ERROR, output_id,
                                             "mca: base: components_open: component %s "
                                             "/ %s open function failed",
                                             component->mca_type_name,
                                             component->mca_component_name);
		    }
		    opal_output_verbose (MCA_BASE_VERBOSE_COMPONENT, output_id,
                                         "mca: base: components_open: "
                                         "component %s open function failed",
                                         component->mca_component_name);
		}

                mca_base_component_close (component, output_id);

		opal_list_remove_item (components, &cli->super);
		OBJ_RELEASE(cli);
	    }
	}
    }

    /* All done */

    return OPAL_SUCCESS;
}
