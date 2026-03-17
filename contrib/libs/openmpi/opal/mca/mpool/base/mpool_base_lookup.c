/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Mellanox Technologies. All rights reserved.
 * Copyright (c) 2008-2014 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2016 Los Alamos National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#define OPAL_DISABLE_ENABLE_MEM_DEBUG 1
#include "opal_config.h"

#include <stdio.h>
#include <string.h>

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/util/show_help.h"
#include "opal/util/proc.h"

#include "opal/mca/mpool/mpool.h"
#include "opal/mca/mpool/base/base.h"

mca_mpool_base_component_t* mca_mpool_base_component_lookup(const char *name)
{
    mca_base_component_list_item_t *cli;

    /* Traverse the list of available modules; call their init functions. */
    OPAL_LIST_FOREACH(cli, &opal_mpool_base_framework.framework_components, mca_base_component_list_item_t) {
        mca_mpool_base_component_t* component = (mca_mpool_base_component_t *) cli->cli_component;
        if (strcmp(component->mpool_version.mca_component_name, name) == 0) {
            return component;
        }
    }

    return NULL;
}



mca_mpool_base_module_t *mca_mpool_base_module_lookup (const char *hints)
{
    mca_mpool_base_module_t *best_module = mca_mpool_base_default_module;
    mca_base_component_list_item_t *cli;
    int best_priority = mca_mpool_base_default_priority;
    int rc;

    OPAL_LIST_FOREACH(cli, &opal_mpool_base_framework.framework_components, mca_base_component_list_item_t) {
        mca_mpool_base_component_t *component = (mca_mpool_base_component_t *) cli->cli_component;
	mca_mpool_base_module_t *module;
	int priority;

	rc = component->mpool_query (hints, &priority, &module);
	if (OPAL_SUCCESS == rc) {
	    if (priority > best_priority) {
		best_priority = priority;
		best_module = module;
	    }
	}
    }

    return best_module;
}
