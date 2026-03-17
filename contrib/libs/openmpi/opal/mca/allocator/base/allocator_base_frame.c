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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"
#include <stdio.h>
#include <string.h>

#include "opal/mca/mca.h"
#include "opal/mca/base/base.h"
#include "opal/mca/allocator/allocator.h"
#include "opal/mca/allocator/base/base.h"

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */

#include "opal/mca/allocator/base/static-components.h"

/*
 * Global variables
 */
MCA_BASE_FRAMEWORK_DECLARE(opal, allocator, NULL, NULL, NULL, NULL,
                           mca_allocator_base_static_components, 0);

/**
 * Traverses through the list of available components, calling their init
 * functions until it finds the component that has the specified name. It
 * then returns the found component.
 *
 * @param name the name of the component that is being searched for.
 * @retval mca_allocator_base_component_t* pointer to the requested component
 * @retval NULL if the requested component is not found
 */
mca_allocator_base_component_t* mca_allocator_component_lookup(const char* name)
{
    /* Traverse the list of available components; call their init functions. */
    mca_base_component_list_item_t *cli;
    OPAL_LIST_FOREACH(cli, &opal_allocator_base_framework.framework_components, mca_base_component_list_item_t) {
         mca_allocator_base_component_t* component = (mca_allocator_base_component_t *) cli->cli_component;
         if(strcmp(component->allocator_version.mca_component_name,
                   name) == 0) {
             return component;
         }
    }
    return NULL;
}


