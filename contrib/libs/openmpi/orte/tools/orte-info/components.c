/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <stdlib.h>
#include <string.h>

#include "orte/runtime/runtime.h"

#include "opal/util/argv.h"
#include "opal/runtime/opal_info_support.h"
#include "opal/mca/event/base/base.h"
#include "opal/util/output.h"

#include "orte/runtime/orte_info_support.h"
#include "orte/tools/orte-info/orte-info.h"
/*
 * Public variables
 */

static void component_map_construct(orte_info_component_map_t *map)
{
    map->type = NULL;
}
static void component_map_destruct(orte_info_component_map_t *map)
{
    if (NULL != map->type) {
        free(map->type);
    }
    /* the type close functions will release the
     * list of components
     */
}
OBJ_CLASS_INSTANCE(orte_info_component_map_t,
                   opal_list_item_t,
                   component_map_construct,
                   component_map_destruct);

opal_pointer_array_t orte_component_map = {{0}};

/*
 * Private variables
 */

static bool opened_components = false;


void orte_info_components_open(void)
{
    if (opened_components) {
        return;
    }

    opened_components = true;

    /* init the map */
    OBJ_CONSTRUCT(&orte_component_map, opal_pointer_array_t);
    opal_pointer_array_init(&orte_component_map, 256, INT_MAX, 128);

    opal_info_register_framework_params(&orte_component_map);
    orte_info_register_framework_params(&orte_component_map);
}

/*
 * Not to be confused with orte_info_close_components.
 */
void orte_info_components_close(void)
{
    int i;
    orte_info_component_map_t *map;

    if (!opened_components) {
        return;
    }

    orte_info_close_components ();
    opal_info_close_components ();

    for (i=0; i < orte_component_map.size; i++) {
        if (NULL != (map = (orte_info_component_map_t*)opal_pointer_array_get_item(&orte_component_map, i))) {
            OBJ_RELEASE(map);
        }
    }

    OBJ_DESTRUCT(&orte_component_map);

    opened_components = false;
}
