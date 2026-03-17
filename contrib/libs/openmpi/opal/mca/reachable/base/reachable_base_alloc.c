/*
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/class/opal_object.h"

#include "opal/mca/reachable/reachable.h"
#include "opal/mca/reachable/base/base.h"


static void opal_reachable_construct(opal_reachable_t *reachable)
{
    reachable->weights = NULL;
}


static void opal_reachable_destruct(opal_reachable_t * reachable)
{
    if (NULL != reachable->memory) {
        free(reachable->memory);
    }
}


opal_reachable_t * opal_reachable_allocate(unsigned int num_local,
                                           unsigned int num_remote)
{
    char *memory;
    unsigned int i;
    opal_reachable_t *reachable = OBJ_NEW(opal_reachable_t);

    reachable->num_local = num_local;
    reachable->num_remote = num_remote;

    /* allocate all the pieces of the two dimensional array in one
       malloc, rather than a bunch of little allocations */
    memory = malloc(sizeof(int*) * num_local +
                    num_local * (sizeof(int) * num_remote));
    if (memory == NULL) return NULL;

    reachable->memory = (void*)memory;
    reachable->weights = (int**)reachable->memory;
    memory += (sizeof(int*) * num_local);

    for (i = 0; i < num_local; i++) {
        reachable->weights[i] = (int*)memory;
        memory += (sizeof(int) * num_remote);
    }

    return reachable;
}

OBJ_CLASS_INSTANCE(
    opal_reachable_t,
    opal_object_t,
    opal_reachable_construct,
    opal_reachable_destruct
);
