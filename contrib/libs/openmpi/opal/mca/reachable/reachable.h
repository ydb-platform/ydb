/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_REACHABLE_H
#define OPAL_REACHABLE_H

#include "opal_config.h"
#include "opal/types.h"
#include "opal/class/opal_object.h"

#include "opal/mca/mca.h"
#include "opal/mca/if/if.h"


BEGIN_C_DECLS

/**
 * Reachability matrix between endpoints of a given pair of hosts
 *
 * The output of the reachable() call is a opal_reachable_t, which
 * gives an matrix of the connectivity between local and remote
 * ethernet endpoints.  Any given value in weights is the connectivity
 * between the local endpoint index (first index) and the remote
 * endpoint index (second index), and is a value between 0 and INT_MAX
 * representing a relative connectivity.
 */
struct opal_reachable_t {
    opal_object_t super;
    /** number of local interfaces passed to reachable() */
    int num_local;
    /** number of remote interfaces passed to reachable() */
    int num_remote;
    /** matric of connectivity weights */
    int **weights;
    /** \internal */
    void *memory;
};
typedef struct opal_reachable_t opal_reachable_t;
OBJ_CLASS_DECLARATION(opal_reachable_t);

/* Init */
typedef int (*opal_reachable_base_module_init_fn_t)(void);

/* Finalize */
typedef int (*opal_reachable_base_module_fini_fn_t)(void);

/* Build reachability matrix between local and remote ethernet
 * interfaces
 *
 * Given a list of local interfaces and remote interfaces from a
 * single peer, build a reachability matrix between the two peers.
 * This function does not select the best pairing of local and remote
 * interfaces, but only a (comparable) reachability between any pair
 * of local/remote interfaces.
 *
 * @returns a reachable object containing the reachability matrix on
 * success, NULL on failure.
 */
typedef opal_reachable_t*
(*opal_reachable_base_module_reachable_fn_t)(opal_list_t *local_if,
                                             opal_list_t *remote_if);


/*
 * the standard public API data structure
 */
typedef struct {
    /* currently used APIs */
    opal_reachable_base_module_init_fn_t                   init;
    opal_reachable_base_module_fini_fn_t                   finalize;
    opal_reachable_base_module_reachable_fn_t              reachable;
} opal_reachable_base_module_t;

typedef struct {
    mca_base_component_t                      base_version;
    mca_base_component_data_t                 base_data;
    int priority;
} opal_reachable_base_component_t;

/*
 * Macro for use in components that are of type reachable
 */
#define OPAL_REACHABLE_BASE_VERSION_2_0_0             \
    OPAL_MCA_BASE_VERSION_2_1_0("reachable", 2, 0, 0)

/* Global structure for accessing reachability functions */
OPAL_DECLSPEC extern opal_reachable_base_module_t opal_reachable;


END_C_DECLS

#endif
