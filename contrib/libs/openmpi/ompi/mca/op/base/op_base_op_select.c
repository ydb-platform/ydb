/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Lawrence Livermore National Security, LLC.  All
 *                         rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008-2015 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "mpi.h"
#include "ompi/constants.h"

#include "opal/util/output.h"
#include "opal/class/opal_list.h"
#include "opal/class/opal_object.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"


#include "ompi/op/op.h"
#include "ompi/mca/op/op.h"
#include "ompi/mca/op/base/base.h"
#include "ompi/mca/op/base/functions.h"


/*
 * Local types
 */
typedef struct avail_op_t {
    opal_list_item_t super;

    int ao_priority;
    ompi_op_base_module_1_0_0_t *ao_module;
} avail_op_t;


/*
 * Local functions
 */
static opal_list_t *check_components(opal_list_t *components,
                                     ompi_op_t *op);
static int check_one_component(ompi_op_t *op,
                               const mca_base_component_t *component,
                               ompi_op_base_module_1_0_0_t **module);

static int query(const mca_base_component_t *component,
                 ompi_op_t *op, int *priority,
                 ompi_op_base_module_1_0_0_t **module);

static int query_1_0_0(const ompi_op_base_component_1_0_0_t *op_component,
                       ompi_op_t *op, int *priority,
                       ompi_op_base_module_1_0_0_t **module);

/*
 * Stuff for the OBJ interface
 */
static OBJ_CLASS_INSTANCE(avail_op_t, opal_list_item_t, NULL, NULL);


/*
 * This function is called at the initialization time of every
 * *intrinsic* MPI_Op (it is *not* used for user-defined MPI_Ops!).
 * It is used to select which op component(s) will be active for a
 * given MPI_Op.
 *
 * This selection logic is not for the weak.
 */
int ompi_op_base_op_select(ompi_op_t *op)
{
    int i, ret;
    opal_list_t *selectable;
    opal_list_item_t *item;
    ompi_op_base_module_t *module;

    /* Announce */
    opal_output_verbose(10, ompi_op_base_framework.framework_output,
                        "op:base:op_select: new op: %s",
                        op->o_name);

    /* Make a module for all the base functions so that other modules
       can RETAIN it (vs. having NULL for the base function modules,
       and forcing all other modules to check for NULL before calling
       RETAIN). */
    module = OBJ_NEW(ompi_op_base_module_t);

    /* Initialize all functions to point to the corresponding base
       functions.  Set the corresponding module pointers to NULL,
       indicating that these are base functions with no corresponding
       module. */
    memset(&op->o_func, 0, sizeof(op->o_func));
    memset(&op->o_3buff_intrinsic, 0, sizeof(op->o_3buff_intrinsic));
    for (i = 0; i < OMPI_OP_BASE_TYPE_MAX; ++i) {
        op->o_func.intrinsic.fns[i] =
            ompi_op_base_functions[op->o_f_to_c_index][i];
        op->o_func.intrinsic.modules[i] = module;
        OBJ_RETAIN(module);
        op->o_3buff_intrinsic.fns[i] =
            ompi_op_base_3buff_functions[op->o_f_to_c_index][i];
        op->o_3buff_intrinsic.modules[i] = module;
        OBJ_RETAIN(module);
    }

    /* Offset the initial OBJ_NEW */
    OBJ_RELEASE(module);

    /* Check for any components that want to run.  It's not an error
       if there are none; we'll just use all the base functions in
       this case. */
    opal_output_verbose(10, ompi_op_base_framework.framework_output,
                        "op:base:op_select: Checking all available components");
    selectable = check_components(&ompi_op_base_framework.framework_components, op);

    /* Do the selection loop.  The selectable list is in priority
       order; lowest priority first. */
    for (item = opal_list_remove_first(selectable);
         NULL != item;
         item = opal_list_remove_first(selectable)) {
        avail_op_t *avail = (avail_op_t*) item;

        /* Enable the module */
        if (NULL != avail->ao_module->opm_enable) {
            ret = avail->ao_module->opm_enable(avail->ao_module, op);
            if (OMPI_SUCCESS != ret) {
                /* If the module fails to enable, just release it and move
                   on */
                OBJ_RELEASE(avail->ao_module);
                OBJ_RELEASE(avail);
                continue;
            }
        }

        /* Copy over the non-NULL pointers */
        for (i = 0; i < OMPI_OP_BASE_TYPE_MAX; ++i) {
            /* 2-buffer variants */
            if (NULL != avail->ao_module->opm_fns[i]) {
                OBJ_RELEASE(op->o_func.intrinsic.modules[i]);
                op->o_func.intrinsic.fns[i] = avail->ao_module->opm_fns[i];
                op->o_func.intrinsic.modules[i] = avail->ao_module;
                OBJ_RETAIN(avail->ao_module);
            }

            /* 3-buffer variants */
            if (NULL != avail->ao_module->opm_3buff_fns[i]) {
                OBJ_RELEASE(op->o_func.intrinsic.modules[i]);
                op->o_3buff_intrinsic.fns[i] =
                    avail->ao_module->opm_3buff_fns[i];
                op->o_3buff_intrinsic.modules[i] = avail->ao_module;
                OBJ_RETAIN(avail->ao_module);
            }
        }

        /* release the original module reference and the list item */
        OBJ_RELEASE(avail->ao_module);
        OBJ_RELEASE(avail);
    }

    /* Done with the list from the check_components() call so release it. */
    OBJ_RELEASE(selectable);

    /* Sanity check: for intrinsic MPI_Ops, we should have exactly the
       same pointers non-NULL as the corresponding initial table row
       in ompi_op_base_functions / ompi_op_base_3buff_functions.  The
       values may be different, of course, but the pattern of
       NULL/non-NULL should be exactly the same. */
    for (i = 0; i < OMPI_OP_BASE_TYPE_MAX; ++i) {
        if ((NULL == ompi_op_base_functions[op->o_f_to_c_index][i] &&
             NULL != op->o_func.intrinsic.fns[i]) ||
            (NULL != ompi_op_base_functions[op->o_f_to_c_index][i] &&
             NULL == op->o_func.intrinsic.fns[i])) {
            /* Oops -- we found a mismatch.  This shouldn't happen; so
               go release everything and return an error (yes, re-use
               the "i" index because we're going to return without
               completing the outter loop). */
            for (i = 0; i < OMPI_OP_BASE_TYPE_MAX; ++i) {
                OBJ_RELEASE(op->o_func.intrinsic.modules[i]);
                op->o_func.intrinsic.modules[i] = NULL;
                op->o_func.intrinsic.fns[i] = NULL;
            }
            return OMPI_ERR_NOT_FOUND;
        }
    }

    return OMPI_SUCCESS;
}

static int avail_op_compare(opal_list_item_t **itema,
                            opal_list_item_t **itemb)
{
    avail_op_t *availa = (avail_op_t *) itema;
    avail_op_t *availb = (avail_op_t *) itemb;

    if (availa->ao_priority > availb->ao_priority) {
        return 1;
    } else if (availa->ao_priority < availb->ao_priority) {
        return -1;
    } else {
        return 0;
    }
}

/*
 * For each module in the list, check and see if it wants to run, and
 * do the resulting priority comparison.  Make a list of modules to be
 * only those who returned that they want to run, and put them in
 * priority order (lowest to highest).
 */
static opal_list_t *check_components(opal_list_t *components,
                                     ompi_op_t *op)
{
    int priority;
    mca_base_component_list_item_t *cli;
    const mca_base_component_t *component;
    ompi_op_base_module_1_0_0_t *module;
    opal_list_t *selectable;
    avail_op_t *avail;

    /* Make a list of the components that query successfully */
    selectable = OBJ_NEW(opal_list_t);

    /* Scan through the list of components.  This nested loop is O(N^2),
       but we should never have too many components and/or names, so this
       *hopefully* shouldn't matter... */

    OPAL_LIST_FOREACH(cli, components, mca_base_component_list_item_t) {
        component = cli->cli_component;

        priority = check_one_component(op, component, &module);
        if (priority >= 0) {
            /* We have a component that indicated that it wants to run by
               giving us a module */
            avail = OBJ_NEW(avail_op_t);
            avail->ao_priority = priority;
            avail->ao_module = module;

            opal_list_append(selectable, (opal_list_item_t*)avail);
        }
    }

    opal_list_sort(selectable, avail_op_compare);

    /* All done (even if the list is empty; that's ok) */
    return selectable;
}


/*
 * Check a single component
 */
static int check_one_component(ompi_op_t *op,
                               const mca_base_component_t *component,
                               ompi_op_base_module_1_0_0_t **module)
{
    int err;
    int priority = -1;

    err = query(component, op, &priority, module);

    if (OMPI_SUCCESS == err) {
        priority = (priority < 100) ? priority : 100;
        opal_output_verbose(10, ompi_op_base_framework.framework_output,
                            "op:base:op_select: component available: %s, priority: %d",
                            component->mca_component_name, priority);

    } else {
        priority = -1;
        opal_output_verbose(10, ompi_op_base_framework.framework_output,
                            "op:base:op_select: component not available: %s",
                            component->mca_component_name);
    }

    return priority;
}


/**************************************************************************
 * Query functions
 **************************************************************************/

/*
 * Take any version of a op module, query it, and return the right
 * module struct
 */
static int query(const mca_base_component_t *component,
                 ompi_op_t *op,
                 int *priority, ompi_op_base_module_1_0_0_t **module)
{
    *module = NULL;
    if (1 == component->mca_type_major_version &&
        0 == component->mca_type_minor_version &&
        0 == component->mca_type_release_version) {
        const ompi_op_base_component_1_0_0_t *op100 =
            (ompi_op_base_component_1_0_0_t *) component;

        return query_1_0_0(op100, op, priority, module);
    }

    /* Unknown op API version -- return error */

    return OMPI_ERROR;
}


static int query_1_0_0(const ompi_op_base_component_1_0_0_t *component,
                       ompi_op_t *op, int *priority,
                       ompi_op_base_module_1_0_0_t **module)
{
    ompi_op_base_module_1_0_0_t *ret;

    /* There's currently no need for conversion */

    ret = component->opc_op_query(op, priority);
    if (NULL != ret) {
        *module = ret;
        return OMPI_SUCCESS;
    }

    return OMPI_ERROR;
}
