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
 * Copyright (c) 2008-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>

#include "opal/class/opal_list.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"

#include "ompi/mca/topo/topo.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/communicator/communicator.h"

/*
 * Local functions
 */
static void fill_null_pointers(int type, mca_topo_base_module_t *module);


/*
 * This structure is needed so that we can close the modules
 * which are not selected but were opened. mca_base_modules_close
 * which does this job for us requires a opal_list_t which contains
 * these modules
 */
struct queried_module_t {
    opal_list_item_t super;
    mca_topo_base_component_t *om_component;
    mca_topo_base_module_t *om_module;
};
typedef struct queried_module_t queried_module_t;
static OBJ_CLASS_INSTANCE(queried_module_t, opal_list_item_t, NULL, NULL);

/*
 * Only one topo module can be attached to each communicator. The
 * communicator provided as argument is the old communicator, and the
 * newly selected topology is __not__ supposed to be attached to it.
 *
 * This module calls the query funtion on all the components that were
 * detected by topo_base_open. This function is called on a
 * per-communicator basis. This function has the following function.
 *
 * 1. Iterate over the list of available_components.
 * 2. Call the query function on each of these components.
 * 3. The query function returns a module and its priority.
 * 4. Select the module with the highest priority.
 * 5. OBJ_RELEASE all the "losing" modules.
 */
int mca_topo_base_comm_select(const ompi_communicator_t*  comm,
                              mca_topo_base_module_t*     preferred_module,
                              mca_topo_base_module_t**    selected_module,
                              uint32_t                    type)
{
    int priority;
    int best_priority;
    opal_list_item_t *item;
    mca_base_component_list_item_t *cli;
    mca_topo_base_component_t *component;
    mca_topo_base_component_t *best_component;
    mca_topo_base_module_t *module;
    opal_list_t queried;
    queried_module_t *om;
    int err = MPI_SUCCESS;

    /* Ensure the topo framework is initialized */
    if (OMPI_SUCCESS != (err = mca_topo_base_lazy_init())) {
        return err;
    }
    opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                        "topo:base:comm_select: new communicator: %s (cid %d)",
                        comm->c_name, comm->c_contextid);

    /* Check and see if a preferred component was provided. If it was
       provided then it should be used (if possible) */
    if (NULL != preferred_module) {

        /* We have a preferred module. Check if it is available
           and if so, whether it wants to run */

         opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                             "topo:base:comm_select: Checking preferred component: %s",
                             preferred_module->topo_component->topoc_version.mca_component_name);

         /* query the component for its priority and get its module
            structure. This is necessary to proceed */
         component = (mca_topo_base_component_t *)preferred_module->topo_component;
         module = component->topoc_comm_query(comm, &priority, type);
         if (NULL != module) {

             /* this query seems to have returned something legitimate
              * and we can now go ahead fill the NULL functions with the
              * corresponding base version and then return.
              */
             fill_null_pointers(type, module);
             *selected_module = module;
             module->topo_component = component;
             return OMPI_SUCCESS;
         }
         /* If we get here, the preferred component is present, but is
            unable to run.  This is not a good sign.  We should try
            selecting some other component.  We let it fall through
            and select from the list of available components */
    }

    /*
     * We fall here if one of the two things happened:
     * 1. The preferred component was provided but for some reason was
     *    not able to be selected
     * 2. No preferred component was provided
     *
     * All we need to do is to go through the list of available
     * components and find the one which has the highest priority and
     * use that for this communicator
     */

    best_component = NULL;
    best_priority = -1;
    OBJ_CONSTRUCT(&queried, opal_list_t);

    OPAL_LIST_FOREACH(cli, &ompi_topo_base_framework.framework_components, mca_base_component_list_item_t) {
       component = (mca_topo_base_component_t *) cli->cli_component;
       opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                           "select: initialising %s component %s",
                           component->topoc_version.mca_type_name,
                           component->topoc_version.mca_component_name);

       /*
        * we can call the query function only if there is a function :-)
        */
       if (NULL == component->topoc_comm_query) {
          opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                             "select: no query, ignoring the component");
       } else {
           /*
            * call the query function and see what it returns
            */
           module = component->topoc_comm_query(comm, &priority, type);

           if (NULL == module) {
               /*
                * query did not return any action which can be used
                */
               opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                                  "select: query returned failure");
           } else {
               opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                                  "select: query returned priority %d",
                                  priority);
               /*
                * is this the best component we have found till now?
                */
               if (priority > best_priority) {
                   best_priority = priority;
                   best_component = component;
               }

               om = OBJ_NEW(queried_module_t);
               /*
                * check if we have run out of space
                */
               if (NULL == om) {
                   OBJ_DESTRUCT(&queried);
                   return OMPI_ERR_OUT_OF_RESOURCE;
               }
               om->om_component = component;
               om->om_module = module;
               opal_list_append(&queried, (opal_list_item_t *)om);
           } /* end else of if (NULL == module) */
       } /* end else of if (NULL == component->init) */
    } /* end for ... end of traversal */

    /*
     * Now we have alist of components which successfully returned
     * their module struct.  One of these components has the best
     * priority. The rest have to be comm_unqueried to counter the
     * effects of comm_query'ing them. Finalize happens only on
     * components which should are initialized.
     */
    if (NULL == best_component) {
        return OMPI_ERR_NOT_FOUND;
    }

    /*
     * We now have a list of components which have successfully
     * returned their priorities from the query. We now have to
     * unquery() those components which have not been selected and
     * init() the component which was selected
     */
    for (item = opal_list_remove_first(&queried);
         NULL != item;
         item = opal_list_remove_first(&queried)) {
        om = (queried_module_t *) item;
        if (om->om_component == best_component) {
           /*
            * this is the chosen component, we have to initialise the
            * module of this component.
            *
            * ANJU: a component might not have all the functions
            * defined.  Whereever a function pointer is null in the
            * module structure we need to fill it in with the base
            * structure function pointers. This is yet to be done
            */
            fill_null_pointers(type, om->om_module);
            om->om_module->topo_component = best_component;
            *selected_module = om->om_module;
         } else {
             /* this is not the "choosen one", finalize */
              opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                                  "select: component %s is not selected",
                                  om->om_component->topoc_version.mca_component_name);
              OBJ_RELEASE(om->om_module);
          }
          OBJ_RELEASE(om);
    } /* traversing through the entire list */

    opal_output_verbose(10, ompi_topo_base_framework.framework_output,
                       "select: component %s selected",
                        best_component->topoc_version.mca_component_name);
    return OMPI_SUCCESS;
}


/*
 * This function fills in the null function pointers, in other words,
 * those functions which are not implemented by the module with the
 * pointers from the base function. Somewhere, I need to incoroporate
 * a check for the common minimum funtions being implemented by the
 * module.
 */
static void fill_null_pointers(int type, mca_topo_base_module_t *module)
{
    if( OMPI_COMM_CART == type ) {
        if (NULL == module->topo.cart.cart_coords) {
            module->topo.cart.cart_coords = mca_topo_base_cart_coords;
        }
        if (NULL == module->topo.cart.cart_create) {
            module->topo.cart.cart_create = mca_topo_base_cart_create;
        }
        if (NULL == module->topo.cart.cart_get) {
            module->topo.cart.cart_get = mca_topo_base_cart_get;
        }
        if (NULL == module->topo.cart.cartdim_get) {
            module->topo.cart.cartdim_get = mca_topo_base_cartdim_get;
        }
        if (NULL == module->topo.cart.cart_map) {
            module->topo.cart.cart_map = mca_topo_base_cart_map;
        }
        if (NULL == module->topo.cart.cart_rank) {
            module->topo.cart.cart_rank = mca_topo_base_cart_rank;
        }
        if (NULL == module->topo.cart.cart_shift) {
            module->topo.cart.cart_shift = mca_topo_base_cart_shift;
        }
        if (NULL == module->topo.cart.cart_sub) {
            module->topo.cart.cart_sub = mca_topo_base_cart_sub;
        }
    } else if( OMPI_COMM_GRAPH == type ) {
        if (NULL == module->topo.graph.graph_create) {
            module->topo.graph.graph_create = mca_topo_base_graph_create;
        }
        if (NULL == module->topo.graph.graph_get) {
            module->topo.graph.graph_get = mca_topo_base_graph_get;
        }
        if (NULL == module->topo.graph.graph_map) {
            module->topo.graph.graph_map = mca_topo_base_graph_map;
        }
        if (NULL == module->topo.graph.graphdims_get) {
            module->topo.graph.graphdims_get = mca_topo_base_graphdims_get;
        }
        if (NULL == module->topo.graph.graph_neighbors) {
            module->topo.graph.graph_neighbors = mca_topo_base_graph_neighbors;
        }
        if (NULL == module->topo.graph.graph_neighbors_count) {
            module->topo.graph.graph_neighbors_count = mca_topo_base_graph_neighbors_count;
        }
    } else if( OMPI_COMM_DIST_GRAPH == type ) {
        if (NULL == module->topo.dist_graph.dist_graph_create) {
            module->topo.dist_graph.dist_graph_create = mca_topo_base_dist_graph_create;
        }
        if (NULL == module->topo.dist_graph.dist_graph_create_adjacent) {
            module->topo.dist_graph.dist_graph_create_adjacent = mca_topo_base_dist_graph_create_adjacent;
        }
        if (NULL == module->topo.dist_graph.dist_graph_neighbors) {
            module->topo.dist_graph.dist_graph_neighbors = mca_topo_base_dist_graph_neighbors;
        }
        if (NULL == module->topo.dist_graph.dist_graph_neighbors_count) {
            module->topo.dist_graph.dist_graph_neighbors_count = mca_topo_base_dist_graph_neighbors_count;
        }
    }
}
