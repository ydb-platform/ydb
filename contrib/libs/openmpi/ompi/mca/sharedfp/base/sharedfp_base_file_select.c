/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"
#include "ompi/mca/common/ompio/common_ompio.h"

/*
 * This structure is needed so that we can close the modules
 * which are not selected but were opened. mca_base_modules_close
 * which does this job for us requires a opal_list_t which contains
 * these modules
 */
struct queried_module_t {
    opal_list_item_t super;
    mca_sharedfp_base_component_t *om_component;
    mca_sharedfp_base_module_t *om_module;
};
typedef struct queried_module_t queried_module_t;
static OBJ_CLASS_INSTANCE(queried_module_t, opal_list_item_t, NULL, NULL);


/*
 * Only one sharedfp module can be attached to each file.
 *
 * This module calls the query funtion on all the components that were
 * detected by sharedfp_base_open. This function is called on a
 * per-file basis. This function has the following function.
 *
 * 1. Iterate over the list of available_components
 * 2. Call the query function on each of these components.
 * 3. query function returns the structure containing pointers
 *    to its module and its priority
 * 4. Select the module with the highest priority
 * 5. Call the init function on the selected module so that it does the
 *    right setup for the file
 * 6. Call finalize on all the other modules which returned
 *    their module but were unfortunate to not get selected
 */

int mca_sharedfp_base_file_select (struct ompio_file_t *file,
                                   mca_base_component_t *preferred)
{
    int priority;
    int best_priority;
    opal_list_item_t *item;
    mca_base_component_list_item_t *cli;
    mca_sharedfp_base_component_t *component;
    mca_sharedfp_base_component_t *best_component;
    mca_sharedfp_base_module_t *module;
    opal_list_t queried;
    queried_module_t *om;
    char *str;
    int err = MPI_SUCCESS;

    /* Check and see if a preferred component was provided. If it was
     provided then it should be used (if possible) */

    if (NULL != preferred) {

        /* We have a preferred component. Check if it is available
           and if so, whether it wants to run */

         str = &(preferred->mca_component_name[0]);

         opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                             "sharedfp:base:file_select: Checking preferred component: %s",
                             str);

         /* query the component for its priority and get its module
            structure. This is necessary to proceed */

         component = (mca_sharedfp_base_component_t *)preferred;
         module = component->sharedfpm_file_query (file, &priority);
         if (NULL != module &&
             NULL != module->sharedfp_module_init) {

             /* this query seems to have returned something legitimate
              * and we can now go ahead and initialize the
              * file with it * but first, the functions which
              * are null need to be filled in */

             /*fill_null_pointers (module);*/
             file->f_sharedfp = module;
             file->f_sharedfp_component = preferred;

             return module->sharedfp_module_init(file);
         }
            /* His preferred component is present, but is unable to
             * run. This is not a good sign. We should try selecting
             * some other component We let it fall through and select
             * from the list of available components
             */
     } /*end of selection for preferred component */

    /*
     * We fall till here if one of the two things happened:
     * 1. The preferred component was provided but for some reason was
     * not able to be selected
     * 2. No preferred component was provided
     *
     * All we need to do is to go through the list of available
     * components and find the one which has the highest priority and
     * use that for this file
     */

    best_component = NULL;
    best_priority = -1;
    OBJ_CONSTRUCT(&queried, opal_list_t);

    OPAL_LIST_FOREACH(cli, &ompi_sharedfp_base_framework.framework_components, mca_base_component_list_item_t) {
       component = (mca_sharedfp_base_component_t *) cli->cli_component;
       opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                           "select: initialising %s component %s",
                           component->sharedfpm_version.mca_type_name,
                           component->sharedfpm_version.mca_component_name);

       /*
        * we can call the query function only if there is a function :-)
        */
       if (NULL == component->sharedfpm_file_query) {
          opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                             "select: no query, ignoring the component");
       } else {
           /*
            * call the query function and see what it returns
            */
           module = component->sharedfpm_file_query (file, &priority);

           if (NULL == module ||
               NULL == module->sharedfp_module_init) {
               /*
                * query did not return any action which can be used
                */
               opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                                  "select: query returned failure");
           } else {
               opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
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
       } /* end else of if (NULL == component->sharedfpm_init) */
    } /* end for ... end of traversal */

   /*
     * Now we have alist of components which successfully returned
     * their module struct.  One of these components has the best
     * priority. The rest have to be comm_unqueried to counter the
     * effects of file_query'ing them. Finalize happens only on
     * components which should are initialized.
     */
    if (NULL == best_component) {
       /*
        * This typically means that there was no component which was
        * able to run properly this time. So, we need to abort
        */
        OBJ_DESTRUCT(&queried);
        return OMPI_ERROR;
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

            /*
             * We don return here coz we still need to go through and
             * elease the other objects
             */

            /*fill_null_pointers (om->om_module);*/
            file->f_sharedfp = om->om_module;
            err = om->om_module->sharedfp_module_init(file);
            file->f_sharedfp_component = (mca_base_component_t *)best_component;

         } else {
            /*
             * this is not the "choosen one", finalize
             */
             if (NULL != om->om_component->sharedfpm_file_unquery) {
                /* unquery the component only if they have some clean
                 * up job to do. Components which are queried but do
                 * not actually do anything typically do not have a
                 * unquery. Hence this check is necessary
                 */
                 (void) om->om_component->sharedfpm_file_unquery(file);
                 opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                                     "select: component %s is not selected",
                                     om->om_component->sharedfpm_version.mca_component_name);
               } /* end if */
          } /* if not best component */
          OBJ_RELEASE(om);
    } /* traversing through the entire list */

    opal_output_verbose(10, ompi_sharedfp_base_framework.framework_output,
                       "select: component %s selected",
                        best_component->sharedfpm_version.mca_component_name);

    OBJ_DESTRUCT(&queried);

    return err;
}
