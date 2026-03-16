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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
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
#include "opal/class/opal_list.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/info.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/fs/base/base.h"

/*
 * Local types
 */
struct avail_io_t {
    opal_list_item_t super;

    mca_io_base_version_t ai_version;

    int ai_priority;
    mca_io_base_components_t ai_component;
    struct mca_io_base_delete_t *ai_private_data;
};
typedef struct avail_io_t avail_io_t;

/*
 * Local functions
 */
static opal_list_t *check_components(opal_list_t *components,
                                     const char *filename, struct opal_info_t *info,
                                     char **names, int num_names);
static avail_io_t *check_one_component(const mca_base_component_t *component,
                                       const char *filename, struct opal_info_t *info);

static avail_io_t *query(const mca_base_component_t *component,
                         const char *filename, struct opal_info_t *info);
static avail_io_t *query_2_0_0(const mca_io_base_component_2_0_0_t *io_component,
                               const char *filename, struct opal_info_t *info);

static void unquery(avail_io_t *avail, const char *filename, struct opal_info_t *info);

static int delete_file(avail_io_t *avail, const char *filename, struct opal_info_t *info);

extern opal_mutex_t ompi_mpi_ompio_bootstrap_mutex;


/*
 * Stuff for the OBJ interface
 */
static OBJ_CLASS_INSTANCE(avail_io_t, opal_list_item_t, NULL, NULL);


/*
 */
int mca_io_base_delete(const char *filename, struct opal_info_t *info)
{
    int err;
    opal_list_t *selectable;
    opal_list_item_t *item;
    avail_io_t *avail, selected;

    /* Announce */

    opal_output_verbose(10, ompi_io_base_framework.framework_output,
                        "io:base:delete: deleting file: %s",
                        filename);

    /* See if a set of component was requested by the MCA parameter.
       Don't check for error. */

    /* Compute the intersection of all of my available components with
       the components from all the other processes in this file */

    /* JMS CONTINUE HERE */

    /* See if there were any listed in the MCA parameter; parse them
       and check them all */

    err = OMPI_ERROR;
    opal_output_verbose(10, ompi_io_base_framework.framework_output,
                        "io:base:delete: Checking all available modules");
    selectable = check_components(&ompi_io_base_framework.framework_components,
                                  filename, info, NULL, 0);

    /* Upon return from the above, the modules list will contain the
       list of modules that returned (priority >= 0).  If we have no
       io modules available, it's an error */

    if (NULL == selectable) {
        /* There's no modules available.  Doh! */
        /* show_help */
        return OMPI_ERROR;
    }
    /* Do some kind of collective operation to find a module that
       everyone has available */
#if 1
    /* For the moment, just take the top module off the list */
    /* MSC actually take the buttom */
    item = opal_list_remove_last(selectable);
    avail = (avail_io_t *) item;
    selected = *avail;
    OBJ_RELEASE(avail);
#else
    /* JMS CONTINUE HERE */
#endif

    /* Everything left in the selectable list is therefore unwanted,
       and we call their unquery() method (because they all had
       query() invoked, but will never have init() invoked in this
       scope). */

    for (item = opal_list_remove_first(selectable); item != NULL;
         item = opal_list_remove_first(selectable)) {
        avail = (avail_io_t *) item;
        unquery(avail, filename, info);
        OBJ_RELEASE(item);
    }
    OBJ_RELEASE(selectable);

    if (!strcmp (selected.ai_component.v2_0_0.io_version.mca_component_name,
                 "ompio")) {
        int ret;

        opal_mutex_lock(&ompi_mpi_ompio_bootstrap_mutex);
        if (OMPI_SUCCESS != (ret = mca_base_framework_open(&ompi_fs_base_framework, 0))) {
            opal_mutex_unlock(&ompi_mpi_ompio_bootstrap_mutex);
            return err;
        }
        opal_mutex_unlock(&ompi_mpi_ompio_bootstrap_mutex);

        if (OMPI_SUCCESS !=
            (ret = mca_fs_base_find_available(OPAL_ENABLE_PROGRESS_THREADS, 1))) {
            return err;
        }
    }

    
    /* Finally -- delete the file with the selected component */
    if (OMPI_SUCCESS != (err = delete_file(&selected, filename, info))) {
        return err;
    }

    /* Announce the winner */

    opal_output_verbose(10, ompi_io_base_framework.framework_output,
                        "io:base:delete: Selected io component %s",
                        selected.ai_component.v2_0_0.io_version.mca_component_name);

    return OMPI_SUCCESS;
}


static int avail_io_compare (opal_list_item_t **itema,
                             opal_list_item_t **itemb)
{
    const avail_io_t *availa = (const avail_io_t *) *itema;
    const avail_io_t *availb = (const avail_io_t *) *itemb;

    /* highest component last */
    if (availa->ai_priority > availb->ai_priority) {
        return 1;
    } else if (availa->ai_priority < availb->ai_priority) {
        return -1;
    } else {
        return 0;
    }
}

/*
 * For each module in the list, if it is in the list of names (or the
 * list of names is NULL), then check and see if it wants to run, and
 * do the resulting priority comparison.  Make a list of components to
 * be only those who returned that they want to run, and put them in
 * priority order.
 */
static opal_list_t *check_components(opal_list_t *components,
                                     const char *filename, struct opal_info_t *info,
                                     char **names, int num_names)
{
    int i;
    const mca_base_component_t *component;
    mca_base_component_list_item_t *cli;
    bool want_to_check;
    opal_list_t *selectable;
    avail_io_t *avail;

    /* Make a list of the components that query successfully */

    selectable = OBJ_NEW(opal_list_t);

    /* Scan through the list of components.  This nested loop is
       O(N^2), but we should never have too many components and/or
       names, so this *hopefully* shouldn't matter... */

    OPAL_LIST_FOREACH(cli, components, mca_base_component_list_item_t) {
        component = cli->cli_component;

        /* If we have a list of names, scan through it */

        if (0 == num_names) {
            want_to_check = true;
        } else {
            want_to_check = false;
            for (i = 0; i < num_names; ++i) {
                if (0 == strcmp(names[i], component->mca_component_name)) {
                    want_to_check = true;
                }
            }
        }

        /* If we determined that we want to check this component, then
           do so */

        if (want_to_check) {
            avail = check_one_component(component, filename, info);
            if (NULL != avail) {

                /* Put this item on the list in priority order
                   (highest priority first).  Should it go first? */
                /* MSC actually put it Lowest priority first */
                /* NTH sort this out later */
                opal_list_append(selectable, (opal_list_item_t*)avail);
            }
        }
    }

    /* If we didn't find any available components, return an error */

    if (0 == opal_list_get_size(selectable)) {
        OBJ_RELEASE(selectable);
        return NULL;
    }

    opal_list_sort(selectable, avail_io_compare);

    /* All done */

    return selectable;
}


/*
 * Check a single component
 */
static avail_io_t *check_one_component(const mca_base_component_t *component,
                                       const char *filename, struct opal_info_t *info)
{
    avail_io_t *avail;

    avail = query(component, filename, info);
    if (NULL != avail) {
        avail->ai_priority = (avail->ai_priority < 100) ?
            avail->ai_priority : 100;
        avail->ai_priority = (avail->ai_priority < 0) ?
            0 : avail->ai_priority;
        opal_output_verbose(10, ompi_io_base_framework.framework_output,
                            "io:base:delete: component available: %s, priority: %d",
                            component->mca_component_name,
                            avail->ai_priority);
    } else {
        opal_output_verbose(10, ompi_io_base_framework.framework_output,
                            "io:base:delete: component not available: %s",
                            component->mca_component_name);
    }

    return avail;
}


/**************************************************************************
 * Query functions
 **************************************************************************/

/*
 * Take any version of a io module, query it, and return the right
 * module struct
 */
static avail_io_t *query(const mca_base_component_t *component,
                         const char *filename, struct opal_info_t *info)
{
    const mca_io_base_component_2_0_0_t *ioc_200;

    /* io v2.0.0 */

    if (MCA_BASE_VERSION_MAJOR == component->mca_major_version &&
        MCA_BASE_VERSION_MINOR == component->mca_minor_version &&
        MCA_BASE_VERSION_RELEASE == component->mca_release_version) {
        ioc_200 = (mca_io_base_component_2_0_0_t *) component;

        return query_2_0_0(ioc_200, filename, info);
    }

    /* Unknown io API version -- return error */

    return NULL;
}


static avail_io_t *query_2_0_0(const mca_io_base_component_2_0_0_t *component,
                               const char *filename, struct opal_info_t *info)
{
    bool usable;
    int priority, ret;
    avail_io_t *avail;
    struct mca_io_base_delete_t *private_data;

    /* Query v2.0.0 */

    avail = NULL;
    private_data = NULL;
    usable = false;
    ret = component->io_delete_query(filename, info, &private_data, &usable,
                                     &priority);
    if (OMPI_SUCCESS == ret && usable) {
        avail = OBJ_NEW(avail_io_t);
        avail->ai_version = MCA_IO_BASE_V_2_0_0;
        avail->ai_priority = priority;
        avail->ai_component.v2_0_0 = *component;
        avail->ai_private_data = private_data;
    }

    return avail;
}


/**************************************************************************
 * Unquery functions
 **************************************************************************/

static void unquery(avail_io_t *avail, const char *filename, struct opal_info_t *info)
{
    const mca_io_base_component_2_0_0_t *ioc_200;

    switch(avail->ai_version) {
    case MCA_IO_BASE_V_2_0_0:
        ioc_200 = &(avail->ai_component.v2_0_0);
        if (NULL != ioc_200->io_delete_unquery) {
            ioc_200->io_delete_unquery(filename, info, avail->ai_private_data);
        }
        break;

    default:
        break;
    }
}


/**************************************************************************
 * File delete functions
 **************************************************************************/

/*
 * Invoke the component's delete function
 */
static int delete_file(avail_io_t *avail, const char *filename, struct opal_info_t *info)
{
    const mca_io_base_component_2_0_0_t *ioc_200;

    switch(avail->ai_version) {
    case MCA_IO_BASE_V_2_0_0:
        ioc_200 = &(avail->ai_component.v2_0_0);
        return ioc_200->io_delete_select(filename, info,
                                         avail->ai_private_data);
        break;

    default:
        return OMPI_ERROR;
        break;
    }

    /* No way to reach here */
}
