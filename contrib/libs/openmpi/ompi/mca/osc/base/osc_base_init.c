/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/constants.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/info/info.h"
#include "ompi/communicator/communicator.h"
#include "ompi/win/win.h"

int
ompi_osc_base_select(ompi_win_t *win,
                     void **base,
                     size_t size,
                     int disp_unit,
                     ompi_communicator_t *comm,
                     opal_info_t *info,
                     int flavor,
                     int *model)
{
    opal_list_item_t *item;
    ompi_osc_base_component_t *best_component = NULL;
    int best_priority = -1, priority;

    if (opal_list_get_size(&ompi_osc_base_framework.framework_components) <= 0) {
        /* we don't have any components to support us... */
        return OMPI_ERR_NOT_SUPPORTED;
    }

    for (item = opal_list_get_first(&ompi_osc_base_framework.framework_components) ;
         item != opal_list_get_end(&ompi_osc_base_framework.framework_components) ;
         item = opal_list_get_next(item)) {
        ompi_osc_base_component_t *component = (ompi_osc_base_component_t*)
            ((mca_base_component_list_item_t*) item)->cli_component;

        priority = component->osc_query(win, base, size, disp_unit, comm, info, flavor);
        if (priority < 0) {
            if (MPI_WIN_FLAVOR_SHARED == flavor && OMPI_ERR_RMA_SHARED == priority) {
                /* NTH: quick fix to return OMPI_ERR_RMA_SHARED */
                return OMPI_ERR_RMA_SHARED;
            }
            continue;
        }

        if (priority > best_priority) {
            best_component = component;
            best_priority = priority;
        }
    }

    if (NULL == best_component) return OMPI_ERR_NOT_SUPPORTED;

    return best_component->osc_select(win, base, size, disp_unit, comm, info, flavor, model);
}
