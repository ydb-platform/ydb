/*
 * Copyright (c) 2011-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2015 INRIA.  All rights reserved.
 * Copyright (c) 2011-2015 Université Bordeaux 1
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>

#include "mpi.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/mca/topo/treematch/topo_treematch.h"

/*
 * Local functions
 */
static void treematch_module_constructor(mca_topo_treematch_module_t *u);
static void treematch_module_destructor(mca_topo_treematch_module_t *u);

OBJ_CLASS_INSTANCE(mca_topo_treematch_module_t, mca_topo_base_module_t,
                   treematch_module_constructor, treematch_module_destructor);


static void treematch_module_constructor(mca_topo_treematch_module_t *u)
{
    mca_topo_base_module_t *m = &(u->super);

    memset(&m->topo, 0, sizeof(m->topo));
}


static void treematch_module_destructor(mca_topo_treematch_module_t *u)
{
    /* Do whatever is necessary to clean up / destroy the module */
}
