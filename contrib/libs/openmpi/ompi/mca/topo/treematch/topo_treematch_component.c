/*
 * Copyright (c) 2011-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2015 INRIA.  All rights reserved.
 * Copyright (c) 2011-2015 Université Bordeaux 1
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/mca/topo/treematch/topo_treematch.h"

/*
 * Public string showing the topo treematch module version number
 */
const char *mca_topo_treematch_component_version_string =
    "Open MPI treematch topology MCA component version" OMPI_VERSION;

/*
 * Local funtions
 */
static int init_query(bool enable_progress_threads, bool enable_mpi_threads);
static struct mca_topo_base_module_t *
comm_query(const ompi_communicator_t *comm, int *priority, uint32_t type);
static int mca_topo_treematch_component_register(void);

/*
 * Public component structure
 */
mca_topo_treematch_component_2_2_0_t mca_topo_treematch_component =
    {
        {
            {
                MCA_TOPO_BASE_VERSION_2_2_0,

                "treematch",
                OMPI_MAJOR_VERSION,
                OMPI_MINOR_VERSION,
                OMPI_RELEASE_VERSION,
                NULL,  /* component open */
                NULL,  /* component close */
                NULL, /* component query */
                mca_topo_treematch_component_register, /* component register */
            },

            {
                /* The component is checkpoint ready */
                MCA_BASE_METADATA_PARAM_CHECKPOINT
            },

            init_query,
            comm_query
        },
        0  /* reorder: by default centralized */
    };


static int init_query(bool enable_progress_threads, bool enable_mpi_threads)
{
    /* The first time this function is called is too early in the process and
     * the HWLOC topology information is not available. Thus we should not check
     * for the topology here, but instead delay the check until we really need
     * the topology information.
     */
    return OMPI_SUCCESS;
}


static struct mca_topo_base_module_t *
comm_query(const ompi_communicator_t *comm, int *priority, uint32_t type)
{
    mca_topo_treematch_module_t *treematch;

    if( OMPI_COMM_DIST_GRAPH != type ) {
        return NULL;
    }
    treematch = OBJ_NEW(mca_topo_treematch_module_t);
    if (NULL == treematch) {
        return NULL;
    }
    treematch->super.topo.dist_graph.dist_graph_create = mca_topo_treematch_dist_graph_create;

    /* This component has very low priority -- it's an treematch, after
       all! */
    *priority = 42;
    treematch->super.type = OMPI_COMM_DIST_GRAPH;
    return &(treematch->super);
}

static int mca_topo_treematch_component_register(void)
{
    (void)mca_base_component_var_register(&mca_topo_treematch_component.super.topoc_version,
                                          "reorder_mode", "If set the reordering will be done in a partially distributed way (default=0). If partially-distributed only local knowledge will be used, possibly leading to less accurate reordering.", MCA_BASE_VAR_TYPE_INT,
                                          NULL, 0, 0, OPAL_INFO_LVL_2,
                                          MCA_BASE_VAR_SCOPE_READONLY, &mca_topo_treematch_component.reorder_mode);
    return OMPI_SUCCESS;
}

