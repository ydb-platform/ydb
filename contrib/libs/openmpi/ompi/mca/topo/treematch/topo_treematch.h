/*
 * Copyright (c) 2011-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2015 INRIA.  All rights reserved.
 * Copyright (c) 2011-2015 Bordeaux Polytechnic Institute
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_TOPO_UNTIY_H
#define MCA_TOPO_UNTIY_H

#include "ompi_config.h"
#include "ompi/mca/topo/topo.h"

/*
 * ******************************************************************
 * ******** functions which provide MCA interface comppliance *******
 * ******************************************************************
 * These functions are:
 *       - mca_topo_treematch_module_open
 *       - mca_topo_treematch_module_close
 *       - mca_topo_treematch_module_query
 *       - mca_topo_treematch_module_finalize
 * These functions are always found on the mca_topo_treematch_module
 * structure. They are the "meta" functions to ensure smooth op.
 * ******************************************************************
 */
BEGIN_C_DECLS

/*
 * Public component instance
 */
typedef struct mca_topo_treematch_component_2_2_0_t {
    mca_topo_base_component_2_2_0_t super;

    int reorder_mode;
} mca_topo_treematch_component_2_2_0_t;

OMPI_MODULE_DECLSPEC extern mca_topo_treematch_component_2_2_0_t
    mca_topo_treematch_component;

/*
 * A unique module class for the module so that we can both cache
 * module-specific information on the module and have a
 * module-specific constructor and destructor.
 */
typedef struct {
    mca_topo_base_module_t super;

    /* Modules can add their own information here */
} mca_topo_treematch_module_t;

OBJ_CLASS_DECLARATION(mca_topo_treematch_module_t);


/*
 * Module functions
 */

int mca_topo_treematch_dist_graph_create(mca_topo_base_module_t* module,
                                         ompi_communicator_t *comm_old,
                                         int n, const int nodes[],
                                         const int degrees[], const int targets[],
                                         const int weights[],
                                         struct opal_info_t *info, int reorder,
                                         ompi_communicator_t **newcomm);
/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */

END_C_DECLS

#endif /* MCA_TOPO_EXAMPLE_H */
