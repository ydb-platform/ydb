/*
 * Copyright (c) 2011-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 * Copyright (c) 2011-2013 INRIA.  All rights reserved.
 * Copyright (c) 2011-2013 Université Bordeaux 1
 *                         reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_TOPO_BASIC_H
#define MCA_TOPO_BASIC_H

#include "ompi_config.h"
#include "ompi/mca/topo/topo.h"

BEGIN_C_DECLS

typedef mca_topo_base_component_2_2_0_t mca_topo_basic_component_t;
/* Public component instance */
OMPI_MODULE_DECLSPEC extern mca_topo_basic_component_t
    mca_topo_basic_component;

END_C_DECLS

#endif /* MCA_TOPO_BASIC_H */

