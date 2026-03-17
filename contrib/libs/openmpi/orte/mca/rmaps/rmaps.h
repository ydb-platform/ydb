/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The Open RTE Resource MAPping Subsystem (RMAPS)
 *
 * The resource mapping subsystem is responsible for mapping processes
 * to specific nodes/cpus within a given job. In many systems, this
 * functionality will not be supported - the system will map processes
 * wherever it chooses and does not allow the user to specify the
 * mapping. RMAPS components, therefore, provide services for those
 * systems that do permit such mappings.
 *
 * RMAPS checks the MCA parameters to see if a mapping algorithm has
 * been specified.  If the user selected a mapping algorithm, the
 * indicated RMAPS component will take information from the registry
 * to determine the number of applications/processes to be run, and
 * the identified resources that have been allocated to this job. The
 * selected RMAP component will then assign processes to resources
 * according to its algorithm, with the results stored on the
 * appropriate job segment - the assigned nodename for each process is
 * stored in that respective process' container on the segment.
 *
 */

#ifndef ORTE_MCA_RMAPS_H
#define ORTE_MCA_RMAPS_H

#include "orte_config.h"
#include "orte/types.h"

#include "orte/mca/mca.h"

#include "orte/runtime/orte_globals.h"

#include "orte/mca/rmaps/rmaps_types.h"

BEGIN_C_DECLS

/*
 * rmaps module functions
 */

/**
* RMAPS module functions - these are not accessible to the outside world,
* but are defined here by convention
*/

/* map a job - used by the HNP to compute the #procs on each node.
 * This is passed to the backend daemons as a regex which they
 * use to create an orte_job_map_t for the job */
typedef int (*orte_rmaps_base_module_map_fn_t)(orte_job_t *jdata);

/* assign a location to each process. Used by the backend daemons,
 * this function takes the orte_job_map_t created from the regex
 * and assigns each process to a specific location within the
 * hardware topology based on the --map-by directive */
typedef int (*orte_rmaps_base_module_assign_loc_fn_t)(orte_job_t *jdata);

/*
 * rmaps module version 3.0.0
 */
struct orte_rmaps_base_module_3_0_0_t {
    /** Mapping function pointer */
    orte_rmaps_base_module_map_fn_t         map_job;
    /* assign locations */
    orte_rmaps_base_module_assign_loc_fn_t  assign_locations;
};
/** Convenience typedef */
typedef struct orte_rmaps_base_module_3_0_0_t orte_rmaps_base_module_3_0_0_t;
/** Convenience typedef */
typedef orte_rmaps_base_module_3_0_0_t orte_rmaps_base_module_t;


/*
 * rmaps component
 */

/**
 * rmaps component version 3.0.0
 */
struct orte_rmaps_base_component_3_0_0_t {
    /** Base MCA structure */
    mca_base_component_t base_version;
    /** Base MCA data */
    mca_base_component_data_t base_data;
};
/** Convenience typedef */
typedef struct orte_rmaps_base_component_3_0_0_t orte_rmaps_base_component_3_0_0_t;
/** Convenience typedef */
typedef orte_rmaps_base_component_3_0_0_t orte_rmaps_base_component_t;


END_C_DECLS

#endif
