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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_ESS_SLURM_H
#define ORTE_ESS_SLURM_H

BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_ess_base_component_t mca_ess_slurm_component;

/*
 * Module open / close
 */
int orte_ess_slurm_component_open(void);
int orte_ess_slurm_component_close(void);
int orte_ess_slurm_component_query(mca_base_module_t **module, int *priority);

END_C_DECLS

#endif /* ORTE_ESS_SLURM_H */
