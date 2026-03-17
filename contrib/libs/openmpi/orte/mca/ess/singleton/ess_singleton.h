/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_ESS_SINGLETON_H
#define ORTE_ESS_SINGLETON_H

BEGIN_C_DECLS


typedef struct {
    orte_ess_base_component_t super;
    char *server_uri;
    bool isolated;
} orte_ess_singleton_component_t;
ORTE_MODULE_DECLSPEC extern orte_ess_singleton_component_t mca_ess_singleton_component;

END_C_DECLS

#endif /* ORTE_ESS_SINGLETON_H */
