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
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file:
 */

#ifndef ORTE_ODLS_PSPAWN_H
#define ORTE_ODLS_PSPAWN_H

#include "orte_config.h"

#include "orte/mca/mca.h"

#include "orte/mca/odls/odls.h"

BEGIN_C_DECLS

/*
 * ODLS Pspawn module
 */
extern orte_odls_base_module_t orte_odls_pspawn_module;
ORTE_MODULE_DECLSPEC extern orte_odls_base_component_t mca_odls_pspawn_component;

END_C_DECLS

#endif /* ORTE_ODLS_PSPAWN_H */
