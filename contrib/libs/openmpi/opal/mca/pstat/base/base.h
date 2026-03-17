/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#ifndef OPAL_PSTAT_BASE_H
#define OPAL_PSTAT_BASE_H

#include "opal_config.h"
#include "opal/mca/base/mca_base_framework.h"
#include "opal/mca/pstat/pstat.h"

/*
 * Global functions for MCA overall pstat open and close
 */

BEGIN_C_DECLS

/**
 * Framework structure declaration for this framework
 */
OPAL_DECLSPEC extern mca_base_framework_t opal_pstat_base_framework;

/**
 * Select an available component.
 *
 * @return OPAL_SUCCESS Upon success.
 * @return OPAL_NOT_FOUND If no component can be selected.
 * @return OPAL_ERROR Upon other failure.
 *
 * At the end of this process, we'll either have a single
 * component that is selected and initialized, or no component was
 * selected.  If no component was selected, subsequent invocation
 * of the pstat functions will return an error indicating no data
 * could be obtained
 */
OPAL_DECLSPEC int opal_pstat_base_select(void);

OPAL_DECLSPEC extern opal_pstat_base_component_t *opal_pstat_base_component;

END_C_DECLS

#endif /* OPAL_BASE_PSTAT_H */
