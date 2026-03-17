/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_RTE_BASE_H
#define OMPI_RTE_BASE_H

#include "ompi_config.h"

#include "opal/class/opal_list.h"

#include "ompi/mca/rte/rte.h"

/*
 * Global functions for MCA overall rte open and close
 */

BEGIN_C_DECLS

OMPI_DECLSPEC extern mca_base_framework_t ompi_rte_base_framework;

END_C_DECLS

#endif /* OMPI_BASE_RTE_H */
