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
 * Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Processor stats for Posix systems.
 *
 */


#ifndef MCA_PSTAT_LINUX_EXPORT_H
#define MCA_PSTAT_LINUX_EXPORT_H

#include "opal_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/pstat/pstat.h"


BEGIN_C_DECLS

/**
 * Globally exported variable
 */
OPAL_DECLSPEC extern const opal_pstat_base_component_t mca_pstat_linux_component;

OPAL_DECLSPEC extern const opal_pstat_base_module_t opal_pstat_linux_module;

END_C_DECLS
#endif /* MCA_PSTAT_LINUX_EXPORT_H */
