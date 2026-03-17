/* -*- C -*-
 *
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
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PLOG_DEFAULT_H
#define PLOG_DEFAULT_H

#include "pmix_config.h"

#include "src/mca/plog/plog.h"

BEGIN_C_DECLS

/*
 * Plog interfaces
 */

PMIX_EXPORT extern pmix_plog_base_component_t mca_plog_default_component;
extern pmix_plog_module_t pmix_plog_default_module;

END_C_DECLS

#endif
