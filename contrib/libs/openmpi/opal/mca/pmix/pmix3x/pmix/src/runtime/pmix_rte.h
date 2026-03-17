/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#ifndef PMIX_RTE_H
#define PMIX_RTE_H

#include "pmix_config.h"
#include "pmix_common.h"
#include "src/class/pmix_object.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include PMIX_EVENT_HEADER

#include "src/include/pmix_globals.h"
#include "src/mca/ptl/ptl_types.h"

BEGIN_C_DECLS

#if PMIX_ENABLE_TIMING
extern char *pmix_timing_sync_file;
extern char *pmix_timing_output;
extern bool pmix_timing_overhead;
#endif

extern int pmix_initialized;
extern char *pmix_net_private_ipv4;
extern int pmix_event_caching_window;
extern bool pmix_suppress_missing_data_warning;

/** version string of pmix */
extern const char pmix_version_string[];

/**
 * Initialize the PMIX layer, including the MCA system.
 *
 * @retval PMIX_SUCCESS Upon success.
 * @retval PMIX_ERROR Upon failure.
 *
 */
PMIX_EXPORT pmix_status_t pmix_rte_init(pmix_proc_type_t type,
                                        pmix_info_t info[], size_t ninfo,
                                        pmix_ptl_cbfunc_t cbfunc);

/**
 * Finalize the PMIX layer, including the MCA system.
 *
 */
PMIX_EXPORT void pmix_rte_finalize(void);

/**
 * Internal function.  Do not call.
 */
PMIX_EXPORT pmix_status_t pmix_register_params(void);
PMIX_EXPORT pmix_status_t pmix_deregister_params(void);

END_C_DECLS

#endif /* PMIX_RTE_H */
