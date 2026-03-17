/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * The Open RTE Error and Recovery Manager (ErrMgr)
 *
 * This framework is the logically central clearing house for process/daemon
 * state updates. In particular when a process fails and another process detects
 * it, then that information is reported through this framework. This framework
 * then (depending on the active component) decides how to handle the failure.
 *
 * For example, if a process fails this may activate an automatic recovery
 * of the process from a previous checkpoint, or initial state. Conversely,
 * the active component could decide not to continue the job, and request that
 * it be terminated. The error and recovery policy is determined by individual
 * components within this framework.
 *
 */

#ifndef ORTE_MCA_ERRMGR_H
#define ORTE_MCA_ERRMGR_H

/*
 * includes
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/class/opal_object.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/output.h"
#include "opal/util/error.h"

#include "orte/runtime/orte_globals.h"
#include "orte/mca/plm/plm_types.h"

BEGIN_C_DECLS

/*
 * Macro definitions
 */
/*
 * Thess macros and associated error name array are used to output intelligible error
 * messages.
 */

#define ORTE_ERROR_NAME(n)  opal_strerror(n)
#define ORTE_ERROR_LOG(n)                       \
        orte_errmgr.logfn(n, __FILE__, __LINE__);

/*
 * Framework Interfaces
 */
/**
 * Module initialization function.
 *
 * @retval ORTE_SUCCESS The operation completed successfully
 * @retval ORTE_ERROR   An unspecifed error occurred
 */
typedef int (*orte_errmgr_base_module_init_fn_t)(void);

/**
 * Module finalization function.
 *
 * @retval ORTE_SUCCESS The operation completed successfully
 * @retval ORTE_ERROR   An unspecifed error occurred
 */
typedef int (*orte_errmgr_base_module_finalize_fn_t)(void);

/**
 * This is not part of any module so it can be used at any time!
 */
typedef void (*orte_errmgr_base_module_log_fn_t)(int error_code, char *filename, int line);

/**
 * Alert - self aborting
 * This function is called when a process is aborting due to some internal error.
 * It will finalize the process
 * itself, and then exit - it takes no other actions. The intent here is to provide
 * a last-ditch exit procedure that attempts to clean up a little.
 */
typedef void (*orte_errmgr_base_module_abort_fn_t)(int error_code, char *fmt, ...)
__opal_attribute_format_funcptr__(__printf__, 2, 3);

/**
 * Alert - abort peers
 *  This function is called when a process wants to abort one or more peer processes.
 *  For example, MPI_Abort(comm) will use this function to terminate peers in the
 *  communicator group before aborting itself.
 */
typedef int (*orte_errmgr_base_module_abort_peers_fn_t)(orte_process_name_t *procs,
                                                        orte_std_cntr_t num_procs,
                                                        int error_code);

/*
 * Module Structure
 */
struct orte_errmgr_base_module_2_3_0_t {
    /** Initialization Function */
    orte_errmgr_base_module_init_fn_t                       init;
    /** Finalization Function */
    orte_errmgr_base_module_finalize_fn_t                   finalize;

    orte_errmgr_base_module_log_fn_t                        logfn;
    orte_errmgr_base_module_abort_fn_t                      abort;
    orte_errmgr_base_module_abort_peers_fn_t                abort_peers;
};
typedef struct orte_errmgr_base_module_2_3_0_t orte_errmgr_base_module_2_3_0_t;
typedef orte_errmgr_base_module_2_3_0_t orte_errmgr_base_module_t;
ORTE_DECLSPEC extern orte_errmgr_base_module_t orte_errmgr;

/*
 * ErrMgr Component
 */
struct orte_errmgr_base_component_3_0_0_t {
    /** MCA base component */
    mca_base_component_t base_version;
    /** MCA base data */
    mca_base_component_data_t base_data;

    /** Verbosity Level */
    int verbose;
    /** Output Handle for opal_output */
    int output_handle;
    /** Default Priority */
    int priority;
};
typedef struct orte_errmgr_base_component_3_0_0_t orte_errmgr_base_component_3_0_0_t;
typedef orte_errmgr_base_component_3_0_0_t orte_errmgr_base_component_t;

/*
 * Macro for use in components that are of type errmgr
 */
#define ORTE_ERRMGR_BASE_VERSION_3_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("errmgr", 3, 0, 0)

END_C_DECLS

#endif
