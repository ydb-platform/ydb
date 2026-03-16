/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal_config.h"

#include "opal/util/malloc.h"
#include "opal/util/arch.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/util/proc.h"
#include "opal/memoryhooks/memory.h"
#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/runtime/opal.h"
#include "opal/util/net.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/mca/installdirs/base/base.h"
#include "opal/mca/memory/base/base.h"
#include "opal/mca/patcher/base/base.h"
#include "opal/mca/memcpy/base/base.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/mca/reachable/base/base.h"
#include "opal/mca/timer/base/base.h"
#include "opal/mca/memchecker/base/base.h"
#include "opal/mca/if/base/base.h"
#include "opal/dss/dss.h"
#include "opal/mca/shmem/base/base.h"
#if OPAL_ENABLE_FT_CR    == 1
#include "opal/mca/compress/base/base.h"
#endif
#include "opal/threads/threads.h"

#include "opal/runtime/opal_cr.h"
#include "opal/mca/crs/base/base.h"

#include "opal/runtime/opal_progress.h"
#include "opal/mca/event/base/base.h"
#include "opal/mca/backtrace/base/base.h"

#include "opal/constants.h"
#include "opal/util/error.h"
#include "opal/util/stacktrace.h"
#include "opal/util/keyval_parse.h"
#include "opal/util/sys_limits.h"
#include "opal/util/timings.h"

#if OPAL_CC_USE_PRAGMA_IDENT
#pragma ident OPAL_IDENT_STRING
#elif OPAL_CC_USE_IDENT
#ident OPAL_IDENT_STRING
#endif
const char opal_version_string[] = OPAL_IDENT_STRING;

int opal_initialized = 0;
bool opal_init_called = false;
int opal_util_initialized = 0;
/* We have to put a guess in here in case hwloc is not available.  If
   hwloc is available, this value will be overwritten when the
   hwloc data is loaded. */
int opal_cache_line_size = 128;
bool opal_warn_on_fork = true;

static int
opal_err2str(int errnum, const char **errmsg)
{
    const char *retval;

    switch (errnum) {
    case OPAL_SUCCESS:
        retval = "Success";
        break;
    case OPAL_ERROR:
        retval = "Error";
        break;
    case OPAL_ERR_OUT_OF_RESOURCE:
        retval = "Out of resource";
        break;
    case OPAL_ERR_TEMP_OUT_OF_RESOURCE:
        retval = "Temporarily out of resource";
        break;
    case OPAL_ERR_RESOURCE_BUSY:
        retval = "Resource busy";
        break;
    case OPAL_ERR_BAD_PARAM:
        retval = "Bad parameter";
        break;
    case OPAL_ERR_FATAL:
        retval = "Fatal";
        break;
    case OPAL_ERR_NOT_IMPLEMENTED:
        retval = "Not implemented";
        break;
    case OPAL_ERR_NOT_SUPPORTED:
        retval = "Not supported";
        break;
    case OPAL_ERR_INTERRUPTED:
        retval = "Interrupted";
        break;
    case OPAL_ERR_WOULD_BLOCK:
        retval = "Would block";
        break;
    case OPAL_ERR_IN_ERRNO:
        retval = "In errno";
        break;
    case OPAL_ERR_UNREACH:
        retval = "Unreachable";
        break;
    case OPAL_ERR_NOT_FOUND:
        retval = "Not found";
        break;
    case OPAL_EXISTS:
        retval = "Exists";
        break;
    case OPAL_ERR_TIMEOUT:
        retval = "Timeout";
        break;
    case OPAL_ERR_NOT_AVAILABLE:
        retval = "Not available";
        break;
    case OPAL_ERR_PERM:
        retval = "No permission";
        break;
    case OPAL_ERR_VALUE_OUT_OF_BOUNDS:
        retval = "Value out of bounds";
        break;
    case OPAL_ERR_FILE_READ_FAILURE:
        retval = "File read failure";
        break;
    case OPAL_ERR_FILE_WRITE_FAILURE:
        retval = "File write failure";
        break;
    case OPAL_ERR_FILE_OPEN_FAILURE:
        retval = "File open failure";
        break;
    case OPAL_ERR_PACK_MISMATCH:
        retval = "Pack data mismatch";
        break;
    case OPAL_ERR_PACK_FAILURE:
        retval = "Data pack failed";
        break;
    case OPAL_ERR_UNPACK_FAILURE:
        retval = "Data unpack failed";
        break;
    case OPAL_ERR_UNPACK_INADEQUATE_SPACE:
        retval = "Data unpack had inadequate space";
        break;
    case OPAL_ERR_UNPACK_READ_PAST_END_OF_BUFFER:
        retval = "Data unpack would read past end of buffer";
        break;
    case OPAL_ERR_OPERATION_UNSUPPORTED:
        retval = "Requested operation is not supported on referenced data type";
        break;
    case OPAL_ERR_UNKNOWN_DATA_TYPE:
        retval = "Unknown data type";
        break;
    case OPAL_ERR_BUFFER:
        retval = "Buffer type (described vs non-described) mismatch - operation not allowed";
        break;
    case OPAL_ERR_DATA_TYPE_REDEF:
        retval = "Attempt to redefine an existing data type";
        break;
    case OPAL_ERR_DATA_OVERWRITE_ATTEMPT:
        retval = "Attempt to overwrite a data value";
        break;
    case OPAL_ERR_MODULE_NOT_FOUND:
        retval = "Framework requires at least one active module, but none found";
        break;
    case OPAL_ERR_TOPO_SLOT_LIST_NOT_SUPPORTED:
        retval = "OS topology does not support slot_list process affinity";
        break;
    case OPAL_ERR_TOPO_SOCKET_NOT_SUPPORTED:
        retval = "Could not obtain socket topology information";
        break;
    case OPAL_ERR_TOPO_CORE_NOT_SUPPORTED:
        retval = "Could not obtain core topology information";
        break;
    case OPAL_ERR_NOT_ENOUGH_SOCKETS:
        retval = "Not enough sockets to meet request";
        break;
    case OPAL_ERR_NOT_ENOUGH_CORES:
        retval = "Not enough cores to meet request";
        break;
    case OPAL_ERR_INVALID_PHYS_CPU:
        retval = "Invalid physical cpu number returned";
        break;
    case OPAL_ERR_MULTIPLE_AFFINITIES:
        retval = "Multiple methods for assigning process affinity were specified";
        break;
    case OPAL_ERR_SLOT_LIST_RANGE:
        retval = "Provided slot_list range is invalid";
        break;
    case OPAL_ERR_NETWORK_NOT_PARSEABLE:
        retval = "Provided network specification is not parseable";
        break;
    case OPAL_ERR_SILENT:
        retval = NULL;
        break;
    case OPAL_ERR_NOT_INITIALIZED:
        retval = "Not initialized";
        break;
    case OPAL_ERR_NOT_BOUND:
        retval = "Not bound";
        break;
    case OPAL_ERR_TAKE_NEXT_OPTION:
        retval = "Take next option";
        break;
    case OPAL_ERR_PROC_ENTRY_NOT_FOUND:
        retval = "Database entry not found";
        break;
    case OPAL_ERR_DATA_VALUE_NOT_FOUND:
        retval = "Data for specified key not found";
        break;
    case OPAL_ERR_CONNECTION_FAILED:
        retval = "Connection failed";
        break;
    case OPAL_ERR_AUTHENTICATION_FAILED:
        retval = "Authentication failed";
        break;
    case OPAL_ERR_COMM_FAILURE:
        retval = "Comm failure";
        break;
    case OPAL_ERR_SERVER_NOT_AVAIL:
        retval = "Server not available";
        break;
    case OPAL_ERR_IN_PROCESS:
        retval = "Operation in process";
        break;
    case OPAL_ERR_DEBUGGER_RELEASE:
        retval = "Release debugger";
        break;
    case OPAL_ERR_HANDLERS_COMPLETE:
        retval = "Event handlers complete";
        break;
    case OPAL_ERR_PARTIAL_SUCCESS:
        retval = "Partial success";
        break;
    case OPAL_ERR_PROC_ABORTED:
        retval = "Process abnormally terminated";
        break;
    case OPAL_ERR_PROC_REQUESTED_ABORT:
        retval = "Process requested abort";
        break;
    case OPAL_ERR_PROC_ABORTING:
        retval = "Process is aborting";
        break;
    case OPAL_ERR_NODE_DOWN:
        retval = "Node has gone down";
        break;
    case OPAL_ERR_NODE_OFFLINE:
        retval = "Node has gone offline";
        break;
    case OPAL_ERR_JOB_TERMINATED:
        retval = "Job terminated";
        break;
    case OPAL_ERR_PROC_RESTART:
        retval = "Process restarted";
        break;
    case OPAL_ERR_PROC_CHECKPOINT:
        retval = "Process checkpoint";
        break;
    case OPAL_ERR_PROC_MIGRATE:
        retval = "Process migrate";
        break;
    case OPAL_ERR_EVENT_REGISTRATION:
        retval = "Event registration";
        break;
    case OPAL_ERR_HEARTBEAT_ALERT:
        retval = "Heartbeat not received";
        break;
    case OPAL_ERR_FILE_ALERT:
        retval = "File alert - proc may have stalled";
        break;
    case OPAL_ERR_MODEL_DECLARED:
        retval = "Model declared";
        break;
    case OPAL_PMIX_LAUNCH_DIRECTIVE:
        retval = "Launch directive";
        break;

    default:
        retval = "UNRECOGNIZED";
    }

    *errmsg = retval;
    return OPAL_SUCCESS;
}


int opal_init_psm(void)
{
    /* Very early in the init sequence -- before *ANY* MCA components
       are opened -- we need to disable some behavior from the PSM and
       PSM2 libraries (by default): at least some old versions of
       these libraries hijack signal handlers during their library
       constructors and then do not un-hijack them when the libraries
       are unloaded.

       It is a bit of an abstraction break that we have to put
       vendor/transport-specific code in the OPAL core, but we're
       out of options, unfortunately.

       NOTE: We only disable this behavior if the corresponding
       environment variables are not already set (i.e., if the
       user/environment has indicated a preference for this behavior,
       we won't override it). */
    if (NULL == getenv("IPATH_NO_BACKTRACE")) {
        opal_setenv("IPATH_NO_BACKTRACE", "1", true, &environ);
    }
    if (NULL == getenv("HFI_NO_BACKTRACE")) {
        opal_setenv("HFI_NO_BACKTRACE", "1", true, &environ);
    }

    return OPAL_SUCCESS;
}


int
opal_init_util(int* pargc, char*** pargv)
{
    int ret;
    char *error = NULL;
    char hostname[OPAL_MAXHOSTNAMELEN];
    OPAL_TIMING_ENV_INIT(otmng);

    if( ++opal_util_initialized != 1 ) {
        if( opal_util_initialized < 1 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    opal_thread_set_main();

    opal_init_called = true;

    /* set the nodename right away so anyone who needs it has it. Note
     * that we don't bother with fqdn and prefix issues here - we let
     * the RTE later replace this with a modified name if the user
     * requests it */
    gethostname(hostname, sizeof(hostname));
    opal_process_info.nodename = strdup(hostname);

    /* initialize the memory allocator */
    opal_malloc_init();

    OPAL_TIMING_ENV_NEXT(otmng, "opal_malloc_init");

    /* initialize the output system */
    opal_output_init();

    /* initialize install dirs code */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_installdirs_base_framework, 0))) {
        fprintf(stderr, "opal_installdirs_base_open() failed -- process will likely abort (%s:%d, returned %d instead of OPAL_SUCCESS)\n",
                __FILE__, __LINE__, ret);
        return ret;
    }

    /* initialize the help system */
    opal_show_help_init();

    OPAL_TIMING_ENV_NEXT(otmng, "opal_show_help_init");

    /* register handler for errnum -> string converstion */
    if (OPAL_SUCCESS !=
        (ret = opal_error_register("OPAL",
                                   OPAL_ERR_BASE, OPAL_ERR_MAX, opal_err2str))) {
        error = "opal_error_register";
        goto return_error;
    }

    /* keyval lex-based parser */
    if (OPAL_SUCCESS != (ret = opal_util_keyval_parse_init())) {
        error = "opal_util_keyval_parse_init";
        goto return_error;
    }

    // Disable PSM signal hijacking (see comment in function for more
    // details)
    opal_init_psm();

    OPAL_TIMING_ENV_NEXT(otmng, "opal_init_psm");

    /* Setup the parameter system */
    if (OPAL_SUCCESS != (ret = mca_base_var_init())) {
        error = "mca_base_var_init";
        goto return_error;
    }
    OPAL_TIMING_ENV_NEXT(otmng, "opal_var_init");

    /* read any param files that were provided */
    if (OPAL_SUCCESS != (ret = mca_base_var_cache_files(false))) {
        error = "failed to cache files";
        goto return_error;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "opal_var_cache");


    /* register params for opal */
    if (OPAL_SUCCESS != (ret = opal_register_params())) {
        error = "opal_register_params";
        goto return_error;
    }

    if (OPAL_SUCCESS != (ret = opal_net_init())) {
        error = "opal_net_init";
        goto return_error;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "opal_net_init");

    /* pretty-print stack handlers */
    if (OPAL_SUCCESS != (ret = opal_util_register_stackhandlers())) {
        error = "opal_util_register_stackhandlers";
        goto return_error;
    }

    /* set system resource limits - internally protected against
     * doing so twice in cases where the launch agent did it for us
     */
    if (OPAL_SUCCESS != (ret = opal_util_init_sys_limits(&error))) {
        opal_show_help("help-opal-runtime.txt",
                        "opal_init:syslimit", false,
                        error);
        return OPAL_ERR_SILENT;
    }

    /* initialize the arch string */
    if (OPAL_SUCCESS != (ret = opal_arch_init ())) {
        error = "opal_arch_init";
        goto return_error;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "opal_arch_init");

    /* initialize the datatype engine */
    if (OPAL_SUCCESS != (ret = opal_datatype_init ())) {
        error = "opal_datatype_init";
        goto return_error;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "opal_datatype_init");

    /* Initialize the data storage service. */
    if (OPAL_SUCCESS != (ret = opal_dss_open())) {
        error = "opal_dss_open";
        goto return_error;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "opal_dss_open");

    /* initialize the mca */
    if (OPAL_SUCCESS != (ret = mca_base_open())) {
        error = "mca_base_open";
        goto return_error;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "mca_base_open");

    /* initialize if framework */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_if_base_framework, 0))) {
        fprintf(stderr, "opal_if_base_open() failed -- process will likely abort (%s:%d, returned %d instead of OPAL_SUCCESS)\n",
                __FILE__, __LINE__, ret);
        return ret;
    }

    OPAL_TIMING_ENV_NEXT(otmng, "opal_if_init");

    return OPAL_SUCCESS;

 return_error:
    if (OPAL_ERR_SILENT != ret) {
        opal_show_help( "help-opal-runtime.txt",
                        "opal_init:startup:internal-failure", true,
                        error, ret );
    }
    return ret;
}


int
opal_init(int* pargc, char*** pargv)
{
    int ret;
    char *error = NULL;

    if( ++opal_initialized != 1 ) {
        if( opal_initialized < 1 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    /* initialize util code */
    if (OPAL_SUCCESS != (ret = opal_init_util(pargc, pargv))) {
        return ret;
    }

    /* open hwloc - since this is a static framework, no
     * select is required
     */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_hwloc_base_framework, 0))) {
        error = "opal_hwloc_base_open";
        goto return_error;
    }

    /* the memcpy component should be one of the first who get
     * loaded in order to make sure we have all the available
     * versions of memcpy correctly configured.
     */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_memcpy_base_framework, 0))) {
        error = "opal_memcpy_base_open";
        goto return_error;
    }

    /* initialize the memory manager / tracker */
    if (OPAL_SUCCESS != (ret = opal_mem_hooks_init())) {
        error = "opal_mem_hooks_init";
        goto return_error;
    }

    /* initialize the memory checker, to allow early support for annotation */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_memchecker_base_framework, 0))) {
        error = "opal_memchecker_base_open";
        goto return_error;
    }

    /* select the memory checker */
    if (OPAL_SUCCESS != (ret = opal_memchecker_base_select())) {
        error = "opal_memchecker_base_select";
        goto return_error;
    }

    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_backtrace_base_framework, 0))) {
        error = "opal_backtrace_base_open";
        goto return_error;
    }

    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_timer_base_framework, 0))) {
        error = "opal_timer_base_open";
        goto return_error;
    }

    /*
     * Need to start the event and progress engines if none else is.
     * opal_cr_init uses the progress engine, so it is lumped together
     * into this set as well.
     */
    /*
     * Initialize the event library
     */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_event_base_framework, 0))) {
        error = "opal_event_base_open";
        goto return_error;
    }

    /*
     * Initialize the general progress engine
     */
    if (OPAL_SUCCESS != (ret = opal_progress_init())) {
        error = "opal_progress_init";
        goto return_error;
    }
    /* we want to tick the event library whenever possible */
    opal_progress_event_users_increment();

    /* setup the shmem framework */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_shmem_base_framework, 0))) {
        error = "opal_shmem_base_open";
        goto return_error;
    }

    if (OPAL_SUCCESS != (ret = opal_shmem_base_select())) {
        error = "opal_shmem_base_select";
        goto return_error;
    }

    /* Load reachable framework */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_reachable_base_framework, 0))){
        error = "opal_reachable_base_framework";
        goto return_error;
    }
    if (OPAL_SUCCESS != (ret = opal_reachable_base_select())) {
        error = "opal_reachable_base_select";
        goto return_error;
    }

#if OPAL_ENABLE_FT_CR    == 1
    /*
     * Initialize the compression framework
     * Note: Currently only used in C/R so it has been marked to only
     *       initialize when C/R is enabled. If other places in the code
     *       wish to use this framework, it is safe to remove the protection.
     */
    if( OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_compress_base_framework, 0)) ) {
        error = "opal_compress_base_open";
        goto return_error;
    }
    if( OPAL_SUCCESS != (ret = opal_compress_base_select()) ) {
        error = "opal_compress_base_select";
        goto return_error;
    }
#endif

    /*
     * Initalize the checkpoint/restart functionality
     * Note: Always do this so we can detect if the user
     * attempts to checkpoint a non checkpointable job,
     * otherwise the tools may hang or not clean up properly.
     */
    if (OPAL_SUCCESS != (ret = opal_cr_init() ) ) {
        error = "opal_cr_init";
        goto return_error;
    }

    return OPAL_SUCCESS;

 return_error:
    opal_show_help( "help-opal-runtime.txt",
                    "opal_init:startup:internal-failure", true,
                    error, ret );
    return ret;
}

int opal_init_test(void)
{
    int ret;
    char *error;

    /* initialize the memory allocator */
    opal_malloc_init();

    /* initialize the output system */
    opal_output_init();

    /* initialize install dirs code */
    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_installdirs_base_framework, 0))) {
        fprintf(stderr, "opal_installdirs_base_open() failed -- process will likely abort (%s:%d, returned %d instead of OPAL_SUCCESS)\n",
                __FILE__, __LINE__, ret);
        return ret;
    }

    /* initialize the help system */
    opal_show_help_init();

    /* register handler for errnum -> string converstion */
    if (OPAL_SUCCESS !=
        (ret = opal_error_register("OPAL",
                                   OPAL_ERR_BASE, OPAL_ERR_MAX, opal_err2str))) {
        error = "opal_error_register";
        goto return_error;
    }

    /* keyval lex-based parser */
    if (OPAL_SUCCESS != (ret = opal_util_keyval_parse_init())) {
        error = "opal_util_keyval_parse_init";
        goto return_error;
    }

    if (OPAL_SUCCESS != (ret = opal_net_init())) {
        error = "opal_net_init";
        goto return_error;
    }

    /* Setup the parameter system */
    if (OPAL_SUCCESS != (ret = mca_base_var_init())) {
        error = "mca_base_var_init";
        goto return_error;
    }

    /* register params for opal */
    if (OPAL_SUCCESS != (ret = opal_register_params())) {
        error = "opal_register_params";
        goto return_error;
    }

    /* pretty-print stack handlers */
    if (OPAL_SUCCESS != (ret = opal_util_register_stackhandlers())) {
        error = "opal_util_register_stackhandlers";
        goto return_error;
    }

    /* Initialize the data storage service. */
    if (OPAL_SUCCESS != (ret = opal_dss_open())) {
        error = "opal_dss_open";
        goto return_error;
    }

    /* initialize the mca */
    if (OPAL_SUCCESS != (ret = mca_base_open())) {
        error = "mca_base_open";
        goto return_error;
    }

    if (OPAL_SUCCESS != (ret = mca_base_framework_open(&opal_event_base_framework, 0))) {
        error = "opal_event_base_open";
        goto return_error;
    }

    return OPAL_SUCCESS;

 return_error:
    opal_show_help( "help-opal-runtime.txt",
                    "opal_init:startup:internal-failure", true,
                    error, ret );
    return ret;
}

static bool fork_warning_issued = false;
static bool atfork_called = false;

static void warn_fork_cb(void)
{
    if (opal_initialized && !fork_warning_issued) {
        opal_show_help("help-opal-runtime.txt", "opal_init:warn-fork", true,
                       OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), getpid());
        fork_warning_issued = true;
    }
}

void opal_warn_fork(void)
{
    if (opal_warn_on_fork && !atfork_called) {
        pthread_atfork(warn_fork_cb, NULL, NULL);
        atfork_called = true;
    }
}
