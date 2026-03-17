/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>
#include <time.h>

#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/runtime/params.h"
#include "ompi/mca/rte/rte.h"

#include "opal/mca/pmix/base/base.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_params.h"
/*
 * Global variables
 *
 * As a deviation from the norm, ompi_mpi_param_check is also
 * extern'ed in src/mpi/interface/c/bindings.h because it is already
 * included in all MPI function imlementation files
 *
 * The values below are the default values.
 */
bool ompi_mpi_param_check = true;
bool ompi_debug_show_handle_leaks = false;
int ompi_debug_show_mpi_alloc_mem_leaks = 0;
bool ompi_debug_no_free_handles = false;
bool ompi_mpi_show_mca_params = false;
char *ompi_mpi_show_mca_params_file = NULL;
bool ompi_mpi_keep_fqdn_hostnames = false;
bool ompi_have_sparse_group_storage = OPAL_INT_TO_BOOL(OMPI_GROUP_SPARSE);
bool ompi_use_sparse_group_storage = OPAL_INT_TO_BOOL(OMPI_GROUP_SPARSE);

bool ompi_mpi_yield_when_idle = false;
int ompi_mpi_event_tick_rate = -1;
char *ompi_mpi_show_mca_params_string = NULL;
bool ompi_mpi_have_sparse_group_storage = !!(OMPI_GROUP_SPARSE);
bool ompi_mpi_preconnect_mpi = false;

bool ompi_async_mpi_init = false;
bool ompi_async_mpi_finalize = false;

#define OMPI_ADD_PROCS_CUTOFF_DEFAULT 0
uint32_t ompi_add_procs_cutoff = OMPI_ADD_PROCS_CUTOFF_DEFAULT;
bool ompi_mpi_dynamics_enabled = true;

char *ompi_mpi_spc_attach_string = NULL;
bool ompi_mpi_spc_dump_enabled = false;

static bool show_default_mca_params = false;
static bool show_file_mca_params = false;
static bool show_enviro_mca_params = false;
static bool show_override_mca_params = false;

int ompi_mpi_register_params(void)
{
    int value;

    /* Whether we want MPI API function parameter checking or not. Disable this by default if
       parameter checking is compiled out. */

    ompi_mpi_param_check = !!(MPI_PARAM_CHECK);
    (void) mca_base_var_register("ompi", "mpi", NULL, "param_check",
                                 "Whether you want MPI API parameters checked at run-time or not.  Possible values are 0 (no checking) and 1 (perform checking at run-time)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_param_check);
    if (ompi_mpi_param_check && !MPI_PARAM_CHECK) {
        opal_show_help("help-mpi-runtime.txt",
                       "mpi-param-check-enabled-but-compiled-out",
                       true);
        ompi_mpi_param_check = false;
    }

    /*
     * opal_progress: decide whether to yield and the event library
     * tick rate
     */
    /* JMS: Need ORTE data here -- set this to 0 when
       exactly/under-subscribed, or 1 when oversubscribed */
    ompi_mpi_yield_when_idle = false;
    (void) mca_base_var_register("ompi", "mpi", NULL, "yield_when_idle",
                                 "Yield the processor when waiting for MPI communication (for MPI processes, will default to 1 when oversubscribing nodes)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_yield_when_idle);

    ompi_mpi_event_tick_rate = -1;
    (void) mca_base_var_register("ompi", "mpi", NULL, "event_tick_rate",
                                 "How often to progress TCP communications (0 = never, otherwise specified in microseconds)",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_event_tick_rate);

    /* Whether or not to show MPI handle leaks */
    ompi_debug_show_handle_leaks = false;
    (void) mca_base_var_register("ompi", "mpi", NULL, "show_handle_leaks",
                                 "Whether MPI_FINALIZE shows all MPI handles that were not freed or not",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_debug_show_handle_leaks);

    /* Whether or not to free MPI handles.  Useless without run-time
       param checking, so implicitly set that to true if we don't want
       to free the handles. */
    ompi_debug_no_free_handles = false;
    (void) mca_base_var_register("ompi", "mpi", NULL, "no_free_handles",
                                 "Whether to actually free MPI objects when their handles are freed",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_debug_no_free_handles);
    if (ompi_debug_no_free_handles) {
        ompi_mpi_param_check = true;
        if (!MPI_PARAM_CHECK) {
            opal_output(0, "WARNING: MCA parameter mpi_no_free_handles set to true, but MPI");
            opal_output(0, "WARNING: parameter checking has been compiled out of Open MPI.");
            opal_output(0, "WARNING: mpi_no_free_handles is therefore only partially effective!");
        }
    }

    /* Whether or not to show MPI_ALLOC_MEM leaks */
    ompi_debug_show_mpi_alloc_mem_leaks = 0;
    (void) mca_base_var_register("ompi", "mpi", NULL, "show_mpi_alloc_mem_leaks",
                                 "If >0, MPI_FINALIZE will show up to this many instances of memory allocated by MPI_ALLOC_MEM that was not freed by MPI_FREE_MEM",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_debug_show_mpi_alloc_mem_leaks);

    /* Whether or not to print all MCA parameters in MPI_INIT */
    ompi_mpi_show_mca_params_string = NULL;
    (void) mca_base_var_register("ompi", "mpi", NULL, "show_mca_params",
                                 "Whether to show all MCA parameter values during MPI_INIT or not (good for reproducability of MPI jobs "
                                 "for debug purposes). Accepted values are all, default, file, api, and enviro - or a comma "
                                 "delimited combination of them",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_show_mca_params_string);
    if (NULL != ompi_mpi_show_mca_params_string) {
        char **args;
        int i;

        ompi_mpi_show_mca_params = true;
        args = opal_argv_split(ompi_mpi_show_mca_params_string, ',');
        if (NULL == args) {
            opal_output(0, "WARNING: could not parse mpi_show_mca_params request - defaulting to show \"all\"");
            show_default_mca_params = true;
            show_file_mca_params = true;
            show_enviro_mca_params = true;
            show_override_mca_params = true;
        } else {
            for (i=0; NULL != args[i]; i++) {
                if (0 == strcasecmp(args[i], "all")  || 0 == strcmp(args[i], "1")) {
                    show_default_mca_params = true;
                    show_file_mca_params = true;
                    show_enviro_mca_params = true;
                    show_override_mca_params = true;
                } else if (0 == strcasecmp(args[i], "default")) {
                    show_default_mca_params = true;
                } else if (0 == strcasecmp(args[i], "file")) {
                    show_file_mca_params = true;
                } else if (0 == strncasecmp(args[i], "env", 3)) {
                    show_enviro_mca_params = true;
                } else if (0 == strcasecmp(args[i], "api")) {
                    show_override_mca_params = true;
                }
            }
            opal_argv_free(args);
        }
    }

    /* File to use when dumping the parameters */
    (void) mca_base_var_register("ompi", "mpi", NULL, "show_mca_params_file",
                                 "If mpi_show_mca_params is true, setting this string to a valid filename tells Open MPI to dump all the MCA parameter values into a file suitable for reading via the mca_param_files parameter (good for reproducability of MPI jobs)",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_show_mca_params_file);

    /* User-level process pinning controls */

    ompi_mpi_preconnect_mpi = false;
    value = mca_base_var_register("ompi", "mpi", NULL, "preconnect_mpi",
                                  "Whether to force MPI processes to fully "
                                  "wire-up the MPI connections between MPI "
                                  "processes during "
                                  "MPI_INIT (vs. making connections lazily -- "
                                  "upon the first MPI traffic between each "
                                  "process peer pair)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &ompi_mpi_preconnect_mpi);
    mca_base_var_register_synonym(value, "ompi", "mpi", NULL, "preconnect_all",
                                  MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    /* Sparse group storage support */
    (void) mca_base_var_register("ompi", "mpi", NULL, "have_sparse_group_storage",
                                 "Whether this Open MPI installation supports storing of data in MPI groups in \"sparse\" formats (good for extremely large process count MPI jobs that create many communicators/groups)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0,
                                 MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_CONSTANT,
                                 &ompi_mpi_have_sparse_group_storage);

    ompi_use_sparse_group_storage = ompi_mpi_have_sparse_group_storage;
    (void) mca_base_var_register("ompi", "mpi", NULL, "use_sparse_group_storage",
                                 "Whether to use \"sparse\" storage formats for MPI groups (only relevant if mpi_have_sparse_group_storage is 1)",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0,
                                 ompi_mpi_have_sparse_group_storage ? 0 : MCA_BASE_VAR_FLAG_DEFAULT_ONLY,
                                 OPAL_INFO_LVL_9,
                                 ompi_mpi_have_sparse_group_storage ? MCA_BASE_VAR_SCOPE_READONLY : MCA_BASE_VAR_SCOPE_CONSTANT,
                                 &ompi_use_sparse_group_storage);
    if (ompi_use_sparse_group_storage && !ompi_mpi_have_sparse_group_storage) {
        opal_show_help("help-mpi-runtime.txt",
                       "sparse groups enabled but compiled out",
                       true);
        ompi_use_sparse_group_storage = false;
    }

    value = mca_base_var_find ("opal", "opal", NULL, "cuda_support");
    if (0 <= value) {
        mca_base_var_register_synonym(value, "ompi", "mpi", NULL, "cuda_support",
                                      MCA_BASE_VAR_SYN_FLAG_DEPRECATED);
    }

    value = mca_base_var_find ("opal", "opal", NULL, "built_with_cuda_support");
    if (0 <= value) {
        mca_base_var_register_synonym(value, "ompi", "mpi", NULL, "built_with_cuda_support", 0);
    }

    if (opal_cuda_support && !opal_built_with_cuda_support) {
        opal_show_help("help-mpi-runtime.txt", "no cuda support",
                       true);
        ompi_rte_abort(1, NULL);
    }

    ompi_add_procs_cutoff = OMPI_ADD_PROCS_CUTOFF_DEFAULT;
    (void) mca_base_var_register ("ompi", "mpi", NULL, "add_procs_cutoff",
                                  "Maximum world size for pre-allocating resources for all "
                                  "remote processes. Increasing this limit may improve "
                                  "communication performance at the cost of memory usage",
                                  MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL,
                                  0, 0, OPAL_INFO_LVL_3, MCA_BASE_VAR_SCOPE_LOCAL,
                                  &ompi_add_procs_cutoff);

    ompi_mpi_dynamics_enabled = true;
    (void) mca_base_var_register("ompi", "mpi", NULL, "dynamics_enabled",
                                 "Is the MPI dynamic process functionality enabled (e.g., MPI_COMM_SPAWN)?  Default is yes, but certain transports and/or environments may disable it.",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_4,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_dynamics_enabled);

    ompi_async_mpi_init = false;
    (void) mca_base_var_register("ompi", "async", "mpi", "init",
                                 "Do not perform a barrier at the end of MPI_Init",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_async_mpi_init);

    ompi_async_mpi_finalize = false;
    (void) mca_base_var_register("ompi", "async", "mpi", "finalize",
                                 "Do not perform a barrier at the beginning of MPI_Finalize",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_async_mpi_finalize);

    value = mca_base_var_find ("opal", "opal", NULL, "abort_delay");
    if (0 <= value) {
        (void) mca_base_var_register_synonym(value, "ompi", "mpi", NULL, "abort_delay",
                                      MCA_BASE_VAR_SYN_FLAG_DEPRECATED);
    }

    value = mca_base_var_find ("opal", "opal", NULL, "abort_print_stack");
    if (0 <= value) {
        (void) mca_base_var_register_synonym(value, "ompi", "mpi", NULL, "abort_print_stack",
                                      MCA_BASE_VAR_SYN_FLAG_DEPRECATED);
    }

    ompi_mpi_spc_attach_string = NULL;
    (void) mca_base_var_register("ompi", "mpi", NULL, "spc_attach",
                                 "A comma delimeted string listing the software-based performance counters (SPCs) to enable.",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                 OPAL_INFO_LVL_4,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_spc_attach_string);

    ompi_mpi_spc_dump_enabled = false;
    (void) mca_base_var_register("ompi", "mpi", NULL, "spc_dump_enabled",
                                 "A boolean value for whether (true) or not (false) to enable dumping SPC counters in MPI_Finalize.",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_4,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &ompi_mpi_spc_dump_enabled);

    return OMPI_SUCCESS;
}

int ompi_show_all_mca_params(int32_t rank, int requested, char *nodename) {
    const mca_base_var_t *var;
    int var_count, i, ret;
    FILE *fp = NULL;
    time_t timestamp;
    char **var_dump;

    if (rank != 0) {
        return OMPI_SUCCESS;
    }

    timestamp = time(NULL);

    /* Open the file if one is specified */
    if (NULL != ompi_mpi_show_mca_params_file &&
        0 != strlen(ompi_mpi_show_mca_params_file)) {
        if ( NULL == (fp = fopen(ompi_mpi_show_mca_params_file, "w")) ) {
            opal_output(0, "Unable to open file <%s> to write MCA parameters", ompi_mpi_show_mca_params_file);
            return OMPI_ERR_FILE_OPEN_FAILURE;
        }
        fprintf(fp, "#\n");
        fprintf(fp, "# This file was automatically generated on %s", ctime(&timestamp));
        fprintf(fp, "# by MPI_COMM_WORLD rank %d (out of a total of %d) on %s\n", rank, requested, nodename );
        fprintf(fp, "#\n");
    }

    var_count = mca_base_var_get_count ();
    for (i = 0 ; i < var_count ; ++i) {
        ret = mca_base_var_get (i, &var);
        if (OPAL_SUCCESS != ret) {
            continue;
        }

        /* If this is an internal param, don't print it */
        if (MCA_BASE_VAR_FLAG_INTERNAL & var->mbv_flags) {
            continue;
        }

        /* is this a default value and we are not displaying
         * defaults, ignore this one
         */
        if (MCA_BASE_VAR_SOURCE_DEFAULT == var->mbv_source && !show_default_mca_params) {
            continue;
        }

        /* is this a file value and we are not displaying files,
         * ignore it
         */
        if ((MCA_BASE_VAR_SOURCE_FILE == var->mbv_source ||
             MCA_BASE_VAR_SOURCE_OVERRIDE == var->mbv_source) &&
            !show_file_mca_params) {
            continue;
        }

        /* is this an enviro value and we are not displaying enviros,
         * ignore it
         */
        if (MCA_BASE_VAR_SOURCE_ENV == var->mbv_source && !show_enviro_mca_params) {
            continue;
        }

        /* is this an API value and we are not displaying APIs,
         * ignore it
         */
        if (MCA_BASE_VAR_SOURCE_OVERRIDE == var->mbv_source && !show_override_mca_params) {
            continue;
        }

        ret = mca_base_var_dump (i, &var_dump, MCA_BASE_VAR_DUMP_SIMPLE);
        if (OPAL_SUCCESS != ret) {
            continue;
        }

        /* Print the parameter */
        if (NULL != ompi_mpi_show_mca_params_file &&
            0 != strlen(ompi_mpi_show_mca_params_file)) {
            fprintf(fp, "%s\n", var_dump[0]);
        } else {
            opal_output(0, "%s\n", var_dump[0]);
        }
        free (var_dump[0]);
        free (var_dump);
    }

    /* Close file, cleanup allocated memory*/
    if (NULL != ompi_mpi_show_mca_params_file &&
        0 != strlen(ompi_mpi_show_mca_params_file)) {
        fclose(fp);
    }

    return OMPI_SUCCESS;
}
