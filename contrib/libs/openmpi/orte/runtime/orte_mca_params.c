/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009-2010 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2018 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#include <stdio.h>

#include "opal/mca/base/mca_base_var.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/util/output.h"
#include "opal/util/argv.h"

#include "orte/util/proc_info.h"
#include "orte/mca/errmgr/errmgr.h"

#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"

static bool passed_thru = false;
static int orte_progress_thread_debug_level = -1;
static char *orte_xml_file = NULL;
static char *orte_fork_agent_string = NULL;
static char *orte_tmpdir_base = NULL;
static char *orte_local_tmpdir_base = NULL;
static char *orte_remote_tmpdir_base = NULL;
static char *orte_top_session_dir = NULL;
static char *orte_jobfam_session_dir = NULL;

int orte_register_params(void)
{
    int id;
    opal_output_stream_t lds;

    /* only go thru this once - mpirun calls it twice, which causes
     * any error messages to show up twice
     */
    if (passed_thru) {
        return ORTE_SUCCESS;
    }
    passed_thru = true;

    /* get a clean output channel too - need to do this here because
     * we use it below, and orterun and some other tools call this
     * function prior to calling orte_init
     */
    OBJ_CONSTRUCT(&lds, opal_output_stream_t);
    lds.lds_want_stdout = true;
    orte_clean_output = opal_output_open(&lds);
    OBJ_DESTRUCT(&lds);

    orte_help_want_aggregate = true;
    (void) mca_base_var_register ("orte", "orte", "base", "help_aggregate",
                                  "If orte_base_help_aggregate is true, duplicate help messages will be aggregated rather than displayed individually.  This can be helpful for parallel jobs that experience multiple identical failures; rather than print out the same help/failure message N times, display it once with a count of how many processes sent the same message.",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &orte_help_want_aggregate);

    /* LOOK FOR A TMP DIRECTORY BASE */
    /* Several options are provided to cover a range of possibilities:
     *
     * (a) all processes need to use a specified location as the base
     *     for tmp directories
     * (b) daemons on remote nodes need to use a specified location, but
     *     one different from that used by mpirun
     * (c) mpirun needs to use a specified location, but one different
     *     from that used on remote nodes
     */
    orte_tmpdir_base = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "tmpdir_base",
                                  "Base of the session directory tree to be used by all processes",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &orte_tmpdir_base);

    orte_local_tmpdir_base = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "local_tmpdir_base",
                                  "Base of the session directory tree to be used by orterun/mpirun",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &orte_local_tmpdir_base);

    orte_remote_tmpdir_base = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "remote_tmpdir_base",
                                  "Base of the session directory tree on remote nodes, if required to be different from head node",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &orte_remote_tmpdir_base);

    /* if a global tmpdir was specified, then we do not allow specification
     * of the local or remote values to avoid confusion
     */
    if (NULL != orte_tmpdir_base &&
        (NULL != orte_local_tmpdir_base || NULL != orte_remote_tmpdir_base)) {
        opal_output(orte_clean_output,
                    "------------------------------------------------------------------\n"
                    "The MCA param orte_tmpdir_base was specified, which sets the base\n"
                    "of the temporary directory tree for all procs. However, values for\n"
                    "the local and/or remote tmpdir base were also given. This can lead\n"
                    "to confusion and is therefore not allowed. Please specify either a\n"
                    "global tmpdir base OR a local/remote tmpdir base value\n"
                    "------------------------------------------------------------------");
        exit(1);
    }

    if (NULL != orte_tmpdir_base) {
        if (NULL != orte_process_info.tmpdir_base) {
            free(orte_process_info.tmpdir_base);
        }
        orte_process_info.tmpdir_base = strdup (orte_tmpdir_base);
    } else if (ORTE_PROC_IS_HNP && NULL != orte_local_tmpdir_base) {
        /* orterun will pickup the value for its own use */
        if (NULL != orte_process_info.tmpdir_base) {
            free(orte_process_info.tmpdir_base);
        }
        orte_process_info.tmpdir_base = strdup (orte_local_tmpdir_base);
    } else if (ORTE_PROC_IS_DAEMON && NULL != orte_remote_tmpdir_base) {
        /* orterun will pickup the value and forward it along, but must not
         * use it in its own work. So only a daemon needs to get it, and the
         * daemon will pass it down to its application procs. Note that orterun
         * will pass -its- value to any procs local to it
         */
        if (NULL != orte_process_info.tmpdir_base) {
            free(orte_process_info.tmpdir_base);
        }
        orte_process_info.tmpdir_base = strdup (orte_remote_tmpdir_base);
    }

    orte_top_session_dir = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "top_session_dir",
                                  "Top of the session directory tree for applications",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &orte_top_session_dir);

    if (NULL != orte_top_session_dir) {
         if (NULL != orte_process_info.top_session_dir) {
            free(orte_process_info.top_session_dir);
        }
        orte_process_info.top_session_dir = strdup(orte_top_session_dir);
    }

    orte_jobfam_session_dir = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "jobfam_session_dir",
                                  "The jobfamily session directory for applications",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL_EQ,
                                  &orte_jobfam_session_dir);

    if (NULL != orte_jobfam_session_dir) {
        if (NULL != orte_process_info.jobfam_session_dir) {
            free(orte_process_info.jobfam_session_dir);
        }
        orte_process_info.jobfam_session_dir = strdup(orte_jobfam_session_dir);
    }

    orte_prohibited_session_dirs = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "no_session_dirs",
                                  "Prohibited locations for session directories (multiple locations separated by ',', default=NULL)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_prohibited_session_dirs);

    orte_create_session_dirs = true;
    (void) mca_base_var_register ("orte", "orte", NULL, "create_session_dirs",
                                  "Create session directories",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_create_session_dirs);

    orte_execute_quiet = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "execute_quiet",
                                  "Do not output error and help messages",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_execute_quiet);

    orte_report_silent_errors = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "report_silent_errors",
                                  "Report all errors, including silent ones",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_report_silent_errors);

    orte_debug_flag = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "debug",
                                  "Top-level ORTE debug switch (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_debug_flag);

    orte_debug_verbosity = -1;
    (void) mca_base_var_register ("orte", "orte", NULL, "debug_verbose",
                                  "Verbosity level for ORTE debug messages (default: 1)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_debug_verbosity);

    orte_debug_daemons_file_flag = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "debug_daemons_file",
                                  "Whether want stdout/stderr of daemons to go to a file or not",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_debug_daemons_file_flag);
    /* If --debug-daemons-file was specified, that also implies
       --debug-daemons */
    if (orte_debug_daemons_file_flag) {
        orte_debug_daemons_flag = true;

        /* value can't change */
        (void) mca_base_var_register ("orte", "orte", NULL, "debug_daemons",
                                      "Whether to debug the ORTE daemons or not",
                                      MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                      OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_CONSTANT,
                                      &orte_debug_daemons_flag);
    } else {
        orte_debug_daemons_flag = false;

        (void) mca_base_var_register ("orte", "orte", NULL, "debug_daemons",
                                      "Whether to debug the ORTE daemons or not",
                                      MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                      OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                      &orte_debug_daemons_flag);
    }

    orte_progress_thread_debug_level = -1;
    (void) mca_base_var_register ("orte", "orte", NULL, "progress_thread_debug",
                                  "Debug level for ORTE progress threads",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_progress_thread_debug_level);

    if (0 <= orte_progress_thread_debug_level) {
        orte_progress_thread_debug = opal_output_open(NULL);
        opal_output_set_verbosity(orte_progress_thread_debug,
                                  orte_progress_thread_debug_level);
    }

    /* do we want session output left open? */
    orte_leave_session_attached = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "leave_session_attached",
                                  "Whether applications and/or daemons should leave their sessions "
                                  "attached so that any output can be received - this allows X forwarding "
                                  "without all the attendant debugging output",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_leave_session_attached);

    /* if any debug level is set, ensure we output debug level dumps */
    if (orte_debug_flag || orte_debug_daemons_flag || orte_leave_session_attached) {
        orte_devel_level_output = true;
    }

    /* See comment in orte/tools/orterun/orterun.c about this MCA
       param (this param is internal) */
    orte_in_parallel_debugger = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "in_parallel_debugger",
                                  "Whether the application is being debugged "
                                  "in a parallel debugger (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_in_parallel_debugger);

    orte_debugger_dump_proctable = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "output_debugger_proctable",
                                  "Whether or not to output the debugger proctable after launch (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_debugger_dump_proctable);

    orte_debugger_test_daemon = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "debugger_test_daemon",
                                  "Name of the executable to be used to simulate a debugger colaunch (relative or absolute path)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_debugger_test_daemon);

    orte_debugger_test_attach = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "debugger_test_attach",
                                  "Test debugger colaunch after debugger attachment",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_debugger_test_attach);

    orte_debugger_check_rate = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "debugger_check_rate",
                                  "Set rate (in secs) for auto-detect of debugger attachment (0 => do not check)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_debugger_check_rate);

    orte_do_not_launch = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "do_not_launch",
                                  "Perform all necessary operations to prepare to launch the application, but do not actually launch it",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_do_not_launch);

    orted_spin_flag = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "daemon_spin",
                                  "Have any orteds spin until we can connect a debugger to them",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orted_spin_flag);

    orted_debug_failure = ORTE_VPID_INVALID;
    (void) mca_base_var_register ("orte", "orte", NULL, "daemon_fail",
                                  "Have the specified orted fail after init for debugging purposes",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orted_debug_failure);

    orted_debug_failure_delay = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "daemon_fail_delay",
                                  "Have the specified orted fail after specified number of seconds (default: 0 => no delay)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orted_debug_failure_delay);

    orte_startup_timeout = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "startup_timeout",
                                  "Seconds to wait for startup or job launch before declaring failed_to_start (default: 0 => do not check)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_startup_timeout);

    /* User-level debugger info string */
    orte_base_user_debugger = "totalview @mpirun@ -a @mpirun_args@ : ddt -n @np@ -start @executable@ @executable_argv@ @single_app@ : fxp @mpirun@ -a @mpirun_args@";
    (void) mca_base_var_register ("orte", "orte", NULL, "base_user_debugger",
                                  "Sequence of user-level debuggers to search for in orterun",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_base_user_debugger);

#if 0
    mca_base_param_reg_int_name("orte", "abort_timeout",
                                "Max time to wait [in secs] before aborting an ORTE operation (default: 1sec)",
                                false, false, 1, &value);
    orte_max_timeout = 1000000.0 * value;  /* convert to usec */

    mca_base_param_reg_int_name("orte", "timeout_step",
                                "Time to wait [in usecs/proc] before aborting an ORTE operation (default: 1000 usec/proc)",
                                false, false, 1000, &orte_timeout_usec_per_proc);
#endif

    /* default hostfile */
    orte_default_hostfile = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "default_hostfile",
                                  "Name of the default hostfile (relative or absolute path, \"none\" to ignore environmental or default MCA param setting)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_default_hostfile);

    if (NULL == orte_default_hostfile) {
        /* nothing was given, so define the default */
        asprintf(&orte_default_hostfile, "%s/openmpi-default-hostfile", opal_install_dirs.sysconfdir);
        /* flag that nothing was given */
        orte_default_hostfile_given = false;
    } else if (0 == strcmp(orte_default_hostfile, "none")) {
        free (orte_default_hostfile);
        orte_default_hostfile = NULL;
        /* flag that it was given */
        orte_default_hostfile_given = true;
    } else {
        /* flag that it was given */
        orte_default_hostfile_given = true;
    }

    /* default dash-host */
    orte_default_dash_host = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "default_dash_host",
                                  "Default -host setting (specify \"none\" to ignore environmental or default MCA param setting)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_default_dash_host);
    if (NULL != orte_default_dash_host &&
        0 == strcmp(orte_default_dash_host, "none")) {
        free(orte_default_dash_host);
        orte_default_dash_host = NULL;
    }

    /* regex of nodes in system */
    orte_node_regex = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "node_regex",
                                  "Regular expression defining nodes in the system",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_node_regex);

    /* whether or not to keep FQDN hostnames */
    orte_keep_fqdn_hostnames = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "keep_fqdn_hostnames",
                                  "Whether or not to keep FQDN hostnames [default: no]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_keep_fqdn_hostnames);

    /* whether or not to retain aliases of hostnames */
    orte_retain_aliases = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "retain_aliases",
                                  "Whether or not to keep aliases for host names [default: no]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_retain_aliases);

    orte_hostname_cutoff = 1000;
    (void) mca_base_var_register ("orte", "orte", NULL, "hostname_cutoff",
                                  "Pass hostnames to all procs when #nodes is less than cutoff [default:1000]",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_3, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_hostname_cutoff);

    /* which alias to use in MPIR_proctab */
    orte_use_hostname_alias = 1;
    (void) mca_base_var_register ("orte", "orte", NULL, "hostname_alias_index",
                                  "If hostname aliases are being retained, which one to use for the debugger proc table [default: 1st alias]",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_use_hostname_alias);

    orte_xml_output = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "xml_output",
                                  "Display all output in XML format (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_xml_output);

    /* whether to tag output */
    /* if we requested xml output, be sure to tag the output as well */
    orte_tag_output = orte_xml_output;
    (void) mca_base_var_register ("orte", "orte", NULL, "tag_output",
                                  "Tag all output with [job,rank] (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_tag_output);
    if (orte_xml_output) {
        orte_tag_output = true;
    }


    orte_xml_file = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "xml_file",
                                  "Provide all output in XML format to the specified file",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_xml_file);
    if (NULL != orte_xml_file) {
        if (ORTE_PROC_IS_HNP && NULL == orte_xml_fp) {
            /* only the HNP opens this file! Make sure it only happens once */
            orte_xml_fp = fopen(orte_xml_file, "w");
            if (NULL == orte_xml_fp) {
                opal_output(0, "Could not open specified xml output file: %s", orte_xml_file);
                return ORTE_ERROR;
            }
        }
        /* ensure we set the flags to tag output */
        orte_xml_output = true;
        orte_tag_output = true;
    } else {
        /* default to stdout */
        orte_xml_fp = stdout;
    }

    /* whether to timestamp output */
    orte_timestamp_output = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "timestamp_output",
                                  "Timestamp all application process output (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_timestamp_output);

    orte_show_resolved_nodenames = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "show_resolved_nodenames",
                                  "Display any node names that are resolved to a different name (default: false)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_show_resolved_nodenames);

    /* allow specification of the launch agent */
    orte_launch_agent = "orted";
    (void) mca_base_var_register ("orte", "orte", NULL, "launch_agent",
                                  "Command used to start processes on remote nodes (default: orted)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_launch_agent);

    orte_fork_agent_string = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "fork_agent",
                                  "Command used to fork processes on remote nodes (default: NULL)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_fork_agent_string);

    if (NULL != orte_fork_agent_string) {
        orte_fork_agent = opal_argv_split(orte_fork_agent_string, ' ');
    }

    /* whether or not to require RM allocation */
    orte_allocation_required = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "allocation_required",
                                  "Whether or not an allocation by a resource manager is required [default: no]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_allocation_required);

    /* whether or not to map stddiag to stderr */
    orte_map_stddiag_to_stderr = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "map_stddiag_to_stderr",
                                  "Map output from opal_output to stderr of the local process [default: no]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_map_stddiag_to_stderr);

    /* whether or not to map stddiag to stderr */
    orte_map_stddiag_to_stdout = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "map_stddiag_to_stdout",
                                  "Map output from opal_output to stdout of the local process [default: no]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_map_stddiag_to_stdout);
    if( orte_map_stddiag_to_stderr && orte_map_stddiag_to_stdout ) {
        opal_output(0, "The options \"orte_map_stddiag_to_stderr\" and \"orte_map_stddiag_to_stdout\" are mutually exclusive. They cannot both be set to true.");
        return ORTE_ERROR;
    }

    /* generate new terminal windows to display output from specified ranks */
    orte_xterm = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "xterm",
                                  "Create a new xterm window and display output from the specified ranks there [default: none]",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_xterm);
    if (NULL != orte_xterm) {
        /* if an xterm request is given, we have to leave any ssh
         * sessions attached so the xterm window manager can get
         * back to the controlling terminal
         */
        orte_leave_session_attached = true;
        /* also want to redirect stddiag output from opal_output
         * to stderr from the process so those messages show
         * up in the xterm window instead of being forwarded to mpirun
         */
        orte_map_stddiag_to_stderr = true;
    }

    /* whether or not to report launch progress */
    orte_report_launch_progress = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "report_launch_progress",
                                  "Output a brief periodic report on launch progress [default: no]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_report_launch_progress);

    /* cluster hardware info detected by orte only */
    orte_local_cpu_type = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "cpu_type",
                                  "cpu type detected in node",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_local_cpu_type);

    orte_local_cpu_model = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "cpu_model",
                                  "cpu model detected in node",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_local_cpu_model);

    /* tool communication controls */
    orte_report_events_uri = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "report_events",
                                  "URI to which events are to be reported (default: NULL)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_report_events_uri);
    if (NULL != orte_report_events_uri) {
        orte_report_events = true;
    }

    /* barrier control */
    orte_do_not_barrier = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "do_not_barrier",
                                  "Do not barrier in orte_init",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_do_not_barrier);

    orte_enable_recovery = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "enable_recovery",
                                  "Enable recovery from process failure [Default = disabled]",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_enable_recovery);

    orte_max_restarts = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "max_restarts",
                                  "Max number of times to restart a failed process",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_max_restarts);

    if (!orte_enable_recovery && orte_max_restarts != 0) {
        if (ORTE_PROC_IS_HNP) {
            opal_output(orte_clean_output,
                        "------------------------------------------------------------------\n"
                        "The MCA param orte_enable_recovery was not set to true, but\n"
                        "a value was provided for the number of restarts:\n\n"
                        "Max restarts: %d\n"
                        "We are enabling process recovery and continuing execution. To avoid\n"
                        "this warning in the future, please set the orte_enable_recovery\n"
                        "param to non-zero.\n"
                        "------------------------------------------------------------------",
                        orte_max_restarts);
        }
        orte_enable_recovery = true;
    }

    orte_abort_non_zero_exit = true;
    (void) mca_base_var_register ("orte", "orte", NULL, "abort_on_non_zero_status",
                                  "Abort the job if any process returns a non-zero exit status - no restart in such cases",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_abort_non_zero_exit);

    orte_allowed_exit_without_sync = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "allowed_exit_without_sync",
                                  "Process exiting without calling finalize will not trigger job termination",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_allowed_exit_without_sync);

    orte_report_child_jobs_separately = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "report_child_jobs_separately",
                                  "Return the exit status of the primary job only",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_report_child_jobs_separately);


#if 0
    /* XXX -- unused parameter */
    mca_base_param_reg_int_name("orte", "child_time_to_exit",
                                "Max time a spawned child job is allowed to run after the primary job has terminated (seconds)",
                                false, false,
                                INT_MAX, &value);
    orte_child_time_to_exit.tv_sec = value;
    orte_child_time_to_exit.tv_usec = 0;
#endif

    orte_stat_history_size = 1;
    (void) mca_base_var_register ("orte", "orte", NULL, "stat_history_size",
                                  "Number of stat samples to keep",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_stat_history_size);

    orte_no_vm = false;
    id = mca_base_var_register ("orte", "orte", NULL, "no_vm",
                                "Do not build the VM at start to detect topologies",
                                MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                &orte_no_vm);
    /* register a synonym for old name */
    mca_base_var_register_synonym (id, "orte", "state", "novm", "select", MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    orte_max_vm_size = -1;
    (void) mca_base_var_register ("orte", "orte", NULL, "max_vm_size",
                                  "Maximum size of virtual machine - used to subdivide allocation",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_max_vm_size);

    if (opal_hwloc_use_hwthreads_as_cpus) {
        orte_set_slots = "hwthreads";
    } else {
        orte_set_slots = "cores";
    }
    (void) mca_base_var_register ("orte", "orte", NULL, "set_default_slots",
                                  "Set the number of slots on nodes that lack such info to the"
                                  " number of specified objects [a number, \"cores\" (default),"
                                  " \"numas\", \"sockets\", \"hwthreads\" (default if hwthreads_as_cpus is set),"
                                  " or \"none\" to skip this option]",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_set_slots);

    /* should we display the allocation after determining it? */
    orte_display_allocation = false;
    id = mca_base_var_register ("orte", "orte", NULL, "display_alloc",
                                "Whether to display the allocation after it is determined",
                                MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                &orte_display_allocation);
    /* register a synonym for old name -- should we remove this now? */
    mca_base_var_register_synonym (id, "orte", "ras", "base", "display_alloc", MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    /* should we display a detailed (developer-quality) version of the allocation after determining it? */
    orte_devel_level_output = false;
    id = mca_base_var_register ("orte", "orte", NULL, "display_devel_alloc",
                                "Whether to display a developer-detail allocation after it is determined",
                                MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                &orte_devel_level_output);
    /* register a synonym for old name -- should we remove this now? */
    mca_base_var_register_synonym (id, "orte", "ras", "base", "display_devel_alloc", MCA_BASE_VAR_SYN_FLAG_DEPRECATED);

    if (orte_devel_level_output) {
        orte_display_allocation = true;
    }

    /* should we treat any -host directives as "soft" - i.e., desired
     * but not required
     */
    orte_soft_locations = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "soft_locations",
                                  "Treat -host directives as desired, but not required",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_soft_locations);

    /* allow specification of the cores to be used by daemons */
    orte_daemon_cores = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "daemon_cores",
                                  "Restrict the ORTE daemons (including mpirun) to operate on the specified cores (comma-separated list of ranges)",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_5, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_daemon_cores);

    /* get the conduit params */
    orte_coll_transport = "fabric,ethernet";
    (void) mca_base_var_register("orte", "orte", "coll", "transports",
                                 "Comma-separated list of transports to use for ORTE collectives",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_coll_transport);

    orte_mgmt_transport = "oob";
    (void) mca_base_var_register("orte", "orte", "mgmt", "transports",
                                 "Comma-separated list of transports to use for ORTE management messages",
                                 MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0, OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY, &orte_mgmt_transport);

    /* Amount of time to wait for a stack trace to return from the daemons */
    orte_stack_trace_wait_timeout = 30;
    (void) mca_base_var_register ("orte", "orte", NULL, "timeout_for_stack_trace",
                                  "Seconds to wait for stack traces to return before terminating "
                                  "the job (<= 0 wait forever)",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_stack_trace_wait_timeout);

    orte_fwd_mpirun_port = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "fwd_mpirun_port",
                                  "Forward the port used by mpirun so all daemons will use it",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_fwd_mpirun_port);

    /* register the URI of the UNIVERSAL data server */
    orte_data_server_uri = NULL;
    (void) mca_base_var_register ("orte", "pmix", NULL, "server_uri",
                                  "URI of a session-level keyval server for publish/lookup operations",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                  OPAL_INFO_LVL_3, MCA_BASE_VAR_SCOPE_ALL,
                                  &orte_data_server_uri);

    orte_enable_instant_on_support = false;
    (void) mca_base_var_register ("orte", "orte", NULL, "enable_instant_on_support",
                                  "Enable PMIx-based instant on launch support (experimental)",
                                  MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_enable_instant_on_support);

    return ORTE_SUCCESS;
}
