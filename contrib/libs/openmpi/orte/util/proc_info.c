/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009-2016 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <ctype.h>

#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_var.h"
#include "opal/util/argv.h"
#include "opal/util/if.h"
#include "opal/util/net.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"

#include "orte/util/attr.h"

#include "orte/util/proc_info.h"

/* provide a connection to a reqd variable */
extern bool orte_keep_fqdn_hostnames;

#define ORTE_NAME_INVALID {ORTE_JOBID_INVALID, ORTE_VPID_INVALID}

ORTE_DECLSPEC orte_proc_info_t orte_process_info = {
    .my_name =                         ORTE_NAME_INVALID,
    .my_daemon =                       ORTE_NAME_INVALID,
    .my_daemon_uri =                   NULL,
    .my_hnp =                          ORTE_NAME_INVALID,
    .my_hnp_uri =                      NULL,
    .my_parent =                       ORTE_NAME_INVALID,
    .hnp_pid =                         0,
    .app_num =                         0,
    .num_procs =                       1,
    .max_procs =                       1,
    .num_daemons =                     1,
    .num_nodes =                       1,
    .nodename =                        NULL,
    .aliases =                         NULL,
    .pid =                             0,
    .proc_type =                       ORTE_PROC_TYPE_NONE,
    .my_port =                         0,
    .num_restarts =                    0,
    .my_node_rank =                    ORTE_NODE_RANK_INVALID,
    .my_local_rank =                   ORTE_LOCAL_RANK_INVALID,
    .num_local_peers =                 0,
    .tmpdir_base =                     NULL,
    .top_session_dir =                 NULL,
    .jobfam_session_dir =              NULL,
    .job_session_dir =                 NULL,
    .proc_session_dir =                NULL,
    .sock_stdin =                      NULL,
    .sock_stdout =                     NULL,
    .sock_stderr =                     NULL,
    .cpuset =                          NULL,
    .app_rank =                        -1,
    .my_hostid =                       ORTE_VPID_INVALID
};

static bool init=false;
static int orte_ess_node_rank;
static char *orte_strip_prefix;

int orte_proc_info(void)
{

    int idx, i;
    char *ptr;
    char hostname[OPAL_MAXHOSTNAMELEN];
    char **prefixes;
    bool match;

    if (init) {
        return ORTE_SUCCESS;
    }

    init = true;

    OBJ_CONSTRUCT(&orte_process_info.super, opal_proc_t);

    orte_process_info.my_hnp_uri = NULL;
    mca_base_var_register ("orte", "orte", NULL, "hnp_uri",
                           "HNP contact info",
                           MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                           MCA_BASE_VAR_FLAG_INTERNAL,
                           OPAL_INFO_LVL_9,
                           MCA_BASE_VAR_SCOPE_READONLY,
                           &orte_process_info.my_hnp_uri);

    if (NULL != orte_process_info.my_hnp_uri) {
        ptr = orte_process_info.my_hnp_uri;
        /* the uri value passed to us will have quote marks around it to protect
        * the value if passed on the command line. We must remove those
        * to have a correct uri string
        */
        if ('"' == ptr[0]) {
            /* if the first char is a quote, then so will the last one be */
            ptr[strlen(ptr)-1] = '\0';
            memmove (ptr, ptr + 1, strlen (ptr));
        }
    }

    orte_process_info.my_daemon_uri = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "local_daemon_uri",
                                  "Daemon contact info",
                                  MCA_BASE_VAR_TYPE_STRING, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_process_info.my_daemon_uri);

    if (NULL != orte_process_info.my_daemon_uri) {
        ptr = orte_process_info.my_daemon_uri;
        /* the uri value passed to us may have quote marks around it to protect
         * the value if passed on the command line. We must remove those
         * to have a correct uri string
         */
        if ('"' == ptr[0]) {
            /* if the first char is a quote, then so will the last one be */
            ptr[strlen(ptr)-1] = '\0';
            memmove (ptr, ptr + 1, strlen (ptr) - 1);
        }
    }

    orte_process_info.app_num = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "app_num",
                                  "Index of the app_context that defines this proc",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_process_info.app_num);

    /* get the process id */
    orte_process_info.pid = getpid();

    /* get the nodename */
    gethostname(hostname, sizeof(hostname));
    /* add this to our list of aliases */
    opal_argv_append_nosize(&orte_process_info.aliases, hostname);

    // Strip off the FQDN if present, ignore IP addresses
    if( !orte_keep_fqdn_hostnames && !opal_net_isaddr(hostname) ) {
        if (NULL != (ptr = strchr(hostname, '.'))) {
            *ptr = '\0';
            /* add this to our list of aliases */
            opal_argv_append_nosize(&orte_process_info.aliases, hostname);
        }
    }

    orte_strip_prefix = NULL;
    (void) mca_base_var_register ("orte", "orte", NULL, "strip_prefix",
                  "Prefix(es) to match when deciding whether to strip leading characters and zeroes from "
                  "node names returned by daemons", MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                  OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
                  &orte_strip_prefix);

    /* we have to strip node names here, if user directs, to ensure that
     * the names exchanged in the modex match the names found locally
     */
    if (NULL != orte_strip_prefix) {
        prefixes = opal_argv_split(orte_strip_prefix, ',');
        match = false;
        for (i=0; NULL != prefixes[i]; i++) {
            if (0 == strncmp(hostname, prefixes[i], strlen(prefixes[i]))) {
                /* remove the prefix and leading zeroes */
                idx = strlen(prefixes[i]);
                while (idx < (int)strlen(hostname) &&
                       (hostname[idx] <= '0' || '9' < hostname[idx])) {
                    idx++;
                }
                if ((int)strlen(hostname) <= idx) {
                    /* there were no non-zero numbers in the name */
                    orte_process_info.nodename = strdup(&hostname[strlen(prefixes[i])]);
                } else {
                    orte_process_info.nodename = strdup(&hostname[idx]);
                }
                /* add this to our list of aliases */
                opal_argv_append_nosize(&orte_process_info.aliases, orte_process_info.nodename);
                match = true;
                break;
            }
        }
        /* if we didn't find a match, then just use the hostname as-is */
        if (!match) {
            orte_process_info.nodename = strdup(hostname);
        }
        opal_argv_free(prefixes);
    } else {
        orte_process_info.nodename = strdup(hostname);
    }

    /* add "localhost" to our list of aliases */
    opal_argv_append_nosize(&orte_process_info.aliases, "localhost");

    /* get the number of nodes in the job */
    orte_process_info.num_nodes = 1;
    (void) mca_base_var_register ("orte", "orte", NULL, "num_nodes",
                                  "Number of nodes in the job",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_process_info.num_nodes);

    /* get the number of times this proc has restarted */
    orte_process_info.num_restarts = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "num_restarts",
                                  "Number of times this proc has restarted",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_process_info.num_restarts);

    orte_process_info.app_rank = 0;
    (void) mca_base_var_register ("orte", "orte", NULL, "app_rank",
                                  "Rank of this proc within its app_context",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_READONLY,
                                  &orte_process_info.app_rank);

    /* get my node rank in case we are using static ports - this won't
     * be present for daemons, so don't error out if we don't have it
     */
    orte_ess_node_rank = ORTE_NODE_RANK_INVALID;
    (void) mca_base_var_register ("orte", "orte", NULL, "ess_node_rank", "Process node rank",
                                  MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                  MCA_BASE_VAR_FLAG_INTERNAL,
                                  OPAL_INFO_LVL_9,
                                  MCA_BASE_VAR_SCOPE_CONSTANT,
                                  &orte_ess_node_rank);
    orte_process_info.my_node_rank = (orte_node_rank_t) orte_ess_node_rank;

    return ORTE_SUCCESS;
}


int orte_proc_info_finalize(void)
{
    if (!init) {
        return ORTE_SUCCESS;
    }

    if (NULL != orte_process_info.tmpdir_base) {
        free(orte_process_info.tmpdir_base);
        orte_process_info.tmpdir_base = NULL;
    }

    if (NULL != orte_process_info.top_session_dir) {
        free(orte_process_info.top_session_dir);
        orte_process_info.top_session_dir = NULL;
    }

    if (NULL != orte_process_info.jobfam_session_dir) {
        free(orte_process_info.jobfam_session_dir);
        orte_process_info.jobfam_session_dir = NULL;
    }

    if (NULL != orte_process_info.job_session_dir) {
        free(orte_process_info.job_session_dir);
        orte_process_info.job_session_dir = NULL;
    }

    if (NULL != orte_process_info.proc_session_dir) {
        free(orte_process_info.proc_session_dir);
        orte_process_info.proc_session_dir = NULL;
    }

    if (NULL != orte_process_info.nodename) {
        free(orte_process_info.nodename);
        orte_process_info.nodename = NULL;
    }

    if (NULL != orte_process_info.cpuset) {
        free(orte_process_info.cpuset);
        orte_process_info.cpuset = NULL;
    }

    if (NULL != orte_process_info.sock_stdin) {
        free(orte_process_info.sock_stdin);
        orte_process_info.sock_stdin = NULL;
    }

    if (NULL != orte_process_info.sock_stdout) {
        free(orte_process_info.sock_stdout);
        orte_process_info.sock_stdout = NULL;
    }

    if (NULL != orte_process_info.sock_stderr) {
        free(orte_process_info.sock_stderr);
        orte_process_info.sock_stderr = NULL;
    }

    orte_process_info.proc_type = ORTE_PROC_TYPE_NONE;

    opal_argv_free(orte_process_info.aliases);

    init = false;
    return ORTE_SUCCESS;
}

bool orte_ifislocal(const char *hostname)
{
    int i;

    /* see if it matches any of our known aliases */
    if (NULL != orte_process_info.aliases) {
        for (i=0; NULL != orte_process_info.aliases[i]; i++) {
            if (0 == strcmp(hostname, orte_process_info.aliases[i])) {
                return true;
            }
        }
    }

    /* okay, have to resolve the address - the opal_ifislocal
     * function will not attempt to resolve the address if
     * told not to do so */
    if (opal_ifislocal(hostname)) {
        /* add this to our known aliases */
        opal_argv_append_nosize(&orte_process_info.aliases, hostname);
        return true;
    }

    /* not me */
    return false;
}
