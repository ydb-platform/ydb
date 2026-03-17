/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2010 Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2007      Evergrid, Inc. All rights reserved.
 * Copyright (c) 2008-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Rutgers, The State University of New Jersey.
 *                         All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/*
 * There is a complicated sequence of events that occurs when the
 * parent forks a child process that is intended to launch the target
 * executable.
 *
 * Before the child process exec's the target executable, it might tri
 * to set the affinity of that new child process according to a
 * complex series of rules.  This binding may fail in a myriad of
 * different ways.  A lot of this code deals with reporting that error
 * occurately to the end user.  This is a complex task in itself
 * because the child process is not "really" an ORTE process -- all
 * error reporting must be proxied up to the parent who can use normal
 * ORTE error reporting mechanisms.
 *
 * Here's a high-level description of what is occurring in this file:
 *
 * - parent opens a pipe
 * - parent forks a child
 * - parent blocks reading on the pipe: the pipe will either close
 *   (indicating that the child successfully exec'ed) or the child will
 *   write some proxied error data up the pipe
 *
 * - the child tries to set affinity and do other housekeeping in
 *   preparation of exec'ing the target executable
 * - if the child fails anywhere along the way, it sends a message up
 *   the pipe to the parent indicating what happened -- including a
 *   rendered error message detailing the problem (i.e., human-readable).
 * - it is important that the child renders the error message: there
 *   are so many errors that are possible that the child is really the
 *   only entity that has enough information to make an accuate error string
 *   to report back to the user.
 * - the parent reads this message + rendered string in and uses ORTE
 *   reporting mechanisms to display it to the user
 * - if the problem was only a warning, the child continues processing
 *   (potentially eventually exec'ing the target executable).
 * - if the problem was an error, the child exits and the parent
 *   handles the death of the child as appropriate (i.e., this ODLS
 *   simply reports the error -- other things decide what to do).
 */

#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include <string.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#include <signal.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <stdlib.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif  /* HAVE_SYS_STAT_H */
#include <stdarg.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#include <ctype.h>
#ifdef HAVE_UTIL_H
#include <util.h>
#endif
#ifdef HAVE_PTY_H
#include <pty.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_TERMIOS_H
#include <termios.h>
# ifdef HAVE_TERMIO_H
#  include <termio.h>
# endif
#endif
#ifdef HAVE_LIBUTIL_H
#include <libutil.h>
#endif

#include <spawn.h>

#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/opal_environ.h"
#include "opal/util/show_help.h"
#include "opal/util/sys_limits.h"
#include "opal/util/fd.h"

#include "orte/util/show_help.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/iof/base/iof_base_setup.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rtc/rtc.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"

#include "orte/mca/odls/base/base.h"
#include "orte/mca/odls/base/odls_private.h"
#include "orte/mca/odls/pspawn/odls_pspawn.h"
#include "orte/orted/pmix/pmix_server.h"

/*
 * Module functions (function pointers used in a struct)
 */
static int orte_odls_pspawn_launch_local_procs(opal_buffer_t *data);
static int orte_odls_pspawn_kill_local_procs(opal_pointer_array_t *procs);
static int orte_odls_pspawn_signal_local_procs(const orte_process_name_t *proc, int32_t signal);
static int orte_odls_pspawn_restart_proc(orte_proc_t *child);


/*
 * Module
 */
orte_odls_base_module_t orte_odls_pspawn_module = {
    .get_add_procs_data = orte_odls_base_default_get_add_procs_data,
    .launch_local_procs = orte_odls_pspawn_launch_local_procs,
    .kill_local_procs = orte_odls_pspawn_kill_local_procs,
    .signal_local_procs = orte_odls_pspawn_signal_local_procs,
    .restart_proc = orte_odls_pspawn_restart_proc
};


/* deliver a signal to a specified pid. */
static int odls_pspawn_kill_local(pid_t pid, int signum)
{
    pid_t pgrp;

#if HAVE_SETPGID
    pgrp = getpgid(pid);
    if (-1 != pgrp) {
        /* target the lead process of the process
         * group so we ensure that the signal is
         * seen by all members of that group. This
         * ensures that the signal is seen by any
         * child processes our child may have
         * started
         */
        pid = -pgrp;
    }
#endif

    if (0 != kill(pid, signum)) {
        if (ESRCH != errno) {
            OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                                 "%s odls:pspawn:SENT KILL %d TO PID %d GOT ERRNO %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), signum, (int)pid, errno));
            return errno;
        }
    }
    OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                         "%s odls:pspawn:SENT KILL %d TO PID %d SUCCESS",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), signum, (int)pid));
    return 0;
}

int orte_odls_pspawn_kill_local_procs(opal_pointer_array_t *procs)
{
    int rc;

    if (ORTE_SUCCESS != (rc = orte_odls_base_default_kill_local_procs(procs,
                                            odls_pspawn_kill_local))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    return ORTE_SUCCESS;
}



/* close all open file descriptors w/ exception of stdin/stdout/stderr
   and the pipe up to the parent. */
static int close_open_file_descriptors(posix_spawn_file_actions_t *factions)
{
    DIR *dir = opendir("/proc/self/fd");
    if (NULL == dir) {
        return ORTE_ERR_FILE_OPEN_FAILURE;
    }
    struct dirent *files;

    /* grab the fd of the opendir above so we don't close in the 
     * middle of the scan. */
    int dir_scan_fd = dirfd(dir);
    if(dir_scan_fd < 0 ) {
        return ORTE_ERR_FILE_OPEN_FAILURE;
    }

    while (NULL != (files = readdir(dir))) {
        if (!isdigit(files->d_name[0])) {
            continue;
        }
        int fd = strtol(files->d_name, NULL, 10);
        if (errno == EINVAL || errno == ERANGE) {
            closedir(dir);
            return ORTE_ERR_TYPE_MISMATCH;
        }
        if (fd >=3 && fd != dir_scan_fd) {
            posix_spawn_file_actions_addclose(factions, fd);
        }
    }
    closedir(dir);
    return ORTE_SUCCESS;
}

/**
 *  posix_spawn the specified processes
 */
static int odls_pspawn_fork_local_proc(void *cdptr)
{
    orte_odls_spawn_caddy_t *cd = (orte_odls_spawn_caddy_t*)cdptr;
    pid_t pid;
    orte_proc_t *child = cd->child;
    posix_spawn_file_actions_t factions;
    posix_spawnattr_t attrs;
    sigset_t sigs;
    int rc;
    orte_iof_base_io_conf_t *opts = &cd->opts;

    ORTE_FLAG_UNSET(cd->child, ORTE_PROC_FLAG_ALIVE);

    /* setup the attrs object */
    rc = posix_spawnattr_init(&attrs);
    if (0 != rc) {
        child->state = ORTE_PROC_STATE_FAILED_TO_START;
        child->exit_code = 1;
        return ORTE_ERROR;
    }
    /* set the signal mask in the child process */
    sigprocmask(0, 0, &sigs);
    sigprocmask(SIG_UNBLOCK, &sigs, 0);
    posix_spawnattr_setsigmask(&attrs, &sigs);

    /* setup to close all fd's other than stdin/out/err */
    rc = posix_spawn_file_actions_init(&factions);
    if (0 != rc) {
        posix_spawnattr_destroy(&attrs);
        child->state = ORTE_PROC_STATE_FAILED_TO_START;
        child->exit_code = 1;
        return ORTE_ERROR;
    }
    if (ORTE_SUCCESS != close_open_file_descriptors(&factions)) {
        posix_spawn_file_actions_destroy(&factions);
        posix_spawnattr_destroy(&attrs);
        child->state = ORTE_PROC_STATE_FAILED_TO_START;
        child->exit_code = 1;
        return ORTE_ERROR;
    }
    /* close the parent end of the pipes in the child */
    if (opts->connect_stdin) {
        posix_spawn_file_actions_addclose(&factions, opts->p_stdin[1]);
    }
    posix_spawn_file_actions_addclose(&factions, opts->p_stdout[0]);
    if( !orte_iof_base.redirect_app_stderr_to_stdout ) {
        posix_spawn_file_actions_addclose(&factions, opts->p_stderr[0]);
    }
    /* dup the stdin/stdout/stderr descriptors */
    if (opts->usepty) {
        /* disable echo */
        struct termios term_attrs;
        if (tcgetattr(opts->p_stdout[1], &term_attrs) < 0) {
            return ORTE_ERR_PIPE_SETUP_FAILURE;
        }
        term_attrs.c_lflag &= ~ (ECHO | ECHOE | ECHOK |
                                 ECHOCTL | ECHOKE | ECHONL);
        term_attrs.c_iflag &= ~ (ICRNL | INLCR | ISTRIP | INPCK | IXON);
        term_attrs.c_oflag &= ~ (
#ifdef OCRNL
                                 /* OS X 10.3 does not have this
                                    value defined */
                                 OCRNL |
#endif
                                 ONLCR);
        if (tcsetattr(opts->p_stdout[1], TCSANOW, &term_attrs) == -1) {
            return ORTE_ERR_PIPE_SETUP_FAILURE;
        }
        posix_spawn_file_actions_adddup2(&factions, fileno(stdout), opts->p_stdout[1]);
        if (orte_iof_base.redirect_app_stderr_to_stdout) {
            posix_spawn_file_actions_adddup2(&factions, fileno(stderr), opts->p_stdout[1]);
        }
    } else {
        if (opts->p_stdout[1] != fileno(stdout)) {
            posix_spawn_file_actions_adddup2(&factions, fileno(stdout), opts->p_stdout[1]);
        }
        if (orte_iof_base.redirect_app_stderr_to_stdout) {
            posix_spawn_file_actions_adddup2(&factions, fileno(stderr), opts->p_stdout[1]);
        }
    }
    if (opts->connect_stdin) {
        if (opts->p_stdin[0] != fileno(stdin)) {
            posix_spawn_file_actions_adddup2(&factions, fileno(stdin), opts->p_stdin[0]);
        }
    }
    if (opts->p_stderr[1] != fileno(stderr) && !orte_iof_base.redirect_app_stderr_to_stdout) {
        posix_spawn_file_actions_adddup2(&factions, fileno(stderr), opts->p_stderr[1]);
    }

    /* Fork off the child */
    rc = posix_spawn(&pid, cd->app->app, &factions, &attrs, cd->argv, cd->env);
    posix_spawn_file_actions_destroy(&factions);
    posix_spawnattr_destroy(&attrs);

    /* as the parent, close the other ends of the pipes */
    if (cd->opts.connect_stdin) {
        close(cd->opts.p_stdin[0]);
    }
    close(cd->opts.p_stdout[1]);
    if( !orte_iof_base.redirect_app_stderr_to_stdout ) {
        close(cd->opts.p_stderr[1]);
    }

    if (rc < 0) {
        ORTE_ERROR_LOG(ORTE_ERR_SYS_LIMITS_CHILDREN);
        child->state = ORTE_PROC_STATE_FAILED_TO_START;
        child->exit_code = ORTE_ERR_SYS_LIMITS_CHILDREN;
        return ORTE_ERR_SYS_LIMITS_CHILDREN;
    }

    cd->child->state = ORTE_PROC_STATE_RUNNING;
    cd->child->pid = pid;
    ORTE_FLAG_SET(cd->child, ORTE_PROC_FLAG_ALIVE);
    return ORTE_SUCCESS;
}


/**
 * Launch all processes allocated to the current node.
 */

int orte_odls_pspawn_launch_local_procs(opal_buffer_t *data)
{
    int rc;
    orte_jobid_t job;

    /* construct the list of children we are to launch */
    if (ORTE_SUCCESS != (rc = orte_odls_base_default_construct_child_list(data, &job))) {
        OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                             "%s odls:pspawn:launch:local failed to construct child list on error %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_ERROR_NAME(rc)));
        return rc;
    }

    /* launch the local procs */
    ORTE_ACTIVATE_LOCAL_LAUNCH(job, odls_pspawn_fork_local_proc);

    return ORTE_SUCCESS;
}


/**
 * Send a signal to a pid.  Note that if we get an error, we set the
 * return value and let the upper layer print out the message.
 */
static int send_signal(pid_t pd, int signal)
{
    int rc = ORTE_SUCCESS;
    pid_t pid;

    if (orte_odls_globals.signal_direct_children_only) {
        pid = pd;
    } else {
#if HAVE_SETPGID
        /* send to the process group so that any children of our children
         * also receive the signal*/
        pid = -pd;
#else
        pid = pd;
#endif
    }

    OPAL_OUTPUT_VERBOSE((1, orte_odls_base_framework.framework_output,
                         "%s sending signal %d to pid %ld",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         signal, (long)pid));

    if (kill(pid, signal) != 0) {
        switch(errno) {
            case EINVAL:
                rc = ORTE_ERR_BAD_PARAM;
                break;
            case ESRCH:
                /* This case can occur when we deliver a signal to a
                   process that is no longer there.  This can happen if
                   we deliver a signal while the job is shutting down.
                   This does not indicate a real problem, so just
                   ignore the error.  */
                break;
            case EPERM:
                rc = ORTE_ERR_PERM;
                break;
            default:
                rc = ORTE_ERROR;
        }
    }

    return rc;
}

static int orte_odls_pspawn_signal_local_procs(const orte_process_name_t *proc, int32_t signal)
{
    int rc;

    if (ORTE_SUCCESS != (rc = orte_odls_base_default_signal_local_procs(proc, signal, send_signal))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    return ORTE_SUCCESS;
}

static int orte_odls_pspawn_restart_proc(orte_proc_t *child)
{
    int rc;

    /* restart the local proc */
    if (ORTE_SUCCESS != (rc = orte_odls_base_default_restart_proc(child, odls_pspawn_fork_local_proc))) {
        OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                             "%s odls:pspawn:restart_proc failed to launch on error %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_ERROR_NAME(rc)));
    }
    return rc;
}
