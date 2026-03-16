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
#include "orte/mca/odls/default/odls_default.h"
#include "orte/orted/pmix/pmix_server.h"

/*
 * Module functions (function pointers used in a struct)
 */
static int orte_odls_default_launch_local_procs(opal_buffer_t *data);
static int orte_odls_default_kill_local_procs(opal_pointer_array_t *procs);
static int orte_odls_default_signal_local_procs(const orte_process_name_t *proc, int32_t signal);
static int orte_odls_default_restart_proc(orte_proc_t *child);

/*
 * Explicitly declared functions so that we can get the noreturn
 * attribute registered with the compiler.
 */
static void send_error_show_help(int fd, int exit_status,
                                 const char *file, const char *topic, ...)
    __opal_attribute_noreturn__;

static int do_child(orte_odls_spawn_caddy_t *cd, int write_fd)
    __opal_attribute_noreturn__;


/*
 * Module
 */
orte_odls_base_module_t orte_odls_default_module = {
    .get_add_procs_data = orte_odls_base_default_get_add_procs_data,
    .launch_local_procs = orte_odls_default_launch_local_procs,
    .kill_local_procs = orte_odls_default_kill_local_procs,
    .signal_local_procs = orte_odls_default_signal_local_procs,
    .restart_proc = orte_odls_default_restart_proc
};


/* deliver a signal to a specified pid. */
static int odls_default_kill_local(pid_t pid, int signum)
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
                                 "%s odls:default:SENT KILL %d TO PID %d GOT ERRNO %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), signum, (int)pid, errno));
            return errno;
        }
    }
    OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                         "%s odls:default:SENT KILL %d TO PID %d SUCCESS",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), signum, (int)pid));
    return 0;
}

int orte_odls_default_kill_local_procs(opal_pointer_array_t *procs)
{
    int rc;

    if (ORTE_SUCCESS != (rc = orte_odls_base_default_kill_local_procs(procs,
                                            odls_default_kill_local))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    return ORTE_SUCCESS;
}


static void set_handler_default(int sig)
{
    struct sigaction act;

    act.sa_handler = SIG_DFL;
    act.sa_flags = 0;
    sigemptyset(&act.sa_mask);

    sigaction(sig, &act, (struct sigaction *)0);
}

/*
 * Internal function to write a rendered show_help message back up the
 * pipe to the waiting parent.
 */
static int write_help_msg(int fd, orte_odls_pipe_err_msg_t *msg, const char *file,
                          const char *topic, va_list ap)
{
    int ret;
    char *str;

    if (NULL == file || NULL == topic) {
        return OPAL_ERR_BAD_PARAM;
    }

    str = opal_show_help_vstring(file, topic, true, ap);

    msg->file_str_len = (int) strlen(file);
    if (msg->file_str_len > ORTE_ODLS_MAX_FILE_LEN) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }
    msg->topic_str_len = (int) strlen(topic);
    if (msg->topic_str_len > ORTE_ODLS_MAX_TOPIC_LEN) {
        ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
        return ORTE_ERR_BAD_PARAM;
    }
    msg->msg_str_len = (int) strlen(str);

    /* Only keep writing if each write() succeeds */
    if (OPAL_SUCCESS != (ret = opal_fd_write(fd, sizeof(*msg), msg))) {
        goto out;
    }
    if (msg->file_str_len > 0 &&
        OPAL_SUCCESS != (ret = opal_fd_write(fd, msg->file_str_len, file))) {
        goto out;
    }
    if (msg->topic_str_len > 0 &&
        OPAL_SUCCESS != (ret = opal_fd_write(fd, msg->topic_str_len, topic))) {
        goto out;
    }
    if (msg->msg_str_len > 0 &&
        OPAL_SUCCESS != (ret = opal_fd_write(fd, msg->msg_str_len, str))) {
        goto out;
    }

 out:
    free(str);
    return ret;
}


/* Called from the child to send an error message up the pipe to the
   waiting parent. */
static void send_error_show_help(int fd, int exit_status,
                                 const char *file, const char *topic, ...)
{
    va_list ap;
    orte_odls_pipe_err_msg_t msg;

    msg.fatal = true;
    msg.exit_status = exit_status;

    /* Send it */
    va_start(ap, topic);
    write_help_msg(fd, &msg, file, topic, ap);
    va_end(ap);

    exit(exit_status);
}

/* close all open file descriptors w/ exception of stdin/stdout/stderr
   and the pipe up to the parent. */
static int close_open_file_descriptors(int write_fd,
                                      orte_iof_base_io_conf_t opts) {
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
        if (fd >=3 &&
#if OPAL_PMIX_V1
            fd != opts.p_internal[1] &&
#endif
            fd != write_fd && 
            fd != dir_scan_fd) {
            close(fd);
        }
    }
    closedir(dir);
    return ORTE_SUCCESS;
}

static int do_child(orte_odls_spawn_caddy_t *cd, int write_fd)
{
    int i;
    sigset_t sigs;
    long fd, fdmax = sysconf(_SC_OPEN_MAX);
    char dir[MAXPATHLEN];

#if HAVE_SETPGID
    /* Set a new process group for this child, so that any
     * signals we send to it will reach any children it spawns */
    setpgid(0, 0);
#endif

    /* Setup the pipe to be close-on-exec */
    opal_fd_set_cloexec(write_fd);

    if (NULL != cd->child) {
        /* setup stdout/stderr so that any error messages that we
           may print out will get displayed back at orterun.

           NOTE: Definitely do this AFTER we check contexts so
           that any error message from those two functions doesn't
           come out to the user. IF we didn't do it in this order,
           THEN a user who gives us a bad executable name or
           working directory would get N error messages, where
           N=num_procs. This would be very annoying for large
           jobs, so instead we set things up so that orterun
           always outputs a nice, single message indicating what
           happened
        */
        if (ORTE_FLAG_TEST(cd->jdata, ORTE_JOB_FLAG_FORWARD_OUTPUT)) {
            if (ORTE_SUCCESS != (i = orte_iof_base_setup_child(&cd->opts, &cd->env))) {
                ORTE_ERROR_LOG(i);
                send_error_show_help(write_fd, 1,
                                     "help-orte-odls-default.txt",
                                     "iof setup failed",
                                     orte_process_info.nodename, cd->app->app);
                /* Does not return */
            }
        }

        /* now set any child-level controls such as binding */
        orte_rtc.set(cd->jdata, cd->child, &cd->env, write_fd);

    } else if (!ORTE_FLAG_TEST(cd->jdata, ORTE_JOB_FLAG_FORWARD_OUTPUT)) {
        /* tie stdin/out/err/internal to /dev/null */
        int fdnull;
        for (i=0; i < 3; i++) {
            fdnull = open("/dev/null", O_RDONLY, 0);
            if (fdnull > i && i != write_fd) {
                dup2(fdnull, i);
            }
            close(fdnull);
        }
#if OPAL_PMIX_V1
        fdnull = open("/dev/null", O_RDONLY, 0);
        if (fdnull > cd->opts.p_internal[1]) {
            dup2(fdnull, cd->opts.p_internal[1]);
        }
        close(fdnull);
#endif
    }

    /* close all open file descriptors w/ exception of stdin/stdout/stderr,
       the pipe used for the IOF INTERNAL messages, and the pipe up to
       the parent. */
    if (ORTE_SUCCESS != close_open_file_descriptors(write_fd, cd->opts)) {
        // close *all* file descriptors -- slow
        for(fd=3; fd<fdmax; fd++) {
            if (
#if OPAL_PMIX_V1
                fd != cd->opts.p_internal[1] &&
#endif
                fd != write_fd) {
                close(fd);
            }
        }
    }

    if (cd->argv == NULL) {
        cd->argv = malloc(sizeof(char*)*2);
        cd->argv[0] = strdup(cd->app->app);
        cd->argv[1] = NULL;
    }

    /* Set signal handlers back to the default.  Do this close to
       the exev() because the event library may (and likely will)
       reset them.  If we don't do this, the event library may
       have left some set that, at least on some OS's, don't get
       reset via fork() or exec().  Hence, the launched process
       could be unkillable (for example). */

    set_handler_default(SIGTERM);
    set_handler_default(SIGINT);
    set_handler_default(SIGHUP);
    set_handler_default(SIGPIPE);
    set_handler_default(SIGCHLD);

    /* Unblock all signals, for many of the same reasons that we
       set the default handlers, above.  This is noticable on
       Linux where the event library blocks SIGTERM, but we don't
       want that blocked by the launched process. */
    sigprocmask(0, 0, &sigs);
    sigprocmask(SIG_UNBLOCK, &sigs, 0);

    /* take us to the correct wdir */
    if (NULL != cd->wdir) {
        if (0 != chdir(cd->wdir)) {
            send_error_show_help(write_fd, 1,
                                 "help-orterun.txt",
                                 "orterun:wdir-not-found",
                                 "orted",
                                 cd->wdir,
                                 orte_process_info.nodename,
                                 (NULL == cd->child) ? 0 : cd->child->app_rank);
            /* Does not return */
        }
    }

    /* Exec the new executable */
    execve(cd->cmd, cd->argv, cd->env);
    getcwd(dir, sizeof(dir));
    send_error_show_help(write_fd, 1,
                         "help-orte-odls-default.txt", "execve error",
                         orte_process_info.nodename, dir, cd->app->app, strerror(errno));
    /* Does not return */
}


static int do_parent(orte_odls_spawn_caddy_t *cd, int read_fd)
{
    int rc;
    orte_odls_pipe_err_msg_t msg;
    char file[ORTE_ODLS_MAX_FILE_LEN + 1], topic[ORTE_ODLS_MAX_TOPIC_LEN + 1], *str = NULL;

    if (cd->opts.connect_stdin) {
        close(cd->opts.p_stdin[0]);
    }
    close(cd->opts.p_stdout[1]);
    if( !orte_iof_base.redirect_app_stderr_to_stdout ) {
        close(cd->opts.p_stderr[1]);
    }
#if OPAL_PMIX_V1
    close(cd->opts.p_internal[1]);
#endif

    /* Block reading a message from the pipe */
    while (1) {
        rc = opal_fd_read(read_fd, sizeof(msg), &msg);

        /* If the pipe closed, then the child successfully launched */
        if (OPAL_ERR_TIMEOUT == rc) {
            break;
        }

        /* If Something Bad happened in the read, error out */
        if (OPAL_SUCCESS != rc) {
            ORTE_ERROR_LOG(rc);
            close(read_fd);

            if (NULL != cd->child) {
                cd->child->state = ORTE_PROC_STATE_UNDEF;
            }
            return rc;
        }

        /* Otherwise, we got a warning or error message from the child */
        if (NULL != cd->child) {
            if (msg.fatal) {
                ORTE_FLAG_UNSET(cd->child, ORTE_PROC_FLAG_ALIVE);
            } else {
                ORTE_FLAG_SET(cd->child, ORTE_PROC_FLAG_ALIVE);
            }
        }

        /* Read in the strings; ensure to terminate them with \0 */
        if (msg.file_str_len > 0) {
            rc = opal_fd_read(read_fd, msg.file_str_len, file);
            if (OPAL_SUCCESS != rc) {
                orte_show_help("help-orte-odls-default.txt", "syscall fail",
                               true,
                               orte_process_info.nodename, cd->app->app,
                               "opal_fd_read", __FILE__, __LINE__);
                if (NULL != cd->child) {
                    cd->child->state = ORTE_PROC_STATE_UNDEF;
                }
                return rc;
            }
            file[msg.file_str_len] = '\0';
        }
        if (msg.topic_str_len > 0) {
            rc = opal_fd_read(read_fd, msg.topic_str_len, topic);
            if (OPAL_SUCCESS != rc) {
                orte_show_help("help-orte-odls-default.txt", "syscall fail",
                               true,
                               orte_process_info.nodename, cd->app->app,
                               "opal_fd_read", __FILE__, __LINE__);
                if (NULL != cd->child) {
                    cd->child->state = ORTE_PROC_STATE_UNDEF;
                }
                return rc;
            }
            topic[msg.topic_str_len] = '\0';
        }
        if (msg.msg_str_len > 0) {
            str = calloc(1, msg.msg_str_len + 1);
            if (NULL == str) {
                orte_show_help("help-orte-odls-default.txt", "syscall fail",
                               true,
                               orte_process_info.nodename, cd->app->app,
                               "opal_fd_read", __FILE__, __LINE__);
                if (NULL != cd->child) {
                    cd->child->state = ORTE_PROC_STATE_UNDEF;
                }
                return rc;
            }
            rc = opal_fd_read(read_fd, msg.msg_str_len, str);
        }

        /* Print out what we got.  We already have a rendered string,
           so use orte_show_help_norender(). */
        if (msg.msg_str_len > 0) {
            orte_show_help_norender(file, topic, false, str);
            free(str);
            str = NULL;
        }

        /* If msg.fatal is true, then the child exited with an error.
           Otherwise, whatever we just printed was a warning, so loop
           around and see what else is on the pipe (or if the pipe
           closed, indicating that the child launched
           successfully). */
        if (msg.fatal) {
            if (NULL != cd->child) {
                cd->child->state = ORTE_PROC_STATE_FAILED_TO_START;
                ORTE_FLAG_UNSET(cd->child, ORTE_PROC_FLAG_ALIVE);
            }
            close(read_fd);
            return ORTE_ERR_FAILED_TO_START;
        }
    }

    /* If we got here, it means that the pipe closed without
       indication of a fatal error, meaning that the child process
       launched successfully. */
    if (NULL != cd->child) {
        cd->child->state = ORTE_PROC_STATE_RUNNING;
        ORTE_FLAG_SET(cd->child, ORTE_PROC_FLAG_ALIVE);
    }
    close(read_fd);

    return ORTE_SUCCESS;
}


/**
 *  Fork/exec the specified processes
 */
static int odls_default_fork_local_proc(void *cdptr)
{
    orte_odls_spawn_caddy_t *cd = (orte_odls_spawn_caddy_t*)cdptr;
    int p[2];
    pid_t pid;
    orte_proc_t *child = cd->child;

    /* A pipe is used to communicate between the parent and child to
       indicate whether the exec ultimately succeeded or failed.  The
       child sets the pipe to be close-on-exec; the child only ever
       writes anything to the pipe if there is an error (e.g.,
       executable not found, exec() fails, etc.).  The parent does a
       blocking read on the pipe; if the pipe closed with no data,
       then the exec() succeeded.  If the parent reads something from
       the pipe, then the child was letting us know why it failed. */
    if (pipe(p) < 0) {
        ORTE_ERROR_LOG(ORTE_ERR_SYS_LIMITS_PIPES);
        if (NULL != child) {
            child->state = ORTE_PROC_STATE_FAILED_TO_START;
            child->exit_code = ORTE_ERR_SYS_LIMITS_PIPES;
        }
        return ORTE_ERR_SYS_LIMITS_PIPES;
    }

    /* Fork off the child */
    pid = fork();
    if (NULL != child) {
        child->pid = pid;
    }

    if (pid < 0) {
        ORTE_ERROR_LOG(ORTE_ERR_SYS_LIMITS_CHILDREN);
        if (NULL != child) {
            child->state = ORTE_PROC_STATE_FAILED_TO_START;
            child->exit_code = ORTE_ERR_SYS_LIMITS_CHILDREN;
        }
        return ORTE_ERR_SYS_LIMITS_CHILDREN;
    }

    if (pid == 0) {
        close(p[0]);
        do_child(cd, p[1]);
        /* Does not return */
    }

    close(p[1]);
    return do_parent(cd, p[0]);
}


/**
 * Launch all processes allocated to the current node.
 */

int orte_odls_default_launch_local_procs(opal_buffer_t *data)
{
    int rc;
    orte_jobid_t job;

    /* construct the list of children we are to launch */
    if (ORTE_SUCCESS != (rc = orte_odls_base_default_construct_child_list(data, &job))) {
        OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                             "%s odls:default:launch:local failed to construct child list on error %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_ERROR_NAME(rc)));
        return rc;
    }

    /* launch the local procs */
    ORTE_ACTIVATE_LOCAL_LAUNCH(job, odls_default_fork_local_proc);

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

static int orte_odls_default_signal_local_procs(const orte_process_name_t *proc, int32_t signal)
{
    int rc;

    if (ORTE_SUCCESS != (rc = orte_odls_base_default_signal_local_procs(proc, signal, send_signal))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    return ORTE_SUCCESS;
}

static int orte_odls_default_restart_proc(orte_proc_t *child)
{
    int rc;

    /* restart the local proc */
    if (ORTE_SUCCESS != (rc = orte_odls_base_default_restart_proc(child, odls_default_fork_local_proc))) {
        OPAL_OUTPUT_VERBOSE((2, orte_odls_base_framework.framework_output,
                             "%s odls:default:restart_proc failed to launch on error %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_ERROR_NAME(rc)));
    }
    return rc;
}
