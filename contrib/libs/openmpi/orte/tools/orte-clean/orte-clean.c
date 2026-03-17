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
 * Copyright (c) 2007-2008 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2011-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      UT-Battelle, LLC. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <stdlib.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif  /* HAVE_SYS_STAT_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif  /* HAVE_SYS_WAIT_H */
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif  /* HAVE_SYS_PARAM_H */
#include <string.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif  /* HAVE_DIRENT_H */
#include <signal.h>
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif  /* HAVE_PWD_H */

#include "opal/util/cmd_line.h"
#include "opal/util/opal_environ.h"
#include "opal/util/os_dirpath.h"
#include "opal/util/basename.h"
#include "opal/util/error.h"
#include "opal/mca/base/base.h"
#include "opal/util/show_help.h"

#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"

#include "opal/runtime/opal.h"
#if OPAL_ENABLE_FT_CR == 1
#include "opal/runtime/opal_cr.h"
#endif
#include "orte/runtime/runtime.h"

/******************
 * Local Functions
 ******************/
static int parse_args(int argc, char *argv[]);
static void kill_procs(void);

/*****************************************
 * Global Vars for Command line Arguments
 *****************************************/
typedef struct {
    bool help;
    bool verbose;
    bool debug;
} orte_clean_globals_t;

orte_clean_globals_t orte_clean_globals = {0};

opal_cmd_line_init_t cmd_line_opts[] = {
    { NULL,
      'h', NULL, "help",
      0,
      &orte_clean_globals.help, OPAL_CMD_LINE_TYPE_BOOL,
      "This help message" },

    { NULL,
      'v', NULL, "verbose",
      0,
      &orte_clean_globals.verbose, OPAL_CMD_LINE_TYPE_BOOL,
      "Generate verbose output" },

    { NULL,
      'd', NULL, "debug",
      0,
      &orte_clean_globals.debug, OPAL_CMD_LINE_TYPE_BOOL,
      "Extra debug output for developers to ensure that orte-clean is working" },

    /* End of list */
    { NULL,
      '\0', NULL, NULL,
      0,
      NULL, OPAL_CMD_LINE_TYPE_NULL,
      NULL }
};

/*
 * This utility will do a brute force clean of a node.  It will
 * attempt to clean up any files in the user's session directory.
 * It will also look for any orted and orterun processes that are
 * not part of this job, and kill them off.
*/
int
main(int argc, char *argv[])
{
    int ret = ORTE_SUCCESS;
#if OPAL_ENABLE_FT_CR == 1
    char *tmp_env_var;
#endif
    char *legacy;

    /* This is needed so we can print the help message */
    if (ORTE_SUCCESS != (ret = opal_init_util(&argc, &argv))) {
        return ret;
    }

    if (ORTE_SUCCESS != (ret = parse_args(argc, argv))) {
        return ret;
    }

#if OPAL_ENABLE_FT_CR == 1
    /* Disable the checkpoint notification routine for this
     * tool. As we will never need to checkpoint this tool.
     * Note: This must happen before opal_init().
     */
    opal_cr_set_enabled(false);

    /* Select the none component, since we don't actually use a checkpointer */
    (void) mca_base_var_env_name("crs", &tmp_env_var);
    opal_setenv(tmp_env_var,
                "none",
                true, &environ);
    free(tmp_env_var);
    tmp_env_var = NULL;

    (void) mca_base_var_env_name("opal_cr_is_tool", &tmp_env_var);
    opal_setenv(tmp_env_var,
                "1", true, NULL);
    free(tmp_env_var);
#endif

    if (ORTE_SUCCESS != (ret = orte_init(&argc, &argv, ORTE_PROC_TOOL))) {
        return ret;
    }

    /*
     * Clean out all session directories - we don't have to protect
     * our own session directory because (since we are a tool) we
     * didn't create one!
     */
    if (orte_clean_globals.verbose) {
        fprintf(stderr, "orte-clean: cleaning session dir tree %s\n",
                orte_process_info.top_session_dir);
    }
    opal_os_dirpath_destroy(orte_process_info.top_session_dir, true, NULL);

    /* also get rid of any legacy session directories */
    asprintf(&legacy, "%s/openmpi-sessions-%d@%s_0",
             orte_process_info.tmpdir_base,
             (int)geteuid(), orte_process_info.nodename);
    opal_os_dirpath_destroy(legacy, true, NULL);
    free(legacy);

    /* and finally get rid of any lingering pmix-related artifacts */
    asprintf(&legacy, "rm -rf %s/pmix*", orte_process_info.tmpdir_base);
    system(legacy);
    free(legacy);

    /* now kill any lingering procs, if we can */
    kill_procs();

    orte_finalize();

    return ORTE_SUCCESS;
}
/*
 * Parse the command line arguments using the functions command
 * line utility functions.
 */
static int parse_args(int argc, char *argv[]) {
    int ret;
    opal_cmd_line_t cmd_line;
    orte_clean_globals_t tmp = { false, false, false };

    /* NOTE: There is a bug in the PGI 6.2 series that causes the
       compiler to choke when copying structs containing bool members
       by value.  So do a memcpy here instead. */
    memcpy(&orte_clean_globals, &tmp, sizeof(tmp));

    /*
     * Initialize list of available command line options.
     */
    opal_cmd_line_create(&cmd_line, cmd_line_opts);
    ret = opal_cmd_line_parse(&cmd_line, false, false, argc, argv);

    if (OPAL_SUCCESS != ret) {
        if (OPAL_ERR_SILENT != ret) {
            fprintf(stderr, "%s: command line error (%s)\n", argv[0],
                    opal_strerror(ret));
        }
        return ret;
    }

    /**
     * Now start parsing our specific arguments
     */
    if (orte_clean_globals.help) {
        char *str, *args = NULL;
        args = opal_cmd_line_get_usage_msg(&cmd_line);
        str = opal_show_help_string("help-orte-clean.txt", "usage", true,
                                    args);
        if (NULL != str) {
            printf("%s", str);
            free(str);
        }
        free(args);
        /* If we show the help message, that should be all we do */
        exit(0);
    }

    OBJ_DESTRUCT(&cmd_line);

    return ORTE_SUCCESS;
}

static char *orte_getline(FILE *fp)
{
    char *ret, *buff;
    char input[1024];
    int i;

    ret = fgets(input, 1024, fp);
    if (NULL != ret) {
        /* remove trailing spaces */
        for (i=strlen(input)-2; i > 0; i--) {
            if (input[i] != ' ') {
                input[i+1] = '\0';
                break;
            }
        }
        buff = strdup(input);
        return buff;
    }

    return NULL;
}

/*
 * This function makes a call to "ps" to find out the processes that
 * are running on this node.  It then attempts to kill off any orteds
 * and orteruns that are not related to this job.
 */
static
void kill_procs(void) {
    int ortedpid;
    char *fullprocname;
    char *procname;
    char *pidstr;
    char *user;
    int procpid;
    FILE *psfile;
    char *inputline;
    char *this_user;
    int uid;
    char *separator = " \t";  /* output can be delimited by space or tab */

    /*
     * This is the command that is used to get the information about
     * all the processes that are running.  The output looks like the
     * following:
     * COMMAND    PID     UID
     * tcsh     12556    1000
     * ps       14424    1000
     * etc.
     */

    /*
     * The configure determines if there is a valid ps command for us to
     * use.  If it is set to unknown, then we skip this section.
     */
    char command[] = ORTE_CLEAN_PS_CMD;
    if (0 == strcmp("unknown", command)) {
        return;
    }

    if (orte_clean_globals.verbose) {
        fprintf(stderr, "orte-clean: killing any lingering procs\n");
    }

    /*
     * Get our parent pid which is the pid of the orted.
     */
    ortedpid = getppid();

    /* get the userid of the user */
    uid = getuid();
    asprintf(&this_user, "%d", uid);

    /*
     * There is a race condition here.  The problem is that we are looking
     * for any processes named orted.  However, one may erroneously find more
     * orteds then there really are because the orted is doing a series of
     * fork/execs. If we run with more than one orte-clean on a node, then
     * one of the orte-cleans may catch the other one while it has forked,
     * but not exec'ed.  It will therefore kill an orte-clean.  Now one
     * can argue it is silly to run more than one orte-clean on a node, and
     * this is true.  We will have to figure out how to prevent this.  For
     * now, we use a big hammer and just sleep a second to decrease the
     * probability.
     */
    sleep(1);

    psfile = popen(command, "r");
    /*
     * Read the first line of the output.  We just throw it away
     * as it is the header consisting of the words COMMAND, PID and UID.
     */
    if (NULL == (inputline = orte_getline(psfile))) {
        free(this_user);
        pclose(psfile);
        return;
    }
    free(inputline);  /* dump the header line */

    while (NULL != (inputline = orte_getline(psfile))) {

        /* The three fields are typically seperated by spaces */
        fullprocname = strtok(inputline, separator);
        pidstr = strtok(NULL, separator);
        user = strtok(NULL, separator);

        if (orte_clean_globals.debug) {
            fprintf(stdout, "\norte-clean: user(pid)=%s, me=%s\n",
                    user, this_user);
        }

        /* If the user is not us, and the user is not root, then skip
         * further checking.  If the user is root, then continue on as
         * we want root to kill off everybody. */
        if ((0 != strcmp(user, this_user)) && (0 != strcmp("0", this_user))) {
            /* not us */
            free(inputline);
            continue;
        }

        procpid = atoi(pidstr);
        procname = opal_basename(fullprocname);
        if (orte_clean_globals.debug) {
            fprintf(stdout, "orte-clean: fullname=%s, basename=%s, pid=%d\n",
                    fullprocname, procname, procpid);
        }

        /*
         * Look for any orteds that are not our parent and attempt to
         * kill them.  We currently do not worry whether we are the
         * owner or not.  If we are not, we will just fail to send
         * the signal and that is OK.  This also allows a root process
         * to kill all orteds.
         *
         * NOTE: need to also look for "(orted)" as a non-active
         * proc is sometimes reported that way
         */
        if (0 == strncmp("orted", procname, strlen("orted")) ||
            0 == strncmp("(orted)", procname, strlen("(orted)")) ||
            0 == strncmp("orte-dvm", procname, strlen("orte-dvm")) ||
            0 == strncmp("(orte-dvm)", procname, strlen("(orte-dvm)"))) {
            if (procpid != ortedpid) {
                if (orte_clean_globals.verbose) {
                    fprintf(stderr, "orte-clean: found potential rogue orted process"
                            " (pid=%d,uid=%s), sending SIGKILL...\n",
                            procpid, user);
                }
                /*
                 * We ignore the return code here as we do not really
                 * care whether this worked or not.
                 */
                (void)kill(procpid, SIGKILL);
            }
        }

        /*
         * Now check for any orteruns.
         */
        if (0 == strncmp("orterun", procname, strlen("orterun")) ||
            0 == strncmp("mpirun", procname, strlen("mpirun"))) {
            /* if we are on the same node as the HNP, then the ortedpid
             * is the same as that of our orterun, so don't kill it
             */
            if (procpid != ortedpid) {
                if (orte_clean_globals.verbose) {
                    fprintf(stderr, "orte-clean: found potential rogue orterun process"
                            " (pid=%d,uid=%s), sending SIGKILL...\n",
                            procpid, user);

                }
                /* if we are a singleton, check the hnp_pid as well */
                if (ORTE_PROC_IS_SINGLETON) {
                    if (procpid != orte_process_info.hnp_pid) {
                        (void)kill(procpid, SIGKILL);
                    }
                } else {
                    /* We ignore the return code here as we do not really
                     * care whether this worked or not.
                     */
                    (void)kill(procpid, SIGKILL);
                }
            }
        }
        free(inputline);
        free(procname);
    }
    free(this_user);
    pclose(psfile);
    return;
}
