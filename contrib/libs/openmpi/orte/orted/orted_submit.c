/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2007-2009 Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2007-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif  /* HAVE_STRINGS_H */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <errno.h>
#include <signal.h>
#include <ctype.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif  /* HAVE_SYS_WAIT_H */
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif  /* HAVE_SYS_TIME_H */
#include <fcntl.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#include <poll.h>

#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/mca/base/base.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/basename.h"
#include "opal/util/cmd_line.h"
#include "opal/util/opal_environ.h"
#include "opal/util/opal_getcwd.h"
#include "opal/util/show_help.h"
#include "opal/util/fd.h"
#include "opal/sys/atomic.h"
#if OPAL_ENABLE_FT_CR == 1
#include "opal/runtime/opal_cr.h"
#endif

#include "opal/version.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_info_support.h"
#include "opal/util/os_path.h"
#include "opal/util/path.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss.h"

#include "orte/mca/odls/odls_types.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/rmaps/base/base.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/oob/base/base.h"
#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/schizo/base/base.h"
#include "orte/mca/state/state.h"

#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_quit.h"
#include "orte/util/pre_condition_transports.h"
#include "orte/util/show_help.h"

#include "orted_submit.h"

/**
 * Global struct for catching orte command line options.
 */
orte_cmd_options_t orte_cmd_options = {0};
opal_cmd_line_t *orte_cmd_line = NULL;

static char **global_mca_env = NULL;
static orte_std_cntr_t total_num_apps = 0;
static bool want_prefix_by_default = (bool) ORTE_WANT_ORTERUN_PREFIX_BY_DEFAULT;
static opal_pointer_array_t tool_jobs;
static int timeout_seconds;
static orte_timer_t *orte_memprofile_timeout;

int orte_debugger_attach_fd = -1;
bool orte_debugger_fifo_active=false;
opal_event_t *orte_debugger_attach=NULL;

/*
 * Local functions
 */
static int create_app(int argc, char* argv[],
                      orte_job_t *jdata,
                      orte_app_context_t **app,
                      bool *made_app, char ***app_env);
static int init_globals(void);
static int parse_globals(int argc, char* argv[], opal_cmd_line_t *cmd_line);
static int parse_locals(orte_job_t *jdata, int argc, char* argv[]);
static void set_classpath_jar_file(orte_app_context_t *app, int index, char *jarfile);
static int parse_appfile(orte_job_t *jdata, char *filename, char ***env);
static void orte_timeout_wakeup(int sd, short args, void *cbdata);
static void orte_profile_wakeup(int sd, short args, void *cbdata);
static void profile_recv(int status, orte_process_name_t* sender,
                         opal_buffer_t *buffer, orte_rml_tag_t tag,
                         void* cbdata);
static void launch_recv(int status, orte_process_name_t* sender,
                        opal_buffer_t *buffer,
                        orte_rml_tag_t tag, void *cbdata);
static void complete_recv(int status, orte_process_name_t* sender,
                          opal_buffer_t *buffer,
                          orte_rml_tag_t tag, void *cbdata);
static void attach_debugger(int fd, short event, void *arg);
static void build_debugger_args(orte_app_context_t *debugger);
static void open_fifo (void);
static void run_debugger(char *basename, opal_cmd_line_t *cmd_line,
                         int argc, char *argv[], int num_procs);
static void print_help(void);

/* instance the standard MPIR interfaces */
#define MPIR_MAX_PATH_LENGTH 512
#define MPIR_MAX_ARG_LENGTH 1024
struct MPIR_PROCDESC *MPIR_proctable = NULL;
int MPIR_proctable_size = 0;
volatile int MPIR_being_debugged = 0;
volatile int MPIR_debug_state = 0;
int MPIR_i_am_starter = 0;
int MPIR_partial_attach_ok = 1;
char MPIR_executable_path[MPIR_MAX_PATH_LENGTH] = {0};
char MPIR_server_arguments[MPIR_MAX_ARG_LENGTH] = {0};
volatile int MPIR_forward_output = 0;
volatile int MPIR_forward_comm = 0;
char MPIR_attach_fifo[MPIR_MAX_PATH_LENGTH] = {0};
int MPIR_force_to_main = 0;
static void orte_debugger_init_before_spawn(orte_job_t *jdata);

ORTE_DECLSPEC void* __opal_attribute_optnone__ MPIR_Breakpoint(void);

/*
 * Breakpoint function for parallel debuggers
 */
void* MPIR_Breakpoint(void)
{
    return NULL;
}

/* local objects */
typedef struct {
    opal_object_t super;
    orte_job_t *jdata;
    int index;
    orte_submit_cbfunc_t launch_cb;
    void *launch_cbdata;
    orte_submit_cbfunc_t complete_cb;
    void *complete_cbdata;
} trackr_t;
static void tcon(trackr_t *p)
{
    p->jdata = NULL;
    p->launch_cb = NULL;
    p->launch_cbdata = NULL;
    p->complete_cb = NULL;
    p->complete_cbdata = NULL;
}
static void tdes(trackr_t *p)
{
    if (NULL != p->jdata) {
        OBJ_RELEASE(p->jdata);
    }
}
static OBJ_CLASS_INSTANCE(trackr_t,
                          opal_object_t,
                          tcon, tdes);

int orte_submit_init(int argc, char *argv[],
                     opal_cmd_line_init_t *opts)
{
    int rc, i;
    char *param;

    /* init the globals */
    memset(&orte_cmd_options, 0, sizeof(orte_cmd_options));

    /* find our basename (the name of the executable) so that we can
       use it in pretty-print error messages */
    orte_basename = opal_basename(argv[0]);

    /* search the argv for MCA params */
    for (i=0; NULL != argv[i]; i++) {
        if (':' == argv[i][0] ||
            NULL == argv[i+1] || NULL == argv[i+2]) {
            break;
        }
        if (0 == strncmp(argv[i], "-"OPAL_MCA_CMD_LINE_ID, strlen("-"OPAL_MCA_CMD_LINE_ID)) ||
            0 == strncmp(argv[i], "--"OPAL_MCA_CMD_LINE_ID, strlen("--"OPAL_MCA_CMD_LINE_ID)) ||
            0 == strncmp(argv[i], "-g"OPAL_MCA_CMD_LINE_ID, strlen("-g"OPAL_MCA_CMD_LINE_ID)) ||
            0 == strncmp(argv[i], "--g"OPAL_MCA_CMD_LINE_ID, strlen("--g"OPAL_MCA_CMD_LINE_ID))) {
            (void) mca_base_var_env_name (argv[i+1], &param);
            opal_setenv(param, argv[i+2], true, &environ);
            free(param);
        } else if (0 == strcmp(argv[i], "-am") ||
                   0 == strcmp(argv[i], "--am")) {
            (void)mca_base_var_env_name("mca_base_param_file_prefix", &param);
            opal_setenv(param, argv[i+1], true, &environ);
            free(param);
        } else if (0 == strcmp(argv[i], "-tune") ||
                   0 == strcmp(argv[i], "--tune")) {
            (void)mca_base_var_env_name("mca_base_envar_file_prefix", &param);
            opal_setenv(param, argv[i+1], true, &environ);
            free(param);
        }
    }

    /* init only the util portion of OPAL */
    if (OPAL_SUCCESS != (rc = opal_init_util(&argc, &argv))) {
        return rc;
    }
    /* open the SCHIZO framework so we can setup the command line */
    if (ORTE_SUCCESS != (rc = mca_base_framework_open(&orte_schizo_base_framework, 0))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    if (ORTE_SUCCESS != (rc = orte_schizo_base_select())) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    OBJ_CONSTRUCT(&tool_jobs, opal_pointer_array_t);
    opal_pointer_array_init(&tool_jobs, 256, INT_MAX, 128);


    /* setup the cmd line */
    orte_cmd_line = OBJ_NEW(opal_cmd_line_t);

    /* if they were provided, add the opts */
    if (NULL != opts) {
        if (OPAL_SUCCESS != (rc = opal_cmd_line_add(orte_cmd_line, opts))) {
            return rc;
        }
    }

    /* setup the rest of the cmd line only once */
    if (OPAL_SUCCESS != (rc = orte_schizo.define_cli(orte_cmd_line))) {
        return rc;
    }

    /* now that options have been defined, finish setup */
    mca_base_cmd_line_setup(orte_cmd_line);

    /* parse the result to get values */
    if (OPAL_SUCCESS != (rc = opal_cmd_line_parse(orte_cmd_line,
                                                  true, false, argc, argv)) ) {
        if (OPAL_ERR_SILENT != rc) {
            fprintf(stderr, "%s: command line error (%s)\n", argv[0],
                    opal_strerror(rc));
        }
        return rc;
    }

    /* see if print version is requested. Do this before
     * check for help so that --version --help works as
     * one might expect. */
     if (orte_cmd_options.version) {
        char *str, *project_name = NULL;
        if (0 == strcmp(orte_basename, "mpirun")) {
            project_name = "Open MPI";
        } else {
            project_name = "OpenRTE";
        }
        str = opal_info_make_version_str("all",
                                         OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                         OPAL_RELEASE_VERSION,
                                         OPAL_GREEK_VERSION,
                                         OPAL_REPO_REV);
        if (NULL != str) {
            fprintf(stdout, "%s (%s) %s\n\nReport bugs to %s\n",
                    orte_basename, project_name, str, PACKAGE_BUGREPORT);
            free(str);
        }
        exit(0);
    }

    /* check if we are running as root - if we are, then only allow
     * us to proceed if the allow-run-as-root flag was given. Otherwise,
     * exit with a giant warning flag
     */
    if (0 == geteuid() && !orte_cmd_options.run_as_root) {
        /* check for two envars that allow override of this protection */
        char *r1, *r2;
        if (NULL != (r1 = getenv("OMPI_ALLOW_RUN_AS_ROOT")) &&
            NULL != (r2 = getenv("OMPI_ALLOW_RUN_AS_ROOT_CONFIRM"))) {
            if (0 == strcmp(r1, "1") && 0 == strcmp(r2, "1")) {
                goto moveon;
            }
        }
        /* show_help is not yet available, so print an error manually */
        fprintf(stderr, "--------------------------------------------------------------------------\n");
        if (orte_cmd_options.help) {
            fprintf(stderr, "%s cannot provide the help message when run as root.\n\n", orte_basename);
        } else {
            fprintf(stderr, "%s has detected an attempt to run as root.\n\n", orte_basename);
        }

        fprintf(stderr, "Running as root is *strongly* discouraged as any mistake (e.g., in\n");
        fprintf(stderr, "defining TMPDIR) or bug can result in catastrophic damage to the OS\n");
        fprintf(stderr, "file system, leaving your system in an unusable state.\n\n");

        fprintf(stderr, "We strongly suggest that you run %s as a non-root user.\n\n", orte_basename);

        fprintf(stderr, "You can override this protection by adding the --allow-run-as-root option\n");
        fprintf(stderr, "to the cmd line or by setting two environment variables in the following way:\n");
        fprintf(stderr, "the variable OMPI_ALLOW_RUN_AS_ROOT=1 to indicate the desire to override this\n");
        fprintf(stderr, "protection, and OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 to confirm the choice and\n");
        fprintf(stderr, "add one more layer of certainty that you want to do so.\n");
        fprintf(stderr, "We reiterate our advice against doing so - please proceed at your own risk.\n");
        fprintf(stderr, "--------------------------------------------------------------------------\n");
        exit(1);
    }

  moveon:
    /* process any mca params */
    rc = mca_base_cmd_line_process_args(orte_cmd_line, &environ, &environ);
    if (ORTE_SUCCESS != rc) {
        return rc;
    }

    /* Need to initialize OPAL so that install_dirs are filled in */
    if (OPAL_SUCCESS != (rc = opal_init(&argc, &argv))) {
        return rc;
    }

    /* Check for help request */
    if (NULL != orte_cmd_options.help) {
        print_help();

        /* If someone asks for help, that should be all we do */
        exit(0);
    }

    /* if they already set our proc type, then leave it alone */
    if (ORTE_PROC_TYPE_NONE == orte_process_info.proc_type) {
   /* set the flags - if they gave us a -hnp option, then
         * we are a tool. If not, then we are an HNP */
        if (NULL == orte_cmd_options.hnp) {
            orte_process_info.proc_type = ORTE_PROC_HNP;
        } else {
            orte_process_info.proc_type = ORTE_PROC_TOOL;
        }
    }
    if (ORTE_PROC_IS_TOOL) {
        if (0 == strncasecmp(orte_cmd_options.hnp, "file", strlen("file"))) {
            char input[1024], *filename;
            FILE *fp;

            /* it is a file - get the filename */
            filename = strchr(orte_cmd_options.hnp, ':');
            if (NULL == filename) {
                /* filename is not correctly formatted */
                orte_show_help("help-orte-top.txt", "orte-top:hnp-filename-bad", true, "uri", orte_cmd_options.hnp);
                exit(1);
            }
            ++filename; /* space past the : */

            if (0 >= strlen(filename)) {
                /* they forgot to give us the name! */
                orte_show_help("help-orte-top.txt", "orte-top:hnp-filename-bad", true, "uri", orte_cmd_options.hnp);
                exit(1);
            }

            /* open the file and extract the uri */
            fp = fopen(filename, "r");
            if (NULL == fp) { /* can't find or read file! */
                orte_show_help("help-orte-top.txt", "orte-top:hnp-filename-access", true, orte_cmd_options.hnp);
                exit(1);
            }
            /* initialize the input to NULLs to ensure any input
             * string is NULL-terminated */
            memset(input, 0, 1024);
            if (NULL == fgets(input, 1024, fp)) {
                /* something malformed about file */
                fclose(fp);
                orte_show_help("help-orte-top.txt", "orte-top:hnp-file-bad", true, orte_cmd_options.hnp);
                exit(1);
            }
            fclose(fp);
            input[strlen(input)-1] = '\0';  /* remove newline */
            /* construct the target hnp info */
            opal_setenv(OPAL_MCA_PREFIX"orte_hnp_uri", input, true, &environ);
        } else {
            /* should just be the uri itself - construct the target hnp info */
            opal_setenv(OPAL_MCA_PREFIX"orte_hnp_uri", orte_cmd_options.hnp, true, &environ);
        }
        /* we are never allowed to operate as a distributed tool,
         * so insist on the ess/tool component */
        opal_setenv(OPAL_MCA_PREFIX"ess", "tool", true, &environ);
    } else {
        /* may look strange, but the way we handle prefix is a little weird
         * and probably needs to be addressed more fully at some future point.
         * For now, we have a conflict between app_files and cmd line usage.
         * Since app_files are used by the C/R system, we will make an
         * adjustment here to avoid perturbing that system.
         *
         * We cannot just have the cmd line parser place any found value
         * in the global struct as the app_file parser would replace it.
         * So handle this specific cmd line option manually.
         */
        orte_cmd_options.prefix = NULL;
        orte_cmd_options.path_to_mpirun = NULL;
        if (opal_cmd_line_is_taken(orte_cmd_line, "prefix") ||
            '/' == argv[0][0] || want_prefix_by_default) {
            size_t param_len;
            if ('/' == argv[0][0]) {
                char* tmp_basename = NULL;
                /* If they specified an absolute path, strip off the
                   /bin/<exec_name>" and leave just the prefix */
                orte_cmd_options.path_to_mpirun = opal_dirname(argv[0]);
                /* Quick sanity check to ensure we got
                   something/bin/<exec_name> and that the installation
                   tree is at least more or less what we expect it to
                   be */
                tmp_basename = opal_basename(orte_cmd_options.path_to_mpirun);
                if (0 == strcmp("bin", tmp_basename)) {
                    char* tmp = orte_cmd_options.path_to_mpirun;
                    orte_cmd_options.path_to_mpirun = opal_dirname(tmp);
                    free(tmp);
                } else {
                    free(orte_cmd_options.path_to_mpirun);
                    orte_cmd_options.path_to_mpirun = NULL;
                }
                free(tmp_basename);
            }
            /* if both are given, check to see if they match */
            if (opal_cmd_line_is_taken(orte_cmd_line, "prefix") &&
                NULL != orte_cmd_options.path_to_mpirun) {
                char *tmp_basename;
                /* if they don't match, then that merits a warning */
                param = strdup(opal_cmd_line_get_param(orte_cmd_line, "prefix", 0, 0));
                /* ensure we strip any trailing '/' */
                if (0 == strcmp(OPAL_PATH_SEP, &(param[strlen(param)-1]))) {
                    param[strlen(param)-1] = '\0';
                }
                tmp_basename = strdup(orte_cmd_options.path_to_mpirun);
                if (0 == strcmp(OPAL_PATH_SEP, &(tmp_basename[strlen(tmp_basename)-1]))) {
                    tmp_basename[strlen(tmp_basename)-1] = '\0';
                }
                if (0 != strcmp(param, tmp_basename)) {
                    orte_show_help("help-orterun.txt", "orterun:double-prefix",
                                   true, orte_basename, orte_basename,
                                   param, tmp_basename, orte_basename);
                    /* use the prefix over the path-to-mpirun so that
                     * people can specify the backend prefix as different
                     * from the local one
                     */
                    free(orte_cmd_options.path_to_mpirun);
                    orte_cmd_options.path_to_mpirun = NULL;
                }
                free(tmp_basename);
            } else if (NULL != orte_cmd_options.path_to_mpirun) {
                param = strdup(orte_cmd_options.path_to_mpirun);
            } else if (opal_cmd_line_is_taken(orte_cmd_line, "prefix")){
                /* must be --prefix alone */
                param = strdup(opal_cmd_line_get_param(orte_cmd_line, "prefix", 0, 0));
            } else {
                /* --enable-orterun-prefix-default was given to orterun */
                param = strdup(opal_install_dirs.prefix);
            }

            if (NULL != param) {
                /* "Parse" the param, aka remove superfluous path_sep. */
                param_len = strlen(param);
                while (0 == strcmp (OPAL_PATH_SEP, &(param[param_len-1]))) {
                    param[param_len-1] = '\0';
                    param_len--;
                    if (0 == param_len) {
                        orte_show_help("help-orterun.txt", "orterun:empty-prefix",
                                       true, orte_basename, orte_basename);
                        free(param);
                        return ORTE_ERR_FATAL;
                    }
                }

                orte_cmd_options.prefix = param;
            }
            want_prefix_by_default = true;
        }
    }

    /* Setup MCA params */
    orte_register_params();

    if (orte_cmd_options.debug) {
        orte_devel_level_output = true;
    }

    /* Initialize our Open RTE environment
      * Set the flag telling orte_init that I am NOT a
      * singleton, but am "infrastructure" - prevents setting
      * up incorrect infrastructure that only a singleton would
      * require
      */
    if (ORTE_SUCCESS != (rc = orte_init(&argc, &argv,
                                        orte_process_info.proc_type))) {
        /* cannot call ORTE_ERROR_LOG as it could be the errmgr
         * never got loaded!
         */
        return rc;
    }
    /* finalize OPAL. As it was opened again from orte_init->opal_init
     * we continue to have a reference count on it. So we have to finalize it twice...
     */
    opal_finalize();

    if (ORTE_PROC_IS_TOOL) {
        opal_value_t val;
        /* extract the name */
        if (ORTE_SUCCESS != orte_rml_base_parse_uris(orte_process_info.my_hnp_uri, ORTE_PROC_MY_HNP, NULL)) {
            orte_show_help("help-orte-top.txt", "orte-top:hnp-uri-bad", true, orte_process_info.my_hnp_uri);
            exit(1);
        }
        /* set the info in our contact table */
        OBJ_CONSTRUCT(&val, opal_value_t);
        val.key = OPAL_PMIX_PROC_URI;
        val.type = OPAL_STRING;
        val.data.string = orte_process_info.my_hnp_uri;
        if (OPAL_SUCCESS != opal_pmix.store_local(ORTE_PROC_MY_HNP, &val)) {
            val.key = NULL;
            val.data.string = NULL;
            OBJ_DESTRUCT(&val);
            orte_show_help("help-orte-top.txt", "orte-top:hnp-uri-bad", true, orte_process_info.my_hnp_uri);
            orte_finalize();
            exit(1);
        }
        val.key = NULL;
        val.data.string = NULL;
        OBJ_DESTRUCT(&val);

        /* set the route to be direct */
        if (ORTE_SUCCESS != orte_routed.update_route(NULL, ORTE_PROC_MY_HNP, ORTE_PROC_MY_HNP)) {
            orte_show_help("help-orte-top.txt", "orte-top:hnp-uri-bad", true, orte_process_info.my_hnp_uri);
            orte_finalize();
            exit(1);
        }

        /* set the target hnp as our lifeline so we will terminate if it exits */
        orte_routed.set_lifeline(NULL, ORTE_PROC_MY_HNP);

        /* setup to listen for HNP response to my commands */
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_NOTIFY_COMPLETE,
                                ORTE_RML_PERSISTENT, complete_recv, NULL);
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_LAUNCH_RESP,
                                ORTE_RML_PERSISTENT, launch_recv, NULL);
    } else {
        /* save the environment for launch purposes. This MUST be
         * done so that we can pass it to any local procs we
         * spawn - otherwise, those local procs won't see any
         * non-MCA envars were set in the enviro prior to calling
         * orterun
         */
        orte_launch_environ = opal_argv_copy(environ);
        /* clear params from the environment so our children
         * don't pick them up */
        opal_unsetenv(OPAL_MCA_PREFIX"ess", &orte_launch_environ);
        opal_unsetenv(OPAL_MCA_PREFIX"pmix", &orte_launch_environ);
    }

    return ORTE_SUCCESS;
}

static void print_help()
{
    char *str = NULL, *args;
    char *project_name = NULL;

    if (0 == strcmp(orte_basename, "mpirun")) {
        project_name = "Open MPI";
    } else {
        project_name = "OpenRTE";
    }
    args = opal_cmd_line_get_usage_msg(orte_cmd_line);
    str = opal_show_help_string("help-orterun.txt", "orterun:usage", false,
                                 orte_basename, project_name, OPAL_VERSION,
                                 orte_basename, args,
                                 PACKAGE_BUGREPORT);
    if (NULL != str) {
        printf("%s", str);
        free(str);
    }
    free(args);
}

void orte_submit_finalize(void)
{
    trackr_t *trk;
    int i, rc;

    for (i=0; i < tool_jobs.size; i++) {
        if (NULL != (trk = (trackr_t*)opal_pointer_array_get_item(&tool_jobs, i))) {
            OBJ_RELEASE(trk);
        }
    }
    OBJ_DESTRUCT(&tool_jobs);

    /* close the SCHIZO framework */
    if (ORTE_SUCCESS != (rc = mca_base_framework_close(&orte_schizo_base_framework))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* finalize only the util portion of OPAL */
    if (OPAL_SUCCESS != (rc = opal_finalize_util())) {
        return;
    }

    /* destruct the cmd line object */
    if (NULL != orte_cmd_line) {
        OBJ_RELEASE(orte_cmd_line);
    }

    /* if it was created, remove the debugger attach fifo */
    if (0 <= orte_debugger_attach_fd) {
        if (orte_debugger_fifo_active) {
            opal_event_del(orte_debugger_attach);
            free(orte_debugger_attach);
        }
        close(orte_debugger_attach_fd);
        unlink(MPIR_attach_fifo);
    }

    if (NULL != orte_cmd_options.prefix) {
        free(orte_cmd_options.prefix);
    }
    if (NULL != orte_launch_environ) {
        opal_argv_free(orte_launch_environ);
    }
    if (NULL != orte_basename) {
        free(orte_basename);
    }
}

int orte_submit_cancel(int index) {

    int rc;
    trackr_t *trk;
    opal_buffer_t *req;
    orte_daemon_cmd_flag_t cmd = ORTE_DAEMON_TERMINATE_JOB_CMD;

    /* get the tracker */
    if (NULL == (trk = (trackr_t*)opal_pointer_array_get_item(&tool_jobs, index))) {
        opal_output(0, "TRACKER ID %d RETURNED INDEX TO NULL OBJECT", index);
        return ORTE_ERROR;
    }

    /* create and send request with command and jobid */
    req = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(req, &cmd, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(req, &trk->jdata->jobid, 1, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                 ORTE_PROC_MY_HNP, req, ORTE_RML_TAG_DAEMON,
                                 orte_rml_send_callback, NULL);
    if (ORTE_SUCCESS != rc) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    return ORTE_ERR_OP_IN_PROGRESS;
}


int orte_submit_halt(void)
{
    int rc;
    opal_buffer_t *req;
    orte_daemon_cmd_flag_t cmd = ORTE_DAEMON_HALT_DVM_CMD;

    req = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(req, &cmd, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                 ORTE_PROC_MY_HNP, req,
                                 ORTE_RML_TAG_DAEMON,
                                 orte_rml_send_callback, NULL);
    if (ORTE_SUCCESS != rc) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(req);
        return rc;
    }

    return ORTE_ERR_OP_IN_PROGRESS;
}

//
// The real thing
//
int orte_submit_job(char *argv[], int *index,
                    orte_submit_cbfunc_t launch_cb,
                    void *launch_cbdata,
                    orte_submit_cbfunc_t complete_cb,
                    void *complete_cbdata)
{
    opal_buffer_t *req;
    int rc, n;
    orte_app_idx_t i;
    orte_daemon_cmd_flag_t cmd = ORTE_DAEMON_SPAWN_JOB_CMD;
    char *param;
    orte_job_t *jdata = NULL, *daemons;
    orte_app_context_t *app, *dapp;
    trackr_t *trk;
    int argc;

    /* bozo check - we don't allow recursive calls of submit */
    if (NULL != getenv("OMPI_UNIVERSE_SIZE")) {
        fprintf(stderr, "\n\n**********************************************************\n\n");
        fprintf(stderr, "%s does not support recursive calls\n", orte_basename);
        fprintf(stderr, "\n**********************************************************\n");
        return ORTE_ERR_FATAL;
    }

    /* reset the globals every time thru as the argv
     * will modify them */
    init_globals();

    argc = opal_argv_count(argv);

    /* parse the cmd line - do this every time thru so we can
     * repopulate the globals */
    if (OPAL_SUCCESS != (rc = opal_cmd_line_parse(orte_cmd_line, true, false,
                                                  argc, argv)) ) {
        if (OPAL_ERR_SILENT != rc) {
            fprintf(stderr, "%s: command line error (%s)\n", argv[0],
                    opal_strerror(rc));
        }
        return rc;
    }

    /* Check for some "global" command line params */
    parse_globals(argc, argv, orte_cmd_line);

    /* create a new job object to hold the info for this one - the
     * jobid field will be filled in by the PLM when the job is
     * launched
     */
    jdata = OBJ_NEW(orte_job_t);
    if (NULL == jdata) {
        /* cannot call ORTE_ERROR_LOG as the errmgr
         * hasn't been loaded yet!
         */
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    /* see if they specified the personality */
    if (NULL != orte_cmd_options.personality) {
        jdata->personality = opal_argv_split(orte_cmd_options.personality, ',');
    } else {
        /* default to OMPI */
        opal_argv_append_nosize(&jdata->personality, "ompi");
    }

    trk = OBJ_NEW(trackr_t);
    trk->jdata = jdata;
    trk->launch_cb = launch_cb;
    trk->launch_cbdata = launch_cbdata;
    trk->complete_cb = complete_cb;
    trk->complete_cbdata = complete_cbdata;
    trk->index = opal_pointer_array_add(&tool_jobs, trk);


    /* pass our tracker ID */
    orte_set_attribute(&jdata->attributes, ORTE_JOB_ROOM_NUM, ORTE_ATTR_GLOBAL, &trk->index, OPAL_INT);

    /* check for stdout/err directives */
    /* if we were asked to tag output, mark it so */
    if (orte_cmd_options.tag_output) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_TAG_OUTPUT, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }
    /* if we were asked to timestamp output, mark it so */
    if (orte_cmd_options.timestamp_output) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_TIMESTAMP_OUTPUT, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }
    /* if we were asked to output to files, pass it along */
    if (NULL != orte_cmd_options.output_filename) {
        /* if the given filename isn't an absolute path, then
         * convert it to one so the name will be relative to
         * the directory where prun was given as that is what
         * the user will have seen */
        if (!opal_path_is_absolute(orte_cmd_options.output_filename)) {
            char cwd[OPAL_PATH_MAX], *path;
            getcwd(cwd, sizeof(cwd));
            path = opal_os_path(false, cwd, orte_cmd_options.output_filename, NULL);
            orte_set_attribute(&jdata->attributes, ORTE_JOB_OUTPUT_TO_FILE, ORTE_ATTR_GLOBAL, path, OPAL_STRING);
            free(path);
        } else {
            orte_set_attribute(&jdata->attributes, ORTE_JOB_OUTPUT_TO_FILE, ORTE_ATTR_GLOBAL, orte_cmd_options.output_filename, OPAL_STRING);
        }
    }
    /* if we were asked to merge stderr to stdout, mark it so */
    if (orte_cmd_options.merge) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_MERGE_STDERR_STDOUT, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }

    /* check what user wants us to do with stdin */
    if (NULL != orte_cmd_options.stdin_target) {
        if (0 == strcmp(orte_cmd_options.stdin_target, "all")) {
            jdata->stdin_target = ORTE_VPID_WILDCARD;
        } else if (0 == strcmp(orte_cmd_options.stdin_target, "none")) {
            jdata->stdin_target = ORTE_VPID_INVALID;
        } else {
            jdata->stdin_target = strtoul(orte_cmd_options.stdin_target, NULL, 10);
        }
    }

    /* if we want the argv's indexed, indicate that */
    if (orte_cmd_options.index_argv) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_INDEX_ARGV, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }

    /* Parse each app, adding it to the job object */
    parse_locals(jdata, argc, argv);

    if (0 == jdata->num_apps) {
        /* This should never happen -- this case should be caught in
           create_app(), but let's just double check... */
        orte_show_help("help-orterun.txt", "orterun:nothing-to-do",
                       true, orte_basename);
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return ORTE_ERR_FATAL;
    }

    /* create the map object to communicate policies */
    jdata->map = OBJ_NEW(orte_job_map_t);

    if (NULL != orte_cmd_options.mapping_policy) {
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_set_mapping_policy(jdata, &jdata->map->mapping, NULL, orte_cmd_options.mapping_policy))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    } else if (orte_cmd_options.pernode) {
        ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_PPR);
        ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_GIVEN);
        /* define the ppr */
        jdata->map->ppr = strdup("1:node");
    } else if (0 < orte_cmd_options.npernode) {
        ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_PPR);
        ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_GIVEN);
        /* define the ppr */
        (void)asprintf(&jdata->map->ppr, "%d:node", orte_cmd_options.npernode);
    } else if (0 < orte_cmd_options.npersocket) {
        ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_PPR);
        ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_GIVEN);
        /* define the ppr */
        (void)asprintf(&jdata->map->ppr, "%d:socket", orte_cmd_options.npersocket);
    }


    /* if the user specified cpus/rank, set it */
    if (0 < orte_cmd_options.cpus_per_proc) {
        jdata->map->cpus_per_rank = orte_cmd_options.cpus_per_proc;
    }
    /* if the user specified a ranking policy, then set it */
    if (NULL != orte_cmd_options.ranking_policy) {
        if (ORTE_SUCCESS != (rc = orte_rmaps_base_set_ranking_policy(&jdata->map->ranking,
                                                                     jdata->map->mapping,
                                                                     orte_cmd_options.ranking_policy))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }
    /* if the user specified a binding policy, then set it */
    if (NULL != orte_cmd_options.binding_policy) {
        if (ORTE_SUCCESS != (rc = opal_hwloc_base_set_binding_policy(&jdata->map->binding,
                                                                     orte_cmd_options.binding_policy))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
    }

    /* if they asked for nolocal, mark it so */
    if (orte_cmd_options.nolocal) {
        ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_USE_LOCAL);
    }
    if (orte_cmd_options.no_oversubscribe) {
        ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
    }
    if (orte_cmd_options.oversubscribe) {
        ORTE_UNSET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_NO_OVERSUBSCRIBE);
        ORTE_SET_MAPPING_DIRECTIVE(jdata->map->mapping, ORTE_MAPPING_SUBSCRIBE_GIVEN);
    }
    if (orte_cmd_options.report_bindings) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_REPORT_BINDINGS, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }
    if (orte_cmd_options.cpu_list) {
        orte_set_attribute(&jdata->attributes, ORTE_JOB_CPU_LIST, ORTE_ATTR_GLOBAL, orte_cmd_options.cpu_list, OPAL_STRING);
    }

    /* if recovery was enabled on the cmd line, do so */
    if (orte_enable_recovery) {
        ORTE_FLAG_SET(jdata, ORTE_JOB_FLAG_RECOVERABLE);
        if (0 == orte_max_restarts) {
            /* mark this job as continuously operating */
            orte_set_attribute(&jdata->attributes, ORTE_JOB_CONTINUOUS_OP, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
        }
    }
    /* record the max restarts */
    if (0 < orte_max_restarts) {
        for (i=0; i < jdata->num_apps; i++) {
            if (NULL != (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
                orte_set_attribute(&app->attributes, ORTE_APP_MAX_RESTARTS, ORTE_ATTR_GLOBAL, &orte_max_restarts, OPAL_INT32);
            }
        }
    }
    /* if continuous operation was specified */
    if (orte_cmd_options.continuous) {
        /* mark this job as continuously operating */
        orte_set_attribute(&jdata->attributes, ORTE_JOB_CONTINUOUS_OP, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    }

    /* check for debugger test envars and forward them if necessary */
    if (NULL != getenv("ORTE_TEST_DEBUGGER_ATTACH")) {
        char *evar;
        evar = getenv("ORTE_TEST_DEBUGGER_SLEEP");
        for (n=0; n < (int)jdata->num_apps; n++) {
            if (NULL != (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, n))) {
                opal_setenv("ORTE_TEST_DEBUGGER_ATTACH", "1", true, &app->env);
                if (NULL != evar) {
                    opal_setenv("ORTE_TEST_DEBUGGER_SLEEP", evar, true, &app->env);
                }
            }
        }
    }

    /* check for suicide test directives */
    if (NULL != getenv("ORTE_TEST_HNP_SUICIDE") ||
        NULL != getenv("ORTE_TEST_ORTED_SUICIDE")) {
        /* don't forward IO from this process so we can
         * see any debug after daemon termination */
        ORTE_FLAG_UNSET(jdata, ORTE_JOB_FLAG_FORWARD_OUTPUT);
    }

    /* check for a job timeout specification, to be provided in seconds
     * as that is what MPICH used
     */
    param = NULL;
    if (0 < orte_cmd_options.timeout ||
        NULL != (param = getenv("MPIEXEC_TIMEOUT"))) {
        if (NULL != param) {
            timeout_seconds = strtol(param, NULL, 10);
            /* both cannot be present, or they must agree */
            if (0 < orte_cmd_options.timeout && timeout_seconds != orte_cmd_options.timeout) {
                orte_show_help("help-orterun.txt", "orterun:timeoutconflict", false,
                               orte_basename, orte_cmd_options.timeout, param);
                exit(1);
            }
        } else {
            timeout_seconds = orte_cmd_options.timeout;
        }
        if (NULL == (orte_mpiexec_timeout = OBJ_NEW(orte_timer_t))) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERR_OUT_OF_RESOURCE);
            //goto DONE;
        }
        orte_mpiexec_timeout->tv.tv_sec = timeout_seconds;
        orte_mpiexec_timeout->tv.tv_usec = 0;
        opal_event_evtimer_set(orte_event_base, orte_mpiexec_timeout->ev,
                               orte_timeout_wakeup, jdata);
        opal_event_set_priority(orte_mpiexec_timeout->ev, ORTE_ERROR_PRI);
        opal_event_evtimer_add(orte_mpiexec_timeout->ev, &orte_mpiexec_timeout->tv);
    }

    /* check for diagnostic memory profile */
    if (NULL != (param = getenv("OMPI_MEMPROFILE"))) {
        timeout_seconds = strtol(param, NULL, 10);
        if (NULL == (orte_memprofile_timeout = OBJ_NEW(orte_timer_t))) {
            ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERR_OUT_OF_RESOURCE);
            //goto DONE;
        }
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_MEMPROFILE,
                                ORTE_RML_PERSISTENT, profile_recv, NULL);
        orte_memprofile_timeout->tv.tv_sec = timeout_seconds;
        orte_memprofile_timeout->tv.tv_usec = 0;
        opal_event_evtimer_set(orte_event_base, orte_memprofile_timeout->ev,
                               orte_profile_wakeup, jdata);
        opal_event_set_priority(orte_memprofile_timeout->ev, ORTE_ERROR_PRI);
        opal_event_evtimer_add(orte_memprofile_timeout->ev, &orte_memprofile_timeout->tv);
    }
    if (ORTE_PROC_IS_HNP) {
        /* get the daemon job object */
        daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

        /* check for request to report uri */
        if (NULL != orte_cmd_options.report_uri) {
            FILE *fp;
            char *rml_uri;
            orte_oob_base_get_addr(&rml_uri);
            if (0 == strcmp(orte_cmd_options.report_uri, "-")) {
                /* if '-', then output to stdout */
                printf("%s\n",  (NULL == rml_uri) ? "NULL" : rml_uri);
            } else if (0 == strcmp(orte_cmd_options.report_uri, "+")) {
                /* if '+', output to stderr */
                fprintf(stderr, "%s\n",  (NULL == rml_uri) ? "NULL" : rml_uri);
            } else {
                fp = fopen(orte_cmd_options.report_uri, "w");
                if (NULL == fp) {
                    orte_show_help("help-orterun.txt", "orterun:write_file", false,
                                   orte_basename, "uri", orte_cmd_options.report_uri);
                    exit(1);
                }
                fprintf(fp, "%s\n", (NULL == rml_uri) ? "NULL" : rml_uri);
                fclose(fp);
            }
            if (NULL != rml_uri) {
                free(rml_uri);
            }
        }
        /* If we have a prefix, then modify the PATH and
           LD_LIBRARY_PATH environment variables in our copy. This
           will ensure that any locally-spawned children will
           have our executables and libraries in their path

           For now, default to the prefix_dir provided in the first app_context.
           Since there always MUST be at least one app_context, we are safe in
           doing this.
        */
        param = NULL;
        if (NULL != (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, 0)) &&
            orte_get_attribute(&app->attributes, ORTE_APP_PREFIX_DIR, (void**)&param, OPAL_STRING)) {
            char *oldenv, *newenv, *lib_base, *bin_base;

            /* copy the prefix into the daemon job so that any launcher
             * can find the orteds when we launch the virtual machine
             */
            if (NULL == (dapp = (orte_app_context_t*)opal_pointer_array_get_item(daemons->apps, 0))) {
                /* that's an error in the ess */
                ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                return ORTE_ERR_NOT_FOUND;
            }
            orte_set_attribute(&dapp->attributes, ORTE_APP_PREFIX_DIR, ORTE_ATTR_LOCAL, param, OPAL_STRING);

            lib_base = opal_basename(opal_install_dirs.libdir);
            bin_base = opal_basename(opal_install_dirs.bindir);

            /* Reset PATH */
            newenv = opal_os_path( false, param, bin_base, NULL );
            oldenv = getenv("PATH");
            if (NULL != oldenv) {
                char *temp;
                asprintf(&temp, "%s:%s", newenv, oldenv );
                free( newenv );
                newenv = temp;
            }
            opal_setenv("PATH", newenv, true, &orte_launch_environ);
            if (orte_debug_flag) {
                opal_output(0, "%s: reset PATH: %s", orte_basename, newenv);
            }
            free(newenv);
            free(bin_base);

            /* Reset LD_LIBRARY_PATH */
            newenv = opal_os_path( false, param, lib_base, NULL );
            oldenv = getenv("LD_LIBRARY_PATH");
            if (NULL != oldenv) {
                char* temp;
                asprintf(&temp, "%s:%s", newenv, oldenv);
                free(newenv);
                newenv = temp;
            }
            opal_setenv("LD_LIBRARY_PATH", newenv, true, &orte_launch_environ);
            if (orte_debug_flag) {
                opal_output(0, "%s: reset LD_LIBRARY_PATH: %s",
                            orte_basename, newenv);
            }
            free(newenv);
            free(lib_base);
            free(param);
        }

        /* setup for debugging */
        orte_debugger_init_before_spawn(jdata);

        rc = orte_plm.spawn(jdata);
    } else {
        /* flag that we are using the DVM */
        orte_set_attribute(&jdata->attributes, ORTE_JOB_DVM_JOB, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
        /* flag that the allocation is static - i.e., the DVM is not allowed
         * to be adjusted once started, and all unused nodes are to be
         * removed from the node pool */
        orte_set_attribute(&jdata->attributes, ORTE_JOB_FIXED_DVM, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
        // pack the ORTE_DAEMON_SPAWN_JOB_CMD command and job object and send to HNP at tag ORTE_RML_TAG_DAEMON
        req = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(req, &cmd, 1, ORTE_DAEMON_CMD))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(req, &jdata, 1, ORTE_JOB))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(req, &trk->index, 1, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                ORTE_PROC_MY_HNP, req, ORTE_RML_TAG_DAEMON,
                                orte_rml_send_callback, NULL);

        /* Inform the caller of the tracker index if they passed a index pointer */
        if (NULL != index) {
            *index = trk->index;
        }
    }

    return ORTE_SUCCESS;

}


static int init_globals(void)
{
    /* Reset the other fields every time */
    orte_cmd_options.help = NULL;
    orte_cmd_options.version = false;
    orte_cmd_options.num_procs =  0;
    if (NULL != orte_cmd_options.appfile) {
        free(orte_cmd_options.appfile);
        orte_cmd_options.appfile = NULL;
    }
    if (NULL != orte_cmd_options.wdir) {
        free(orte_cmd_options.wdir);
        orte_cmd_options.wdir = NULL;
    }
    orte_cmd_options.set_cwd_to_session_dir = false;
    if (NULL != orte_cmd_options.path) {
        free(orte_cmd_options.path);
        orte_cmd_options.path = NULL;
    }
    if (NULL != orte_cmd_options.hnp) {
        free(orte_cmd_options.hnp);
        orte_cmd_options.hnp = NULL;
    }
    if (NULL != orte_cmd_options.stdin_target) {
        free(orte_cmd_options.stdin_target);
        orte_cmd_options.stdin_target = NULL ;
    }
    if (NULL != orte_cmd_options.output_filename) {
        free(orte_cmd_options.output_filename);
        orte_cmd_options.output_filename = NULL ;
    }
    if (NULL != orte_cmd_options.binding_policy) {
        free(orte_cmd_options.binding_policy);
        orte_cmd_options.binding_policy = NULL;
    }
    if (NULL != orte_cmd_options.mapping_policy) {
        free(orte_cmd_options.mapping_policy);
        orte_cmd_options.mapping_policy = NULL;
    }
    if (NULL != orte_cmd_options.ranking_policy) {
        free(orte_cmd_options.ranking_policy);
        orte_cmd_options.ranking_policy = NULL;
    }

    if (NULL != orte_cmd_options.report_pid) {
        free(orte_cmd_options.report_pid);
        orte_cmd_options.report_pid = NULL;
    }
    if (NULL != orte_cmd_options.report_uri) {
        free(orte_cmd_options.report_uri);
        orte_cmd_options.report_uri = NULL;
    }
    if (NULL != orte_cmd_options.cpu_list) {
        free(orte_cmd_options.cpu_list);
        orte_cmd_options.cpu_list= NULL;
    }
    orte_cmd_options.preload_binaries = false;
    if (NULL != orte_cmd_options.preload_files) {
        free(orte_cmd_options.preload_files);
        orte_cmd_options.preload_files  = NULL;
    }


    /* All done */
    return ORTE_SUCCESS;
}


static int parse_globals(int argc, char* argv[], opal_cmd_line_t *cmd_line)
{
    /* check for request to report pid */
    if (NULL != orte_cmd_options.report_pid) {
        FILE *fp;
        if (0 == strcmp(orte_cmd_options.report_pid, "-")) {
            /* if '-', then output to stdout */
            printf("%d\n", (int)getpid());
        } else if (0 == strcmp(orte_cmd_options.report_pid, "+")) {
            /* if '+', output to stderr */
            fprintf(stderr, "%d\n", (int)getpid());
        } else {
            fp = fopen(orte_cmd_options.report_pid, "w");
            if (NULL == fp) {
                orte_show_help("help-orterun.txt", "orterun:write_file", false,
                               orte_basename, "pid", orte_cmd_options.report_pid);
                exit(0);
            }
            fprintf(fp, "%d\n", (int)getpid());
            fclose(fp);
        }
    }

    /* Do we want a user-level debugger? */

    if (orte_cmd_options.debugger) {
        run_debugger(orte_basename, cmd_line, argc, argv, orte_cmd_options.num_procs);
    }

    return ORTE_SUCCESS;
}


static int parse_locals(orte_job_t *jdata, int argc, char* argv[])
{
    int i, rc, app_num;
    int temp_argc;
    char **temp_argv, **env;
    orte_app_context_t *app;
    bool made_app;
    orte_std_cntr_t j, size1;

    /* Make the apps */
    temp_argc = 0;
    temp_argv = NULL;
    opal_argv_append(&temp_argc, &temp_argv, argv[0]);

    /* NOTE: This bogus env variable is necessary in the calls to
       create_app(), below.  See comment immediately before the
       create_app() function for an explanation. */

    env = NULL;
    for (app_num = 0, i = 1; i < argc; ++i) {
        if (0 == strcmp(argv[i], ":")) {
            /* Make an app with this argv */
            if (opal_argv_count(temp_argv) > 1) {
                if (NULL != env) {
                    opal_argv_free(env);
                    env = NULL;
                }
                app = NULL;
                rc = create_app(temp_argc, temp_argv, jdata, &app, &made_app, &env);
                /** keep track of the number of apps - point this app_context to that index */
                if (ORTE_SUCCESS != rc) {
                    /* Assume that the error message has already been
                       printed; no need to cleanup -- we can just
                       exit */
                    exit(1);
                }
                if (made_app) {
                    app->idx = app_num;
                    ++app_num;
                    opal_pointer_array_add(jdata->apps, app);
                    ++jdata->num_apps;
                }

                /* Reset the temps */

                temp_argc = 0;
                temp_argv = NULL;
                opal_argv_append(&temp_argc, &temp_argv, argv[0]);
            }
        } else {
            opal_argv_append(&temp_argc, &temp_argv, argv[i]);
        }
    }

    if (opal_argv_count(temp_argv) > 1) {
        app = NULL;
        rc = create_app(temp_argc, temp_argv, jdata, &app, &made_app, &env);
        if (ORTE_SUCCESS != rc) {
            /* Assume that the error message has already been printed;
               no need to cleanup -- we can just exit */
            exit(1);
        }
        if (made_app) {
            app->idx = app_num;
            ++app_num;
            opal_pointer_array_add(jdata->apps, app);
            ++jdata->num_apps;
        }
    }
    if (NULL != env) {
        opal_argv_free(env);
    }
    opal_argv_free(temp_argv);

   /* Once we've created all the apps, add the global MCA params to
       each app's environment (checking for duplicates, of
       course -- yay opal_environ_merge()).  */

    if (NULL != global_mca_env) {
        size1 = (size_t)opal_pointer_array_get_size(jdata->apps);
        /* Iterate through all the apps */
        for (j = 0; j < size1; ++j) {
            app = (orte_app_context_t *)
                opal_pointer_array_get_item(jdata->apps, j);
            if (NULL != app) {
                /* Use handy utility function */
                env = opal_environ_merge(global_mca_env, app->env);
                opal_argv_free(app->env);
                app->env = env;
            }
        }
    }

    /* Now take a subset of the MCA params and set them as MCA
       overrides here in orterun (so that when we orte_init() later,
       all the components see these MCA params).  Here's how we decide
       which subset of the MCA params we set here in orterun:

       1. If any global MCA params were set, use those
       2. If no global MCA params were set and there was only one app,
          then use its app MCA params
       3. Otherwise, don't set any
    */

    env = NULL;
    if (NULL != global_mca_env) {
        env = global_mca_env;
    } else {
        if (opal_pointer_array_get_size(jdata->apps) >= 1) {
            /* Remember that pointer_array's can be padded with NULL
               entries; so only use the app's env if there is exactly
               1 non-NULL entry */
            app = (orte_app_context_t *)
                opal_pointer_array_get_item(jdata->apps, 0);
            if (NULL != app) {
                env = app->env;
                for (j = 1; j < opal_pointer_array_get_size(jdata->apps); ++j) {
                    if (NULL != opal_pointer_array_get_item(jdata->apps, j)) {
                        env = NULL;
                        break;
                    }
                }
            }
        }
    }

    if (NULL != env) {
        size1 = opal_argv_count(env);
        for (j = 0; j < size1; ++j) {
            /* Use-after-Free error possible here.  putenv does not copy
             * the string passed to it, and instead stores only the pointer.
             * env[j] may be freed later, in which case the pointer
             * in environ will now be left dangling into a deallocated
             * region.
             * So we make a copy of the variable.
             */
            char *value, *s = strdup(env[j]);

            if (NULL == s) {
                return OPAL_ERR_OUT_OF_RESOURCE;
            }

            value = strchr(s, '=');
            if (NULL != value) {
                value++;
            }
            opal_setenv(s, value, true, &environ);
            free(s);
        }
    }

    /* All done */

    return ORTE_SUCCESS;
}


/*
 * This function takes a "char ***app_env" parameter to handle the
 * specific case:
 *
 *   orterun --mca foo bar -app appfile
 *
 * That is, we'll need to keep foo=bar, but the presence of the app
 * file will cause an invocation of parse_appfile(), which will cause
 * one or more recursive calls back to create_app().  Since the
 * foo=bar value applies globally to all apps in the appfile, we need
 * to pass in the "base" environment (that contains the foo=bar value)
 * when we parse each line in the appfile.
 *
 * This is really just a special case -- when we have a simple case like:
 *
 *   orterun --mca foo bar -np 4 hostname
 *
 * Then the upper-level function (parse_locals()) calls create_app()
 * with a NULL value for app_env, meaning that there is no "base"
 * environment that the app needs to be created from.
 */
static int create_app(int argc, char* argv[],
                      orte_job_t *jdata,
                      orte_app_context_t **app_ptr,
                      bool *made_app, char ***app_env)
{
    char cwd[OPAL_PATH_MAX];
    int i, j, count, rc;
    char *param, *value;
    orte_app_context_t *app = NULL;
    bool found = false;
    char *appname = NULL;

    *made_app = false;

    /* Pre-process the command line if we are going to parse an appfile later.
     * save any mca command line args so they can be passed
     * separately to the daemons.
     * Use Case:
     *  $ cat launch.appfile
     *  -np 1 -mca aaa bbb ./my-app -mca ccc ddd
     *  -np 1 -mca aaa bbb ./my-app -mca eee fff
     *  $ mpirun -np 2 -mca foo bar --app launch.appfile
     * Only pick up '-mca foo bar' on this pass.
     */
    if (NULL != orte_cmd_options.appfile) {
        if (ORTE_SUCCESS != (rc = orte_schizo.parse_cli(argc, 0, argv))) {
            goto cleanup;
        }
    }

    /* Parse application command line options. */
    init_globals();
    /* parse the cmd line - do this every time thru so we can
     * repopulate the globals */
    if (OPAL_SUCCESS != (rc = opal_cmd_line_parse(orte_cmd_line, true, false,
                                                  argc, argv)) ) {
        if (OPAL_ERR_SILENT != rc) {
            fprintf(stderr, "%s: command line error (%s)\n", argv[0],
                    opal_strerror(rc));
        }
        return rc;
    }

    /* Is there an appfile in here? */
    if (NULL != orte_cmd_options.appfile) {
        return parse_appfile(jdata, strdup(orte_cmd_options.appfile), app_env);
    }

    /* Setup application context */
    app = OBJ_NEW(orte_app_context_t);
    opal_cmd_line_get_tail(orte_cmd_line, &count, &app->argv);

    /* See if we have anything left */
    if (0 == count) {
        orte_show_help("help-orterun.txt", "orterun:executable-not-specified",
                       true, orte_basename, orte_basename);
        rc = ORTE_ERR_NOT_FOUND;
        goto cleanup;
    }

    /*
     * Get mca parameters so we can pass them to the daemons.
     * Use the count determined above to make sure we do not go past
     * the executable name. Example:
     *   mpirun -np 2 -mca foo bar ./my-app -mca bip bop
     * We want to pick up '-mca foo bar' but not '-mca bip bop'
     */
    if (ORTE_SUCCESS != (rc = orte_schizo.parse_cli(argc, count, argv))) {
        goto cleanup;
    }

    /* Grab all MCA environment variables */

    app->env = opal_argv_copy(*app_env);
    if (ORTE_SUCCESS != (rc = orte_schizo.parse_env(orte_cmd_options.path,
                                                    orte_cmd_line,
                                                    environ, &app->env))) {
        goto cleanup;
    }


    /* Did the user request a specific wdir? */

    if (NULL != orte_cmd_options.wdir) {
        /* if this is a relative path, convert it to an absolute path */
        if (opal_path_is_absolute(orte_cmd_options.wdir)) {
            app->cwd = strdup(orte_cmd_options.wdir);
        } else {
            /* get the cwd */
            if (OPAL_SUCCESS != (rc = opal_getcwd(cwd, sizeof(cwd)))) {
                orte_show_help("help-orterun.txt", "orterun:init-failure",
                               true, "get the cwd", rc);
                goto cleanup;
            }
            /* construct the absolute path */
            app->cwd = opal_os_path(false, cwd, orte_cmd_options.wdir, NULL);
        }
        orte_set_attribute(&app->attributes, ORTE_APP_USER_CWD, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    } else if (orte_cmd_options.set_cwd_to_session_dir) {
        orte_set_attribute(&app->attributes, ORTE_APP_SSNDIR_CWD, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
        orte_set_attribute(&app->attributes, ORTE_APP_USER_CWD, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
    } else {
        if (OPAL_SUCCESS != (rc = opal_getcwd(cwd, sizeof(cwd)))) {
            orte_show_help("help-orterun.txt", "orterun:init-failure",
                           true, "get the cwd", rc);
            goto cleanup;
        }
        app->cwd = strdup(cwd);
    }

    /* if this is the first app_context, check for prefix directions.
     * We only do this for the first app_context because the launchers
     * only look at the first one when setting the prefix - we do NOT
     * support per-app_context prefix settings!
     */
    if (0 == total_num_apps) {
        /* Check to see if the user explicitly wanted to disable automatic
           --prefix behavior */

        if (opal_cmd_line_is_taken(orte_cmd_line, "noprefix")) {
            want_prefix_by_default = false;
        }

        /* Did the user specify a prefix, or want prefix by default? */
        if (opal_cmd_line_is_taken(orte_cmd_line, "prefix") || want_prefix_by_default) {
            size_t param_len;
            /* if both the prefix was given and we have a prefix
             * given above, check to see if they match
             */
            if (opal_cmd_line_is_taken(orte_cmd_line, "prefix") &&
                NULL != orte_cmd_options.prefix) {
                /* if they don't match, then that merits a warning */
                param = strdup(opal_cmd_line_get_param(orte_cmd_line, "prefix", 0, 0));
                /* ensure we strip any trailing '/' */
                if (0 == strcmp(OPAL_PATH_SEP, &(param[strlen(param)-1]))) {
                    param[strlen(param)-1] = '\0';
                }
                value = strdup(orte_cmd_options.prefix);
                if (0 == strcmp(OPAL_PATH_SEP, &(value[strlen(value)-1]))) {
                    value[strlen(value)-1] = '\0';
                }
                if (0 != strcmp(param, value)) {
                    orte_show_help("help-orterun.txt", "orterun:app-prefix-conflict",
                                   true, orte_basename, value, param);
                    /* let the global-level prefix take precedence since we
                     * know that one is being used
                     */
                    free(param);
                    param = strdup(orte_cmd_options.prefix);
                }
                free(value);
            } else if (NULL != orte_cmd_options.prefix) {
                param = strdup(orte_cmd_options.prefix);
            } else if (opal_cmd_line_is_taken(orte_cmd_line, "prefix")){
                /* must be --prefix alone */
                param = strdup(opal_cmd_line_get_param(orte_cmd_line, "prefix", 0, 0));
            } else {
                /* --enable-orterun-prefix-default was given to orterun */
                param = strdup(opal_install_dirs.prefix);
            }

            if (NULL != param) {
                /* "Parse" the param, aka remove superfluous path_sep. */
                param_len = strlen(param);
                while (0 == strcmp (OPAL_PATH_SEP, &(param[param_len-1]))) {
                    param[param_len-1] = '\0';
                    param_len--;
                    if (0 == param_len) {
                        orte_show_help("help-orterun.txt", "orterun:empty-prefix",
                                       true, orte_basename, orte_basename);
                        free(param);
                        return ORTE_ERR_FATAL;
                    }
                }
                orte_set_attribute(&app->attributes, ORTE_APP_PREFIX_DIR, ORTE_ATTR_GLOBAL, param, OPAL_STRING);
                free(param);
            }
        }
    }

    /* Did the user specify a hostfile. Need to check for both
     * hostfile and machine file.
     * We can only deal with one hostfile per app context, otherwise give an error.
     */
    if (0 < (j = opal_cmd_line_get_ninsts(orte_cmd_line, "hostfile"))) {
        if(1 < j) {
            orte_show_help("help-orterun.txt", "orterun:multiple-hostfiles",
                           true, orte_basename, NULL);
            return ORTE_ERR_FATAL;
        } else {
            value = opal_cmd_line_get_param(orte_cmd_line, "hostfile", 0, 0);
            orte_set_attribute(&app->attributes, ORTE_APP_HOSTFILE, ORTE_ATTR_GLOBAL, value, OPAL_STRING);
        }
    }
    if (0 < (j = opal_cmd_line_get_ninsts(orte_cmd_line, "machinefile"))) {
        if(1 < j || orte_get_attribute(&app->attributes, ORTE_APP_HOSTFILE, NULL, OPAL_STRING)) {
            orte_show_help("help-orterun.txt", "orterun:multiple-hostfiles",
                           true, orte_basename, NULL);
            return ORTE_ERR_FATAL;
        } else {
            value = opal_cmd_line_get_param(orte_cmd_line, "machinefile", 0, 0);
            orte_set_attribute(&app->attributes, ORTE_APP_HOSTFILE, ORTE_ATTR_GLOBAL, value, OPAL_STRING);
        }
    }

    /* Did the user specify any hosts? */
    if (0 < (j = opal_cmd_line_get_ninsts(orte_cmd_line, "host"))) {
        char **targ=NULL, *tval;
        for (i = 0; i < j; ++i) {
            value = opal_cmd_line_get_param(orte_cmd_line, "host", i, 0);
            opal_argv_append_nosize(&targ, value);
        }
        tval = opal_argv_join(targ, ',');
        orte_set_attribute(&app->attributes, ORTE_APP_DASH_HOST, ORTE_ATTR_GLOBAL, tval, OPAL_STRING);
        opal_argv_free(targ);
        free(tval);
    } else if (NULL != orte_default_dash_host) {
        orte_set_attribute(&app->attributes, ORTE_APP_DASH_HOST, ORTE_ATTR_LOCAL,
                           orte_default_dash_host, OPAL_STRING);
    }

    /* check for bozo error */
    if (0 > orte_cmd_options.num_procs) {
        orte_show_help("help-orterun.txt", "orterun:negative-nprocs",
                       true, orte_basename, app->argv[0],
                       orte_cmd_options.num_procs, NULL);
        return ORTE_ERR_FATAL;
    }

    app->num_procs = (orte_std_cntr_t)orte_cmd_options.num_procs;
    total_num_apps++;

    /* see if we need to preload the binary to
     * find the app - don't do this for java apps, however, as we
     * can't easily find the class on the cmd line. Java apps have to
     * preload their binary via the preload_files option
     */
    if (NULL == strstr(app->argv[0], "java")) {
        if (orte_cmd_options.preload_binaries) {
            orte_set_attribute(&app->attributes, ORTE_APP_SSNDIR_CWD, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
            orte_set_attribute(&app->attributes, ORTE_APP_PRELOAD_BIN, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
            /* no harm in setting this attribute twice as the function will simply ignore it */
            orte_set_attribute(&app->attributes, ORTE_APP_USER_CWD, ORTE_ATTR_GLOBAL, NULL, OPAL_BOOL);
        }
    }
    if (NULL != orte_cmd_options.preload_files) {
        orte_set_attribute(&app->attributes, ORTE_APP_PRELOAD_FILES, ORTE_ATTR_GLOBAL,
                           orte_cmd_options.preload_files, OPAL_STRING);
    }

    /* Do not try to find argv[0] here -- the starter is responsible
       for that because it may not be relevant to try to find it on
       the node where orterun is executing.  So just strdup() argv[0]
       into app. */

    app->app = strdup(app->argv[0]);
    if (NULL == app->app) {
        orte_show_help("help-orterun.txt", "orterun:call-failed",
                       true, orte_basename, "library", "strdup returned NULL", errno);
        rc = ORTE_ERR_NOT_FOUND;
        goto cleanup;
    }

    /* if this is a Java application, we have a bit more work to do. Such
     * applications actually need to be run under the Java virtual machine
     * and the "java" command will start the "executable". So we need to ensure
     * that all the proper java-specific paths are provided
     */
    appname = opal_basename(app->app);
    if (0 == strcmp(appname, "java")) {
        /* see if we were given a library path */
        found = false;
        for (i=1; NULL != app->argv[i]; i++) {
            if (NULL != strstr(app->argv[i], "java.library.path")) {
                char *dptr;
                /* find the '=' that delineates the option from the path */
                if (NULL == (dptr = strchr(app->argv[i], '='))) {
                    /* that's just wrong */
                    rc = ORTE_ERR_BAD_PARAM;
                    goto cleanup;
                }
                /* step over the '=' */
                ++dptr;
                /* yep - but does it include the path to the mpi libs? */
                found = true;
                if (NULL == strstr(app->argv[i], opal_install_dirs.libdir)) {
                    /* doesn't appear to - add it to be safe */
                    if (':' == app->argv[i][strlen(app->argv[i]-1)]) {
                        asprintf(&value, "-Djava.library.path=%s%s", dptr, opal_install_dirs.libdir);
                    } else {
                        asprintf(&value, "-Djava.library.path=%s:%s", dptr, opal_install_dirs.libdir);
                    }
                    free(app->argv[i]);
                    app->argv[i] = value;
                }
                break;
            }
        }
        if (!found) {
            /* need to add it right after the java command */
            asprintf(&value, "-Djava.library.path=%s", opal_install_dirs.libdir);
            opal_argv_insert_element(&app->argv, 1, value);
            free(value);
        }

        /* see if we were given a class path */
        found = false;
        for (i=1; NULL != app->argv[i]; i++) {
            if (NULL != strstr(app->argv[i], "cp") ||
                NULL != strstr(app->argv[i], "classpath")) {
                /* yep - but does it include the path to the mpi libs? */
                found = true;
                /* check if mpi.jar exists - if so, add it */
                value = opal_os_path(false, opal_install_dirs.libdir, "mpi.jar", NULL);
                if (access(value, F_OK ) != -1) {
                    set_classpath_jar_file(app, i+1, "mpi.jar");
                }
                free(value);
                /* check for oshmem support */
                value = opal_os_path(false, opal_install_dirs.libdir, "shmem.jar", NULL);
                if (access(value, F_OK ) != -1) {
                    set_classpath_jar_file(app, i+1, "shmem.jar");
                }
                free(value);
                /* always add the local directory */
                asprintf(&value, "%s:%s", app->cwd, app->argv[i+1]);
                free(app->argv[i+1]);
                app->argv[i+1] = value;
                break;
            }
        }
        if (!found) {
            /* check to see if CLASSPATH is in the environment */
            found = false;  // just to be pedantic
            for (i=0; NULL != environ[i]; i++) {
                if (0 == strncmp(environ[i], "CLASSPATH", strlen("CLASSPATH"))) {
                    value = strchr(environ[i], '=');
                    ++value; /* step over the = */
                    opal_argv_insert_element(&app->argv, 1, value);
                    /* check for mpi.jar */
                    value = opal_os_path(false, opal_install_dirs.libdir, "mpi.jar", NULL);
                    if (access(value, F_OK ) != -1) {
                        set_classpath_jar_file(app, 1, "mpi.jar");
                    }
                    free(value);
                    /* check for shmem.jar */
                    value = opal_os_path(false, opal_install_dirs.libdir, "shmem.jar", NULL);
                    if (access(value, F_OK ) != -1) {
                        set_classpath_jar_file(app, 1, "shmem.jar");
                    }
                    free(value);
                    /* always add the local directory */
                    (void)asprintf(&value, "%s:%s", app->cwd, app->argv[1]);
                    free(app->argv[1]);
                    app->argv[1] = value;
                    opal_argv_insert_element(&app->argv, 1, "-cp");
                    found = true;
                    break;
                }
            }
            if (!found) {
                /* need to add it right after the java command - have
                 * to include the working directory and trust that
                 * the user set cwd if necessary
                 */
                char *str, *str2;
                /* always start with the working directory */
                str = strdup(app->cwd);
                /* check for mpi.jar */
                value = opal_os_path(false, opal_install_dirs.libdir, "mpi.jar", NULL);
                if (access(value, F_OK ) != -1) {
                    (void)asprintf(&str2, "%s:%s", str, value);
                    free(str);
                    str = str2;
                }
                free(value);
                /* check for shmem.jar */
                value = opal_os_path(false, opal_install_dirs.libdir, "shmem.jar", NULL);
                if (access(value, F_OK ) != -1) {
                    asprintf(&str2, "%s:%s", str, value);
                    free(str);
                    str = str2;
                }
                free(value);
                opal_argv_insert_element(&app->argv, 1, str);
                free(str);
                opal_argv_insert_element(&app->argv, 1, "-cp");
            }
        }
        /* try to find the actual command - may not be perfect */
        for (i=1; i < opal_argv_count(app->argv); i++) {
            if (NULL != strstr(app->argv[i], "java.library.path")) {
                continue;
            } else if (NULL != strstr(app->argv[i], "cp") ||
                       NULL != strstr(app->argv[i], "classpath")) {
                /* skip the next field */
                i++;
                continue;
            }
            /* declare this the winner */
            opal_setenv("OMPI_COMMAND", app->argv[i], true, &app->env);
            /* collect everything else as the cmd line */
            if ((i+1) < opal_argv_count(app->argv)) {
                value = opal_argv_join(&app->argv[i+1], ' ');
                opal_setenv("OMPI_ARGV", value, true, &app->env);
                free(value);
            }
            break;
        }
    } else {
        /* add the cmd to the environment for MPI_Info to pickup */
        opal_setenv("OMPI_COMMAND", appname, true, &app->env);
        if (1 < opal_argv_count(app->argv)) {
            value = opal_argv_join(&app->argv[1], ' ');
            opal_setenv("OMPI_ARGV", value, true, &app->env);
            free(value);
        }
    }

    *app_ptr = app;
    app = NULL;
    *made_app = true;

    /* All done */

 cleanup:
    if (NULL != app) {
        OBJ_RELEASE(app);
    }
    if (NULL != appname) {
        free(appname);
    }
    return rc;
}

static void set_classpath_jar_file(orte_app_context_t *app, int index, char *jarfile)
{
    if (NULL == strstr(app->argv[index], jarfile)) {
        /* nope - need to add it */
        char *fmt = ':' == app->argv[index][strlen(app->argv[index]-1)]
                    ? "%s%s/%s" : "%s:%s/%s";
        char *str;
        asprintf(&str, fmt, app->argv[index], opal_install_dirs.libdir, jarfile);
        free(app->argv[index]);
        app->argv[index] = str;
    }
}

static int parse_appfile(orte_job_t *jdata, char *filename, char ***env)
{
    size_t i, len;
    FILE *fp;
    char line[BUFSIZ];
    int rc, argc, app_num;
    char **argv;
    orte_app_context_t *app;
    bool blank, made_app;
    char bogus[] = "bogus ";
    char **tmp_env;

    /*
     * Make sure to clear out this variable so we don't do anything odd in
     * app_create()
     */
    if (NULL != orte_cmd_options.appfile) {
        free(orte_cmd_options.appfile);
        orte_cmd_options.appfile = NULL;
    }

    /* Try to open the file */

    fp = fopen(filename, "r");
    if (NULL == fp) {
        orte_show_help("help-orterun.txt", "orterun:appfile-not-found", true,
                       filename);
        return ORTE_ERR_NOT_FOUND;
    }

    /* Read in line by line */

    line[sizeof(line) - 1] = '\0';
    app_num = 0;
    do {

        /* We need a bogus argv[0] (because when argv comes in from
           the command line, argv[0] is "orterun", so the parsing
           logic ignores it).  So create one here rather than making
           an argv and then pre-pending a new argv[0] (which would be
           rather inefficient). */

        line[0] = '\0';
        strcat(line, bogus);

        if (NULL == fgets(line + sizeof(bogus) - 1,
                          sizeof(line) - sizeof(bogus) - 1, fp)) {
            break;
        }

        /* Remove a trailing newline */

        len = strlen(line);
        if (len > 0 && '\n' == line[len - 1]) {
            line[len - 1] = '\0';
            if (len > 0) {
                --len;
            }
        }

        /* Remove comments */

        for (i = 0; i < len; ++i) {
            if ('#' == line[i]) {
                line[i] = '\0';
                break;
            } else if (i + 1 < len && '/' == line[i] && '/' == line[i + 1]) {
                line[i] = '\0';
                break;
            }
        }

        /* Is this a blank line? */

        len = strlen(line);
        for (blank = true, i = sizeof(bogus); i < len; ++i) {
            if (!isspace(line[i])) {
                blank = false;
                break;
            }
        }
        if (blank) {
            continue;
        }

        /* We got a line with *something* on it.  So process it */

        argv = opal_argv_split(line, ' ');
        argc = opal_argv_count(argv);
        if (argc > 0) {

            /* Create a temporary env to use in the recursive call --
               that is: don't disturb the original env so that we can
               have a consistent global env.  This allows for the
               case:

                   orterun --mca foo bar --appfile file

               where the "file" contains multiple apps.  In this case,
               each app in "file" will get *only* foo=bar as the base
               environment from which its specific environment is
               constructed. */

            if (NULL != *env) {
                tmp_env = opal_argv_copy(*env);
                if (NULL == tmp_env) {
                    fclose(fp);
                    opal_argv_free(argv);
                    return ORTE_ERR_OUT_OF_RESOURCE;
                }
            } else {
                tmp_env = NULL;
            }

            rc = create_app(argc, argv, jdata, &app, &made_app, &tmp_env);
            if (ORTE_SUCCESS != rc) {
                /* Assume that the error message has already been
                   printed; no need to cleanup -- we can just exit */
                exit(1);
            }
            if (NULL != tmp_env) {
                opal_argv_free(tmp_env);
            }
            if (made_app) {
                app->idx = app_num;
                ++app_num;
                opal_pointer_array_add(jdata->apps, app);
                ++jdata->num_apps;
            }
        }
        opal_argv_free(argv);
    } while (!feof(fp));
    fclose(fp);

    /* All done */

    free(filename);

    return ORTE_SUCCESS;
}

static void launch_recv(int status, orte_process_name_t* sender,
                        opal_buffer_t *buffer,
                        orte_rml_tag_t tag, void *cbdata)
{
    int rc;
    int32_t ret;
    int32_t cnt;
    orte_jobid_t jobid;
    orte_app_context_t *app;
    orte_proc_t *proc;
    orte_node_t *node;
    int tool_job_index;
    trackr_t *trk;

    /* unpack the completion status */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ret, &cnt, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(rc);
        return;
    }
    /* update our exit status to match */
    ORTE_UPDATE_EXIT_STATUS(ret);

    /* unpack the jobid */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &jobid, &cnt, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(rc);
        return;
    }

    /* unpack our tracking id */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tool_job_index, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(rc);
        return;
    }

    // Store the job id in the job data
    if (NULL == (trk = (trackr_t*)opal_pointer_array_get_item(&tool_jobs, tool_job_index))) {
        opal_output(0, "SPAWN OF TRACKER ID %d RETURNED INDEX TO NULL OBJECT", tool_job_index);
        return;
    }
    trk->jdata->jobid = jobid;

    if (ORTE_SUCCESS == ret) {
        printf("[ORTE] Task: %d is launched! (Job ID: %s)\n", tool_job_index, ORTE_JOBID_PRINT(jobid));
    } else {
        /* unpack the offending proc and node, if sent */
        cnt = 1;
        if (OPAL_SUCCESS == opal_dss.unpack(buffer, &trk->jdata->state, &cnt, ORTE_JOB_STATE_T)) {
            cnt = 1;
            opal_dss.unpack(buffer, &proc, &cnt, ORTE_PROC);
            proc->exit_code = ret;
            app = (orte_app_context_t*)opal_pointer_array_get_item(trk->jdata->apps, proc->app_idx);
            cnt = 1;
            opal_dss.unpack(buffer, &node, &cnt, ORTE_NODE);
            orte_print_aborted_job(trk->jdata, app, proc, node);
        }
    }

    /* Inform client */
    if (NULL != trk->launch_cb) {
        trk->launch_cb(tool_job_index, trk->jdata, ret, trk->launch_cbdata);
    }

    /* if the job failed to launch, then we remove the tracker */
    if (ORTE_SUCCESS != ret) {
        opal_pointer_array_set_item(&tool_jobs, tool_job_index, NULL);
        OBJ_RELEASE(trk);
    }
}

static void complete_recv(int status, orte_process_name_t* sender,
                          opal_buffer_t *buffer,
                          orte_rml_tag_t tag, void *cbdata)
{
    int rc, ret;
    int32_t cnt;
    orte_jobid_t jobid;
    orte_app_context_t *app;
    orte_proc_t *proc;
    orte_node_t *node;
    int tool_job_index;
    trackr_t *trk;

    /* unpack the completion status */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &ret, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(rc);
        return;
    }

    /* unpack the jobid */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &jobid, &cnt, ORTE_JOBID))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(rc);
        return;
    }

    /* unpack our tracking id */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tool_job_index, &cnt, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        ORTE_UPDATE_EXIT_STATUS(rc);
        return;
    }

    /* get the tracker */
    if (NULL == (trk = (trackr_t*)opal_pointer_array_get_item(&tool_jobs, tool_job_index))) {
        opal_output(0, "TRACKER ID %d RETURNED INDEX TO NULL OBJECT", tool_job_index);
        return;
    }

    if (ORTE_SUCCESS == ret) {
        printf("[ORTE] Task: %d returned: %d (Job ID: %s)\n", tool_job_index, ret, ORTE_JOBID_PRINT(jobid));
    } else {
        /* unpack the offending proc and node */
        cnt = 1;
        opal_dss.unpack(buffer, &trk->jdata->state, &cnt, ORTE_JOB_STATE_T);
        cnt = 1;
        opal_dss.unpack(buffer, &proc, &cnt, ORTE_PROC);
        proc->exit_code = ret;
        app = (orte_app_context_t*)opal_pointer_array_get_item(trk->jdata->apps, proc->app_idx);
        cnt = 1;
        opal_dss.unpack(buffer, &node, &cnt, ORTE_NODE);
        orte_print_aborted_job(trk->jdata, app, proc, node);
    }

    /* Inform client */
    if (NULL != trk && NULL != trk->complete_cb) {
        trk->complete_cb(tool_job_index, trk->jdata, ret, trk->complete_cbdata);
    }
    /* cleanup */
    opal_pointer_array_set_item(&tool_jobs, tool_job_index, NULL);
    OBJ_RELEASE(trk);
}


/****    DEBUGGER CODE ****/
/*
 * Debugger support for orterun
 *
 * We interpret the MPICH debugger interface as follows:
 *
 * a) The launcher
 *      - spawns the other processes,
 *      - fills in the table MPIR_proctable, and sets MPIR_proctable_size
 *      - sets MPIR_debug_state to MPIR_DEBUG_SPAWNED ( = 1)
 *      - calls MPIR_Breakpoint() which the debugger will have a
 *    breakpoint on.
 *
 *  b) Applications start and then spin until MPIR_debug_gate is set
 *     non-zero by the debugger.
 *
 * This file implements (a).
 *
 **************************************************************************
 *
 * Note that we have presently tested both TotalView and DDT parallel
 * debuggers.  They both nominally subscribe to the Etnus attaching
 * interface, but there are differences between the two.
 *
 * TotalView: user launches "totalview mpirun -a ...<mpirun args>...".
 * TV launches mpirun.  mpirun launches the application and then calls
 * MPIR_Breakpoint().  This is the signal to TV that it's a parallel
 * MPI job.  TV then reads the proctable in mpirun and attaches itself
 * to all the processes (it takes care of launching itself on the
 * remote nodes).  Upon attaching to all the MPI processes, the
 * variable MPIR_being_debugged is set to 1.  When it has finished
 * attaching itself to all the MPI processes that it wants to,
 * MPIR_Breakpoint() returns.
 *
 * DDT: user launches "ddt bin -np X <mpi app name>".  DDT fork/exec's
 * mpirun to launch ddt-debugger on the back-end nodes via "mpirun -np
 * X ddt-debugger" (not the lack of other arguments -- we can't pass
 * anything to mpirun).  This app will eventually fork/exec the MPI
 * app.  DDT does not current set MPIR_being_debugged in the MPI app.
 *
 **************************************************************************
 *
 * We support two ways of waiting for attaching debuggers.  The
 * implementation spans this file and ompi/debuggers/ompi_debuggers.c.
 *
 * 1. If using orterun: MPI processes will have the
 * orte_in_parallel_debugger MCA param set to true (because not all
 * debuggers consistently set MPIR_being_debugged in both the launcher
 * and in the MPI procs).  The HNP will call MPIR_Breakpoint() and
 * then RML send a message to VPID 0 (MCW rank 0) when it returns
 * (MPIR_Breakpoint() doesn't return until the debugger has attached
 * to all relevant processes).  Meanwhile, VPID 0 blocks waiting for
 * the RML message.  All other VPIDs immediately call the grpcomm
 * barrier (and therefore block until the debugger attaches).  Once
 * VPID 0 receives the RML message, we know that the debugger has
 * attached to all processes that it cares about, and VPID 0 then
 * joins the grpcomm barrier, allowing the job to continue.  This
 * scheme has the side effect of nicely supporting partial attaches by
 * parallel debuggers (i.e., attaching to only some of the MPI
 * processes; not necessarily all of them).
 *
 * 2. If not using orterun: in this case, we know that there will not be an RML message
 * sent to VPID 0.  So we have to look for a magic environment
 * variable from the launcher to know if the jobs will be attached by
 * a debugger (e.g., set by yod, srun, ...etc.), and if so, spin on
 * MPIR_debug_gate.  These environment variable names must be
 * hard-coded in the OMPI layer (see ompi/debuggers/ompi_debuggers.c).
 */

/* local globals and functions */
#define DUMP_INT(X) fprintf(stderr, "  %s = %d\n", # X, X);
#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

struct MPIR_PROCDESC {
    char *host_name;        /* something that can be passed to inet_addr */
    char *executable_name;  /* name of binary */
    int pid;                /* process pid */
};


/**
 * Initialization of data structures for running under a debugger
 * using the MPICH/TotalView parallel debugger interface.  Before the
 * spawn we need to check if we are being run under a TotalView-like
 * debugger; if so then inform applications via an MCA parameter.
 */
static bool mpir_warning_printed = false;

static void orte_debugger_init_before_spawn(orte_job_t *jdata)
{
    char *env_name;
    orte_app_context_t *app;
    int i;
    char *attach_fifo;

    if (!MPIR_being_debugged && !orte_in_parallel_debugger) {
        /* if we were given a test debugger, then we still want to
         * colaunch it - unless we are testing attach to a running job
         */
        if (NULL != orte_debugger_test_daemon && !orte_debugger_test_attach) {
            opal_output_verbose(2, orte_debug_output,
                                "%s Debugger test daemon specified: %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                orte_debugger_test_daemon);
            goto launchit;
        }
        /* if we were given an auto-detect rate, then we want to setup
         * an event so we periodically do the check
         */
        if (0 < orte_debugger_check_rate) {
            opal_output_verbose(2, orte_debug_output,
                                "%s Setting debugger attach check rate for %d seconds",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                orte_debugger_check_rate);
            ORTE_TIMER_EVENT(orte_debugger_check_rate, 0, attach_debugger, ORTE_SYS_PRI);
        } else if (orte_create_session_dirs) {
            /* create the attachment FIFO and setup readevent - cannot be
             * done if no session dirs exist!
             */
            attach_fifo = opal_os_path(false, orte_process_info.job_session_dir,
                                       "debugger_attach_fifo", NULL);
            if ((mkfifo(attach_fifo, FILE_MODE) < 0) && errno != EEXIST) {
                opal_output(0, "CANNOT CREATE FIFO %s: errno %d", attach_fifo, errno);
                free(attach_fifo);
                return;
            }
            strncpy(MPIR_attach_fifo, attach_fifo, MPIR_MAX_PATH_LENGTH - 1);
            free(attach_fifo);
            open_fifo();
        }
        return;
    }

 launchit:
    opal_output_verbose(1, orte_debug_output, "Info: Spawned by a debugger");

    /* if we haven't previously warned about it */
    if (!mpir_warning_printed) {
        mpir_warning_printed = true;
        /* check for silencing envar */
        if (NULL == getenv("OMPI_MPIR_DO_NOT_WARN")) {
            orte_show_help("help-orted.txt", "mpir-debugger-detected", true);
        }
    }

    /* tell the procs they are being debugged */
    (void) mca_base_var_env_name ("orte_in_parallel_debugger", &env_name);

    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        opal_setenv(env_name, "1", true, &app->env);
    }
    free(env_name);

    /* setup the attach fifo in case someone wants to re-attach */
    if (orte_create_session_dirs) {
        /* create the attachment FIFO and setup readevent - cannot be
         * done if no session dirs exist!
         */
        attach_fifo = opal_os_path(false, orte_process_info.job_session_dir,
                                   "debugger_attach_fifo", NULL);
        if ((mkfifo(attach_fifo, FILE_MODE) < 0) && errno != EEXIST) {
            opal_output(0, "CANNOT CREATE FIFO %s: errno %d", attach_fifo, errno);
            free(attach_fifo);
            return;
        }
        strncpy(MPIR_attach_fifo, attach_fifo, MPIR_MAX_PATH_LENGTH - 1);
        free(attach_fifo);
        open_fifo();
    }
}

static bool mpir_breakpoint_fired = false;

static void _send_notification(int status)
{
    opal_buffer_t buf;
    orte_grpcomm_signature_t sig;
    int rc;
    opal_value_t kv, *kvptr;

    OBJ_CONSTRUCT(&buf, opal_buffer_t);

    /* pack the debugger_attached status */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&buf, &status, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&buf);
        return;
    }

    /* the source is me */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&buf, ORTE_PROC_MY_NAME, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&buf);
        return;
    }

    /* instruct that it is to go only to non-default evhandlers */
    status = 1;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&buf, &status, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&buf);
        return;
    }
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_EVENT_NON_DEFAULT);
    kv.type = OPAL_BOOL;
    kv.data.flag = true;
    kvptr = &kv;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(&buf, &kvptr, 1, OPAL_VALUE))) {
        ORTE_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        OBJ_DESTRUCT(&buf);
        return;
    }
    OBJ_DESTRUCT(&kv);

    /* xcast it to everyone */
    OBJ_CONSTRUCT(&sig, orte_grpcomm_signature_t);
    sig.signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    sig.signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig.signature[0].vpid = ORTE_VPID_WILDCARD;
    sig.sz = 1;

    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(&sig, ORTE_RML_TAG_NOTIFICATION, &buf))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_DESTRUCT(&sig);
    OBJ_DESTRUCT(&buf);
}

static void orte_debugger_dump(void)
{
    int i;

    DUMP_INT(MPIR_being_debugged);
    DUMP_INT(MPIR_debug_state);
    DUMP_INT(MPIR_partial_attach_ok);
    DUMP_INT(MPIR_i_am_starter);
    DUMP_INT(MPIR_forward_output);
    DUMP_INT(MPIR_proctable_size);
    fprintf(stderr, "  MPIR_proctable:\n");
    for (i = 0; i < MPIR_proctable_size; i++) {
        fprintf(stderr,
                "    (i, host, exe, pid) = (%d, %s, %s, %d)\n",
                i,
                MPIR_proctable[i].host_name,
                MPIR_proctable[i].executable_name,
                MPIR_proctable[i].pid);
    }
    fprintf(stderr, "MPIR_executable_path: %s\n",
            ('\0' == MPIR_executable_path[0]) ?
            "NULL" : (char*) MPIR_executable_path);
    fprintf(stderr, "MPIR_server_arguments: %s\n",
            ('\0' == MPIR_server_arguments[0]) ?
            "NULL" : (char*) MPIR_server_arguments);
}

static void setup_debugger_job(orte_jobid_t jobid)
{
    orte_job_t *debugger;
    orte_app_context_t *app;
    int rc;
    char cwd[OPAL_PATH_MAX];
    bool flag = true;

    /* setup debugger daemon job */
    debugger = OBJ_NEW(orte_job_t);
    /* create a jobid for these daemons - this is done solely
     * to avoid confusing the rest of the system's bookkeeping
     */
    orte_plm_base_create_jobid(debugger);
    /* set the personality to ORTE */
    opal_argv_append_nosize(&debugger->personality, "orte");
    /* flag the job as being debugger daemons */
    ORTE_FLAG_SET(debugger, ORTE_JOB_FLAG_DEBUGGER_DAEMON);
    /* unless directed, we do not forward output */
    if (!MPIR_forward_output) {
        ORTE_FLAG_SET(debugger, ORTE_JOB_FLAG_FORWARD_OUTPUT);
    }
    /* dont push stdin */
    debugger->stdin_target = ORTE_VPID_INVALID;
    /* add it to the global job pool */
    opal_hash_table_set_value_uint32(orte_job_data, debugger->jobid, debugger);
    /* create an app_context for the debugger daemon */
    app = OBJ_NEW(orte_app_context_t);
    if (NULL != orte_debugger_test_daemon) {
        app->app = strdup(orte_debugger_test_daemon);
    } else {
        app->app = strdup((char*)MPIR_executable_path);
    }
    /* don't currently have an option to pass the debugger
     * cwd - probably should add one someday
     */
    if (OPAL_SUCCESS != (rc = opal_getcwd(cwd, sizeof(cwd)))) {
        orte_show_help("help-orterun.txt", "orterun:init-failure",
                       true, "get the cwd", rc);
        return;
    }
    app->cwd = strdup(cwd);
    orte_set_attribute(&app->attributes, ORTE_APP_USER_CWD, ORTE_ATTR_GLOBAL, &flag, OPAL_BOOL);
    opal_argv_append_nosize(&app->argv, app->app);
    build_debugger_args(app);
    opal_pointer_array_add(debugger->apps, app);
    debugger->num_apps = 1;
    /* create the map object and set the policy to 1ppn */
    debugger->map = OBJ_NEW(orte_job_map_t);
    ORTE_SET_MAPPING_POLICY(debugger->map->mapping, ORTE_MAPPING_PPR);
    ORTE_SET_MAPPING_DIRECTIVE(debugger->map->mapping, ORTE_MAPPING_GIVEN);
    ORTE_SET_MAPPING_DIRECTIVE(debugger->map->mapping, ORTE_MAPPING_DEBUGGER);
    /* define the ppr */
    debugger->map->ppr = strdup("1:node");
    /* mark that we do not want the daemon bound */
    if (ORTE_SUCCESS != (rc = opal_hwloc_base_set_binding_policy(&debugger->map->binding, "none"))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    /* spawn it */
    rc = orte_plm.spawn(debugger);
    if (ORTE_SUCCESS != rc) {
        ORTE_ERROR_LOG(rc);
    }
}

/*
 * Initialization of data structures for running under a debugger
 * using the MPICH/TotalView parallel debugger interface. This stage
 * of initialization must occur after spawn
 *
 * NOTE: We -always- perform this step to ensure that any debugger
 * that attaches to us post-launch of the application can get a
 * completed proctable
 */
void orte_debugger_init_after_spawn(int fd, short event, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    orte_job_t *jdata = caddy->jdata;
    orte_proc_t *proc;
    orte_app_context_t *appctx;
    orte_vpid_t i, j;
    char **aliases, *aptr;

    /* if we couldn't get thru the mapper stage, we might
     * enter here with no procs. Avoid the "zero byte malloc"
     * message by checking here
     */
    if (MPIR_proctable || 0 == jdata->num_procs) {

        /* already initialized */
        opal_output_verbose(5, orte_debug_output,
                            "%s: debugger already initialized or zero procs",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

        if (MPIR_being_debugged || NULL != orte_debugger_test_daemon ||
            NULL != getenv("ORTE_TEST_DEBUGGER_ATTACH")) {
            OBJ_RELEASE(caddy);
            /* if we haven't previously warned about it */
            if (!mpir_warning_printed) {
                mpir_warning_printed = true;
                /* check for silencing envar */
                if (NULL == getenv("OMPI_MPIR_DO_NOT_WARN")) {
                    orte_show_help("help-orted.txt", "mpir-debugger-detected", true);
                }
            }
            if (!mpir_breakpoint_fired) {
                /* record that we have triggered the debugger */
                mpir_breakpoint_fired = true;

                /* trigger the debugger */
                MPIR_Breakpoint();

                opal_output_verbose(5, orte_debug_output,
                                    "%s NOTIFYING DEBUGGER RELEASE",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                /* notify all procs that the debugger is ready */
                _send_notification(OPAL_ERR_DEBUGGER_RELEASE);
            }
        }
        return;
    }

    /* fill in the proc table for the application processes */

    opal_output_verbose(5, orte_debug_output,
                        "%s: Setting up debugger process table for applications",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));

    MPIR_debug_state = 1;

    /* set the total number of processes in the job */
    MPIR_proctable_size = jdata->num_procs;

    /* allocate MPIR_proctable */
    MPIR_proctable = (struct MPIR_PROCDESC *)malloc(sizeof(struct MPIR_PROCDESC) *
                                                    MPIR_proctable_size);
    if (MPIR_proctable == NULL) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        OBJ_RELEASE(caddy);
        return;
    }

    if (orte_debugger_dump_proctable) {
        opal_output(orte_clean_output, "MPIR Proctable for job %s", ORTE_JOBID_PRINT(jdata->jobid));
    }

    /* initialize MPIR_proctable */
    for (j=0; j < jdata->num_procs; j++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, j))) {
            continue;
        }
        /* store this data in the location whose index
         * corresponds to the proc's rank
         */
        i = proc->name.vpid;
        if (NULL == (appctx = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, proc->app_idx))) {
            continue;
        }

        /* take the indicated alias as the hostname, if aliases exist */
        if (orte_retain_aliases) {
            aliases = NULL;
            aptr = NULL;
            if (orte_get_attribute(&proc->node->attributes, ORTE_NODE_ALIAS, (void**)&aptr, OPAL_STRING)) {
                aliases = opal_argv_split(aptr, ',');
                free(aptr);
                if (orte_use_hostname_alias <= opal_argv_count(aliases)) {
                    MPIR_proctable[i].host_name = strdup(aliases[orte_use_hostname_alias-1]);
                }
                opal_argv_free(aliases);
            }
        } else {
            /* just use the default name */
            MPIR_proctable[i].host_name = strdup(proc->node->name);
        }

        if ( 0 == strncmp(appctx->app, OPAL_PATH_SEP, 1 )) {
            MPIR_proctable[i].executable_name =
                opal_os_path( false, appctx->app, NULL );
        } else {
            MPIR_proctable[i].executable_name =
                opal_os_path( false, appctx->cwd, appctx->app, NULL );
        }
        MPIR_proctable[i].pid = proc->pid;
        if (orte_debugger_dump_proctable) {
            opal_output(orte_clean_output, "%s: Host %s Exe %s Pid %d",
                        ORTE_VPID_PRINT(i), MPIR_proctable[i].host_name,
                        MPIR_proctable[i].executable_name, MPIR_proctable[i].pid);
        }
    }

    if (0 < opal_output_get_verbosity(orte_debug_output)) {
        orte_debugger_dump();
    }

    /* if we are being launched under a debugger, then we must wait
     * for it to be ready to go and do some things to start the job
     */
    if (MPIR_being_debugged || NULL != orte_debugger_test_daemon ||
        NULL != getenv("ORTE_TEST_DEBUGGER_ATTACH")) {
        /* if we haven't previously warned about it */
        if (!mpir_warning_printed) {
            mpir_warning_printed = true;
            /* check for silencing envar */
            if (NULL == getenv("OMPI_MPIR_DO_NOT_WARN")) {
                orte_show_help("help-orted.txt", "mpir-debugger-detected", true);
            }
        }

        /* if we are not launching debugger daemons, then trigger
         * the debugger - otherwise, we need to wait for the debugger
         * daemons to be started
         */
        if ('\0' == MPIR_executable_path[0] && NULL == orte_debugger_test_daemon) {
            /* record that we have triggered the debugger */
            mpir_breakpoint_fired = true;

            /* trigger the debugger */
            MPIR_Breakpoint();

            opal_output_verbose(2, orte_debug_output,
                                "%s NOTIFYING DEBUGGER RELEASE",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            /* notify all procs that the debugger is ready */
            _send_notification(OPAL_ERR_DEBUGGER_RELEASE);
        } else if (!orte_debugger_test_attach) {
            /* if I am launching debugger daemons, then I need to do so now
             * that the job has been started and I know which nodes have
             * apps on them
             */
            opal_output_verbose(2, orte_debug_output,
                                "%s Cospawning debugger daemons %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                (NULL == orte_debugger_test_daemon) ?
                                MPIR_executable_path : orte_debugger_test_daemon);
            setup_debugger_job(jdata->jobid);
        }
        /* we don't have anything else to do */
        OBJ_RELEASE(caddy);
        return;
    }

    /* if we are not being debugged, then just cleanup and depart */
    OBJ_RELEASE(caddy);
}

/*
 * Process one line from the orte_base_user_debugger MCA param and
 * look for that debugger in the path.  If we find it, fill in
 * new_argv.
 */
static int process(char *orig_line, char *basename, opal_cmd_line_t *cmd_line,
                   int argc, char **argv, char ***new_argv, int num_procs)
{
    int ret = ORTE_SUCCESS;
    int i, j, count;
    char *line = NULL, *tmp = NULL, *full_line = strdup(orig_line);
    char **orterun_argv = NULL, **executable_argv = NULL, **line_argv = NULL;
    char cwd[OPAL_PATH_MAX];
    bool used_num_procs = false;
    bool single_app = false;
    bool fail_needed_executable = false;

    line = full_line;
    if (NULL == line) {
        ret = ORTE_ERR_OUT_OF_RESOURCE;
        goto out;
    }

    /* Trim off whitespace at the beginning and ending of line */

    for (i = 0; '\0' != line[i] && isspace(line[i]); ++line) {
        continue;
    }
    for (i = strlen(line) - 2; i > 0 && isspace(line[i]); ++i) {
        line[i] = '\0';
    }
    if (strlen(line) <= 0) {
        ret = ORTE_ERROR;
        goto out;
    }

    /* Get the tail of the command line (i.e., the user executable /
       argv) */

    opal_cmd_line_get_tail(cmd_line, &i, &executable_argv);

    /* Make a new copy of the orterun command line args, without the
       orterun token itself, and without the --debug, --debugger, and
       -tv flags. */

    orterun_argv = opal_argv_copy(argv);
    count = opal_argv_count(orterun_argv);
    opal_argv_delete(&count, &orterun_argv, 0, 1);
    for (i = 0; NULL != orterun_argv[i]; ++i) {
        count = opal_argv_count(orterun_argv);
        if (0 == strcmp(orterun_argv[i], "-debug") ||
            0 == strcmp(orterun_argv[i], "--debug")) {
            opal_argv_delete(&count, &orterun_argv, i, 1);
        } else if (0 == strcmp(orterun_argv[i], "-tv") ||
                   0 == strcmp(orterun_argv[i], "--tv")) {
            opal_argv_delete(&count, &orterun_argv, i, 1);
        } else if (0 == strcmp(orterun_argv[i], "--debugger") ||
                   0 == strcmp(orterun_argv[i], "-debugger")) {
            opal_argv_delete(&count, &orterun_argv, i, 2);
        }
    }

    /* Replace @@ tokens - line should never realistically be bigger
       than MAX_INT, so just cast to int to remove compiler warning */

    *new_argv = NULL;
    line_argv = opal_argv_split(line, ' ');
    if (NULL == line_argv) {
        ret = ORTE_ERR_NOT_FOUND;
        goto out;
    }
    for (i = 0; NULL != line_argv[i]; ++i) {
        if (0 == strcmp(line_argv[i], "@mpirun@") ||
            0 == strcmp(line_argv[i], "@orterun@")) {
            opal_argv_append_nosize(new_argv, argv[0]);
        } else if (0 == strcmp(line_argv[i], "@mpirun_args@") ||
                   0 == strcmp(line_argv[i], "@orterun_args@")) {
            for (j = 0; NULL != orterun_argv && NULL != orterun_argv[j]; ++j) {
                opal_argv_append_nosize(new_argv, orterun_argv[j]);
            }
        } else if (0 == strcmp(line_argv[i], "@np@")) {
            used_num_procs = true;
            asprintf(&tmp, "%d", num_procs);
            opal_argv_append_nosize(new_argv, tmp);
            free(tmp);
        } else if (0 == strcmp(line_argv[i], "@single_app@")) {
            /* This token is only a flag; it is not replaced with any
               alternate text */
            single_app = true;
        } else if (0 == strcmp(line_argv[i], "@executable@")) {
            /* If we found the executable, paste it in.  Otherwise,
               this is a possible error. */
            if (NULL != executable_argv) {
                opal_argv_append_nosize(new_argv, executable_argv[0]);
            } else {
                fail_needed_executable = true;
            }
        } else if (0 == strcmp(line_argv[i], "@executable_argv@")) {
            /* If we found the tail, paste in the argv.  Otherwise,
               this is a possible error. */
            if (NULL != executable_argv) {
                for (j = 1; NULL != executable_argv[j]; ++j) {
                    opal_argv_append_nosize(new_argv, executable_argv[j]);
                }
            } else {
                fail_needed_executable = true;
            }
        } else {
            /* It wasn't a special token, so just copy it over */
            opal_argv_append_nosize(new_argv, line_argv[i]);
        }
    }

    /* Can we find argv[0] in the path? */

    getcwd(cwd, OPAL_PATH_MAX);
    tmp = opal_path_findv((*new_argv)[0], X_OK, environ, cwd);
    if (NULL != tmp) {
        free(tmp);

        /* Ok, we found a good debugger.  Check for some error
           conditions. */
        tmp = opal_argv_join(argv, ' ');

        /* We do not support launching a debugger that requires the
           -np value if the user did not specify -np on the command
           line. */
        if (used_num_procs && 0 == num_procs) {
            free(tmp);
            tmp = opal_argv_join(orterun_argv, ' ');
            orte_show_help("help-orterun.txt", "debugger requires -np",
                           true, (*new_argv)[0], argv[0], tmp,
                           (*new_argv)[0]);
            /* Fall through to free / fail, below */
        }

        /* Some debuggers do not support launching MPMD */
        else if (single_app && NULL != strstr(tmp, " : ")) {
            orte_show_help("help-orterun.txt",
                           "debugger only accepts single app", true,
                           (*new_argv)[0], (*new_argv)[0]);
            /* Fall through to free / fail, below */
        }

        /* Some debuggers do not use orterun/mpirun, and therefore
           must have an executable to run (e.g., cannot use mpirun's
           app context file feature). */
        else if (fail_needed_executable) {
            orte_show_help("help-orterun.txt",
                           "debugger requires executable", true,
                           (*new_argv)[0], argv[0], (*new_argv)[0], argv[0],
                           (*new_argv)[0]);
            /* Fall through to free / fail, below */
        }

        /* Otherwise, we succeeded.  Return happiness. */
        else {
            goto out;
        }
    }

    /* All done -- didn't find it */

    opal_argv_free(*new_argv);
    *new_argv = NULL;
    ret = ORTE_ERR_NOT_FOUND;

 out:
    if (NULL != orterun_argv) {
        opal_argv_free(orterun_argv);
    }
    if (NULL != executable_argv) {
        opal_argv_free(executable_argv);
    }
    if (NULL != line_argv) {
        opal_argv_free(line_argv);
    }
    if (NULL != tmp) {
        free(tmp);
    }
    if (NULL != full_line) {
        free(full_line);
    }
    return ret;
}

static void open_fifo(void)
{
    if (orte_debugger_attach_fd > 0) {
        close(orte_debugger_attach_fd);
    }

    orte_debugger_attach_fd = open(MPIR_attach_fifo, O_RDONLY | O_NONBLOCK, 0);
    if (orte_debugger_attach_fd < 0) {
        opal_output(0, "%s unable to open debugger attach fifo",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return;
    }

    /* Set this fd to be close-on-exec so that children don't see it */
    if (opal_fd_set_cloexec(orte_debugger_attach_fd) != OPAL_SUCCESS) {
        opal_output(0, "%s unable to set debugger attach fifo to CLOEXEC",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        close(orte_debugger_attach_fd);
        orte_debugger_attach_fd = -1;
        return;
    }

    if (orte_debugger_test_attach) {
        opal_output(0, "%s Monitoring debugger attach fifo %s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    MPIR_attach_fifo);
    } else {
        opal_output_verbose(2, orte_debug_output,
                            "%s Monitoring debugger attach fifo %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            MPIR_attach_fifo);
    }
    orte_debugger_attach = (opal_event_t*)malloc(sizeof(opal_event_t));
    opal_event_set(orte_event_base, orte_debugger_attach, orte_debugger_attach_fd,
                   OPAL_EV_READ, attach_debugger, orte_debugger_attach);

    orte_debugger_fifo_active = true;
    opal_event_add(orte_debugger_attach, 0);
}

static bool did_once = false;

static void attach_debugger(int fd, short event, void *arg)
{
    unsigned char fifo_cmd;
    int rc;
    orte_timer_t *tm;

    if (orte_debugger_fifo_active) {
        orte_debugger_attach = (opal_event_t*)arg;
        orte_debugger_fifo_active = false;

        rc = read(orte_debugger_attach_fd, &fifo_cmd, sizeof(fifo_cmd));
        if (!rc) {
            /* release the current event */
            opal_event_free(orte_debugger_attach);
            /* reopen device to clear hangup */
            open_fifo();
            return;
        }
        if (1 != fifo_cmd) {
            /* ignore the cmd */
            orte_debugger_fifo_active = true;
            opal_event_add(orte_debugger_attach, 0);
            return;
        }
    }

    if (!MPIR_being_debugged && !orte_debugger_test_attach) {
        /* false alarm - reset the read or timer event */
        if (0 == orte_debugger_check_rate) {
            orte_debugger_fifo_active = true;
            opal_event_add(orte_debugger_attach, 0);
        } else if (!MPIR_being_debugged) {
            tm = (orte_timer_t*)arg;
            /* re-add the event */
            opal_event_evtimer_add(tm->ev, &tm->tv);
        }
        return;
    }

    opal_output_verbose(1, orte_debug_output,
                        "%s Attaching debugger %s", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (NULL == orte_debugger_test_daemon) ? MPIR_executable_path : orte_debugger_test_daemon);

    /* if we haven't previously warned about it */
    if (!mpir_warning_printed) {
        mpir_warning_printed = true;
        /* check for silencing envar */
        if (NULL == getenv("OMPI_MPIR_DO_NOT_WARN")) {
            orte_show_help("help-orted.txt", "mpir-debugger-detected", true);
        }
    }

    /* a debugger has attached! All the MPIR_Proctable
     * data is already available, so we only need to
     * check to see if we should spawn any daemons
     */
    if ('\0' != MPIR_executable_path[0] || NULL != orte_debugger_test_daemon) {
        opal_output_verbose(2, orte_debug_output,
                            "%s Spawning debugger daemons %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            (NULL == orte_debugger_test_daemon) ?
                            MPIR_executable_path : orte_debugger_test_daemon);
        setup_debugger_job(ORTE_JOBID_WILDCARD);
        did_once = true;
    }

    /* if we are testing, ensure we only do this once */
    if (NULL != orte_debugger_test_daemon && did_once) {
        return;
    }

    /* reset the read or timer event */
    if (0 == orte_debugger_check_rate) {
        orte_debugger_fifo_active = true;
        opal_event_add(orte_debugger_attach, 0);
    } else if (!MPIR_being_debugged) {
        tm = (orte_timer_t*)arg;
        /* re-add the event */
        opal_event_evtimer_add(tm->ev, &tm->tv);
    }
}

static void build_debugger_args(orte_app_context_t *debugger)
{
    int i, j;
    char mpir_arg[MPIR_MAX_ARG_LENGTH];

    if ('\0' != MPIR_server_arguments[0]) {
        j=0;
        memset(mpir_arg, 0, MPIR_MAX_ARG_LENGTH);
        for (i=0; i < MPIR_MAX_ARG_LENGTH; i++) {
            if (MPIR_server_arguments[i] == '\0') {
                if (0 < j) {
                    opal_argv_append_nosize(&debugger->argv, mpir_arg);
                    memset(mpir_arg, 0, MPIR_MAX_ARG_LENGTH);
                    j=0;
                }
            } else {
                mpir_arg[j] = MPIR_server_arguments[i];
                j++;
            }
        }
    }
}

/**
 * Run a user-level debugger
 */
static void run_debugger(char *basename, opal_cmd_line_t *cmd_line,
                         int argc, char *argv[], int num_procs)
{
    int i, id, ret;
    char **new_argv = NULL;
    const char **tmp = NULL;
    char *value, **lines, *env_name;

    /* Get the orte_base_debug MCA parameter and search for a debugger
       that can run */

    id = mca_base_var_find("orte", "orte", NULL, "base_user_debugger");
    if (id < 0) {
        orte_show_help("help-orterun.txt", "debugger-mca-param-not-found",
                       true);
        exit(1);
    }

    ret = mca_base_var_get_value (id, &tmp, NULL, NULL);
    if (OPAL_SUCCESS != ret || NULL == tmp || NULL == tmp[0]) {
        orte_show_help("help-orterun.txt", "debugger-orte_base_user_debugger-empty",
                       true);
        exit(1);
    }

    /* Look through all the values in the MCA param */

    lines = opal_argv_split(tmp[0], ':');
    for (i = 0; NULL != lines[i]; ++i) {
        if (ORTE_SUCCESS == process(lines[i], basename, cmd_line, argc, argv,
                                    &new_argv, num_procs)) {
            break;
        }
    }

    /* If we didn't find one, abort */

    if (NULL == lines[i]) {
        orte_show_help("help-orterun.txt", "debugger-not-found", true);
        exit(1);
    }
    opal_argv_free(lines);

    /* We found one */

    /* cleanup the MPIR arrays in case the debugger doesn't set them */
    memset((char*)MPIR_executable_path, 0, MPIR_MAX_PATH_LENGTH);
    memset((char*)MPIR_server_arguments, 0, MPIR_MAX_ARG_LENGTH);

    /* Set an MCA param so that everyone knows that they are being
       launched under a debugger; not all debuggers are consistent
       about setting MPIR_being_debugged in both the launcher and the
       MPI processes */
    ret = mca_base_var_env_name ("orte_in_parallel_debugger", &env_name);
    if (OPAL_SUCCESS == ret && NULL != env_name) {
        opal_setenv(env_name, "1", true, &environ);
        free(env_name);
    }

    /* if we haven't previously warned about it */
    if (!mpir_warning_printed) {
        mpir_warning_printed = true;
        /* check for silencing envar */
        if (NULL == getenv("OMPI_MPIR_DO_NOT_WARN")) {
            orte_show_help("help-orted.txt", "mpir-debugger-detected", true);
        }
    }

    /* Launch the debugger */
    execvp(new_argv[0], new_argv);
    value = opal_argv_join(new_argv, ' ');
    orte_show_help("help-orterun.txt", "debugger-exec-failed",
                   true, basename, value, new_argv[0]);
    free(value);
    opal_argv_free(new_argv);
    exit(1);
}

void orte_debugger_detached(int fd, short event, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    OBJ_RELEASE(caddy);

    /* need to ensure MPIR_Breakpoint is called again if another debugger attaches */
    mpir_breakpoint_fired = false;
}

static uint32_t ntraces = 0;
static orte_timer_t stack_trace_timer;

static void stack_trace_recv(int status, orte_process_name_t* sender,
                             opal_buffer_t *buffer, orte_rml_tag_t tag,
                             void* cbdata)
{
    opal_buffer_t *blob;
    char *st;
    int32_t cnt;
    orte_process_name_t name;
    char *hostname;
    pid_t pid;

    /* unpack the stack_trace blob */
    cnt = 1;
    while (OPAL_SUCCESS == opal_dss.unpack(buffer, &blob, &cnt, OPAL_BUFFER)) {
        /* first piece is the name of the process */
        cnt = 1;
        if (OPAL_SUCCESS != opal_dss.unpack(blob, &name, &cnt, ORTE_NAME) ||
            OPAL_SUCCESS != opal_dss.unpack(blob, &hostname, &cnt, OPAL_STRING) ||
            OPAL_SUCCESS != opal_dss.unpack(blob, &pid, &cnt, OPAL_PID)) {
            OBJ_RELEASE(blob);
            continue;
        }
        fprintf(stderr, "STACK TRACE FOR PROC %s (%s, PID %lu)\n", ORTE_NAME_PRINT(&name), hostname, (unsigned long) pid);
        free(hostname);
        /* unpack the stack_trace until complete */
        cnt = 1;
        while (OPAL_SUCCESS == opal_dss.unpack(blob, &st, &cnt, OPAL_STRING)) {
            fprintf(stderr, "\t%s", st);  // has its own newline
            free(st);
            cnt = 1;
        }
        fprintf(stderr, "\n");
        OBJ_RELEASE(blob);
        cnt = 1;
    }
    ++ntraces;
    if (orte_process_info.num_procs == ntraces) {
        if( orte_stack_trace_wait_timeout > 0 ) {
            /* cancel the timeout */
            OBJ_DESTRUCT(&stack_trace_timer);
        }
        /* abort the job */
        ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_ALL_JOBS_COMPLETE);
        /* set the global abnormal exit flag  */
        orte_abnormal_term_ordered = true;
    }
}

static void stack_trace_timeout(int sd, short args, void *cbdata)
{
    /* abort the job */
    ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_ALL_JOBS_COMPLETE);
    /* set the global abnormal exit flag  */
    orte_abnormal_term_ordered = true;
}

void orte_timeout_wakeup(int sd, short args, void *cbdata)
{
    orte_job_t *jdata;
    orte_proc_t *proc;
    int i;
    int rc;
    uint32_t key;
    void *nptr;

    /* this function gets called when the job execution time
     * has hit a prescribed limit - so just abort
     */
    orte_show_help("help-orterun.txt", "orterun:timeout",
                   true, timeout_seconds);
    ORTE_UPDATE_EXIT_STATUS(ETIMEDOUT);
    /* if we are testing HNP suicide, then just exit */
    if (ORTE_PROC_IS_HNP &&
        NULL != getenv("ORTE_TEST_HNP_SUICIDE")) {
        opal_output(0, "HNP exiting w/o cleanup");
        exit(1);
    }
    if (orte_cmd_options.report_state_on_timeout) {
        /* cycle across all the jobs and report their state */
        rc = opal_hash_table_get_first_key_uint32(orte_job_data, &key, (void **)&jdata, &nptr);
        while (OPAL_SUCCESS == rc) {
            /* don't use the opal_output system as it may be borked */
            fprintf(stderr, "DATA FOR JOB: %s\n", ORTE_JOBID_PRINT(jdata->jobid));
            fprintf(stderr, "\tNum apps: %d\tNum procs: %d\tJobState: %s\tAbort: %s\n",
                    (int)jdata->num_apps, (int)jdata->num_procs,
                    orte_job_state_to_str(jdata->state),
                    (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_ABORTED)) ? "True" : "False");
            fprintf(stderr, "\tNum launched: %ld\tNum reported: %ld\tNum terminated: %ld\n",
                    (long)jdata->num_launched, (long)jdata->num_reported, (long)jdata->num_terminated);
            fprintf(stderr, "\n\tProcs:\n");
            for (i=0; i < jdata->procs->size; i++) {
                if (NULL != (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
                    fprintf(stderr, "\t\tRank: %s\tNode: %s\tPID: %u\tState: %s\tExitCode %d\n",
                            ORTE_VPID_PRINT(proc->name.vpid),
                            (NULL == proc->node) ? "UNKNOWN" : proc->node->name,
                            (unsigned int)proc->pid,
                            orte_proc_state_to_str(proc->state), proc->exit_code);
                }
            }
            fprintf(stderr, "\n");
            rc = opal_hash_table_get_next_key_uint32(orte_job_data, &key, (void **)&jdata, nptr, &nptr);
        }
    }
    /* if they asked for stack_traces, attempt to get them, but timeout
     * if we cannot do so */
    if (orte_cmd_options.get_stack_traces) {
        orte_daemon_cmd_flag_t command = ORTE_DAEMON_GET_STACK_TRACES;
        opal_buffer_t *buffer;
        orte_grpcomm_signature_t *sig;

        fprintf(stderr, "Waiting for stack traces (this may take a few moments)...\n");

        /* set the recv */
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_STACK_TRACE,
                                ORTE_RML_PERSISTENT, stack_trace_recv, NULL);

        /* setup the buffer */
        buffer = OBJ_NEW(opal_buffer_t);
        /* pack the command */
        if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &command, 1, ORTE_DAEMON_CMD))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buffer);
            goto giveup;
        }
        /* goes to all daemons */
        sig = OBJ_NEW(orte_grpcomm_signature_t);
        sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
        sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
        sig->signature[0].vpid = ORTE_VPID_WILDCARD;
        sig->sz = 1;
        if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_DAEMON, buffer))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buffer);
            OBJ_RELEASE(sig);
            goto giveup;
        }
        OBJ_RELEASE(buffer);
        /* maintain accounting */
        OBJ_RELEASE(sig);
        /* we will terminate after we get the stack_traces, but set a timeout
         * just in case we never hear back from everyone */
        if( orte_stack_trace_wait_timeout > 0 ) {
            OBJ_CONSTRUCT(&stack_trace_timer, orte_timer_t);
            opal_event_evtimer_set(orte_event_base,
                                   stack_trace_timer.ev, stack_trace_timeout, NULL);
            opal_event_set_priority(stack_trace_timer.ev, ORTE_ERROR_PRI);
            stack_trace_timer.tv.tv_sec = orte_stack_trace_wait_timeout;
            opal_event_evtimer_add(stack_trace_timer.ev, &stack_trace_timer.tv);
        }
        return;
    }
  giveup:
    /* abort the job */
    ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_ALL_JOBS_COMPLETE);
    /* set the global abnormal exit flag  */
    orte_abnormal_term_ordered = true;
}

static int nreports = 0;
static orte_timer_t profile_timer;
static int nchecks = 0;

static void profile_timeout(int sd, short args, void *cbdata)
{
    /* abort the job */
    ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_ALL_JOBS_COMPLETE);
    /* set the global abnormal exit flag  */
    orte_abnormal_term_ordered = true;
}


static void profile_recv(int status, orte_process_name_t* sender,
                         opal_buffer_t *buffer, orte_rml_tag_t tag,
                         void* cbdata)
{
    int32_t cnt;
    char *hostname;
    float dpss, pss;

    /* unpack the hostname where this came from */
    cnt = 1;
    if (OPAL_SUCCESS != opal_dss.unpack(buffer, &hostname, &cnt, OPAL_STRING)) {
        goto done;
    }
    /* print the hostname */
    fprintf(stderr, "Memory profile from host: %s\n", hostname);
    free(hostname);

    /* get the PSS of the daemon */
    cnt = 1;
    if (OPAL_SUCCESS != opal_dss.unpack(buffer, &dpss, &cnt, OPAL_FLOAT)) {
        goto done;
    }
    /* get the average PSS of the child procs */
    cnt = 1;
    if (OPAL_SUCCESS != opal_dss.unpack(buffer, &pss, &cnt, OPAL_FLOAT)) {
        goto done;
    }

    fprintf(stderr, "\tDaemon: %8.2fM\tProcs: %8.2fM\n", dpss, pss);

  done:
    --nreports;
    if (nreports == 0) {
        ++nchecks;
        /* cancel the timeout */
        OBJ_DESTRUCT(&profile_timer);
        /* notify to release */
        _send_notification(12345);
        /* if this was the first measurement, then we need to
         * let the probe move along */
        if (2 > nchecks) {
            /* reset the event */
            opal_event_evtimer_set(orte_event_base, orte_memprofile_timeout->ev,
                                   orte_profile_wakeup, NULL);
            opal_event_set_priority(orte_memprofile_timeout->ev, ORTE_ERROR_PRI);
            opal_event_evtimer_add(orte_memprofile_timeout->ev, &orte_memprofile_timeout->tv);
            /* reset the timer */
            OBJ_CONSTRUCT(&profile_timer, orte_timer_t);
            opal_event_evtimer_set(orte_event_base,
                                   profile_timer.ev, profile_timeout, NULL);
            opal_event_set_priority(profile_timer.ev, ORTE_ERROR_PRI);
            profile_timer.tv.tv_sec = 30;
            opal_event_evtimer_add(profile_timer.ev, &profile_timer.tv);
            return;
        }
    }
}

void orte_profile_wakeup(int sd, short args, void *cbdata)
{
    orte_job_t *dmns;
    orte_proc_t *dmn;
    int i;
    int rc;
    orte_daemon_cmd_flag_t command = ORTE_DAEMON_GET_MEMPROFILE;
    opal_buffer_t *buffer;
    orte_process_name_t name;

    /* this function gets called when the job execution time
     * has hit a specified limit - collect profile data and
     * abort this job, but timeout if we cannot do so
     */

    /* set the recv */
    nreports = 1;  // always get a report from ourselves

    /* setup the buffer */
    buffer = OBJ_NEW(opal_buffer_t);
    /* pack the command */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &command, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        goto giveup;
    }
    /* goes to just the first daemon beyond ourselves - no need to get it from everyone */
    dmns = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    if (NULL != (dmn = (orte_proc_t*)opal_pointer_array_get_item(dmns->procs, 1))) {
        ++nreports;
    }

    /* send it out */
    name.jobid = ORTE_PROC_MY_NAME->jobid;
    for (i=0; i < nreports; i++) {
        OBJ_RETAIN(buffer);
        name.vpid = i;
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              &name, buffer,
                                              ORTE_RML_TAG_DAEMON,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buffer);
        }
    }
    OBJ_RELEASE(buffer); // maintain accounting

    /* we will terminate after we get the profile, but set a timeout
     * just in case we never hear back */
    OBJ_CONSTRUCT(&profile_timer, orte_timer_t);
    opal_event_evtimer_set(orte_event_base,
                           profile_timer.ev, profile_timeout, NULL);
    opal_event_set_priority(profile_timer.ev, ORTE_ERROR_PRI);
    profile_timer.tv.tv_sec = 30;
    opal_event_evtimer_add(profile_timer.ev, &profile_timer.tv);
    return;

  giveup:
    /* abort the job */
    ORTE_ACTIVATE_JOB_STATE(NULL, ORTE_JOB_STATE_ALL_JOBS_COMPLETE);
}
