/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <stdio.h>
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif  /* HAVE_SYS_PARAM_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#include <sys/stat.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <errno.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif  /* HAVE_DIRENT_H */
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif  /* HAVE_PWD_H */

#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/os_path.h"
#include "opal/util/os_dirpath.h"
#include "opal/util/basename.h"
#include "opal/util/opal_environ.h"

#include "orte/util/proc_info.h"
#include "orte/util/name_fns.h"
#include "orte/util/show_help.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ras/base/base.h"
#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"

#include "orte/util/session_dir.h"

/*******************************
 * Local function Declarations
 *******************************/
static int orte_create_dir(char *directory);

static bool orte_dir_check_file(const char *root, const char *path);

#define OMPI_PRINTF_FIX_STRING(a) ((NULL == a) ? "(null)" : a)

/****************************
 * Funcationality
 ****************************/
/*
 * Check and create the directory requested
 */
static int orte_create_dir(char *directory)
{
    mode_t my_mode = S_IRWXU;  /* I'm looking for full rights */
    int ret;

    /* Sanity check before creating the directory with the proper mode,
     * Make sure it doesn't exist already */
    if( ORTE_ERR_NOT_FOUND !=
        (ret = opal_os_dirpath_access(directory, my_mode)) ) {
        /* Failure because opal_os_dirpath_access() indicated that either:
         * - The directory exists and we can access it (no need to create it again),
         *    return OPAL_SUCCESS, or
         * - don't have access rights, return OPAL_ERROR
         */
        if (ORTE_SUCCESS != ret) {
            ORTE_ERROR_LOG(ret);
        }
        return(ret);
    }

    /* Get here if the directory doesn't exist, so create it */
    if (ORTE_SUCCESS != (ret = opal_os_dirpath_create(directory, my_mode))) {
        ORTE_ERROR_LOG(ret);
    }
    return ret;
}


static int _setup_tmpdir_base(void)
{
    int rc = ORTE_SUCCESS;

    /* make sure that we have tmpdir_base set
     * if we need it
     */
    if (NULL == orte_process_info.tmpdir_base) {
        orte_process_info.tmpdir_base =
                strdup(opal_tmp_directory());
        if (NULL == orte_process_info.tmpdir_base) {
            rc = ORTE_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
    }
exit:
    if( ORTE_SUCCESS != rc ){
        ORTE_ERROR_LOG(rc);
    }
    return rc;
}

int orte_setup_top_session_dir(void)
{
    int rc = ORTE_SUCCESS;
    /* get the effective uid */
    uid_t uid = geteuid();

    /* construct the top_session_dir if we need */
    if (NULL == orte_process_info.top_session_dir) {
        if (ORTE_SUCCESS != (rc = _setup_tmpdir_base())) {
            return rc;
        }
        if( NULL == orte_process_info.nodename ||
                NULL == orte_process_info.tmpdir_base ){
            /* we can't setup top session dir */
            rc = ORTE_ERR_BAD_PARAM;
            goto exit;
        }

        if (0 > asprintf(&orte_process_info.top_session_dir,
                         "%s/ompi.%s.%lu", orte_process_info.tmpdir_base,
                         orte_process_info.nodename, (unsigned long)uid)) {
            orte_process_info.top_session_dir = NULL;
            rc = ORTE_ERR_OUT_OF_RESOURCE;
            goto exit;
        }
    }
exit:
    if( ORTE_SUCCESS != rc ){
        ORTE_ERROR_LOG(rc);
    }
    return rc;
}

static int _setup_jobfam_session_dir(orte_process_name_t *proc)
{
    int rc = ORTE_SUCCESS;

    /* construct the top_session_dir if we need */
    if (NULL == orte_process_info.jobfam_session_dir) {
        if (ORTE_SUCCESS != (rc = orte_setup_top_session_dir())) {
            return rc;
        }

        if (ORTE_PROC_IS_MASTER) {
            if (0 > asprintf(&orte_process_info.jobfam_session_dir,
                             "%s/dvm", orte_process_info.top_session_dir)) {
                rc = ORTE_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        } else if (ORTE_PROC_IS_HNP) {
            if (0 > asprintf(&orte_process_info.jobfam_session_dir,
                             "%s/pid.%lu", orte_process_info.top_session_dir,
                             (unsigned long)orte_process_info.pid)) {
                rc = ORTE_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        } else {
            /* we were not given one, so define it */
            if (NULL == proc || (ORTE_JOBID_INVALID == proc->jobid)) {
                if (0 > asprintf(&orte_process_info.jobfam_session_dir,
                                 "%s/jobfam", orte_process_info.top_session_dir) ) {
                    rc = ORTE_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
            } else {
                if (0 > asprintf(&orte_process_info.jobfam_session_dir,
                                 "%s/jf.%d", orte_process_info.top_session_dir,
                                 ORTE_JOB_FAMILY(proc->jobid))) {
                    orte_process_info.jobfam_session_dir = NULL;
                    rc = ORTE_ERR_OUT_OF_RESOURCE;
                    goto exit;
                }
            }
        }
    }
exit:
    if( ORTE_SUCCESS != rc ){
        ORTE_ERROR_LOG(rc);
    }
    return rc;
}

static int
_setup_job_session_dir(orte_process_name_t *proc)
{
    int rc = ORTE_SUCCESS;

    /* construct the top_session_dir if we need */
    if( NULL == orte_process_info.job_session_dir ){
        if( ORTE_SUCCESS != (rc = _setup_jobfam_session_dir(proc)) ){
            return rc;
        }
        if (ORTE_JOBID_INVALID != proc->jobid) {
            if (0 > asprintf(&orte_process_info.job_session_dir,
                             "%s/%d", orte_process_info.jobfam_session_dir,
                             ORTE_LOCAL_JOBID(proc->jobid))) {
                orte_process_info.job_session_dir = NULL;
                rc = ORTE_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        } else {
            orte_process_info.job_session_dir = NULL;
        }
    }

exit:
    if( ORTE_SUCCESS != rc ){
        ORTE_ERROR_LOG(rc);
    }
    return rc;
}

static int
_setup_proc_session_dir(orte_process_name_t *proc)
{
    int rc = ORTE_SUCCESS;

    /* construct the top_session_dir if we need */
    if( NULL == orte_process_info.proc_session_dir ){
        if( ORTE_SUCCESS != (rc = _setup_job_session_dir(proc)) ){
            return rc;
        }
        if (ORTE_VPID_INVALID != proc->vpid) {
            if (0 > asprintf(&orte_process_info.proc_session_dir,
                             "%s/%d", orte_process_info.job_session_dir,
                             proc->vpid)) {
                orte_process_info.proc_session_dir = NULL;
                rc = ORTE_ERR_OUT_OF_RESOURCE;
                goto exit;
            }
        } else {
            orte_process_info.proc_session_dir = NULL;
        }
    }

exit:
    if( ORTE_SUCCESS != rc ){
        ORTE_ERROR_LOG(rc);
    }
    return rc;
}

int orte_session_setup_base(orte_process_name_t *proc)
{
    int rc;

    /* Ensure that system info is set */
    orte_proc_info();

    /* setup job and proc session directories */
    if( ORTE_SUCCESS != (rc = _setup_job_session_dir(proc)) ){
        return rc;
    }

    if( ORTE_SUCCESS != (rc = _setup_proc_session_dir(proc)) ){
        return rc;
    }

    /* BEFORE doing anything else, check to see if this prefix is
     * allowed by the system
     */
    if (NULL != orte_prohibited_session_dirs ||
            NULL != orte_process_info.tmpdir_base ) {
        char **list;
        int i, len;
        /* break the string into tokens - it should be
         * separated by ','
         */
        list = opal_argv_split(orte_prohibited_session_dirs, ',');
        len = opal_argv_count(list);
        /* cycle through the list */
        for (i=0; i < len; i++) {
            /* check if prefix matches */
            if (0 == strncmp(orte_process_info.tmpdir_base, list[i], strlen(list[i]))) {
                /* this is a prohibited location */
                orte_show_help("help-orte-runtime.txt",
                               "orte:session:dir:prohibited",
                               true, orte_process_info.tmpdir_base,
                               orte_prohibited_session_dirs);
                opal_argv_free(list);
                return ORTE_ERR_FATAL;
            }
        }
        opal_argv_free(list);  /* done with this */
    }
    return ORTE_SUCCESS;
}

/*
 * Construct the session directory and create it if necessary
 */
int orte_session_dir(bool create, orte_process_name_t *proc)
{
    int rc = ORTE_SUCCESS;

    /*
     * Get the session directory full name
     */
    if (ORTE_SUCCESS != (rc = orte_session_setup_base(proc))) {
        if (ORTE_ERR_FATAL == rc) {
            /* this indicates we should abort quietly */
            rc = ORTE_ERR_SILENT;
        }
        goto cleanup;
    }

    /*
     * Now that we have the full path, go ahead and create it if necessary
     */
    if( create ) {
        if( ORTE_SUCCESS != (rc = orte_create_dir(orte_process_info.proc_session_dir)) ) {
            ORTE_ERROR_LOG(rc);
            goto cleanup;
        }
    }

    if (orte_debug_flag) {
        opal_output(0, "procdir: %s",
                    OMPI_PRINTF_FIX_STRING(orte_process_info.proc_session_dir));
        opal_output(0, "jobdir: %s",
                    OMPI_PRINTF_FIX_STRING(orte_process_info.job_session_dir));
        opal_output(0, "top: %s",
                    OMPI_PRINTF_FIX_STRING(orte_process_info.jobfam_session_dir));
        opal_output(0, "top: %s",
                    OMPI_PRINTF_FIX_STRING(orte_process_info.top_session_dir));
        opal_output(0, "tmp: %s",
                    OMPI_PRINTF_FIX_STRING(orte_process_info.tmpdir_base));
    }

cleanup:
    return rc;
}

/*
 * A job has aborted - so force cleanup of the session directory
 */
int
orte_session_dir_cleanup(orte_jobid_t jobid)
{
    /* special case - if a daemon is colocated with mpirun,
     * then we let mpirun do the rest to avoid a race
     * condition. this scenario always results in the rank=1
     * daemon colocated with mpirun */
    if (orte_ras_base.launch_orted_on_hn &&
        ORTE_PROC_IS_DAEMON &&
        1 == ORTE_PROC_MY_NAME->vpid) {
        return ORTE_SUCCESS;
    }

    if (!orte_create_session_dirs || orte_process_info.rm_session_dirs ) {
        /* we haven't created them or RM will clean them up for us*/
        return ORTE_SUCCESS;
    }

    if (NULL == orte_process_info.jobfam_session_dir ||
        NULL == orte_process_info.proc_session_dir) {
        /* this should never happen - it means we are calling
         * cleanup *before* properly setting up the session
         * dir system. This leaves open the possibility of
         * accidentally removing directories we shouldn't
         * touch
         */
        return ORTE_ERR_NOT_INITIALIZED;
    }


    /* recursively blow the whole session away for our job family,
     * saving only output files
     */
    opal_os_dirpath_destroy(orte_process_info.jobfam_session_dir,
                            true, orte_dir_check_file);

    if (opal_os_dirpath_is_empty(orte_process_info.jobfam_session_dir)) {
        if (orte_debug_flag) {
            opal_output(0, "sess_dir_cleanup: found jobfam session dir empty - deleting");
        }
        rmdir(orte_process_info.jobfam_session_dir);
    } else {
        if (orte_debug_flag) {
            if (OPAL_ERR_NOT_FOUND ==
                    opal_os_dirpath_access(orte_process_info.job_session_dir, 0)) {
                opal_output(0, "sess_dir_cleanup: job session dir does not exist");
            } else {
                opal_output(0, "sess_dir_cleanup: job session dir not empty - leaving");
            }
        }
    }

    if (NULL != orte_process_info.top_session_dir) {
        if (opal_os_dirpath_is_empty(orte_process_info.top_session_dir)) {
            if (orte_debug_flag) {
                opal_output(0, "sess_dir_cleanup: found top session dir empty - deleting");
            }
            rmdir(orte_process_info.top_session_dir);
        } else {
            if (orte_debug_flag) {
                if (OPAL_ERR_NOT_FOUND ==
                        opal_os_dirpath_access(orte_process_info.top_session_dir, 0)) {
                    opal_output(0, "sess_dir_cleanup: top session dir does not exist");
                } else {
                    opal_output(0, "sess_dir_cleanup: top session dir not empty - leaving");
                }
            }
        }
    }

    /* now attempt to eliminate the top level directory itself - this
     * will fail if anything is present, but ensures we cleanup if
     * we are the last one out
     */
    if( NULL != orte_process_info.top_session_dir ){
        opal_os_dirpath_destroy(orte_process_info.top_session_dir,
                                false, orte_dir_check_file);
    }


    return ORTE_SUCCESS;
}


int
orte_session_dir_finalize(orte_process_name_t *proc)
{
    if (!orte_create_session_dirs || orte_process_info.rm_session_dirs ) {
        /* we haven't created them or RM will clean them up for us*/
        return ORTE_SUCCESS;
    }

    if (NULL == orte_process_info.job_session_dir ||
        NULL == orte_process_info.proc_session_dir) {
        /* this should never happen - it means we are calling
         * cleanup *before* properly setting up the session
         * dir system. This leaves open the possibility of
         * accidentally removing directories we shouldn't
         * touch
         */
        return ORTE_ERR_NOT_INITIALIZED;
    }

    opal_os_dirpath_destroy(orte_process_info.proc_session_dir,
                            false, orte_dir_check_file);

    if (opal_os_dirpath_is_empty(orte_process_info.proc_session_dir)) {
        if (orte_debug_flag) {
            opal_output(0, "sess_dir_finalize: found proc session dir empty - deleting");
        }
        rmdir(orte_process_info.proc_session_dir);
    } else {
        if (orte_debug_flag) {
            if (OPAL_ERR_NOT_FOUND ==
                    opal_os_dirpath_access(orte_process_info.proc_session_dir, 0)) {
                opal_output(0, "sess_dir_finalize: proc session dir does not exist");
            } else {
                opal_output(0, "sess_dir_finalize: proc session dir not empty - leaving");
            }
        }
    }

    /* special case - if a daemon is colocated with mpirun,
     * then we let mpirun do the rest to avoid a race
     * condition. this scenario always results in the rank=1
     * daemon colocated with mpirun */
    if (orte_ras_base.launch_orted_on_hn &&
        ORTE_PROC_IS_DAEMON &&
        1 == ORTE_PROC_MY_NAME->vpid) {
        return ORTE_SUCCESS;
    }

    opal_os_dirpath_destroy(orte_process_info.job_session_dir,
                            false, orte_dir_check_file);

    /* only remove the jobfam session dir if we are the
     * local daemon and we are finalizing our own session dir */
    if ((ORTE_PROC_IS_HNP || ORTE_PROC_IS_DAEMON) &&
        (ORTE_PROC_MY_NAME == proc)) {
        opal_os_dirpath_destroy(orte_process_info.jobfam_session_dir,
                                false, orte_dir_check_file);
    }

    if( NULL != orte_process_info.top_session_dir ){
        opal_os_dirpath_destroy(orte_process_info.top_session_dir,
                                false, orte_dir_check_file);
    }

    if (opal_os_dirpath_is_empty(orte_process_info.job_session_dir)) {
        if (orte_debug_flag) {
            opal_output(0, "sess_dir_finalize: found job session dir empty - deleting");
        }
        rmdir(orte_process_info.job_session_dir);
    } else {
        if (orte_debug_flag) {
            if (OPAL_ERR_NOT_FOUND ==
                    opal_os_dirpath_access(orte_process_info.job_session_dir, 0)) {
                opal_output(0, "sess_dir_finalize: job session dir does not exist");
            } else {
                opal_output(0, "sess_dir_finalize: job session dir not empty - leaving");
            }
        }
    }

    if (opal_os_dirpath_is_empty(orte_process_info.jobfam_session_dir)) {
        if (orte_debug_flag) {
            opal_output(0, "sess_dir_finalize: found jobfam session dir empty - deleting");
        }
        rmdir(orte_process_info.jobfam_session_dir);
    } else {
        if (orte_debug_flag) {
            if (OPAL_ERR_NOT_FOUND ==
                    opal_os_dirpath_access(orte_process_info.jobfam_session_dir, 0)) {
                opal_output(0, "sess_dir_finalize: jobfam session dir does not exist");
            } else {
                opal_output(0, "sess_dir_finalize: jobfam session dir not empty - leaving");
            }
        }
    }

    if (opal_os_dirpath_is_empty(orte_process_info.jobfam_session_dir)) {
        if (orte_debug_flag) {
            opal_output(0, "sess_dir_finalize: found jobfam session dir empty - deleting");
        }
        rmdir(orte_process_info.jobfam_session_dir);
    } else {
        if (orte_debug_flag) {
            if (OPAL_ERR_NOT_FOUND ==
                    opal_os_dirpath_access(orte_process_info.jobfam_session_dir, 0)) {
                opal_output(0, "sess_dir_finalize: jobfam session dir does not exist");
            } else {
                opal_output(0, "sess_dir_finalize: jobfam session dir not empty - leaving");
            }
        }
    }

    if (NULL != orte_process_info.top_session_dir) {
        if (opal_os_dirpath_is_empty(orte_process_info.top_session_dir)) {
            if (orte_debug_flag) {
                opal_output(0, "sess_dir_finalize: found top session dir empty - deleting");
            }
            rmdir(orte_process_info.top_session_dir);
        } else {
            if (orte_debug_flag) {
                if (OPAL_ERR_NOT_FOUND ==
                        opal_os_dirpath_access(orte_process_info.top_session_dir, 0)) {
                    opal_output(0, "sess_dir_finalize: top session dir does not exist");
                } else {
                    opal_output(0, "sess_dir_finalize: top session dir not empty - leaving");
                }
            }
        }
    }

    return ORTE_SUCCESS;
}

static bool
orte_dir_check_file(const char *root, const char *path)
{
    struct stat st;
    char *fullpath;

    /*
     * Keep:
     *  - non-zero files starting with "output-"
     */
    if (0 == strncmp(path, "output-", strlen("output-"))) {
        fullpath = opal_os_path(false, &fullpath, root, path, NULL);
        stat(fullpath, &st);
        free(fullpath);
        if (0 == st.st_size) {
            return true;
        }
        return false;
    }

    return true;
}
