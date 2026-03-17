/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/*
 *
 */

#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif  /* HAVE_DIRENT_H */
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/dss/dss.h"

#include "orte/util/show_help.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/util/os_dirpath.h"
#include "opal/util/os_path.h"
#include "opal/util/path.h"
#include "opal/util/basename.h"

#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/rml/rml.h"

#include "orte/mca/filem/filem.h"
#include "orte/mca/filem/base/base.h"

#include "filem_raw.h"

static int raw_init(void);
static int raw_finalize(void);
static int raw_preposition_files(orte_job_t *jdata,
                                 orte_filem_completion_cbfunc_t cbfunc,
                                 void *cbdata);
static int raw_link_local_files(orte_job_t *jdata,
                                orte_app_context_t *app);

orte_filem_base_module_t mca_filem_raw_module = {
    .filem_init = raw_init,
    .filem_finalize = raw_finalize,
    /* we don't use any of the following */
    .put = orte_filem_base_none_put,
    .put_nb = orte_filem_base_none_put_nb,
    .get = orte_filem_base_none_get,
    .get_nb = orte_filem_base_none_get_nb,
    .rm = orte_filem_base_none_rm,
    .rm_nb = orte_filem_base_none_rm_nb,
    .wait = orte_filem_base_none_wait,
    .wait_all = orte_filem_base_none_wait_all,
    /* now the APIs we *do* use */
    .preposition_files = raw_preposition_files,
    .link_local_files = raw_link_local_files
};

static opal_list_t outbound_files;
static opal_list_t incoming_files;
static opal_list_t positioned_files;

static void send_chunk(int fd, short argc, void *cbdata);
static void recv_files(int status, orte_process_name_t* sender,
                       opal_buffer_t* buffer, orte_rml_tag_t tag,
                       void* cbdata);
static void recv_ack(int status, orte_process_name_t* sender,
                     opal_buffer_t* buffer, orte_rml_tag_t tag,
                     void* cbdata);
static void write_handler(int fd, short event, void *cbdata);

static char *filem_session_dir(void)
{
    char *session_dir = orte_process_info.jobfam_session_dir;
    if( NULL == session_dir ){
        /* if no job family session dir was provided -
         * use the job session dir */
        session_dir = orte_process_info.job_session_dir;
    }
    return session_dir;
}

static int raw_init(void)
{
    OBJ_CONSTRUCT(&incoming_files, opal_list_t);

    /* start a recv to catch any files sent to me */
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_FILEM_BASE,
                            ORTE_RML_PERSISTENT,
                            recv_files,
                            NULL);

    /* if I'm the HNP, start a recv to catch acks sent to me */
    if (ORTE_PROC_IS_HNP) {
        OBJ_CONSTRUCT(&outbound_files, opal_list_t);
        OBJ_CONSTRUCT(&positioned_files, opal_list_t);
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                ORTE_RML_TAG_FILEM_BASE_RESP,
                                ORTE_RML_PERSISTENT,
                                recv_ack,
                                NULL);
    }

    return ORTE_SUCCESS;
}

static int raw_finalize(void)
{
    opal_list_item_t *item;

    while (NULL != (item = opal_list_remove_first(&incoming_files))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&incoming_files);

    if (ORTE_PROC_IS_HNP) {
        while (NULL != (item = opal_list_remove_first(&outbound_files))) {
            OBJ_RELEASE(item);
        }
        OBJ_DESTRUCT(&outbound_files);
        while (NULL != (item = opal_list_remove_first(&positioned_files))) {
            OBJ_RELEASE(item);
        }
        OBJ_DESTRUCT(&positioned_files);
    }

    return ORTE_SUCCESS;
}

static void xfer_complete(int status, orte_filem_raw_xfer_t *xfer)
{
    orte_filem_raw_outbound_t *outbound = xfer->outbound;

    /* transfer the status, if not success */
    if (ORTE_SUCCESS != status) {
        outbound->status = status;
    }

    /* this transfer is complete - remove it from list */
    opal_list_remove_item(&outbound->xfers, &xfer->super);
    /* add it to the list of files that have been positioned */
    opal_list_append(&positioned_files, &xfer->super);

    /* if the list is now empty, then the xfer is complete */
    if (0 == opal_list_get_size(&outbound->xfers)) {
        /* do the callback */
        if (NULL != outbound->cbfunc) {
            outbound->cbfunc(outbound->status, outbound->cbdata);
        }
        /* release the object */
        opal_list_remove_item(&outbound_files, &outbound->super);
        OBJ_RELEASE(outbound);
    }
}

static void recv_ack(int status, orte_process_name_t* sender,
                     opal_buffer_t* buffer, orte_rml_tag_t tag,
                     void* cbdata)
{
    opal_list_item_t *item, *itm;
    orte_filem_raw_outbound_t *outbound;
    orte_filem_raw_xfer_t *xfer;
    char *file;
    int st, n, rc;

    /* unpack the file */
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &file, &n, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    /* unpack the status */
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &st, &n, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s filem:raw: recvd ack from %s for file %s status %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(sender), file, st));

    /* find the corresponding outbound object */
    for (item = opal_list_get_first(&outbound_files);
         item != opal_list_get_end(&outbound_files);
         item = opal_list_get_next(item)) {
        outbound = (orte_filem_raw_outbound_t*)item;
        for (itm = opal_list_get_first(&outbound->xfers);
             itm != opal_list_get_end(&outbound->xfers);
             itm = opal_list_get_next(itm)) {
            xfer = (orte_filem_raw_xfer_t*)itm;
            if (0 == strcmp(file, xfer->file)) {
                /* if the status isn't success, record it */
                if (0 != st) {
                    xfer->status = st;
                }
                /* track number of respondents */
                xfer->nrecvd++;
                /* if all daemons have responded, then this is complete */
                if (xfer->nrecvd == orte_process_info.num_procs) {
                    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                         "%s filem:raw: xfer complete for file %s status %d",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         file, xfer->status));
                    xfer_complete(xfer->status, xfer);
                }
                free(file);
                return;
            }
        }
    }
}

static int raw_preposition_files(orte_job_t *jdata,
                                 orte_filem_completion_cbfunc_t cbfunc,
                                 void *cbdata)
{
    orte_app_context_t *app;
    opal_list_item_t *item, *itm, *itm2;
    orte_filem_base_file_set_t *fs;
    int fd;
    orte_filem_raw_xfer_t *xfer, *xptr;
    int flags, i, j;
    char **files=NULL;
    orte_filem_raw_outbound_t *outbound, *optr;
    char *cptr, *nxt, *filestring;
    opal_list_t fsets;
    bool already_sent;

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s filem:raw: preposition files for job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));

    /* cycle across the app_contexts looking for files or
     * binaries to be prepositioned
     */
    OBJ_CONSTRUCT(&fsets, opal_list_t);
    for (i=0; i < jdata->apps->size; i++) {
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, i))) {
            continue;
        }
        if (orte_get_attribute(&app->attributes, ORTE_APP_PRELOAD_BIN, NULL, OPAL_BOOL)) {
            /* add the executable to our list */
            OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: preload executable %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 app->app));
            fs = OBJ_NEW(orte_filem_base_file_set_t);
            fs->local_target = strdup(app->app);
            fs->target_flag = ORTE_FILEM_TYPE_EXE;
            opal_list_append(&fsets, &fs->super);
            /* if we are preloading the binary, then the app must be in relative
             * syntax or we won't find it - the binary will be positioned in the
             * session dir, so ensure the app is relative to that location
             */
            cptr = opal_basename(app->app);
            free(app->app);
            asprintf(&app->app, "./%s", cptr);
            free(app->argv[0]);
            app->argv[0] = strdup(app->app);
            fs->remote_target = strdup(app->app);
        }
        if (orte_get_attribute(&app->attributes, ORTE_APP_PRELOAD_FILES, (void**)&filestring, OPAL_STRING)) {
            files = opal_argv_split(filestring, ',');
            free(filestring);
            for (j=0; NULL != files[j]; j++) {
                fs = OBJ_NEW(orte_filem_base_file_set_t);
                fs->local_target = strdup(files[j]);
                /* check any suffix for file type */
                if (NULL != (cptr = strchr(files[j], '.'))) {
                    if (0 == strncmp(cptr, ".tar", 4)) {
                        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                             "%s filem:raw: marking file %s as TAR",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             files[j]));
                        fs->target_flag = ORTE_FILEM_TYPE_TAR;
                    } else if (0 == strncmp(cptr, ".bz", 3)) {
                        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                             "%s filem:raw: marking file %s as BZIP",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             files[j]));
                        fs->target_flag = ORTE_FILEM_TYPE_BZIP;
                    } else if (0 == strncmp(cptr, ".gz", 3)) {
                        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                             "%s filem:raw: marking file %s as GZIP",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             files[j]));
                        fs->target_flag = ORTE_FILEM_TYPE_GZIP;
                    } else {
                        fs->target_flag = ORTE_FILEM_TYPE_FILE;
                    }
                } else {
                    fs->target_flag = ORTE_FILEM_TYPE_FILE;
                }
                /* if we are flattening directory trees, then the
                 * remote path is just the basename file name
                 */
                if (orte_filem_raw_flatten_trees) {
                    fs->remote_target = opal_basename(files[j]);
                } else {
                    /* if this was an absolute path, then we need
                     * to convert it to a relative path - we do not
                     * allow positioning of files to absolute locations
                     * due to the potential for unintentional overwriting
                     * of files
                     */
                    if (opal_path_is_absolute(files[j])) {
                        fs->remote_target = strdup(&files[j][1]);
                    } else {
                        fs->remote_target = strdup(files[j]);
                    }
                }
                opal_list_append(&fsets, &fs->super);
                /* prep the filename for matching on the remote
                 * end by stripping any leading '.' directories to avoid
                 * stepping above the session dir location - all
                 * files will be relative to that point. Ensure
                 * we *don't* mistakenly strip the dot from a
                 * filename that starts with one
                 */
                cptr = fs->remote_target;
                nxt = cptr;
                nxt++;
                while ('\0' != *cptr) {
                    if ('.' == *cptr) {
                        /* have to check the next character to
                         * see if it's a dotfile or not
                         */
                        if ('.' == *nxt || '/' == *nxt) {
                            cptr = nxt;
                            nxt++;
                        } else {
                            /* if the next character isn't a dot
                             * or a slash, then this is a dot-file
                             * and we need to leave it alone
                             */
                            break;
                        }
                    } else if ('/' == *cptr) {
                        /* move to the next character */
                        cptr = nxt;
                        nxt++;
                    } else {
                        /* the character isn't a dot or a slash,
                         * so this is the beginning of the filename
                         */
                        break;
                    }
                }
                free(files[j]);
                files[j] = strdup(cptr);
            }
            /* replace the app's file list with the revised one so we
             * can find them on the remote end
             */
            filestring = opal_argv_join(files, ',');
            orte_set_attribute(&app->attributes, ORTE_APP_PRELOAD_FILES, ORTE_ATTR_GLOBAL, filestring, OPAL_STRING);
            /* cleanup for the next app */
            opal_argv_free(files);
            free(filestring);
        }
    }
    if (0 == opal_list_get_size(&fsets)) {
        /* nothing to preposition */
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: nothing to preposition",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        if (NULL != cbfunc) {
            cbfunc(ORTE_SUCCESS, cbdata);
        }
        OBJ_DESTRUCT(&fsets);
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s filem:raw: found %d files to position",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (int)opal_list_get_size(&fsets)));

    /* track the outbound file sets */
    outbound = OBJ_NEW(orte_filem_raw_outbound_t);
    outbound->cbfunc = cbfunc;
    outbound->cbdata = cbdata;
    opal_list_append(&outbound_files, &outbound->super);

    /* only the HNP should ever call this function - loop thru the
     * fileset and initiate xcast transfer of each file to every
     * daemon
     */
    while (NULL != (item = opal_list_remove_first(&fsets))) {
        fs = (orte_filem_base_file_set_t*)item;
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: checking prepositioning of file %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             fs->local_target));

        /* have we already sent this file? */
        already_sent = false;
        for (itm = opal_list_get_first(&positioned_files);
             !already_sent && itm != opal_list_get_end(&positioned_files);
             itm = opal_list_get_next(itm)) {
            xptr = (orte_filem_raw_xfer_t*)itm;
            if (0 == strcmp(fs->local_target, xptr->src)) {
                already_sent = true;
            }
        }
        if (already_sent) {
            /* no need to send it again */
            OPAL_OUTPUT_VERBOSE((3, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: file %s is already in position - ignoring",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fs->local_target));
            OBJ_RELEASE(item);
            continue;
        }
        /* also have to check if this file is already in the process
         * of being transferred, or was included multiple times
         * for transfer
         */
        for (itm = opal_list_get_first(&outbound_files);
             !already_sent && itm != opal_list_get_end(&outbound_files);
             itm = opal_list_get_next(itm)) {
            optr = (orte_filem_raw_outbound_t*)itm;
            for (itm2 = opal_list_get_first(&optr->xfers);
                 itm2 != opal_list_get_end(&optr->xfers);
                 itm2 = opal_list_get_next(itm2)) {
                xptr = (orte_filem_raw_xfer_t*)itm2;
                if (0 == strcmp(fs->local_target, xptr->src)) {
                    already_sent = true;
                }
            }
        }
        if (already_sent) {
            /* no need to send it again */
            OPAL_OUTPUT_VERBOSE((3, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: file %s is already queued for output - ignoring",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fs->local_target));
            OBJ_RELEASE(item);
            continue;
        }

        /* attempt to open the specified file */
        if (0 > (fd = open(fs->local_target, O_RDONLY))) {
            opal_output(0, "%s CANNOT ACCESS FILE %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fs->local_target);
            OBJ_RELEASE(item);
            opal_list_remove_item(&outbound_files, &outbound->super);
            OBJ_RELEASE(outbound);
            return ORTE_ERROR;
        }
        /* set the flags to non-blocking */
        if ((flags = fcntl(fd, F_GETFL, 0)) < 0) {
            opal_output(orte_filem_base_framework.framework_output, "[%s:%d]: fcntl(F_GETFL) failed with errno=%d\n",
                        __FILE__, __LINE__, errno);
        } else {
            flags |= O_NONBLOCK;
            if (fcntl(fd, F_SETFL, flags) < 0) {
                opal_output(orte_filem_base_framework.framework_output, "[%s:%d]: fcntl(F_GETFL) failed with errno=%d\n",
                            __FILE__, __LINE__, errno);
            }
        }
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: setting up to position file %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fs->local_target));
        xfer = OBJ_NEW(orte_filem_raw_xfer_t);
        /* save the source so we can avoid duplicate transfers */
        xfer->src = strdup(fs->local_target);
        /* strip any leading '.' directories to avoid
         * stepping above the session dir location - all
         * files will be relative to that point. Ensure
         * we *don't* mistakenly strip the dot from a
         * filename that starts with one
         */
        cptr = fs->remote_target;
        nxt = cptr;
        nxt++;
        while ('\0' != *cptr) {
            if ('.' == *cptr) {
                /* have to check the next character to
                 * see if it's a dotfile or not
                 */
                if ('.' == *nxt || '/' == *nxt) {
                    cptr = nxt;
                    nxt++;
                } else {
                    /* if the next character isn't a dot
                     * or a slash, then this is a dot-file
                     * and we need to leave it alone
                     */
                    break;
                }
            } else if ('/' == *cptr) {
                /* move to the next character */
                cptr = nxt;
                nxt++;
            } else {
                /* the character isn't a dot or a slash,
                 * so this is the beginning of the filename
                 */
                break;
            }
        }
        xfer->file = strdup(cptr);
        xfer->type = fs->target_flag;
        xfer->app_idx = fs->app_idx;
        xfer->outbound = outbound;
        opal_list_append(&outbound->xfers, &xfer->super);
        opal_event_set(orte_event_base, &xfer->ev, fd, OPAL_EV_READ, send_chunk, xfer);
        opal_event_set_priority(&xfer->ev, ORTE_MSG_PRI);
        xfer->pending = true;
        ORTE_POST_OBJECT(xfer);
        opal_event_add(&xfer->ev, 0);
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&fsets);

    /* check to see if anything remains to be sent - if everything
     * is a duplicate, then the list will be empty
     */
    if (0 == opal_list_get_size(&outbound->xfers)) {
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: all duplicate files - no positioning reqd",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        opal_list_remove_item(&outbound_files, &outbound->super);
        OBJ_RELEASE(outbound);
        if (NULL != cbfunc) {
            cbfunc(ORTE_SUCCESS, cbdata);
        }
        return ORTE_SUCCESS;
    }

    if (0 < opal_output_get_verbosity(orte_filem_base_framework.framework_output)) {
        opal_output(0, "%s Files to be positioned:", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        for (itm2 = opal_list_get_first(&outbound->xfers);
             itm2 != opal_list_get_end(&outbound->xfers);
             itm2 = opal_list_get_next(itm2)) {
            xptr = (orte_filem_raw_xfer_t*)itm2;
            opal_output(0, "%s\t%s", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), xptr->src);
        }
    }

    return ORTE_SUCCESS;
}

static int create_link(char *my_dir, char *path,
                       char *link_pt)
{
    char *mypath, *fullname, *basedir;
    struct stat buf;
    int rc = ORTE_SUCCESS;

    /* form the full source path name */
    mypath = opal_os_path(false, my_dir, link_pt, NULL);
    /* form the full target path name */
    fullname = opal_os_path(false, path, link_pt, NULL);
    /* there may have been multiple files placed under the
     * same directory, so check for existence first
     */
    if (0 != stat(fullname, &buf)) {
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: creating symlink to %s\n\tmypath: %s\n\tlink: %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), link_pt,
                             mypath, fullname));
        /* create any required path to the link location */
        basedir = opal_dirname(fullname);
        if (OPAL_SUCCESS != (rc = opal_os_dirpath_create(basedir, S_IRWXU))) {
            ORTE_ERROR_LOG(rc);
            opal_output(0, "%s Failed to symlink %s to %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), mypath, fullname);
            free(basedir);
            free(mypath);
            free(fullname);
            return rc;
        }
        free(basedir);
        /* do the symlink */
        if (0 != symlink(mypath, fullname)) {
            opal_output(0, "%s Failed to symlink %s to %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), mypath, fullname);
            rc = ORTE_ERROR;
        }
    }
    free(mypath);
    free(fullname);
    return rc;
}

static int raw_link_local_files(orte_job_t *jdata,
                                orte_app_context_t *app)
{
    char *session_dir, *path=NULL;
    orte_proc_t *proc;
    int i, j, rc;
    orte_filem_raw_incoming_t *inbnd;
    opal_list_item_t *item;
    char **files=NULL, *bname, *filestring;

    /* check my jobfam session directory for files I have received and
     * symlink them to the proc-level session directory of each
     * local process in the job
     *
     * TODO: @rhc - please check that I've correctly interpret your
     *  intention here
     */
    session_dir = filem_session_dir();
    if( NULL == session_dir){
        /* we were unable to find any suitable directory */
        rc = ORTE_ERR_BAD_PARAM;
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    /* get the list of files this app wants */
    if (orte_get_attribute(&app->attributes, ORTE_APP_PRELOAD_FILES, (void**)&filestring, OPAL_STRING)) {
        files = opal_argv_split(filestring, ',');
        free(filestring);
    }
    if (orte_get_attribute(&app->attributes, ORTE_APP_PRELOAD_BIN, NULL, OPAL_BOOL)) {
        /* add the app itself to the list */
        bname = opal_basename(app->app);
        opal_argv_append_nosize(&files, bname);
        free(bname);
    }

    /* if there are no files to link, then ignore this */
    if (NULL == files) {
        return ORTE_SUCCESS;
    }

    for (i=0; i < orte_local_children->size; i++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(orte_local_children, i))) {
            continue;
        }
        OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                             "%s filem:raw: working symlinks for proc %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name)));
        if (proc->name.jobid != jdata->jobid) {
            OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: proc %s not part of job %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name),
                                 ORTE_JOBID_PRINT(jdata->jobid)));
            continue;
        }
        if (proc->app_idx != app->idx) {
            OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: proc %s not part of app_idx %d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name),
                                 (int)app->idx));
            continue;
        }
        /* ignore children we have already handled */
        if (ORTE_FLAG_TEST(proc, ORTE_PROC_FLAG_ALIVE) ||
            (ORTE_PROC_STATE_INIT != proc->state &&
             ORTE_PROC_STATE_RESTART != proc->state)) {
            continue;
        }

        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: creating symlinks for %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&proc->name)));

        /* get the session dir name in absolute form */
        path = orte_process_info.proc_session_dir;

        /* create it, if it doesn't already exist */
        if (OPAL_SUCCESS != (rc = opal_os_dirpath_create(path, S_IRWXU))) {
            ORTE_ERROR_LOG(rc);
            /* doesn't exist with correct permissions, and/or we can't
             * create it - either way, we are done
             */
            free(files);
            return rc;
        }

        /* cycle thru the incoming files */
        for (item = opal_list_get_first(&incoming_files);
             item != opal_list_get_end(&incoming_files);
             item = opal_list_get_next(item)) {
            inbnd = (orte_filem_raw_incoming_t*)item;
            OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: checking file %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), inbnd->file));

            /* is this file for this app_context? */
            for (j=0; NULL != files[j]; j++) {
                if (0 == strcmp(inbnd->file, files[j])) {
                    /* this must be one of the files we are to link against */
                    if (NULL != inbnd->link_pts) {
                        OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                                             "%s filem:raw: creating links for file %s",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             inbnd->file));
                        /* cycle thru the link points and create symlinks to them */
                        for (j=0; NULL != inbnd->link_pts[j]; j++) {
                            if (ORTE_SUCCESS != (rc = create_link(session_dir, path, inbnd->link_pts[j]))) {
                                ORTE_ERROR_LOG(rc);
                                free(files);
                                return rc;
                            }
                        }
                    } else {
                        OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                                             "%s filem:raw: file %s has no link points",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             inbnd->file));
                    }
                    break;
                }
            }
        }
    }
    opal_argv_free(files);
    return ORTE_SUCCESS;
}

static void send_chunk(int fd, short argc, void *cbdata)
{
    orte_filem_raw_xfer_t *rev = (orte_filem_raw_xfer_t*)cbdata;
    unsigned char data[ORTE_FILEM_RAW_CHUNK_MAX];
    int32_t numbytes;
    int rc;
    opal_buffer_t chunk;
    orte_grpcomm_signature_t *sig;

    ORTE_ACQUIRE_OBJECT(rev);

    /* flag that event has fired */
    rev->pending = false;

    /* read up to the fragment size */
    numbytes = read(fd, data, sizeof(data));

    if (numbytes < 0) {
        /* either we have a connection error or it was a non-blocking read */

        /* non-blocking, retry */
        if (EAGAIN == errno || EINTR == errno) {
            ORTE_POST_OBJECT(rev);
            opal_event_add(&rev->ev, 0);
            return;
        }

        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw:read error on file %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rev->file));

        /* Un-recoverable error. Allow the code to flow as usual in order to
         * to send the zero bytes message up the stream, and then close the
         * file descriptor and delete the event.
         */
        numbytes = 0;
    }

    /* if job termination has been ordered, just ignore the
     * data and delete the read event
     */
    if (orte_job_term_ordered) {
        OBJ_RELEASE(rev);
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s filem:raw:read handler sending chunk %d of %d bytes for file %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rev->nchunk, numbytes, rev->file));

    /* package it for transmission */
    OBJ_CONSTRUCT(&chunk, opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&chunk, &rev->file, 1, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        close(fd);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&chunk, &rev->nchunk, 1, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        close(fd);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&chunk, data, numbytes, OPAL_BYTE))) {
        ORTE_ERROR_LOG(rc);
        close(fd);
        return;
    }
    /* if it is the first chunk, then add file type and index of the app */
    if (0 == rev->nchunk) {
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&chunk, &rev->type, 1, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            close(fd);
            return;
        }
    }

    /* goes to all daemons */
    sig = OBJ_NEW(orte_grpcomm_signature_t);
    sig->signature = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    sig->signature[0].jobid = ORTE_PROC_MY_NAME->jobid;
    sig->signature[0].vpid = ORTE_VPID_WILDCARD;
    if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(sig, ORTE_RML_TAG_FILEM_BASE, &chunk))) {
        ORTE_ERROR_LOG(rc);
        close(fd);
        return;
    }
    OBJ_DESTRUCT(&chunk);
    OBJ_RELEASE(sig);
    rev->nchunk++;

    /* if num_bytes was zero, then we need to terminate the event
     * and close the file descriptor
     */
    if (0 == numbytes) {
        close(fd);
        return;
    } else {
        /* restart the read event */
        rev->pending = true;
        ORTE_POST_OBJECT(rev);
        opal_event_add(&rev->ev, 0);
    }
}

static void send_complete(char *file, int status)
{
    opal_buffer_t *buf;
    int rc;

    buf = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &file, 1, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &status, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
        return;
    }
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          ORTE_PROC_MY_HNP, buf,
                                          ORTE_RML_TAG_FILEM_BASE_RESP,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buf);
    }
}

/* This is a little tricky as the name of the archive doesn't
 * necessarily have anything to do with the paths inside it -
 * so we have to first query the archive to retrieve that info
 */
static int link_archive(orte_filem_raw_incoming_t *inbnd)
{
    FILE *fp;
    char *cmd;
    char path[MAXPATHLEN];

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s filem:raw: identifying links for archive %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         inbnd->fullpath));

    asprintf(&cmd, "tar tf %s", inbnd->fullpath);
    fp = popen(cmd, "r");
    free(cmd);
    if (NULL == fp) {
        ORTE_ERROR_LOG(ORTE_ERR_FILE_OPEN_FAILURE);
        return ORTE_ERR_FILE_OPEN_FAILURE;
    }
    /* because app_contexts might share part or all of a
     * directory tree, but link to different files, we
     * have to link to each individual file
     */
    while (fgets(path, sizeof(path), fp) != NULL) {
        OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                             "%s filem:raw: path %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             path));
        /* protect against an empty result */
        if (0 == strlen(path)) {
            continue;
        }
        /* trim the trailing cr */
        path[strlen(path)-1] = '\0';
        /* ignore directories */
        if ('/' == path[strlen(path)-1]) {
            OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: path %s is a directory - ignoring it",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 path));
            continue;
        }
        /* ignore specific useless directory trees */
        if (NULL != strstr(path, ".deps")) {
            OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                                 "%s filem:raw: path %s includes .deps - ignoring it",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 path));
            continue;
        }
        OPAL_OUTPUT_VERBOSE((10, orte_filem_base_framework.framework_output,
                             "%s filem:raw: adding path %s to link points",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             path));
        opal_argv_append_nosize(&inbnd->link_pts, path);
    }
    /* close */
    pclose(fp);
    return ORTE_SUCCESS;
}

static void recv_files(int status, orte_process_name_t* sender,
                       opal_buffer_t* buffer, orte_rml_tag_t tag,
                       void* cbdata)
{
    char *file, *session_dir;
    int32_t nchunk, n, nbytes;
    unsigned char data[ORTE_FILEM_RAW_CHUNK_MAX];
    int rc;
    orte_filem_raw_output_t *output;
    orte_filem_raw_incoming_t *ptr, *incoming;
    opal_list_item_t *item;
    int32_t type;
    char *cptr;

    /* unpack the data */
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &file, &n, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        send_complete(NULL, rc);
        return;
    }
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &nchunk, &n, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        send_complete(file, rc);
        free(file);
        return;
    }
    /* if the chunk number is < 0, then this is an EOF message */
    if (nchunk < 0) {
        /* just set nbytes to zero so we close the fd */
        nbytes = 0;
    } else {
        nbytes=ORTE_FILEM_RAW_CHUNK_MAX;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, data, &nbytes, OPAL_BYTE))) {
            ORTE_ERROR_LOG(rc);
            send_complete(file, rc);
            free(file);
            return;
        }
    }
    /* if the chunk is 0, then additional info should be present */
    if (0 == nchunk) {
        n=1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &type, &n, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            send_complete(file, rc);
            free(file);
            return;
        }
    }

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s filem:raw: received chunk %d for file %s containing %d bytes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         nchunk, file, nbytes));

    /* do we already have this file on our list of incoming? */
    incoming = NULL;
    for (item = opal_list_get_first(&incoming_files);
         item != opal_list_get_end(&incoming_files);
         item = opal_list_get_next(item)) {
        ptr = (orte_filem_raw_incoming_t*)item;
        if (0 == strcmp(file, ptr->file)) {
            incoming = ptr;
            break;
        }
    }
    if (NULL == incoming) {
        /* nope - add it */
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: adding file %s to incoming list",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), file));
        incoming = OBJ_NEW(orte_filem_raw_incoming_t);
        incoming->file = strdup(file);
        incoming->type = type;
        opal_list_append(&incoming_files, &incoming->super);
    }

    /* if this is the first chunk, we need to open the file descriptor */
    if (0 == nchunk) {
        /* separate out the top-level directory of the target */
        char *tmp;
        tmp = strdup(file);
        if (NULL != (cptr = strchr(tmp, '/'))) {
            *cptr = '\0';
        }
        /* save it */
        incoming->top = strdup(tmp);
        free(tmp);
        /* define the full path to where we will put it */
        session_dir = filem_session_dir();

        incoming->fullpath = opal_os_path(false, session_dir, file, NULL);

        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s filem:raw: opening target file %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), incoming->fullpath));
        /* create the path to the target, if not already existing */
        tmp = opal_dirname(incoming->fullpath);
        if (OPAL_SUCCESS != (rc = opal_os_dirpath_create(tmp, S_IRWXU))) {
            ORTE_ERROR_LOG(rc);
            send_complete(file, ORTE_ERR_FILE_WRITE_FAILURE);
            free(file);
            free(tmp);
            OBJ_RELEASE(incoming);
            return;
        }
        /* open the file descriptor for writing */
        if (ORTE_FILEM_TYPE_EXE == type) {
            if (0 > (incoming->fd = open(incoming->fullpath, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU))) {
                opal_output(0, "%s CANNOT CREATE FILE %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            incoming->fullpath);
                send_complete(file, ORTE_ERR_FILE_WRITE_FAILURE);
                free(file);
                free(tmp);
                return;
            }
        } else {
            if (0 > (incoming->fd = open(incoming->fullpath, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR))) {
                opal_output(0, "%s CANNOT CREATE FILE %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            incoming->fullpath);
                send_complete(file, ORTE_ERR_FILE_WRITE_FAILURE);
                free(file);
                free(tmp);
                return;
            }
        }
        free(tmp);
        opal_event_set(orte_event_base, &incoming->ev, incoming->fd,
                       OPAL_EV_WRITE, write_handler, incoming);
        opal_event_set_priority(&incoming->ev, ORTE_MSG_PRI);
    }
    /* create an output object for this data */
    output = OBJ_NEW(orte_filem_raw_output_t);
    if (0 < nbytes) {
        /* don't copy 0 bytes - we just need to pass
         * the zero bytes so the fd can be closed
         * after it writes everything out
         */
        memcpy(output->data, data, nbytes);
    }
    output->numbytes = nbytes;

    /* add this data to the write list for this fd */
    opal_list_append(&incoming->outputs, &output->super);

    if (!incoming->pending) {
        /* add the event */
        incoming->pending = true;
        ORTE_POST_OBJECT(incoming);
        opal_event_add(&incoming->ev, 0);
    }

    /* cleanup */
    free(file);
}


static void write_handler(int fd, short event, void *cbdata)
{
    orte_filem_raw_incoming_t *sink = (orte_filem_raw_incoming_t*)cbdata;
    opal_list_item_t *item;
    orte_filem_raw_output_t *output;
    int num_written;
    char *dirname, *cmd;
    char homedir[MAXPATHLEN];
    int rc;

    ORTE_ACQUIRE_OBJECT(sink);

    OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                         "%s write:handler writing data to %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         sink->fd));

    /* note that the event is off */
    sink->pending = false;

    while (NULL != (item = opal_list_remove_first(&sink->outputs))) {
        output = (orte_filem_raw_output_t*)item;
        if (0 == output->numbytes) {
            /* indicates we are to close this stream */
            OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                 "%s write:handler zero bytes - reporting complete for file %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 sink->file));
            /* close the file descriptor */
            close(sink->fd);
            sink->fd = -1;
            if (ORTE_FILEM_TYPE_FILE == sink->type ||
                ORTE_FILEM_TYPE_EXE == sink->type) {
                /* just link to the top as this will be the
                 * name we will want in each proc's session dir
                 */
                opal_argv_append_nosize(&sink->link_pts, sink->top);
                send_complete(sink->file, ORTE_SUCCESS);
            } else {
                /* unarchive the file */
                if (ORTE_FILEM_TYPE_TAR == sink->type) {
                    asprintf(&cmd, "tar xf %s", sink->file);
                } else if (ORTE_FILEM_TYPE_BZIP == sink->type) {
                    asprintf(&cmd, "tar xjf %s", sink->file);
                } else if (ORTE_FILEM_TYPE_GZIP == sink->type) {
                    asprintf(&cmd, "tar xzf %s", sink->file);
                } else {
                    ORTE_ERROR_LOG(ORTE_ERR_BAD_PARAM);
                    send_complete(sink->file, ORTE_ERR_FILE_WRITE_FAILURE);
                    return;
                }
                getcwd(homedir, sizeof(homedir));
                dirname = opal_dirname(sink->fullpath);
                chdir(dirname);
                OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                     "%s write:handler unarchiving file %s with cmd: %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     sink->file, cmd));
                system(cmd);
                chdir(homedir);
                free(dirname);
                free(cmd);
                /* setup the link points */
                if (ORTE_SUCCESS != (rc = link_archive(sink))) {
                    ORTE_ERROR_LOG(rc);
                    send_complete(sink->file, ORTE_ERR_FILE_WRITE_FAILURE);
                } else {
                    send_complete(sink->file, ORTE_SUCCESS);
                }
            }
            return;
        }
        num_written = write(sink->fd, output->data, output->numbytes);
        OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                             "%s write:handler wrote %d bytes to file %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             num_written, sink->file));
        if (num_written < 0) {
            if (EAGAIN == errno || EINTR == errno) {
                /* push this item back on the front of the list */
                opal_list_prepend(&sink->outputs, item);
                /* leave the write event running so it will call us again
                 * when the fd is ready.
                 */
                sink->pending = true;
                ORTE_POST_OBJECT(sink);
                opal_event_add(&sink->ev, 0);
                return;
            }
            /* otherwise, something bad happened so all we can do is abort
             * this attempt
             */
            OPAL_OUTPUT_VERBOSE((1, orte_filem_base_framework.framework_output,
                                 "%s write:handler error on write for file %s: %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 sink->file, strerror(errno)));
            OBJ_RELEASE(output);
            opal_list_remove_item(&incoming_files, &sink->super);
            send_complete(sink->file, OPAL_ERR_FILE_WRITE_FAILURE);
            OBJ_RELEASE(sink);
            return;
        } else if (num_written < output->numbytes) {
            /* incomplete write - adjust data to avoid duplicate output */
            memmove(output->data, &output->data[num_written], output->numbytes - num_written);
            /* push this item back on the front of the list */
            opal_list_prepend(&sink->outputs, item);
            /* leave the write event running so it will call us again
             * when the fd is ready
             */
            sink->pending = true;
            ORTE_POST_OBJECT(sink);
            opal_event_add(&sink->ev, 0);
            return;
        }
        OBJ_RELEASE(output);
    }
}

static void xfer_construct(orte_filem_raw_xfer_t *ptr)
{
    ptr->outbound = NULL;
    ptr->app_idx = 0;
    ptr->pending = false;
    ptr->src = NULL;
    ptr->file = NULL;
    ptr->nchunk = 0;
    ptr->status = ORTE_SUCCESS;
    ptr->nrecvd = 0;
}
static void xfer_destruct(orte_filem_raw_xfer_t *ptr)
{
    if (ptr->pending) {
        opal_event_del(&ptr->ev);
    }
    if (NULL != ptr->src) {
        free(ptr->src);
    }
    if (NULL != ptr->file) {
        free(ptr->file);
    }
}
OBJ_CLASS_INSTANCE(orte_filem_raw_xfer_t,
                   opal_list_item_t,
                   xfer_construct, xfer_destruct);

static void out_construct(orte_filem_raw_outbound_t *ptr)
{
    OBJ_CONSTRUCT(&ptr->xfers, opal_list_t);
    ptr->status = ORTE_SUCCESS;
    ptr->cbfunc = NULL;
    ptr->cbdata = NULL;
}
static void out_destruct(orte_filem_raw_outbound_t *ptr)
{
    opal_list_item_t *item;

    while (NULL != (item = opal_list_remove_first(&ptr->xfers))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->xfers);
}
OBJ_CLASS_INSTANCE(orte_filem_raw_outbound_t,
                   opal_list_item_t,
                   out_construct, out_destruct);

static void in_construct(orte_filem_raw_incoming_t *ptr)
{
    ptr->app_idx = 0;
    ptr->pending = false;
    ptr->fd = -1;
    ptr->file = NULL;
    ptr->top = NULL;
    ptr->fullpath = NULL;
    ptr->link_pts = NULL;
    OBJ_CONSTRUCT(&ptr->outputs, opal_list_t);
}
static void in_destruct(orte_filem_raw_incoming_t *ptr)
{
    opal_list_item_t *item;

    if (ptr->pending) {
        opal_event_del(&ptr->ev);
    }
    if (0 <= ptr->fd) {
        close(ptr->fd);
    }
    if (NULL != ptr->file) {
        free(ptr->file);
    }
    if (NULL != ptr->top) {
        free(ptr->top);
    }
    if (NULL != ptr->fullpath) {
        free(ptr->fullpath);
    }
    opal_argv_free(ptr->link_pts);
    while (NULL != (item = opal_list_remove_first(&ptr->outputs))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->outputs);
}
OBJ_CLASS_INSTANCE(orte_filem_raw_incoming_t,
                   opal_list_item_t,
                   in_construct, in_destruct);

static void output_construct(orte_filem_raw_output_t *ptr)
{
    ptr->numbytes = 0;
}
OBJ_CLASS_INSTANCE(orte_filem_raw_output_t,
                   opal_list_item_t,
                   output_construct, NULL);
