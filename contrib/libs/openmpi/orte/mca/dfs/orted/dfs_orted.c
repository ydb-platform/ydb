/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#include <string.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <sys/stat.h>

#include "opal/util/if.h"
#include "opal/util/output.h"
#include "opal/util/uri.h"
#include "opal/dss/dss.h"

#include "orte/util/error_strings.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/session_dir.h"
#include "orte/util/show_help.h"
#include "orte/util/threads.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"

#include "orte/runtime/orte_quit.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/dfs/dfs.h"
#include "orte/mca/dfs/base/base.h"
#include "dfs_orted.h"

/*
 * Module functions: Global
 */
static int init(void);
static int finalize(void);

static void dfs_open(char *uri,
                     orte_dfs_open_callback_fn_t cbfunc,
                     void *cbdata);
static void dfs_close(int fd,
                      orte_dfs_close_callback_fn_t cbfunc,
                      void *cbdata);
static void dfs_get_file_size(int fd,
                              orte_dfs_size_callback_fn_t cbfunc,
                              void *cbdata);
static void dfs_seek(int fd, long offset, int whence,
                     orte_dfs_seek_callback_fn_t cbfunc,
                     void *cbdata);
static void dfs_read(int fd, uint8_t *buffer,
                     long length,
                     orte_dfs_read_callback_fn_t cbfunc,
                     void *cbdata);
static void dfs_post_file_map(opal_buffer_t *bo,
                              orte_dfs_post_callback_fn_t cbfunc,
                              void *cbdata);
static void dfs_get_file_map(orte_process_name_t *target,
                             orte_dfs_fm_callback_fn_t cbfunc,
                             void *cbdata);
static void dfs_load_file_maps(orte_jobid_t jobid,
                               opal_buffer_t *bo,
                               orte_dfs_load_callback_fn_t cbfunc,
                               void *cbdata);
static void dfs_purge_file_maps(orte_jobid_t jobid,
                                orte_dfs_purge_callback_fn_t cbfunc,
                                void *cbdata);
/******************
 * Daemon/HNP module
 ******************/
orte_dfs_base_module_t orte_dfs_orted_module = {
    init,
    finalize,
    dfs_open,
    dfs_close,
    dfs_get_file_size,
    dfs_seek,
    dfs_read,
    dfs_post_file_map,
    dfs_get_file_map,
    dfs_load_file_maps,
    dfs_purge_file_maps
};

static void* worker_thread_engine(opal_object_t *obj);

typedef struct {
    opal_object_t super;
    int idx;
    opal_event_base_t *event_base;
    bool active;
    opal_thread_t thread;
} worker_thread_t;
static void wt_const(worker_thread_t *ptr)
{
    /* create an event base for this thread */
    ptr->event_base = opal_event_base_create();
    /* construct the thread object */
    OBJ_CONSTRUCT(&ptr->thread, opal_thread_t);
    /* fork off a thread to progress it */
    ptr->active = true;
    ptr->thread.t_run = worker_thread_engine;
    ptr->thread.t_arg = ptr;
    opal_thread_start(&ptr->thread);
}
static void wt_dest(worker_thread_t *ptr)
{
    /* stop the thread */
    ptr->active = false;
    /* break the loop */
    opal_event_base_loopbreak(ptr->event_base);
    /* wait for thread to exit */
    opal_thread_join(&ptr->thread, NULL);
    OBJ_DESTRUCT(&ptr->thread);
    /* release the event base */
    opal_event_base_free(ptr->event_base);
}
OBJ_CLASS_INSTANCE(worker_thread_t,
                   opal_object_t,
                   wt_const, wt_dest);

typedef struct {
    opal_object_t super;
    opal_event_t ev;
    uint64_t rid;
    orte_dfs_tracker_t *trk;
    int64_t nbytes;
    int whence;
} worker_req_t;
OBJ_CLASS_INSTANCE(worker_req_t,
                   opal_object_t,
                   NULL, NULL);
#define ORTE_DFS_POST_WORKER(r, cb)                                     \
    do {                                                                \
        worker_thread_t *wt;                                            \
        wt = (worker_thread_t*)opal_pointer_array_get_item(&worker_threads, wt_cntr); \
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,                    \
                            "%s assigning req to worker thread %d",     \
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),         \
                            wt->idx);                                   \
        opal_event_set(wt->event_base, &((r)->ev),                      \
                       -1, OPAL_EV_WRITE, (cb), (r));                   \
        opal_event_active(&((r)->ev), OPAL_EV_WRITE, 1);                \
        /* move to the next thread */                                   \
        wt_cntr++;                                                      \
        if (wt_cntr == orte_dfs_orted_num_worker_threads) {             \
            wt_cntr = 0;                                                \
        }                                                               \
    } while(0);

static opal_list_t requests, active_files, file_maps;
static opal_pointer_array_t worker_threads;
static int wt_cntr = 0;
static int local_fd = 0;
static uint64_t req_id = 0;
static void recv_dfs_cmd(int status, orte_process_name_t* sender,
                         opal_buffer_t* buffer, orte_rml_tag_t tag,
                         void* cbdata);
static void recv_dfs_data(int status, orte_process_name_t* sender,
                          opal_buffer_t* buffer, orte_rml_tag_t tag,
                          void* cbdata);
static void remote_read(int fd, short args, void *cbata);
static void remote_open(int fd, short args, void *cbdata);
static void remote_size(int fd, short args, void *cbdata);
static void remote_seek(int fd, short args, void *cbdata);

static int init(void)
{
    int i;
    worker_thread_t *wt;

    OBJ_CONSTRUCT(&requests, opal_list_t);
    OBJ_CONSTRUCT(&active_files, opal_list_t);
    OBJ_CONSTRUCT(&file_maps, opal_list_t);
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_DFS_CMD,
                            ORTE_RML_PERSISTENT,
                            recv_dfs_cmd,
                            NULL);
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                            ORTE_RML_TAG_DFS_DATA,
                            ORTE_RML_PERSISTENT,
                            recv_dfs_data,
                            NULL);
    OBJ_CONSTRUCT(&worker_threads, opal_pointer_array_t);
    opal_pointer_array_init(&worker_threads, 1, INT_MAX, 1);

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s starting %d worker threads",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        orte_dfs_orted_num_worker_threads);
    for (i=0; i < orte_dfs_orted_num_worker_threads; i++) {
        wt = OBJ_NEW(worker_thread_t);
        wt->idx = i;
        opal_pointer_array_add(&worker_threads, wt);
    }

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    opal_list_item_t *item;
    int i;
    worker_thread_t *wt;

    orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_DFS_CMD);
    orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_DFS_DATA);
    while (NULL != (item = opal_list_remove_first(&requests))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&requests);
    while (NULL != (item = opal_list_remove_first(&active_files))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&active_files);
    while (NULL != (item = opal_list_remove_first(&file_maps))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&file_maps);
    for (i=0; i < worker_threads.size; i++) {
        if (NULL != (wt = (worker_thread_t*)opal_pointer_array_get_item(&worker_threads, i))) {
            OBJ_RELEASE(wt);
        }
    }
    OBJ_DESTRUCT(&worker_threads);

    return ORTE_SUCCESS;
}

static void open_local_file(orte_dfs_request_t *dfs)
{
    char *filename;
    orte_dfs_tracker_t *trk;

    /* extract the filename from the uri */
    if (NULL == (filename = opal_filename_from_uri(dfs->uri, NULL))) {
        /* something wrong - error was reported, so just get out */
        if (NULL != dfs->open_cbfunc) {
            dfs->open_cbfunc(-1, dfs->cbdata);
        }
        return;
    }
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s opening local file %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        filename);
    /* attempt to open the file */
    if (0 > (dfs->remote_fd = open(filename, O_RDONLY))) {
        ORTE_ERROR_LOG(ORTE_ERR_FILE_OPEN_FAILURE);
        if (NULL != dfs->open_cbfunc) {
            dfs->open_cbfunc(dfs->remote_fd, dfs->cbdata);
        }
        return;
    }
    /* otherwise, create a tracker for this file */
    trk = OBJ_NEW(orte_dfs_tracker_t);
    trk->requestor.jobid = ORTE_PROC_MY_NAME->jobid;
    trk->requestor.vpid = ORTE_PROC_MY_NAME->vpid;
    trk->filename = strdup(dfs->uri);
    /* define the local fd */
    trk->local_fd = local_fd++;
    /* record the remote file descriptor */
    trk->remote_fd = dfs->remote_fd;
    /* add it to our list of active files */
    opal_list_append(&active_files, &trk->super);
    /* the file is locally hosted */
    trk->host_daemon.jobid = ORTE_PROC_MY_DAEMON->jobid;
    trk->host_daemon.vpid = ORTE_PROC_MY_DAEMON->vpid;
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s local file %s mapped localfd %d to remotefd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        filename, trk->local_fd, trk->remote_fd);
    /* let the caller know */
    if (NULL != dfs->open_cbfunc) {
        dfs->open_cbfunc(trk->local_fd, dfs->cbdata);
    }
    /* request will be released by the calling routing */
}

static void process_opens(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *dfs = (orte_dfs_request_t*)cbdata;
    int rc;
    opal_buffer_t *buffer = NULL;
    char *scheme = NULL, *host = NULL, *filename = NULL;
    int v;
    orte_node_t *node, *nptr;

    ORTE_ACQUIRE_OBJECT(dfs);

    /* get the scheme to determine if we can process locally or not */
    if (NULL == (scheme = opal_uri_get_scheme(dfs->uri))) {
        OBJ_RELEASE(dfs);
        return;
    }

    if (0 == strcmp(scheme, "nfs")) {
        open_local_file(dfs);
        goto complete;
    }

    if (0 != strcmp(scheme, "file")) {
        /* not yet supported */
        orte_show_help("orte_dfs_help.txt", "unsupported-filesystem",
                       true, dfs->uri);
        if (NULL != dfs->open_cbfunc) {
            dfs->open_cbfunc(-1, dfs->cbdata);
        }
        goto complete;
    }

    free(scheme);
    scheme = NULL;

    /* dissect the uri to extract host and filename/path */
    if (NULL == (filename = opal_filename_from_uri(dfs->uri, &host))) {
        goto complete;
    }
    /* if the host is our own, then treat it as a local file */
    if (NULL == host || orte_ifislocal(host)) {
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s file %s on local host",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            filename);
        open_local_file(dfs);
        goto complete;
    }

    /* ident the daemon on that host */
    node = NULL;
    for (v=0; v < orte_node_pool->size; v++) {
        if (NULL == (nptr = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, v))) {
            continue;
        }
        if (NULL == nptr->daemon) {
            continue;
        }
        if (0 == strcmp(host, nptr->name)) {
            node = nptr;
            break;
        }
    }
    if (NULL == node) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        goto complete;
    }
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s file %s on host %s daemon %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        filename, host, ORTE_NAME_PRINT(&node->daemon->name));

    free(host);
    host = NULL;
    /* double-check: if it is our local daemon, then we
     * treat this as local
     */
    if (node->daemon->name.vpid == ORTE_PROC_MY_DAEMON->vpid) {
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s local file %s on same daemon",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            filename);
        open_local_file(dfs);
        goto complete;
    }
    /* add this request to our local list so we can
     * match it with the returned response when it comes
     */
    dfs->id = req_id++;
    opal_list_append(&requests, &dfs->super);

    /* setup a message for the daemon telling
     * them what file we want to access
     */
    buffer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &dfs->cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        opal_list_remove_item(&requests, &dfs->super);
        goto complete;
    }
    /* pass the request id */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &dfs->id, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        opal_list_remove_item(&requests, &dfs->super);
        goto complete;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &filename, 1, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        opal_list_remove_item(&requests, &dfs->super);
        goto complete;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s sending open file request to %s file %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&node->daemon->name),
                        filename);

    free(filename);
    filename = NULL;
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &node->daemon->name, buffer,
                                          ORTE_RML_TAG_DFS_CMD,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        opal_list_remove_item(&requests, &dfs->super);
        goto complete;
    }
    /* don't release it */
    return;

 complete:
    if (NULL != buffer) {
        OBJ_RELEASE(buffer);
    }
    if (NULL != scheme) {
        free(scheme);
    }
    if (NULL != host) {
        free(host);
    }
    if (NULL != filename) {
        free(filename);
    }
    OBJ_RELEASE(dfs);
}


/* in order to handle the possible opening/reading of files by
 * multiple threads, we have to ensure that all operations are
 * carried out in events - so the "open" cmd simply posts an
 * event containing the required info, and then returns
 */
static void dfs_open(char *uri,
                     orte_dfs_open_callback_fn_t cbfunc,
                     void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s opening file %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), uri);

    /* setup the request */
    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_OPEN_CMD;
    dfs->uri = strdup(uri);
    dfs->open_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_opens, ORTE_SYS_PRI);
}

static void process_close(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *close_dfs = (orte_dfs_request_t*)cbdata;
    orte_dfs_tracker_t *tptr, *trk;
    opal_list_item_t *item;
    opal_buffer_t *buffer;
    int rc;

    ORTE_ACQUIRE_OBJECT(close_dfs);

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s closing fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        close_dfs->local_fd);

    /* look in our local records for this fd */
    trk = NULL;
    for (item = opal_list_get_first(&active_files);
         item != opal_list_get_end(&active_files);
         item = opal_list_get_next(item)) {
        tptr = (orte_dfs_tracker_t*)item;
        if (tptr->local_fd == close_dfs->local_fd) {
            trk = tptr;
            break;
        }
    }
    if (NULL == trk) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        if (NULL != close_dfs->close_cbfunc) {
            close_dfs->close_cbfunc(close_dfs->local_fd, close_dfs->cbdata);
        }
        OBJ_RELEASE(close_dfs);
        return;
    }

    /* if the file is local, close it */
    if (trk->host_daemon.vpid == ORTE_PROC_MY_DAEMON->vpid) {
        close(trk->remote_fd);
        goto complete;
    }

    /* setup a message for the daemon telling
     * them what file to close
     */
    buffer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &close_dfs->cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &trk->remote_fd, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s sending close file request to %s for fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&trk->host_daemon),
                        trk->local_fd);
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &trk->host_daemon, buffer,
                                          ORTE_RML_TAG_DFS_CMD,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        goto complete;
    }

 complete:
    opal_list_remove_item(&active_files, &trk->super);
    OBJ_RELEASE(trk);
    if (NULL != close_dfs->close_cbfunc) {
        close_dfs->close_cbfunc(close_dfs->local_fd, close_dfs->cbdata);
    }
    OBJ_RELEASE(close_dfs);
}

static void dfs_close(int fd,
                      orte_dfs_close_callback_fn_t cbfunc,
                      void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s close called on fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fd);

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_CLOSE_CMD;
    dfs->local_fd = fd;
    dfs->close_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_close, ORTE_SYS_PRI);
}

static void process_sizes(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *size_dfs = (orte_dfs_request_t*)cbdata;
    orte_dfs_tracker_t *tptr, *trk;
    opal_list_item_t *item;
    opal_buffer_t *buffer;
    int rc;
    struct stat buf;

    ORTE_ACQUIRE_OBJECT(size_dfs);

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s processing get_size on fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        size_dfs->local_fd);

    /* look in our local records for this fd */
    trk = NULL;
    for (item = opal_list_get_first(&active_files);
         item != opal_list_get_end(&active_files);
         item = opal_list_get_next(item)) {
        tptr = (orte_dfs_tracker_t*)item;
        if (tptr->local_fd == size_dfs->local_fd) {
            trk = tptr;
            break;
        }
    }
    if (NULL == trk) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OBJ_RELEASE(size_dfs);
        return;
    }

    /* if the file is local, execute the seek on it - we
     * stuck the "whence" value in the remote_fd
     */
    if (trk->host_daemon.vpid == ORTE_PROC_MY_DAEMON->vpid) {
        /* stat the file and get its size */
        if (0 > stat(trk->filename, &buf)) {
            /* cannot stat file */
            opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                "%s could not stat %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                trk->filename);
            if (NULL != size_dfs->size_cbfunc) {
                size_dfs->size_cbfunc(-1, size_dfs->cbdata);
            }
        }
        goto complete;
    }

    /* setup a message for the daemon telling
     * them what file to get the size of
     */
    buffer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &size_dfs->cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &trk->remote_fd, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s sending get_size request to %s for fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&trk->host_daemon),
                        trk->local_fd);
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &trk->host_daemon, buffer,
                                          ORTE_RML_TAG_DFS_CMD,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        if (NULL != size_dfs->size_cbfunc) {
            size_dfs->size_cbfunc(-1, size_dfs->cbdata);
        }
        goto complete;
    }

 complete:
    OBJ_RELEASE(size_dfs);
}

static void dfs_get_file_size(int fd,
                              orte_dfs_size_callback_fn_t cbfunc,
                              void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s get_size called on fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fd);

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_SIZE_CMD;
    dfs->local_fd = fd;
    dfs->size_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_sizes, ORTE_SYS_PRI);
}


static void process_seeks(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *seek_dfs = (orte_dfs_request_t*)cbdata;
    orte_dfs_tracker_t *tptr, *trk;
    opal_list_item_t *item;
    opal_buffer_t *buffer;
    int64_t i64;
    int rc;
    struct stat buf;

    ORTE_ACQUIRE_OBJECT(seek_dfs);

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s processing seek on fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        seek_dfs->local_fd);

    /* look in our local records for this fd */
    trk = NULL;
    for (item = opal_list_get_first(&active_files);
         item != opal_list_get_end(&active_files);
         item = opal_list_get_next(item)) {
        tptr = (orte_dfs_tracker_t*)item;
        if (tptr->local_fd == seek_dfs->local_fd) {
            trk = tptr;
            break;
        }
    }
    if (NULL == trk) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OBJ_RELEASE(seek_dfs);
        return;
    }

    /* if the file is local, execute the seek on it - we
     * stuck the "whence" value in the remote_fd
     */
    if (trk->host_daemon.vpid == ORTE_PROC_MY_DAEMON->vpid) {
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s local seek on fd %d",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            seek_dfs->local_fd);
        /* stat the file and get its size */
        if (0 > stat(trk->filename, &buf)) {
            /* cannot stat file */
            opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                "%s could not stat %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                trk->filename);
            if (NULL != seek_dfs->seek_cbfunc) {
                seek_dfs->seek_cbfunc(-1, seek_dfs->cbdata);
            }
        } else if (buf.st_size < seek_dfs->read_length &&
                   SEEK_SET == seek_dfs->remote_fd) {
            /* seek would take us past EOF */
            if (NULL != seek_dfs->seek_cbfunc) {
                seek_dfs->seek_cbfunc(-1, seek_dfs->cbdata);
            }
        } else if (buf.st_size < (off_t)(trk->location + seek_dfs->read_length) &&
                   SEEK_CUR == seek_dfs->remote_fd) {
            /* seek would take us past EOF */
            if (NULL != seek_dfs->seek_cbfunc) {
                seek_dfs->seek_cbfunc(-1, seek_dfs->cbdata);
            }
        } else {
            lseek(trk->remote_fd, seek_dfs->read_length, seek_dfs->remote_fd);
            if (SEEK_SET == seek_dfs->remote_fd) {
                trk->location = seek_dfs->read_length;
            } else {
                trk->location += seek_dfs->read_length;
            }
        }
        goto complete;
    }
    /* add this request to our local list so we can
     * match it with the returned response when it comes
     */
    seek_dfs->id = req_id++;
    opal_list_append(&requests, &seek_dfs->super);

    /* setup a message for the daemon telling
     * them what file to seek
     */
    buffer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &seek_dfs->cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    /* pass the request id */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &seek_dfs->id, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        opal_list_remove_item(&requests, &seek_dfs->super);
        goto complete;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &trk->remote_fd, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    i64 = (int64_t)seek_dfs->read_length;
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &i64, 1, OPAL_INT64))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &seek_dfs->remote_fd, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s sending seek file request to %s for fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&trk->host_daemon),
                        trk->local_fd);
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &trk->host_daemon, buffer,
                                          ORTE_RML_TAG_DFS_CMD,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        goto complete;
    }

 complete:
    OBJ_RELEASE(seek_dfs);
}


static void dfs_seek(int fd, long offset, int whence,
                     orte_dfs_seek_callback_fn_t cbfunc,
                     void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s seek called on fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), fd);

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_SEEK_CMD;
    dfs->local_fd = fd;
    dfs->read_length = offset;
    dfs->remote_fd = whence;
    dfs->seek_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_seeks, ORTE_SYS_PRI);
}

static void process_reads(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *read_dfs = (orte_dfs_request_t*)cbdata;
    orte_dfs_tracker_t *tptr, *trk;
    long nbytes;
    opal_list_item_t *item;
    opal_buffer_t *buffer;
    int64_t i64;
    int rc;

    ORTE_ACQUIRE_OBJECT(read_dfs);

    /* look in our local records for this fd */
    trk = NULL;
    for (item = opal_list_get_first(&active_files);
         item != opal_list_get_end(&active_files);
         item = opal_list_get_next(item)) {
        tptr = (orte_dfs_tracker_t*)item;
        if (tptr->local_fd == read_dfs->local_fd) {
            trk = tptr;
            break;
        }
    }
    if (NULL == trk) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        OBJ_RELEASE(read_dfs);
        return;
    }

    /* if the file is local, read the desired bytes */
    if (trk->host_daemon.vpid == ORTE_PROC_MY_DAEMON->vpid) {
        nbytes = read(trk->remote_fd, read_dfs->read_buffer, read_dfs->read_length);
        if (0 < nbytes) {
            /* update our location */
            trk->location += nbytes;
        }
        /* pass them back to the caller */
        if (NULL != read_dfs->read_cbfunc) {
            read_dfs->read_cbfunc(nbytes, read_dfs->read_buffer, read_dfs->cbdata);
        }
        /* request is complete */
        OBJ_RELEASE(read_dfs);
        return;
    }
    /* add this request to our pending list */
    read_dfs->id = req_id++;
    opal_list_append(&requests, &read_dfs->super);

    /* setup a message for the daemon telling
     * them what file to read
     */
    buffer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &read_dfs->cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    /* include the request id */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &read_dfs->id, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &trk->remote_fd, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }
    i64 = (int64_t)read_dfs->read_length;
    if (OPAL_SUCCESS != (rc = opal_dss.pack(buffer, &i64, 1, OPAL_INT64))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s sending read file request to %s for fd %d",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&trk->host_daemon),
                        trk->local_fd);
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &trk->host_daemon, buffer,
                                          ORTE_RML_TAG_DFS_CMD,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
    }
    /* don't release the request */
    return;

 complete:
    /* don't need to hang on to this request */
    opal_list_remove_item(&requests, &read_dfs->super);
    OBJ_RELEASE(read_dfs);
}

static void dfs_read(int fd, uint8_t *buffer,
                     long length,
                     orte_dfs_read_callback_fn_t cbfunc,
                     void *cbdata)
{
    orte_dfs_request_t *dfs;

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_READ_CMD;
    dfs->local_fd = fd;
    dfs->read_buffer = buffer;
    dfs->read_length = length;
    dfs->read_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_reads, ORTE_SYS_PRI);
}

static void process_posts(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *dfs = (orte_dfs_request_t*)cbdata;
    orte_dfs_jobfm_t *jptr, *jfm;
    orte_dfs_vpidfm_t *vptr, *vfm;
    opal_list_item_t *item;
    int rc;

    ORTE_ACQUIRE_OBJECT(dfs);

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s posting file map containing %d bytes for target %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (int)dfs->bptr->bytes_used, ORTE_NAME_PRINT(&dfs->target));

    /* lookup the job map */
    jfm = NULL;
    for (item = opal_list_get_first(&file_maps);
         item != opal_list_get_end(&file_maps);
         item = opal_list_get_next(item)) {
        jptr = (orte_dfs_jobfm_t*)item;
        if (jptr->jobid == dfs->target.jobid) {
            jfm = jptr;
            break;
        }
    }
    if (NULL == jfm) {
        /* add it */
        jfm = OBJ_NEW(orte_dfs_jobfm_t);
        jfm->jobid = dfs->target.jobid;
        opal_list_append(&file_maps, &jfm->super);
    }
    /* see if we already have an entry for this source */
    vfm = NULL;
    for (item = opal_list_get_first(&jfm->maps);
         item != opal_list_get_end(&jfm->maps);
         item = opal_list_get_next(item)) {
        vptr = (orte_dfs_vpidfm_t*)item;
        if (vptr->vpid == dfs->target.vpid) {
            vfm = vptr;
            break;
        }
    }
    if (NULL == vfm) {
        /* add it */
        vfm = OBJ_NEW(orte_dfs_vpidfm_t);
        vfm->vpid = dfs->target.vpid;
        opal_list_append(&jfm->maps, &vfm->super);
    }

    /* add this entry to our collection */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(&vfm->data, &dfs->bptr, 1, OPAL_BUFFER))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }
    vfm->num_entries++;
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s target %s now has %d entries",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(&dfs->target),
                        vfm->num_entries);

 cleanup:
    if (NULL != dfs->post_cbfunc) {
        dfs->post_cbfunc(dfs->cbdata);
    }
    OBJ_RELEASE(dfs);
}

static void dfs_post_file_map(opal_buffer_t *buffer,
                              orte_dfs_post_callback_fn_t cbfunc,
                              void *cbdata)
{
    orte_dfs_request_t *dfs;

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_POST_CMD;
    dfs->target.jobid = ORTE_PROC_MY_NAME->jobid;
    dfs->target.vpid = ORTE_PROC_MY_NAME->vpid;
    dfs->bptr = buffer;
    dfs->post_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_posts, ORTE_SYS_PRI);
}

static int get_job_maps(orte_dfs_jobfm_t *jfm,
                        orte_vpid_t vpid,
                        opal_buffer_t *buf)
{
    orte_dfs_vpidfm_t *vfm;
    opal_list_item_t *item;
    int rc;
    int entries=0;

    /* if the target vpid is WILDCARD, then process
     * data for all vpids - else, find the one
     */
    for (item = opal_list_get_first(&jfm->maps);
         item != opal_list_get_end(&jfm->maps);
         item = opal_list_get_next(item)) {
        vfm = (orte_dfs_vpidfm_t*)item;
        if (ORTE_VPID_WILDCARD == vpid ||
            vfm->vpid == vpid) {
            entries++;
            /* indicate data from this vpid */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &vfm->vpid, 1, ORTE_VPID))) {
                ORTE_ERROR_LOG(rc);
                return -1;
            }
            /* pack the number of posts we received from it */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &vfm->num_entries, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                return -1;
            }
            /* copy the data across */
            opal_dss.copy_payload(buf, &vfm->data);
        }
    }
    return entries;
}

static void process_getfm(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *dfs = (orte_dfs_request_t*)cbdata;
    orte_dfs_jobfm_t *jfm;
    opal_list_item_t *item;
    opal_buffer_t xfer;
    int32_t n, ntotal;
    int rc;

    ORTE_ACQUIRE_OBJECT(dfs);

    /* if the target job is WILDCARD, then process
     * data for all jobids - else, find the one
     */
    ntotal = 0;
    n = -1;
    for (item = opal_list_get_first(&file_maps);
         item != opal_list_get_end(&file_maps);
         item = opal_list_get_next(item)) {
        jfm = (orte_dfs_jobfm_t*)item;
        if (ORTE_JOBID_WILDCARD == dfs->target.jobid ||
            jfm->jobid == dfs->target.jobid) {
            n = get_job_maps(jfm, dfs->target.vpid, &dfs->bucket);
            if (n < 0) {
                break;
            }
            ntotal += n;
        }
    }

    if (n < 0) {
        /* indicates an error */
        if (NULL != dfs->fm_cbfunc) {
            dfs->fm_cbfunc(NULL, dfs->cbdata);
        }
    } else {
        OBJ_CONSTRUCT(&xfer, opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(&xfer, &ntotal, 1, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            OBJ_DESTRUCT(&xfer);
            if (NULL != dfs->fm_cbfunc) {
                dfs->fm_cbfunc(NULL, dfs->cbdata);
            }
            return;
        }
        opal_dss.copy_payload(&xfer, &dfs->bucket);
        /* pass it back to caller */
        if (NULL != dfs->fm_cbfunc) {
            dfs->fm_cbfunc(&xfer, dfs->cbdata);
        }
        OBJ_DESTRUCT(&xfer);
    }
    OBJ_RELEASE(dfs);
}

static void dfs_get_file_map(orte_process_name_t *target,
                             orte_dfs_fm_callback_fn_t cbfunc,
                             void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s get file map for %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_NAME_PRINT(target));

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_GETFM_CMD;
    dfs->target.jobid = target->jobid;
    dfs->target.vpid = target->vpid;
    dfs->fm_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_getfm, ORTE_SYS_PRI);
}

static void process_load(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *dfs = (orte_dfs_request_t*)cbdata;
    opal_list_item_t *item;
    orte_dfs_jobfm_t *jfm, *jptr;
    orte_dfs_vpidfm_t *vfm;
    orte_vpid_t vpid;
    int32_t entries, nvpids;
    int cnt, i, j;
    int rc;
    opal_buffer_t *xfer;

    ORTE_ACQUIRE_OBJECT(dfs);

    /* see if we already have a tracker for this job */
    jfm = NULL;
    for (item = opal_list_get_first(&file_maps);
         item != opal_list_get_end(&file_maps);
         item = opal_list_get_next(item)) {
        jptr = (orte_dfs_jobfm_t*)item;
        if (jptr->jobid == dfs->target.jobid) {
            jfm = jptr;
            break;
        }
    }
    if (NULL != jfm) {
        /* need to purge it first */
        while (NULL != (item = opal_list_remove_first(&jfm->maps))) {
            OBJ_RELEASE(item);
        }
    } else {
        jfm = OBJ_NEW(orte_dfs_jobfm_t);
        jfm->jobid = dfs->target.jobid;
        opal_list_append(&file_maps, &jfm->super);
    }

    /* retrieve the number of vpids in the map */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(dfs->bptr, &nvpids, &cnt, OPAL_INT32))) {
        ORTE_ERROR_LOG(rc);
        goto complete;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s loading file maps from %d vpids",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nvpids);

    /* unpack the buffer */
    for (i=0; i < nvpids; i++) {
        /* unpack this vpid */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(dfs->bptr, &vpid, &cnt, ORTE_VPID))) {
            ORTE_ERROR_LOG(rc);
            goto complete;
        }
        /* unpack the number of file maps in this entry */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(dfs->bptr, &entries, &cnt, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            goto complete;
        }
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s loading %d entries in file map for vpid %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            entries, ORTE_VPID_PRINT(vpid));
        /* create the entry */
        vfm = OBJ_NEW(orte_dfs_vpidfm_t);
        vfm->vpid = vpid;
        vfm->num_entries = entries;
        /* copy the data */
        for (j=0; j < entries; j++) {
            cnt = 1;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(dfs->bptr, &xfer, &cnt, OPAL_BUFFER))) {
                ORTE_ERROR_LOG(rc);
                goto complete;
            }
            if (OPAL_SUCCESS != (rc = opal_dss.pack(&vfm->data, &xfer, 1, OPAL_BUFFER))) {
                ORTE_ERROR_LOG(rc);
                goto complete;
            }
            OBJ_RELEASE(xfer);
        }
        opal_list_append(&jfm->maps, &vfm->super);
    }

 complete:
    if (NULL != dfs->load_cbfunc) {
        dfs->load_cbfunc(dfs->cbdata);
    }
    OBJ_RELEASE(dfs);
}

static void dfs_load_file_maps(orte_jobid_t jobid,
                               opal_buffer_t *buf,
                               orte_dfs_load_callback_fn_t cbfunc,
                               void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s loading file maps for %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(jobid));

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_LOAD_CMD;
    dfs->target.jobid = jobid;
    dfs->bptr = buf;
    dfs->load_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_load, ORTE_SYS_PRI);
}

static void process_purge(int fd, short args, void *cbdata)
{
    orte_dfs_request_t *dfs = (orte_dfs_request_t*)cbdata;
    opal_list_item_t *item;
    orte_dfs_jobfm_t *jfm, *jptr;

    ORTE_ACQUIRE_OBJECT(dfs);

    /* find the job tracker */
    jfm = NULL;
    for (item = opal_list_get_first(&file_maps);
         item != opal_list_get_end(&file_maps);
         item = opal_list_get_next(item)) {
        jptr = (orte_dfs_jobfm_t*)item;
        if (jptr->jobid == dfs->target.jobid) {
            jfm = jptr;
            break;
        }
    }
    if (NULL == jfm) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
    } else {
        /* remove it from the list */
        opal_list_remove_item(&file_maps, &jfm->super);
        /* the destructor will release the list of maps
         * in the jobfm object
         */
        OBJ_RELEASE(jfm);
    }

    if (NULL != dfs->purge_cbfunc) {
        dfs->purge_cbfunc(dfs->cbdata);
    }
    OBJ_RELEASE(dfs);
}

static void dfs_purge_file_maps(orte_jobid_t jobid,
                                orte_dfs_purge_callback_fn_t cbfunc,
                                void *cbdata)
{
    orte_dfs_request_t *dfs;

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s purging file maps for job %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(jobid));

    dfs = OBJ_NEW(orte_dfs_request_t);
    dfs->cmd = ORTE_DFS_PURGE_CMD;
    dfs->target.jobid = jobid;
    dfs->purge_cbfunc = cbfunc;
    dfs->cbdata = cbdata;

    /* post it for processing */
    ORTE_THREADSHIFT(dfs, orte_event_base, process_purge, ORTE_SYS_PRI);
}


/* receives take place in an event, so we are free to process
 * the request list without fear of getting things out-of-order
 */
static void recv_dfs_cmd(int status, orte_process_name_t* sender,
                         opal_buffer_t* buffer, orte_rml_tag_t tag,
                         void* cbdata)
{
    orte_dfs_cmd_t cmd;
    int32_t cnt;
    opal_list_item_t *item;
    int my_fd;
    int32_t rc, nmaps;
    char *filename;
    orte_dfs_tracker_t *trk;
    int64_t i64, bytes_read;
    uint8_t *read_buf;
    uint64_t rid;
    int whence;
    struct stat buf;
    orte_process_name_t source;
    opal_buffer_t *bptr, *xfer;
    orte_dfs_request_t *dfs;
    orte_dfs_jobfm_t *jfm, *jptr;
    orte_dfs_vpidfm_t *vfm, *vptr;
    opal_buffer_t *answer, bucket;
    int i, j;
    orte_vpid_t vpid;
    int32_t nentries, ncontributors;
    worker_req_t *wrkr;

    /* unpack the command */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &cmd, &cnt, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s received command %d from %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (int)cmd,
                        ORTE_NAME_PRINT(sender));

    switch (cmd) {
    case ORTE_DFS_OPEN_CMD:
        /* unpack their request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack the filename */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &filename, &cnt, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* create a tracker for this file */
        trk = OBJ_NEW(orte_dfs_tracker_t);
        trk->requestor.jobid = sender->jobid;
        trk->requestor.vpid = sender->vpid;
        trk->host_daemon.jobid = ORTE_PROC_MY_NAME->jobid;
        trk->host_daemon.vpid = ORTE_PROC_MY_NAME->vpid;
        trk->filename = strdup(filename);
        opal_list_append(&active_files, &trk->super);
        /* process the request */
        if (0 < orte_dfs_orted_num_worker_threads) {
            wrkr = OBJ_NEW(worker_req_t);
            wrkr->trk = trk;
            wrkr->rid = rid;
            ORTE_DFS_POST_WORKER(wrkr, remote_open);
            return;
        }
        /* no worker threads, so attempt to open the file */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s opening file %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            filename);
        if (0 > (my_fd = open(filename, O_RDONLY))) {
            ORTE_ERROR_LOG(ORTE_ERR_FILE_OPEN_FAILURE);
            goto answer_open;
        }
        trk->local_fd = my_fd;
    answer_open:
        /* construct the return message */
        answer = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rid, 1, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &my_fd, 1, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* send it */
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              sender, answer,
                                              ORTE_RML_TAG_DFS_DATA,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            return;
        }
        break;

    case ORTE_DFS_CLOSE_CMD:
        /* unpack our fd */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &my_fd, &cnt, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* find the corresponding tracker */
        for (item = opal_list_get_first(&active_files);
             item != opal_list_get_end(&active_files);
             item = opal_list_get_next(item)) {
            trk = (orte_dfs_tracker_t*)item;
            if (my_fd == trk->local_fd) {
                /* remove it */
                opal_list_remove_item(&active_files, item);
                OBJ_RELEASE(item);
                /* close the file */
                close(my_fd);
                break;
            }
        }
        break;

    case ORTE_DFS_SIZE_CMD:
        /* unpack their request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack our fd */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &my_fd, &cnt, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* find the corresponding tracker */
        i64 = -1;
        for (item = opal_list_get_first(&active_files);
             item != opal_list_get_end(&active_files);
             item = opal_list_get_next(item)) {
            trk = (orte_dfs_tracker_t*)item;
            if (my_fd == trk->local_fd) {
                /* process the request */
                if (0 < orte_dfs_orted_num_worker_threads) {
                    wrkr = OBJ_NEW(worker_req_t);
                    wrkr->trk = trk;
                    wrkr->rid = rid;
                    ORTE_DFS_POST_WORKER(wrkr, remote_size);
                    return;
                }
                /* no worker threads, so stat the file and get its size */
                if (0 > stat(trk->filename, &buf)) {
                    /* cannot stat file */
                    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                        "%s could not stat %s",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        trk->filename);
                } else {
                    i64 = buf.st_size;
                }
                break;
            }
        }
        /* construct the return message */
        answer = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rid, 1, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &i64, 1, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* send it */
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              sender, answer,
                                              ORTE_RML_TAG_DFS_DATA,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            return;
        }
        break;

    case ORTE_DFS_SEEK_CMD:
        /* unpack their request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack our fd */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &my_fd, &cnt, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack the offset */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &i64, &cnt, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack the whence */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &whence, &cnt, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* set default error */
        bytes_read = -1;
        /* find the corresponding tracker - we do this to ensure
         * that the local fd we were sent is actually open
         */
        for (item = opal_list_get_first(&active_files);
             item != opal_list_get_end(&active_files);
             item = opal_list_get_next(item)) {
            trk = (orte_dfs_tracker_t*)item;
            if (my_fd == trk->local_fd) {
                /* process the request */
                if (0 < orte_dfs_orted_num_worker_threads) {
                    wrkr = OBJ_NEW(worker_req_t);
                    wrkr->trk = trk;
                    wrkr->rid = rid;
                    wrkr->nbytes = i64;
                    wrkr->whence = whence;
                    ORTE_DFS_POST_WORKER(wrkr, remote_seek);
                    return;
                }
                /* no worker threads, so stat the file and get its size */
                if (0 > stat(trk->filename, &buf)) {
                    /* cannot stat file */
                    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                        "%s seek could not stat %s",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        trk->filename);
                } else if (buf.st_size < i64 && SEEK_SET == whence) {
                    /* seek would take us past EOF */
                    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                        "%s seek SET past EOF on file %s",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        trk->filename);
                    bytes_read = -2;
                } else if (buf.st_size < (off_t)(trk->location + i64) &&
                           SEEK_CUR == whence) {
                    /* seek would take us past EOF */
                    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                        "%s seek CUR past EOF on file %s",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        trk->filename);
                    bytes_read = -3;
                } else {
                    lseek(my_fd, i64, whence);
                    if (SEEK_SET == whence) {
                        trk->location = i64;
                    } else {
                        trk->location += i64;
                    }
                    bytes_read = i64;
                }
                break;
            }
        }
        /* construct the return message */
        answer = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rid, 1, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* return the offset/status */
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &bytes_read, 1, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* send it */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s sending %ld offset back to %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            (long)bytes_read,
                            ORTE_NAME_PRINT(sender));
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              sender, answer,
                                              ORTE_RML_TAG_DFS_DATA,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            return;
        }
        break;

    case ORTE_DFS_READ_CMD:
        /* set default error */
        my_fd = -1;
        bytes_read = -1;
        read_buf = NULL;
        /* unpack their request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack our fd */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &my_fd, &cnt, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            goto answer_read;
        }
        /* unpack the number of bytes to read */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &i64, &cnt, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            goto answer_read;
        }
        /* find the corresponding tracker - we do this to ensure
         * that the local fd we were sent is actually open
         */
        for (item = opal_list_get_first(&active_files);
             item != opal_list_get_end(&active_files);
             item = opal_list_get_next(item)) {
            trk = (orte_dfs_tracker_t*)item;
            if (my_fd == trk->local_fd) {
                if (0 < orte_dfs_orted_num_worker_threads) {
                    wrkr = OBJ_NEW(worker_req_t);
                    wrkr->rid = rid;
                    wrkr->trk = trk;
                    wrkr->nbytes = i64;
                    /* dispatch to the currently indexed thread */
                    ORTE_DFS_POST_WORKER(wrkr, remote_read);
                    return;
                } else {
                    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                        "%s reading %ld bytes from local fd %d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        (long)i64, my_fd);
                    /* do the read */
                    read_buf = (uint8_t*)malloc(i64);
                    if (NULL == read_buf) {
                        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
                        goto answer_read;
                    }
                    bytes_read = read(my_fd, read_buf, (long)i64);
                    if (0 < bytes_read) {
                        /* update our location */
                        trk->location += bytes_read;
                    }
                }
                break;
            }
        }
        answer_read:
        /* construct the return message */
        answer = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            if (NULL != read_buf) {
                free(read_buf);
            }
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rid, 1, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            if (NULL != read_buf) {
                free(read_buf);
            }
            return;
        }
        /* include the number of bytes read */
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &bytes_read, 1, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            if (NULL != read_buf) {
                free(read_buf);
            }
            return;
        }
        /* include the bytes read */
        if (0 < bytes_read) {
            if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, read_buf, bytes_read, OPAL_UINT8))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(answer);
                free(read_buf);
                return;
            }
        }
        if (NULL != read_buf) {
            free(read_buf);
        }
        /* send it */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s sending %ld bytes back to %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            (long)bytes_read,
                            ORTE_NAME_PRINT(sender));
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              sender, answer,
                                              ORTE_RML_TAG_DFS_DATA,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
            return;
        }
        break;

    case ORTE_DFS_POST_CMD:
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s received post command from %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(sender));
        /* unpack their request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            goto answer_post;
        }
        /* unpack the name of the source of this data */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &source, &cnt, ORTE_NAME))) {
            ORTE_ERROR_LOG(rc);
            goto answer_post;
        }
        /* unpack their buffer object */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &bptr, &cnt, OPAL_BUFFER))) {
            ORTE_ERROR_LOG(rc);
            goto answer_post;
        }
        /* add the contents to the storage for this process */
        dfs = OBJ_NEW(orte_dfs_request_t);
        dfs->target.jobid = source.jobid;
        dfs->target.vpid = source.vpid;
        dfs->bptr = bptr;
        dfs->post_cbfunc = NULL;
        process_posts(0, 0, (void*)dfs);
        OBJ_RELEASE(bptr);
    answer_post:
        if (UINT64_MAX != rid) {
            /* return an ack */
            answer = OBJ_NEW(opal_buffer_t);
            if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rid, 1, OPAL_UINT64))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                                  sender, answer,
                                                  ORTE_RML_TAG_DFS_DATA,
                                                  orte_rml_send_callback, NULL))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(answer);
            }
        }
        break;

    case ORTE_DFS_RELAY_POSTS_CMD:
        /* unpack the name of the source of this data */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &source, &cnt, ORTE_NAME))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s received relayed posts from sender %s for source %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            ORTE_NAME_PRINT(sender),
                            ORTE_NAME_PRINT(&source));
        /* lookup the job map */
        jfm = NULL;
        for (item = opal_list_get_first(&file_maps);
             item != opal_list_get_end(&file_maps);
             item = opal_list_get_next(item)) {
            jptr = (orte_dfs_jobfm_t*)item;
            if (jptr->jobid == source.jobid) {
                jfm = jptr;
                break;
            }
        }
        if (NULL == jfm) {
            /* add it */
            jfm = OBJ_NEW(orte_dfs_jobfm_t);
            jfm->jobid = source.jobid;
            opal_list_append(&file_maps, &jfm->super);
        }
        /* see if we already have an entry for this source */
        vfm = NULL;
        for (item = opal_list_get_first(&jfm->maps);
             item != opal_list_get_end(&jfm->maps);
             item = opal_list_get_next(item)) {
            vptr = (orte_dfs_vpidfm_t*)item;
            if (vptr->vpid == source.vpid) {
                vfm = vptr;
                break;
            }
        }
        if (NULL == vfm) {
            /* add it */
            vfm = OBJ_NEW(orte_dfs_vpidfm_t);
            vfm->vpid = source.vpid;
            opal_list_append(&jfm->maps, &vfm->super);
        }
        /* unpack their buffer object */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &bptr, &cnt, OPAL_BUFFER))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* the buffer object came from a call to get_file_maps, so it isn't quite
         * the same as when someone posts directly to us. So process it here by
         * starting with getting the number of vpids that contributed. This
         * should always be one, but leave it open for flexibility
         */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(bptr, &ncontributors, &cnt, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* loop thru the number of contributors */
        for (i=0; i < ncontributors; i++) {
            /* unpack the vpid of the contributor */
            cnt = 1;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(bptr, &vpid, &cnt, ORTE_VPID))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            /* unpack the number of entries */
            cnt = 1;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(bptr, &nentries, &cnt, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            for (j=0; j < nentries; j++) {
                /* get the entry */
                cnt = 1;
                if (OPAL_SUCCESS != (rc = opal_dss.unpack(bptr, &xfer, &cnt, OPAL_BUFFER))) {
                    ORTE_ERROR_LOG(rc);
                    return;
                }
                /* store it */
                if (OPAL_SUCCESS != (rc = opal_dss.pack(&vfm->data, &xfer, 1, OPAL_BUFFER))) {
                    ORTE_ERROR_LOG(rc);
                    return;
                }
                OBJ_RELEASE(xfer);
                vfm->num_entries++;
            }
        }
        OBJ_RELEASE(bptr);
        /* no reply required */
        break;

    case ORTE_DFS_GETFM_CMD:
        /* unpack their request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack the target */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &source, &cnt, ORTE_NAME))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* construct the response */
        answer = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rid, 1, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* search our data tree for matches, assembling them
         * into a byte object
         */
        /* if the target job is WILDCARD, then process
         * data for all jobids - else, find the one
         */
        OBJ_CONSTRUCT(&bucket, opal_buffer_t);
        nmaps = 0;
        for (item = opal_list_get_first(&file_maps);
             item != opal_list_get_end(&file_maps);
             item = opal_list_get_next(item)) {
            jfm = (orte_dfs_jobfm_t*)item;
            if (ORTE_JOBID_WILDCARD == source.jobid ||
                jfm->jobid == source.jobid) {
                rc = get_job_maps(jfm, source.vpid, &bucket);
                if (rc < 0) {
                    break;
                } else {
                    nmaps += rc;
                }
            }
        }
        if (rc < 0) {
            if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &rc, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
        } else {
            if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &nmaps, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                return;
            }
            if (0 < nmaps) {
                opal_dss.copy_payload(answer, &bucket);
            }
        }
        OBJ_DESTRUCT(&bucket);
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s getf-cmd: returning %d maps with %d bytes to sender %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nmaps,
                            (int)answer->bytes_used, ORTE_NAME_PRINT(sender));
        if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                              sender, answer,
                                              ORTE_RML_TAG_DFS_DATA,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(answer);
        }
        break;

    default:
        opal_output(0, "ORTED:DFS:RECV_DFS WTF");
        break;
    }
}

static void recv_dfs_data(int status, orte_process_name_t* sender,
                          opal_buffer_t* buffer, orte_rml_tag_t tag,
                          void* cbdata)
{
    orte_dfs_cmd_t cmd;
    int32_t cnt;
    orte_dfs_request_t *dfs, *dptr;
    opal_list_item_t *item;
    int remote_fd, rc;
    int64_t i64;
    uint64_t rid;
    orte_dfs_tracker_t *trk;

    /* unpack the command this message is responding to */
    cnt = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &cmd, &cnt, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s recvd:data cmd %d from sender %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (int)cmd,
                        ORTE_NAME_PRINT(sender));

    switch (cmd) {
    case ORTE_DFS_OPEN_CMD:
        /* unpack the request id */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* unpack the remote fd */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &remote_fd, &cnt, OPAL_INT))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* search our list of requests to find the matching one */
        dfs = NULL;
        for (item = opal_list_get_first(&requests);
             item != opal_list_get_end(&requests);
             item = opal_list_get_next(item)) {
            dptr = (orte_dfs_request_t*)item;
            if (dptr->id == rid) {
                /* as the request has been fulfilled, remove it */
                opal_list_remove_item(&requests, item);
                dfs = dptr;
                break;
            }
        }
        if (NULL == dfs) {
            opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                "%s recvd:data open file - no corresponding request found for local fd %d",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), local_fd);
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            return;
        }

        /* if the remote_fd < 0, then we had an error, so return
         * the error value to the caller
         */
        if (remote_fd < 0) {
            opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                "%s recvd:data open file response error file %s [error: %d]",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                dfs->uri, remote_fd);
            if (NULL != dfs->open_cbfunc) {
                dfs->open_cbfunc(remote_fd, dfs->cbdata);
            }
            /* release the request */
            OBJ_RELEASE(dfs);
            return;
        }
        /* otherwise, create a tracker for this file */
        trk = OBJ_NEW(orte_dfs_tracker_t);
        trk->requestor.jobid = ORTE_PROC_MY_NAME->jobid;
        trk->requestor.vpid = ORTE_PROC_MY_NAME->vpid;
        trk->host_daemon.jobid = sender->jobid;
        trk->host_daemon.vpid = sender->vpid;
        trk->filename = strdup(dfs->uri);
        /* define the local fd */
        trk->local_fd = local_fd++;
        /* record the remote file descriptor */
        trk->remote_fd = remote_fd;
        /* add it to our list of active files */
        opal_list_append(&active_files, &trk->super);
        /* return the local_fd to the caller for
         * subsequent operations
         */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s recvd:data open file completed for file %s [local fd: %d remote fd: %d]",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            dfs->uri, trk->local_fd, remote_fd);
        if (NULL != dfs->open_cbfunc) {
            dfs->open_cbfunc(trk->local_fd, dfs->cbdata);
        }
        /* release the request */
        OBJ_RELEASE(dfs);
        break;

    case ORTE_DFS_SIZE_CMD:
        /* unpack the request id for this request */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* search our list of requests to find the matching one */
        dfs = NULL;
        for (item = opal_list_get_first(&requests);
             item != opal_list_get_end(&requests);
             item = opal_list_get_next(item)) {
            dptr = (orte_dfs_request_t*)item;
            if (dptr->id == rid) {
                /* request was fulfilled, so remove it */
                opal_list_remove_item(&requests, item);
                dfs = dptr;
                break;
            }
        }
        if (NULL == dfs) {
            opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                "%s recvd:data size - no corresponding request found for local fd %d",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), local_fd);
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            return;
        }
        /* get the size */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &i64, &cnt, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(dfs);
            return;
        }
        /* pass them back to the original caller */
        if (NULL != dfs->read_cbfunc) {
            dfs->size_cbfunc(i64, dfs->cbdata);
        }
        /* release the request */
        OBJ_RELEASE(dfs);
        break;

    case ORTE_DFS_READ_CMD:
        /* unpack the request id for this read */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &rid, &cnt, OPAL_UINT64))) {
            ORTE_ERROR_LOG(rc);
            return;
        }
        /* search our list of requests to find the matching one */
        dfs = NULL;
        for (item = opal_list_get_first(&requests);
             item != opal_list_get_end(&requests);
             item = opal_list_get_next(item)) {
            dptr = (orte_dfs_request_t*)item;
            if (dptr->id == rid) {
                /* request was fulfilled, so remove it */
                opal_list_remove_item(&requests, item);
                dfs = dptr;
                break;
            }
        }
        if (NULL == dfs) {
            opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                                "%s recvd:data read - no corresponding request found for local fd %d",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), local_fd);
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            return;
        }
        /* get the bytes read */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &i64, &cnt, OPAL_INT64))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(dfs);
            return;
        }
        if (0 < i64) {
            cnt = i64;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, dfs->read_buffer, &cnt, OPAL_UINT8))) {
                ORTE_ERROR_LOG(rc);
                OBJ_RELEASE(dfs);
                return;
            }
        }
        /* pass them back to the original caller */
        if (NULL != dfs->read_cbfunc) {
            dfs->read_cbfunc(i64, dfs->read_buffer, dfs->cbdata);
        }
        /* release the request */
        OBJ_RELEASE(dfs);
        break;

    default:
        opal_output(0, "ORTED:DFS:RECV:DATA WTF");
        break;
    }
}

static void* worker_thread_engine(opal_object_t *obj)
{
    opal_thread_t *thread = (opal_thread_t*)obj;
    worker_thread_t *ptr = (worker_thread_t*)thread->t_arg;

    while (ptr->active) {
        opal_event_loop(ptr->event_base, OPAL_EVLOOP_ONCE);
    }
    return OPAL_THREAD_CANCELLED;
}

static void remote_open(int fd, short args, void *cbdata)
{
    worker_req_t *req = (worker_req_t*)cbdata;
    opal_buffer_t *answer;
    orte_dfs_cmd_t cmd = ORTE_DFS_OPEN_CMD;
    int rc;

    /* attempt to open the file */
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s opening file %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        req->trk->filename);
    if (0 > (req->trk->local_fd = open(req->trk->filename, O_RDONLY))) {
        ORTE_ERROR_LOG(ORTE_ERR_FILE_OPEN_FAILURE);
    }
    /* construct the return message */
    answer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &req->rid, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &req->trk->local_fd, 1, OPAL_INT))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &req->trk->requestor, answer,
                                          ORTE_RML_TAG_DFS_DATA,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(answer);
    }
}

static void remote_size(int fd, short args, void *cbdata)
{
    worker_req_t *req = (worker_req_t*)cbdata;
    int rc;
    struct stat buf;
    int64_t i64;
    opal_buffer_t *answer;
    orte_dfs_cmd_t cmd = ORTE_DFS_SIZE_CMD;

    if (0 > stat(req->trk->filename, &buf)) {
        /* cannot stat file */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s could not stat %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            req->trk->filename);
    } else {
        i64 = buf.st_size;
    }
    /* construct the return message */
    answer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &req->rid, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &i64, 1, OPAL_INT64))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &req->trk->requestor, answer,
                                          ORTE_RML_TAG_DFS_DATA,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(answer);
    }
}

static void remote_seek(int fd, short args, void *cbdata)
{
    worker_req_t *req = (worker_req_t*)cbdata;
    opal_buffer_t *answer;
    orte_dfs_cmd_t cmd = ORTE_DFS_SEEK_CMD;
    int rc;
    struct stat buf;
    int64_t i64;

    /* stat the file and get its size */
    if (0 > stat(req->trk->filename, &buf)) {
        /* cannot stat file */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s seek could not stat %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            req->trk->filename);
    } else if (buf.st_size < req->nbytes && SEEK_SET == req->whence) {
        /* seek would take us past EOF */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s seek SET past EOF on file %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            req->trk->filename);
        i64 = -2;
    } else if (buf.st_size < (off_t)(req->trk->location + req->nbytes) &&
               SEEK_CUR == req->whence) {
        /* seek would take us past EOF */
        opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                            "%s seek CUR past EOF on file %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            req->trk->filename);
        i64 = -3;
    } else {
        lseek(req->trk->local_fd, req->nbytes, req->whence);
        if (SEEK_SET == req->whence) {
            req->trk->location = req->nbytes;
        } else {
            req->trk->location += req->nbytes;
        }
        i64 = req->nbytes;
    }

    /* construct the return message */
    answer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &req->rid, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &i64, 1, OPAL_INT64))) {
        ORTE_ERROR_LOG(rc);
        return;
    }
    /* send it */
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &req->trk->requestor, answer,
                                          ORTE_RML_TAG_DFS_DATA,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(answer);
    }
}

static void remote_read(int fd, short args, void *cbdata)
{
    worker_req_t *req = (worker_req_t*)cbdata;
    uint8_t *read_buf;
    opal_buffer_t *answer;
    orte_dfs_cmd_t cmd = ORTE_DFS_READ_CMD;
    int64_t bytes_read;
    int rc;

    /* do the read */
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s issuing read",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
    read_buf = (uint8_t*)malloc(req->nbytes);
    if (NULL == read_buf) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return;
    }
    bytes_read = read(req->trk->local_fd, read_buf, (long)req->nbytes);
    if (0 < bytes_read) {
        /* update our location */
        req->trk->location += bytes_read;
    }
    /* construct the return message */
    answer = OBJ_NEW(opal_buffer_t);
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &cmd, 1, ORTE_DFS_CMD_T))) {
        ORTE_ERROR_LOG(rc);
        free(read_buf);
        return;
    }
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &req->rid, 1, OPAL_UINT64))) {
        ORTE_ERROR_LOG(rc);
        free(read_buf);
        OBJ_RELEASE(answer);
        return;
    }
    /* include the number of bytes read */
    if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, &bytes_read, 1, OPAL_INT64))) {
        ORTE_ERROR_LOG(rc);
        free(read_buf);
        OBJ_RELEASE(answer);
        return;
    }
    /* include the bytes read */
    if (0 < bytes_read) {
        if (OPAL_SUCCESS != (rc = opal_dss.pack(answer, read_buf, bytes_read, OPAL_UINT8))) {
            ORTE_ERROR_LOG(rc);
            free(read_buf);
            OBJ_RELEASE(answer);
            return;
        }
    }
    free(read_buf);
    /* send it */
    opal_output_verbose(1, orte_dfs_base_framework.framework_output,
                        "%s sending %ld bytes back to %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        (long)bytes_read,
                        ORTE_NAME_PRINT(&req->trk->requestor));
    if (0 > (rc = orte_rml.send_buffer_nb(orte_mgmt_conduit,
                                          &req->trk->requestor, answer,
                                          ORTE_RML_TAG_DFS_DATA,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(answer);
        return;
    }
    OBJ_RELEASE(req);
}
