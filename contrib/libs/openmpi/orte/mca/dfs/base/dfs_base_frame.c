/*
 * Copyright (c) 2012-2013 Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2013      Intel, Inc. All rights reserved
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "opal/util/opal_environ.h"
#include "opal/util/output.h"

#include "orte/util/show_help.h"
#include "orte/mca/dfs/base/base.h"

#include "orte/mca/dfs/base/static-components.h"

/*
 * Globals
 */
orte_dfs_base_module_t orte_dfs = {
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

static int orte_dfs_base_close(void)
{
    /* Close selected component */
    if (NULL != orte_dfs.finalize) {
        orte_dfs.finalize();
    }

    return mca_base_framework_components_close(&orte_dfs_base_framework, NULL);
}

/**
 * Function for finding and opening either all MCA components, or the one
 * that was specifically requested via a MCA parameter.
 */
static int orte_dfs_base_open(mca_base_open_flag_t flags)
{
    /* Open up all available components */
    return mca_base_framework_components_open(&orte_dfs_base_framework, flags);
}

MCA_BASE_FRAMEWORK_DECLARE(orte, dfs, "ORTE Distributed File System",
                           NULL, orte_dfs_base_open, orte_dfs_base_close,
                           mca_dfs_base_static_components, 0);


/* instantiate classes */
static void trk_con(orte_dfs_tracker_t *trk)
{
    trk->host_daemon.jobid = ORTE_JOBID_INVALID;
    trk->host_daemon.vpid = ORTE_VPID_INVALID;
    trk->uri = NULL;
    trk->scheme = NULL;
    trk->filename = NULL;
    trk->location = 0;
}
static void trk_des(orte_dfs_tracker_t *trk)
{
    if (NULL != trk->uri) {
        free(trk->uri);
    }
    if (NULL != trk->scheme) {
        free(trk->scheme);
    }
    if (NULL != trk->filename) {
        free(trk->filename);
    }
}
OBJ_CLASS_INSTANCE(orte_dfs_tracker_t,
                   opal_list_item_t,
                   trk_con, trk_des);
static void req_const(orte_dfs_request_t *dfs)
{
    dfs->id = 0;
    dfs->uri = NULL;
    dfs->local_fd = -1;
    dfs->remote_fd = -1;
    dfs->read_length = -1;
    dfs->bptr = NULL;
    OBJ_CONSTRUCT(&dfs->bucket, opal_buffer_t);
    dfs->read_buffer = NULL;
    dfs->open_cbfunc = NULL;
    dfs->close_cbfunc = NULL;
    dfs->size_cbfunc = NULL;
    dfs->seek_cbfunc = NULL;
    dfs->read_cbfunc = NULL;
    dfs->post_cbfunc = NULL;
    dfs->fm_cbfunc = NULL;
    dfs->load_cbfunc = NULL;
    dfs->purge_cbfunc = NULL;
    dfs->cbdata = NULL;
}
static void req_dest(orte_dfs_request_t *dfs)
{
    if (NULL != dfs->uri) {
        free(dfs->uri);
    }
    OBJ_DESTRUCT(&dfs->bucket);
}
OBJ_CLASS_INSTANCE(orte_dfs_request_t,
                   opal_list_item_t,
                   req_const, req_dest);

static void jobfm_const(orte_dfs_jobfm_t *fm)
{
    OBJ_CONSTRUCT(&fm->maps, opal_list_t);
}
static void jobfm_dest(orte_dfs_jobfm_t *fm)
{
    opal_list_item_t *item;

    while (NULL != (item = opal_list_remove_first(&fm->maps))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&fm->maps);
}
OBJ_CLASS_INSTANCE(orte_dfs_jobfm_t,
                   opal_list_item_t,
                   jobfm_const, jobfm_dest);

static void vpidfm_const(orte_dfs_vpidfm_t *fm)
{
    OBJ_CONSTRUCT(&fm->data, opal_buffer_t);
    fm->num_entries = 0;
}
static void vpidfm_dest(orte_dfs_vpidfm_t *fm)
{
    OBJ_DESTRUCT(&fm->data);
}
OBJ_CLASS_INSTANCE(orte_dfs_vpidfm_t,
                   opal_list_item_t,
                   vpidfm_const, vpidfm_dest);
