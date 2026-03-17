/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <time.h>

#include "orte/constants.h"

#include "orte/mca/mca.h"
#include "opal/mca/base/base.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/proc_info.h"

#include "orte/mca/filem/filem.h"
#include "orte/mca/filem/base/base.h"

/******************
 * Local Functions
 ******************/

/******************
 * Object Stuff
 ******************/
static void process_set_construct(orte_filem_base_process_set_t *req) {
    req->source = *ORTE_NAME_INVALID;
    req->sink   = *ORTE_NAME_INVALID;
}

static void process_set_destruct( orte_filem_base_process_set_t *req) {
    req->source = *ORTE_NAME_INVALID;
    req->sink   = *ORTE_NAME_INVALID;
}

OBJ_CLASS_INSTANCE(orte_filem_base_process_set_t,
                   opal_list_item_t,
                   process_set_construct,
                   process_set_destruct);

static void file_set_construct(orte_filem_base_file_set_t *req) {
    req->local_target  = NULL;
    req->local_hint    = ORTE_FILEM_HINT_NONE;

    req->remote_target = NULL;
    req->remote_hint   = ORTE_FILEM_HINT_NONE;

    req->target_flag   = ORTE_FILEM_TYPE_UNKNOWN;

}

static void file_set_destruct( orte_filem_base_file_set_t *req) {
    if( NULL != req->local_target ) {
        free(req->local_target);
        req->local_target = NULL;
    }
    req->local_hint    = ORTE_FILEM_HINT_NONE;

    if( NULL != req->remote_target ) {
        free(req->remote_target);
        req->remote_target = NULL;
    }
    req->remote_hint   = ORTE_FILEM_HINT_NONE;

    req->target_flag   = ORTE_FILEM_TYPE_UNKNOWN;
}

OBJ_CLASS_INSTANCE(orte_filem_base_file_set_t,
                   opal_list_item_t,
                   file_set_construct,
                   file_set_destruct);

static void req_construct(orte_filem_base_request_t *req) {
    OBJ_CONSTRUCT(&req->process_sets,  opal_list_t);
    OBJ_CONSTRUCT(&req->file_sets,     opal_list_t);

    req->num_mv = 0;

    req->is_done = NULL;
    req->is_active = NULL;

    req->exit_status = NULL;

    req->movement_type = ORTE_FILEM_MOVE_TYPE_UNKNOWN;
}

static void req_destruct( orte_filem_base_request_t *req) {
    opal_list_item_t* item = NULL;

    while( NULL != (item = opal_list_remove_first(&req->process_sets)) ) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&req->process_sets);

    while( NULL != (item = opal_list_remove_first(&req->file_sets)) ) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&req->file_sets);

    req->num_mv = 0;

    if( NULL != req->is_done ) {
        free(req->is_done);
        req->is_done = NULL;
    }

    if( NULL != req->is_active ) {
        free(req->is_active);
        req->is_active = NULL;
    }

    if( NULL != req->exit_status ) {
        free(req->exit_status);
        req->exit_status = NULL;
    }

    req->movement_type = ORTE_FILEM_MOVE_TYPE_UNKNOWN;
}

OBJ_CLASS_INSTANCE(orte_filem_base_request_t,
                   opal_list_item_t,
                   req_construct,
                   req_destruct);

/***********************
 * None component stuff
 ************************/
int orte_filem_base_module_init(void)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_module_finalize(void)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_put(orte_filem_base_request_t *request )
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_put_nb(orte_filem_base_request_t *request )
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_get(orte_filem_base_request_t *request)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_get_nb(orte_filem_base_request_t *request)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_rm(orte_filem_base_request_t *request)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_rm_nb(orte_filem_base_request_t *request)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_wait(orte_filem_base_request_t *request)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_wait_all(opal_list_t *request_list)
{
    return ORTE_SUCCESS;
}

int orte_filem_base_none_preposition_files(orte_job_t *jdata,
                                           orte_filem_completion_cbfunc_t cbfunc,
                                           void *cbdata)
{
    if (NULL != cbfunc) {
        cbfunc(ORTE_SUCCESS, cbdata);
    }
    return ORTE_SUCCESS;
}

int orte_filem_base_none_link_local_files(orte_job_t *jdata,
                                          orte_app_context_t *app)
{
    return ORTE_SUCCESS;
}
