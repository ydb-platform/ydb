/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2006-2008 University of Houston.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2012-2013 Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "opal/util/show_help.h"

#include <string.h>

#include "osc_pt2pt.h"
#include "osc_pt2pt_frag.h"
#include "osc_pt2pt_request.h"
#include "osc_pt2pt_data_move.h"

#include "ompi/mca/osc/base/osc_base_obj_convert.h"

static int component_register(void);
static int component_init(bool enable_progress_threads, bool enable_mpi_threads);
static int component_finalize(void);
static int component_query(struct ompi_win_t *win, void **base, size_t size, int disp_unit,
                           struct ompi_communicator_t *comm, struct opal_info_t *info,
                           int flavor);
static int component_select(struct ompi_win_t *win, void **base, size_t size, int disp_unit,
                            struct ompi_communicator_t *comm, struct opal_info_t *info,
                            int flavor, int *model);

ompi_osc_pt2pt_component_t mca_osc_pt2pt_component = {
    { /* ompi_osc_base_component_t */
        .osc_version = {
            OMPI_OSC_BASE_VERSION_3_0_0,
            .mca_component_name = "pt2pt",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                  OMPI_RELEASE_VERSION),
            .mca_register_component_params = component_register,
        },
        .osc_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },
        .osc_init = component_init,
        .osc_query = component_query,
        .osc_select = component_select,
        .osc_finalize = component_finalize,
    }
};


ompi_osc_pt2pt_module_t ompi_osc_pt2pt_module_template = {
    {
        NULL, /* shared_query */

        ompi_osc_pt2pt_attach,
        ompi_osc_pt2pt_detach,
        ompi_osc_pt2pt_free,

        ompi_osc_pt2pt_put,
        ompi_osc_pt2pt_get,
        ompi_osc_pt2pt_accumulate,
        ompi_osc_pt2pt_compare_and_swap,
        ompi_osc_pt2pt_fetch_and_op,
        ompi_osc_pt2pt_get_accumulate,

        ompi_osc_pt2pt_rput,
        ompi_osc_pt2pt_rget,
        ompi_osc_pt2pt_raccumulate,
        ompi_osc_pt2pt_rget_accumulate,

        ompi_osc_pt2pt_fence,

        ompi_osc_pt2pt_start,
        ompi_osc_pt2pt_complete,
        ompi_osc_pt2pt_post,
        ompi_osc_pt2pt_wait,
        ompi_osc_pt2pt_test,

        ompi_osc_pt2pt_lock,
        ompi_osc_pt2pt_unlock,
        ompi_osc_pt2pt_lock_all,
        ompi_osc_pt2pt_unlock_all,

        ompi_osc_pt2pt_sync,
        ompi_osc_pt2pt_flush,
        ompi_osc_pt2pt_flush_all,
        ompi_osc_pt2pt_flush_local,
        ompi_osc_pt2pt_flush_local_all,
    }
};

bool ompi_osc_pt2pt_no_locks = false;
static bool using_thread_multiple = false;

/* look up parameters for configuring this window.  The code first
   looks in the info structure passed by the user, then through mca
   parameters. */
static bool check_config_value_bool(char *key, opal_info_t *info, bool result)
{
    int flag;

    (void) opal_info_get_bool (info, key, &result, &flag);
    return result;
}

static int component_register (void)
{
    ompi_osc_pt2pt_no_locks = false;
    (void) mca_base_component_var_register(&mca_osc_pt2pt_component.super.osc_version,
                                           "no_locks",
                                           "Enable optimizations available only if MPI_LOCK is "
                                           "not used.  "
                                           "Info key of same name overrides this value.",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &ompi_osc_pt2pt_no_locks);

    mca_osc_pt2pt_component.buffer_size = 8192;
    (void) mca_base_component_var_register (&mca_osc_pt2pt_component.super.osc_version, "buffer_size",
					    "Data transfers smaller than this limit may be coalesced before "
					    "being transferred (default: 8k)", MCA_BASE_VAR_TYPE_UNSIGNED_INT,
					    NULL, 0, 0, OPAL_INFO_LVL_9, MCA_BASE_VAR_SCOPE_READONLY,
					    &mca_osc_pt2pt_component.buffer_size);

    mca_osc_pt2pt_component.receive_count = 4;
    (void) mca_base_component_var_register (&mca_osc_pt2pt_component.super.osc_version, "receive_count",
                                            "Number of receives to post for each window for incoming fragments "
                                            "(default: 4)", MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0, 0, OPAL_INFO_LVL_4,
                                            MCA_BASE_VAR_SCOPE_READONLY, &mca_osc_pt2pt_component.receive_count);

    return OMPI_SUCCESS;
}

static int component_progress (void)
{
    int pending_count = opal_list_get_size (&mca_osc_pt2pt_component.pending_operations);
    int recv_count = opal_list_get_size (&mca_osc_pt2pt_component.pending_receives);
    ompi_osc_pt2pt_pending_t *pending, *next;

    if (recv_count) {
        for (int i = 0 ; i < recv_count ; ++i) {
            OPAL_THREAD_LOCK(&mca_osc_pt2pt_component.pending_receives_lock);
            ompi_osc_pt2pt_receive_t *recv = (ompi_osc_pt2pt_receive_t *) opal_list_remove_first (&mca_osc_pt2pt_component.pending_receives);
            OPAL_THREAD_UNLOCK(&mca_osc_pt2pt_component.pending_receives_lock);
            if (NULL == recv) {
                break;
            }

            (void) ompi_osc_pt2pt_process_receive (recv);
        }
    }

    /* process one incoming request */
    if (pending_count) {
        OPAL_THREAD_LOCK(&mca_osc_pt2pt_component.pending_operations_lock);
        OPAL_LIST_FOREACH_SAFE(pending, next, &mca_osc_pt2pt_component.pending_operations, ompi_osc_pt2pt_pending_t) {
            int ret;

            switch (pending->header.base.type) {
            case OMPI_OSC_PT2PT_HDR_TYPE_FLUSH_REQ:
                ret = ompi_osc_pt2pt_process_flush (pending->module, pending->source,
                                                    &pending->header.flush);
                break;
            case OMPI_OSC_PT2PT_HDR_TYPE_UNLOCK_REQ:
                ret = ompi_osc_pt2pt_process_unlock (pending->module, pending->source,
                                                     &pending->header.unlock);
                break;
            default:
                /* shouldn't happen */
                assert (0);
                abort ();
            }

            if (OMPI_SUCCESS == ret) {
                opal_list_remove_item (&mca_osc_pt2pt_component.pending_operations, &pending->super);
                OBJ_RELEASE(pending);
            }
        }
        OPAL_THREAD_UNLOCK(&mca_osc_pt2pt_component.pending_operations_lock);
    }

    return 1;
}

static int
component_init(bool enable_progress_threads,
               bool enable_mpi_threads)
{
    int ret;

    if (enable_mpi_threads) {
        using_thread_multiple = true;
    }

    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.lock, opal_mutex_t);
    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.pending_operations, opal_list_t);
    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.pending_operations_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.pending_receives, opal_list_t);
    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.pending_receives_lock, opal_mutex_t);

    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.modules,
                  opal_hash_table_t);
    opal_hash_table_init(&mca_osc_pt2pt_component.modules, 2);

    mca_osc_pt2pt_component.progress_enable = false;
    mca_osc_pt2pt_component.module_count = 0;

    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.frags, opal_free_list_t);
    ret = opal_free_list_init (&mca_osc_pt2pt_component.frags,
                               sizeof(ompi_osc_pt2pt_frag_t), 8,
                               OBJ_CLASS(ompi_osc_pt2pt_frag_t),
                               mca_osc_pt2pt_component.buffer_size +
                               sizeof (ompi_osc_pt2pt_frag_header_t),
                               8, 1, -1, 1, NULL, 0, NULL, NULL, NULL);
    if (OMPI_SUCCESS != ret) {
	opal_output_verbose(1, ompi_osc_base_framework.framework_output,
			    "%s:%d: opal_free_list_init failed: %d",
			    __FILE__, __LINE__, ret);
	return ret;
    }

    OBJ_CONSTRUCT(&mca_osc_pt2pt_component.requests, opal_free_list_t);
    ret = opal_free_list_init (&mca_osc_pt2pt_component.requests,
                               sizeof(ompi_osc_pt2pt_request_t), 8,
                               OBJ_CLASS(ompi_osc_pt2pt_request_t),
                               0, 0, 0, -1, 32, NULL, 0, NULL, NULL, NULL);
    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_osc_base_framework.framework_output,
                            "%s:%d: opal_free_list_init failed: %d\n",
                            __FILE__, __LINE__, ret);
        return ret;
    }

    return ret;
}


int
component_finalize(void)
{
    size_t num_modules;

    if (mca_osc_pt2pt_component.progress_enable) {
	opal_progress_unregister (component_progress);
    }

    if (0 !=
        (num_modules = opal_hash_table_get_size(&mca_osc_pt2pt_component.modules))) {
        opal_output(ompi_osc_base_framework.framework_output,
                    "WARNING: There were %d Windows created but not freed.",
                    (int) num_modules);
    }

    OBJ_DESTRUCT(&mca_osc_pt2pt_component.frags);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.modules);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.lock);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.requests);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.pending_operations);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.pending_operations_lock);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.pending_receives);
    OBJ_DESTRUCT(&mca_osc_pt2pt_component.pending_receives_lock);

    return OMPI_SUCCESS;
}


static int
component_query(struct ompi_win_t *win, void **base, size_t size, int disp_unit,
                struct ompi_communicator_t *comm, struct opal_info_t *info,
                int flavor)
{
    if (MPI_WIN_FLAVOR_SHARED == flavor) return -1;

    return 10;
}


static int
component_select(struct ompi_win_t *win, void **base, size_t size, int disp_unit,
                 struct ompi_communicator_t *comm, struct opal_info_t *info,
                 int flavor, int *model)
{
    ompi_osc_pt2pt_module_t *module = NULL;
    int ret;
    char *name;

    /* We don't support shared windows; that's for the sm onesided
       component */
    if (MPI_WIN_FLAVOR_SHARED == flavor) return OMPI_ERR_NOT_SUPPORTED;

    /*
     * workaround for issue https://github.com/open-mpi/ompi/issues/2614
     * The following check needs to be removed once 2614 is addressed.
     */
    if (using_thread_multiple) {
        opal_show_help("help-osc-pt2pt.txt", "mpi-thread-multiple-not-supported", true);
        return OMPI_ERR_NOT_SUPPORTED;
    }

    /* create module structure with all fields initialized to zero */
    module = (ompi_osc_pt2pt_module_t*)
        calloc(1, sizeof(ompi_osc_pt2pt_module_t));
    if (NULL == module) return OMPI_ERR_TEMP_OUT_OF_RESOURCE;

    /* fill in the function pointer part */
    memcpy(module, &ompi_osc_pt2pt_module_template,
           sizeof(ompi_osc_base_module_t));

    /* initialize the objects, so that always free in cleanup */
    OBJ_CONSTRUCT(&module->lock, opal_recursive_mutex_t);
    OBJ_CONSTRUCT(&module->cond, opal_condition_t);
    OBJ_CONSTRUCT(&module->locks_pending, opal_list_t);
    OBJ_CONSTRUCT(&module->locks_pending_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&module->outstanding_locks, opal_hash_table_t);
    OBJ_CONSTRUCT(&module->pending_acc, opal_list_t);
    OBJ_CONSTRUCT(&module->pending_acc_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&module->buffer_gc, opal_list_t);
    OBJ_CONSTRUCT(&module->gc_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&module->all_sync, ompi_osc_pt2pt_sync_t);
    OBJ_CONSTRUCT(&module->peer_hash, opal_hash_table_t);
    OBJ_CONSTRUCT(&module->peer_lock, opal_mutex_t);

    ret = opal_hash_table_init (&module->outstanding_locks, 64);
    if (OPAL_SUCCESS != ret) {
        goto cleanup;
    }

    ret = opal_hash_table_init (&module->peer_hash, 128);
    if (OPAL_SUCCESS != ret) {
        goto cleanup;
    }

    /* options */
    /* FIX ME: should actually check this value... */
#if 1
    module->accumulate_ordering = 1;
#else
    ompi_osc_base_config_value_equal("accumulate_ordering", info, "none");
#endif

    /* fill in our part */
    if (MPI_WIN_FLAVOR_ALLOCATE == flavor && size) {
	module->free_after = *base = malloc(size);
	if (NULL == *base) {
	    ret = OMPI_ERR_TEMP_OUT_OF_RESOURCE;
	    goto cleanup;
	}
    }

    /* in the dynamic case base is MPI_BOTTOM */
    if (MPI_WIN_FLAVOR_DYNAMIC != flavor) {
	module->baseptr = *base;
    }

    ret = ompi_comm_dup(comm, &module->comm);
    if (OMPI_SUCCESS != ret) goto cleanup;

    OPAL_OUTPUT_VERBOSE((10, ompi_osc_base_framework.framework_output,
                         "pt2pt component creating window with id %d",
                         ompi_comm_get_cid(module->comm)));

    /* record my displacement unit.  Always resolved at target */
    module->disp_unit = disp_unit;

    /* peer op count data */
    module->epoch_outgoing_frag_count = calloc (ompi_comm_size(comm), sizeof(uint32_t));
    if (NULL == module->epoch_outgoing_frag_count) {
        ret = OMPI_ERR_TEMP_OUT_OF_RESOURCE;
        goto cleanup;
    }

    /* the statement below (from Brian) does not seem correct so disable active target on the
     * window. if this end up being incorrect please revert this one change */
#if 0
    /* initially, we're in that pseudo-fence state, so we allow eager
       sends (yay for Fence).  Other protocols will disable before
       they start their epochs, so this isn't a problem. */
    module->all_sync.type = OMPI_OSC_PT2PT_SYNC_TYPE_FENCE;
    module->all_sync.eager_send_active = true;
#endif

    /* lock data */
    module->no_locks = check_config_value_bool ("no_locks", info, ompi_osc_pt2pt_no_locks);

    /* update component data */
    OPAL_THREAD_LOCK(&mca_osc_pt2pt_component.lock);
    ret = opal_hash_table_set_value_uint32(&mca_osc_pt2pt_component.modules,
                                           ompi_comm_get_cid(module->comm),
                                           module);
    OPAL_THREAD_UNLOCK(&mca_osc_pt2pt_component.lock);
    if (OMPI_SUCCESS != ret) goto cleanup;

    /* fill in window information */
    *model = MPI_WIN_UNIFIED;
    win->w_osc_module = (ompi_osc_base_module_t*) module;
    asprintf(&name, "pt2pt window %d", ompi_comm_get_cid(module->comm));
    ompi_win_set_name(win, name);
    free(name);

    /* sync memory - make sure all initialization completed */
    opal_atomic_mb();

    ret = ompi_osc_pt2pt_frag_start_receive (module);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
	goto cleanup;
    }

    /* barrier to prevent arrival of lock requests before we're
       fully created */
    ret = module->comm->c_coll->coll_barrier(module->comm,
                                            module->comm->c_coll->coll_barrier_module);
    if (OMPI_SUCCESS != ret) goto cleanup;

    if (!mca_osc_pt2pt_component.progress_enable) {
	opal_progress_register (component_progress);
	mca_osc_pt2pt_component.progress_enable = true;
    }

    if (module->no_locks) {
        win->w_flags |= OMPI_WIN_NO_LOCKS;
    }

    OPAL_OUTPUT_VERBOSE((10, ompi_osc_base_framework.framework_output,
                         "done creating pt2pt window %d", ompi_comm_get_cid(module->comm)));

    return OMPI_SUCCESS;

 cleanup:
    /* set the module so we properly cleanup */
    win->w_osc_module = (ompi_osc_base_module_t*) module;
    ompi_osc_pt2pt_free (win);

    return ret;
}


int
ompi_osc_pt2pt_set_info(struct ompi_win_t *win, struct opal_info_t *info)
{
    ompi_osc_pt2pt_module_t *module =
        (ompi_osc_pt2pt_module_t*) win->w_osc_module;

    /* enforce collectiveness... */
    return module->comm->c_coll->coll_barrier(module->comm,
                                             module->comm->c_coll->coll_barrier_module);
}


int
ompi_osc_pt2pt_get_info(struct ompi_win_t *win, struct opal_info_t **info_used)
{
    opal_info_t *info = OBJ_NEW(opal_info_t);
    if (NULL == info) return OMPI_ERR_TEMP_OUT_OF_RESOURCE;

    *info_used = info;

    return OMPI_SUCCESS;
}

OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_pending_t, opal_list_item_t, NULL, NULL);

static void ompi_osc_pt2pt_receive_construct (ompi_osc_pt2pt_receive_t *recv)
{
    recv->buffer = NULL;
    recv->pml_request = NULL;
}

static void ompi_osc_pt2pt_receive_destruct (ompi_osc_pt2pt_receive_t *recv)
{
    free (recv->buffer);
    if (recv->pml_request && MPI_REQUEST_NULL != recv->pml_request) {
        recv->pml_request->req_complete_cb = NULL;
        ompi_request_cancel (recv->pml_request);
        ompi_request_free (&recv->pml_request);
    }
}

OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_receive_t, opal_list_item_t,
                   ompi_osc_pt2pt_receive_construct,
                   ompi_osc_pt2pt_receive_destruct);

static void ompi_osc_pt2pt_peer_construct (ompi_osc_pt2pt_peer_t *peer)
{
    OBJ_CONSTRUCT(&peer->queued_frags, opal_list_t);
    OBJ_CONSTRUCT(&peer->lock, opal_mutex_t);
    peer->active_frag = NULL;
    peer->passive_incoming_frag_count = 0;
    peer->flags = 0;
}

static void ompi_osc_pt2pt_peer_destruct (ompi_osc_pt2pt_peer_t *peer)
{
    OBJ_DESTRUCT(&peer->queued_frags);
    OBJ_DESTRUCT(&peer->lock);
}

OBJ_CLASS_INSTANCE(ompi_osc_pt2pt_peer_t, opal_object_t,
                   ompi_osc_pt2pt_peer_construct,
                   ompi_osc_pt2pt_peer_destruct);
