/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016-2018 IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2018 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2018-2019 Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <dirent.h>
#include <errno.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <time.h>

#include <pmix_common.h>

#include "src/include/pmix_globals.h"
#include "src/class/pmix_list.h"
#include "src/client/pmix_client_ops.h"
#include "src/server/pmix_server_ops.h"
#include "src/util/argv.h"
#include "src/util/compress.h"
#include "src/util/error.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"
#include "src/util/hash.h"
#include "src/mca/preg/preg.h"

#include "src/mca/gds/base/base.h"
#include "src/mca/pshmem/base/base.h"
#include "dstore_common.h"
#include "dstore_base.h"
#include "dstore_segment.h"

#define ESH_REGION_EXTENSION        "EXTENSION_SLOT"
#define ESH_REGION_INVALIDATED      "INVALIDATED"
#define ESH_ENV_INITIAL_SEG_SIZE    "INITIAL_SEG_SIZE"
#define ESH_ENV_NS_META_SEG_SIZE    "NS_META_SEG_SIZE"
#define ESH_ENV_NS_DATA_SEG_SIZE    "NS_DATA_SEG_SIZE"
#define ESH_ENV_LINEAR              "SM_USE_LINEAR_SEARCH"

#define ESH_INIT_SESSION_TBL_SIZE 2
#define ESH_INIT_NS_MAP_TBL_SIZE  2

static int _store_data_for_rank(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_info,
                                pmix_rank_t rank, pmix_buffer_t *buf);
static int _update_ns_elem(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_elem, ns_seg_info_t *info);
static int _put_ns_info_to_initial_segment(pmix_common_dstore_ctx_t *ds_ctx,
                                           const ns_map_data_t *ns_map, pmix_pshmem_seg_t *metaseg,
                                           pmix_pshmem_seg_t *dataseg);
static ns_seg_info_t *_get_ns_info_from_initial_segment(pmix_common_dstore_ctx_t *ds_ctx,
                                                        const ns_map_data_t *ns_map);
static ns_track_elem_t *_get_track_elem_for_namespace(pmix_common_dstore_ctx_t *ds_ctx,
                                                      ns_map_data_t *ns_map);
static rank_meta_info *_get_rank_meta_info(pmix_common_dstore_ctx_t *ds_ctx, pmix_rank_t rank,
                                           pmix_dstore_seg_desc_t *segdesc);
static uint8_t *_get_data_region_by_offset(pmix_common_dstore_ctx_t *ds_ctx,
                                           pmix_dstore_seg_desc_t *segdesc, size_t offset);
static void _update_initial_segment_info(pmix_common_dstore_ctx_t *ds_ctx,
                                         const ns_map_data_t *ns_map);
static void _set_constants_from_env(pmix_common_dstore_ctx_t *ds_ctx);
static inline ssize_t _get_univ_size(pmix_common_dstore_ctx_t *ds_ctx, const char *nspace);

static inline ns_map_data_t * _esh_session_map_search_server(pmix_common_dstore_ctx_t *ds_ctx,
                                                             const char *nspace);
static inline ns_map_data_t * _esh_session_map_search_client(pmix_common_dstore_ctx_t *ds_ctx,
                                                             const char *nspace);
static inline ns_map_data_t * _esh_session_map(pmix_common_dstore_ctx_t *ds_ctx,
                                               const char *nspace, uint32_t local_size,
                                               size_t tbl_idx);
static inline void _esh_session_map_clean(pmix_common_dstore_ctx_t *ds_ctx, ns_map_t *m);
static inline int _esh_jobuid_tbl_search(pmix_common_dstore_ctx_t *ds_ctx,
                                         uid_t jobuid, size_t *tbl_idx);
static inline int _esh_session_tbl_add(pmix_common_dstore_ctx_t *ds_ctx, size_t *tbl_idx);
static int _esh_session_init(pmix_common_dstore_ctx_t *ds_ctx, size_t idx, ns_map_data_t *m,
                             uint32_t local_size, size_t jobuid, int setjobuid);
static void _esh_session_release(pmix_common_dstore_ctx_t *ds_ctx, size_t idx);
static inline void _esh_ns_track_cleanup(pmix_common_dstore_ctx_t *ds_ctx);
static inline void _esh_sessions_cleanup(pmix_common_dstore_ctx_t *ds_ctx);
static inline void _esh_ns_map_cleanup(pmix_common_dstore_ctx_t *ds_ctx);
static inline int _esh_dir_del(const char *dirname);
static inline void _client_compat_save(pmix_common_dstore_ctx_t *ds_ctx, pmix_peer_t *peer);
static inline pmix_peer_t * _client_peer(pmix_common_dstore_ctx_t *ds_ctx);

static inline int _my_client(const char *nspace, pmix_rank_t rank);

static pmix_status_t _dstor_store_modex_cb(pmix_common_dstore_ctx_t *ds_ctx,
                                                struct pmix_namespace_t *nspace,
                                                pmix_list_t *cbs,
                                                pmix_byte_object_t *bo);

static pmix_status_t _dstore_store_nolock(pmix_common_dstore_ctx_t *ds_ctx,
                                   ns_map_data_t *ns_map,
                                   pmix_rank_t rank,
                                   pmix_kval_t *kv);

static pmix_status_t _dstore_fetch(pmix_common_dstore_ctx_t *ds_ctx,
                                   const char *nspace, pmix_rank_t rank,
                                   const char *key, pmix_value_t **kvs);

ns_map_data_t * (*_esh_session_map_search)(const char *nspace) = NULL;

#define _ESH_SESSION_lock(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].lock)

#define _ESH_SESSION_path(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].nspace_path)

#define _ESH_SESSION_lockfile(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].lockfile)

#define _ESH_SESSION_setjobuid(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].setjobuid)

#define _ESH_SESSION_jobuid(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].jobuid)

#define _ESH_SESSION_sm_seg_first(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].sm_seg_first)
#define _ESH_SESSION_sm_seg_last(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].sm_seg_last)

#define _ESH_SESSION_ns_info(session_array, tbl_idx) \
    (PMIX_VALUE_ARRAY_GET_BASE(session_array, session_t)[tbl_idx].ns_info)

#ifdef ESH_PTHREAD_LOCK
#define _ESH_SESSION_pthread_rwlock(tbl_idx) (PMIX_VALUE_ARRAY_GET_BASE(_session_array, session_t)[tbl_idx].rwlock)
#define _ESH_SESSION_pthread_seg(tbl_idx)   (PMIX_VALUE_ARRAY_GET_BASE(_session_array, session_t)[tbl_idx].rwlock_seg)
#define _ESH_SESSION_lock(tbl_idx)         _ESH_SESSION_pthread_rwlock(tbl_idx)
#endif

#ifdef ESH_FCNTL_LOCK
#define _ESH_SESSION_lockfd(tbl_idx)       (PMIX_VALUE_ARRAY_GET_BASE(_session_array, session_t)[tbl_idx].lockfd)
#define _ESH_SESSION_lock(tbl_idx)         _ESH_SESSION_lockfd(tbl_idx)
#endif

#define _ESH_LOCK(ds_ctx, session_id, operation)                               \
__pmix_attribute_extension__ ({                                                \
    pmix_status_t rc = PMIX_SUCCESS;                                           \
    rc = ds_ctx->lock_cbs->operation(_ESH_SESSION_lock(ds_ctx->session_array,  \
                                                    session_id));              \
    rc;                                                                        \
})

static void ncon(ns_track_elem_t *p) {
    memset(&p->ns_map, 0, sizeof(p->ns_map));
    p->meta_seg = NULL;
    p->data_seg = NULL;
    p->num_meta_seg = 0;
    p->num_data_seg = 0;
    p->in_use = true;
}

static void ndes(ns_track_elem_t *p) {
    pmix_common_dstor_delete_sm_desc(p->meta_seg);
    pmix_common_dstor_delete_sm_desc(p->data_seg);
    memset(&p->ns_map, 0, sizeof(p->ns_map));
    p->in_use = false;
}

PMIX_CLASS_INSTANCE(ns_track_elem_t,
                    pmix_value_array_t,
                    ncon, ndes);

static inline void _esh_session_map_clean(pmix_common_dstore_ctx_t *ds_ctx, ns_map_t *m) {
    memset(m, 0, sizeof(*m));
    m->data.track_idx = -1;
}

static inline int _esh_dir_del(const char *path)
{
    DIR *dir;
    struct dirent *d_ptr;
    struct stat st;
    pmix_status_t rc = PMIX_SUCCESS;

    char name[PMIX_PATH_MAX];

    dir = opendir(path);
    if (NULL == dir) {
        rc = PMIX_ERR_BAD_PARAM;
        return rc;
    }

    while (NULL != (d_ptr = readdir(dir))) {
        snprintf(name, PMIX_PATH_MAX, "%s/%s", path, d_ptr->d_name);
        if ( 0 > lstat(name, &st) ){
            /* No fatal error here - just log this event
             * we will hit the error later at rmdir. Keep trying ...
             */
            PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
            continue;
        }

        if(S_ISDIR(st.st_mode)) {
            if(strcmp(d_ptr->d_name, ".") && strcmp(d_ptr->d_name, "..")) {
                rc = _esh_dir_del(name);
                if( PMIX_SUCCESS != rc ){
                    /* No fatal error here - just log this event
                     * we will hit the error later at rmdir. Keep trying ...
                     */
                    PMIX_ERROR_LOG(rc);
                }
            }
        }
        else {
            if( 0 > unlink(name) ){
                /* No fatal error here - just log this event
                 * we will hit the error later at rmdir. Keep trying ...
                 */
                PMIX_ERROR_LOG(PMIX_ERR_NO_PERMISSIONS);
            }
        }
    }
    closedir(dir);

    /* remove the top dir */
    if( 0 > rmdir(path) ){
        rc = PMIX_ERR_NO_PERMISSIONS;
        PMIX_ERROR_LOG(rc);
    }
    return rc;
}

static inline int _esh_tbls_init(pmix_common_dstore_ctx_t *ds_ctx)
{
    pmix_status_t rc = PMIX_SUCCESS;
    size_t idx;

    /* initial settings */
    ds_ctx->ns_track_array = NULL;
    ds_ctx->session_array = NULL;
    ds_ctx->ns_map_array = NULL;

    /* Setup namespace tracking array */
    if (NULL == (ds_ctx->ns_track_array = PMIX_NEW(pmix_value_array_t))) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    if (PMIX_SUCCESS != (rc = pmix_value_array_init(ds_ctx->ns_track_array, sizeof(ns_track_elem_t)))){
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    /* Setup sessions table */
    if (NULL == (ds_ctx->session_array = PMIX_NEW(pmix_value_array_t))){
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    if (PMIX_SUCCESS != (rc = pmix_value_array_init(ds_ctx->session_array, sizeof(session_t)))) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    if (PMIX_SUCCESS != (rc = pmix_value_array_set_size(ds_ctx->session_array, ESH_INIT_SESSION_TBL_SIZE))) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    for (idx = 0; idx < ESH_INIT_SESSION_TBL_SIZE; idx++) {
        memset(pmix_value_array_get_item(ds_ctx->session_array, idx), 0, sizeof(session_t));
    }

    /* Setup namespace map array */
    if (NULL == (ds_ctx->ns_map_array = PMIX_NEW(pmix_value_array_t))) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    if (PMIX_SUCCESS != (rc = pmix_value_array_init(ds_ctx->ns_map_array, sizeof(ns_map_t)))) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    if (PMIX_SUCCESS != (rc = pmix_value_array_set_size(ds_ctx->ns_map_array, ESH_INIT_NS_MAP_TBL_SIZE))) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    for (idx = 0; idx < ESH_INIT_NS_MAP_TBL_SIZE; idx++) {
        _esh_session_map_clean(ds_ctx, pmix_value_array_get_item(ds_ctx->ns_map_array, idx));
    }

    return PMIX_SUCCESS;
err_exit:
    if (NULL != ds_ctx->ns_track_array) {
        PMIX_RELEASE(ds_ctx->ns_track_array);
    }
    if (NULL != ds_ctx->session_array) {
        PMIX_RELEASE(ds_ctx->session_array);
    }
    if (NULL != ds_ctx->ns_map_array) {
        PMIX_RELEASE(ds_ctx->ns_map_array);
    }
    return rc;
}

static inline void _esh_ns_map_cleanup(pmix_common_dstore_ctx_t *ds_ctx)
{
    size_t idx;
    size_t size;
    ns_map_t *ns_map;

    if (NULL == ds_ctx->ns_map_array) {
        return;
    }

    size = pmix_value_array_get_size(ds_ctx->ns_map_array);
    ns_map = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->ns_map_array, ns_map_t);

    for (idx = 0; idx < size; idx++) {
        if(ns_map[idx].in_use) {
            _esh_session_map_clean(ds_ctx, &ns_map[idx]);
        }
    }

    PMIX_RELEASE(ds_ctx->ns_map_array);
    ds_ctx->ns_map_array = NULL;
}

static inline void _esh_sessions_cleanup(pmix_common_dstore_ctx_t *ds_ctx)
{
    size_t idx;
    size_t size;
    session_t *s_tbl;

    if (NULL == ds_ctx->session_array) {
        return;
    }

    size = pmix_value_array_get_size(ds_ctx->session_array);
    s_tbl = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->session_array, session_t);

    for (idx = 0; idx < size; idx++) {
        if(s_tbl[idx].in_use)
            _esh_session_release(ds_ctx, idx);
    }

    PMIX_RELEASE(ds_ctx->session_array);
    ds_ctx->session_array = NULL;
}

static inline void _esh_ns_track_cleanup(pmix_common_dstore_ctx_t *ds_ctx)
{
    int size;
    ns_track_elem_t *ns_trk;

    if (NULL == ds_ctx->ns_track_array) {
        return;
    }

    size = pmix_value_array_get_size(ds_ctx->ns_track_array);
    ns_trk = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->ns_track_array, ns_track_elem_t);

    for (int i = 0; i < size; i++) {
        ns_track_elem_t *trk = ns_trk + i;
        if (trk->in_use) {
            PMIX_DESTRUCT(trk);
        }
    }

    PMIX_RELEASE(ds_ctx->ns_track_array);
    ds_ctx->ns_track_array = NULL;
}

static inline ns_map_data_t * _esh_session_map(pmix_common_dstore_ctx_t *ds_ctx,
                                               const char *nspace, uint32_t local_size,
                                               size_t tbl_idx)
{
    size_t map_idx;
    size_t size = pmix_value_array_get_size(ds_ctx->ns_map_array);
    ns_map_t *ns_map = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->ns_map_array, ns_map_t);
    ns_map_t *new_map = NULL;

    if (NULL == nspace) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return NULL;
    }

    for(map_idx = 0; map_idx < size; map_idx++) {
        if (!ns_map[map_idx].in_use) {
            ns_map[map_idx].in_use = true;
            pmix_strncpy(ns_map[map_idx].data.name, nspace, sizeof(ns_map[map_idx].data.name)-1);
            ns_map[map_idx].data.tbl_idx = tbl_idx;
            return  &ns_map[map_idx].data;
        }
    }

    if (NULL == (new_map = pmix_value_array_get_item(ds_ctx->ns_map_array, map_idx))) {
        PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
        return NULL;
    }

    _esh_session_map_clean(ds_ctx, new_map);
    new_map->in_use = true;
    new_map->data.tbl_idx = tbl_idx;
    pmix_strncpy(new_map->data.name, nspace, sizeof(new_map->data.name)-1);

    return  &new_map->data;
}

static inline int _esh_jobuid_tbl_search(pmix_common_dstore_ctx_t *ds_ctx,
                                         uid_t jobuid, size_t *tbl_idx)
{
    size_t idx, size;
    session_t *session_tbl = NULL;

    size = pmix_value_array_get_size(ds_ctx->session_array);
    session_tbl = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->session_array, session_t);

    for(idx = 0; idx < size; idx++) {
        if (session_tbl[idx].in_use && session_tbl[idx].jobuid == jobuid) {
            *tbl_idx = idx;
            return PMIX_SUCCESS;
        }
    }

    return PMIX_ERR_NOT_FOUND;
}

static inline int _esh_session_tbl_add(pmix_common_dstore_ctx_t *ds_ctx, size_t *tbl_idx)
{
    size_t idx;
    size_t size = pmix_value_array_get_size(ds_ctx->session_array);
    session_t *s_tbl = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->session_array, session_t);
    session_t *new_sesion;
    pmix_status_t rc = PMIX_SUCCESS;

    for(idx = 0; idx < size; idx ++) {
        if (0 == s_tbl[idx].in_use) {
            goto done;
        }
    }

    if (NULL == (new_sesion = pmix_value_array_get_item(ds_ctx->session_array, idx))) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

done:
    s_tbl[idx].in_use = 1;
    *tbl_idx = idx;

    return PMIX_SUCCESS;
}

static inline ns_map_data_t * _esh_session_map_search_server(pmix_common_dstore_ctx_t *ds_ctx,
                                                             const char *nspace)
{
    size_t idx, size = pmix_value_array_get_size(ds_ctx->ns_map_array);
    ns_map_t *ns_map = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->ns_map_array, ns_map_t);
    if (NULL == nspace) {
        return NULL;
    }

    for (idx = 0; idx < size; idx++) {
        if (ns_map[idx].in_use &&
            (0 == strcmp(ns_map[idx].data.name, nspace))) {
                return &ns_map[idx].data;
        }
    }
    return NULL;
}

static inline ns_map_data_t * _esh_session_map_search_client(pmix_common_dstore_ctx_t *ds_ctx,
                                                             const char *nspace)
{
    size_t idx, size = pmix_value_array_get_size(ds_ctx->ns_map_array);
    ns_map_t *ns_map = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->ns_map_array, ns_map_t);

    if (NULL == nspace) {
        return NULL;
    }

    for (idx = 0; idx < size; idx++) {
        if (ns_map[idx].in_use &&
            (0 == strcmp(ns_map[idx].data.name, nspace))) {
                return &ns_map[idx].data;
        }
    }
    return _esh_session_map(ds_ctx, nspace, 0, 0);
}

static int _esh_session_init(pmix_common_dstore_ctx_t *ds_ctx, size_t idx, ns_map_data_t *m,
                             uint32_t local_size, size_t jobuid, int setjobuid)
{
    pmix_dstore_seg_desc_t *seg = NULL;
    session_t *s = &(PMIX_VALUE_ARRAY_GET_ITEM(ds_ctx->session_array, session_t, idx));
    pmix_status_t rc = PMIX_SUCCESS;

    s->setjobuid = setjobuid;
    s->jobuid = jobuid;
    s->nspace_path = strdup(ds_ctx->base_path);

    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        if (0 != mkdir(s->nspace_path, 0770)) {
            if (EEXIST != errno) {
                pmix_output(0, "session init: can not create session directory \"%s\": %s",
                    s->nspace_path, strerror(errno));
                rc = PMIX_ERROR;
                PMIX_ERROR_LOG(rc);
                return rc;
            }
        }
        if (s->setjobuid > 0){
            if (0 > chown(s->nspace_path, (uid_t) s->jobuid, (gid_t) -1)){
                rc = PMIX_ERROR;
                PMIX_ERROR_LOG(rc);
                return rc;
            }
        }
        seg = pmix_common_dstor_create_new_segment(PMIX_DSTORE_INITIAL_SEGMENT, ds_ctx->base_path,
                                                   m->name, 0, ds_ctx->jobuid, ds_ctx->setjobuid);
        if( NULL == seg ){
            rc = PMIX_ERR_OUT_OF_RESOURCE;
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }
    else {
        seg = pmix_common_dstor_attach_new_segment(PMIX_DSTORE_INITIAL_SEGMENT, ds_ctx->base_path, m->name, 0);
        if( NULL == seg ){
            rc = PMIX_ERR_OUT_OF_RESOURCE;
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }
    s->sm_seg_first = seg;
    s->sm_seg_last = s->sm_seg_first;

    return PMIX_SUCCESS;
}

static void _esh_session_release(pmix_common_dstore_ctx_t *ds_ctx, size_t idx)
{
    session_t *s = &(PMIX_VALUE_ARRAY_GET_ITEM(ds_ctx->session_array, session_t, idx));

    if (!s->in_use) {
        return;
    }

    pmix_common_dstor_delete_sm_desc(s->sm_seg_first);

    ds_ctx->lock_cbs->finalize(&_ESH_SESSION_lock(ds_ctx->session_array, idx));

    if (NULL != s->nspace_path) {
        if(PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
            _esh_dir_del(s->nspace_path);
        }
        free(s->nspace_path);
    }
    memset ((char *) s, 0, sizeof(*s));
}

static void _set_constants_from_env(pmix_common_dstore_ctx_t *ds_ctx)
{
    char *str;
    int page_size = pmix_common_dstor_getpagesize();

    if( NULL != (str = getenv(ESH_ENV_INITIAL_SEG_SIZE)) ) {
        ds_ctx->initial_segment_size = strtoul(str, NULL, 10);
        if ((size_t)page_size > ds_ctx->initial_segment_size) {
            ds_ctx->initial_segment_size = (size_t)page_size;
        }
    }
    if (0 == ds_ctx->initial_segment_size) {
        ds_ctx->initial_segment_size = INITIAL_SEG_SIZE;
    }
    if( NULL != (str = getenv(ESH_ENV_NS_META_SEG_SIZE)) ) {
        ds_ctx->meta_segment_size = strtoul(str, NULL, 10);
        if ((size_t)page_size > ds_ctx->meta_segment_size) {
            ds_ctx->meta_segment_size = (size_t)page_size;
        }
    }
    if (0 == ds_ctx->meta_segment_size) {
        ds_ctx->meta_segment_size = NS_META_SEG_SIZE;
    }
    if( NULL != (str = getenv(ESH_ENV_NS_DATA_SEG_SIZE)) ) {
        ds_ctx->data_segment_size = strtoul(str, NULL, 10);
        if ((size_t)page_size > ds_ctx->data_segment_size) {
            ds_ctx->data_segment_size = (size_t)page_size;
        }
    }
    if (0 == ds_ctx->data_segment_size) {
        ds_ctx->data_segment_size = NS_DATA_SEG_SIZE;
    }
    if (NULL != (str = getenv(ESH_ENV_LINEAR))) {
        if (1 == strtoul(str, NULL, 10)) {
            ds_ctx->direct_mode = 1;
        }
    }

    ds_ctx->lock_segment_size = page_size;
    ds_ctx->max_ns_num = (ds_ctx->initial_segment_size - sizeof(size_t) * 2) / sizeof(ns_seg_info_t);
    ds_ctx->max_meta_elems = (ds_ctx->meta_segment_size - sizeof(size_t)) / sizeof(rank_meta_info);

    pmix_common_dstor_init_segment_info(ds_ctx->initial_segment_size, ds_ctx->meta_segment_size,
                                        ds_ctx->data_segment_size);

}

/* This function synchronizes the content of initial shared segment and the local track list. */
static int _update_ns_elem(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_elem,
                           ns_seg_info_t *info)
{
    pmix_dstore_seg_desc_t *seg, *tmp = NULL;
    size_t i, offs;
    ns_map_data_t *ns_map = NULL;
    pmix_status_t rc;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s",
                         __FILE__, __LINE__, __func__));

    if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, info->ns_map.name))) {
        rc = PMIX_ERR_NOT_AVAILABLE;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    tmp = ns_elem->meta_seg;
    if (NULL != tmp) {
        while(NULL != tmp->next) {
            tmp = tmp->next;
        }
    }

    /* synchronize number of meta segments for the target namespace. */
    for (i = ns_elem->num_meta_seg; i < info->num_meta_seg; i++) {
        if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
            seg = pmix_common_dstor_create_new_segment(PMIX_DSTORE_NS_META_SEGMENT, ds_ctx->base_path,
                                                       info->ns_map.name, i, ds_ctx->jobuid,
                                                       ds_ctx->setjobuid);
            if (NULL == seg) {
                rc = PMIX_ERR_OUT_OF_RESOURCE;
                PMIX_ERROR_LOG(rc);
                return rc;
            }
        } else {
            seg = pmix_common_dstor_attach_new_segment(PMIX_DSTORE_NS_META_SEGMENT, ds_ctx->base_path, info->ns_map.name, i);
            if (NULL == seg) {
                rc = PMIX_ERR_NOT_AVAILABLE;
                PMIX_ERROR_LOG(rc);
                return rc;
            }
        }

        if (NULL == tmp) {
            ns_elem->meta_seg = seg;
        } else {
            tmp->next = seg;
        }
        tmp = seg;
        ns_elem->num_meta_seg++;
    }

    tmp = ns_elem->data_seg;
    if (NULL != tmp) {
        while(NULL != tmp->next) {
            tmp = tmp->next;
        }
    }
    /* synchronize number of data segments for the target namespace. */
    for (i = ns_elem->num_data_seg; i < info->num_data_seg; i++) {
        if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
            seg = pmix_common_dstor_create_new_segment(PMIX_DSTORE_NS_DATA_SEGMENT, ds_ctx->base_path,
                                                       info->ns_map.name, i, ds_ctx->jobuid,
                                                       ds_ctx->setjobuid);
            if (NULL == seg) {
                rc = PMIX_ERR_OUT_OF_RESOURCE;
                PMIX_ERROR_LOG(rc);
                return rc;
            }
            offs = sizeof(size_t);//shift on offset field itself
            memcpy(seg->seg_info.seg_base_addr, &offs, sizeof(size_t));
        } else {
            seg = pmix_common_dstor_attach_new_segment(PMIX_DSTORE_NS_DATA_SEGMENT, ds_ctx->base_path, info->ns_map.name, i);
            if (NULL == seg) {
                rc = PMIX_ERR_NOT_AVAILABLE;
                PMIX_ERROR_LOG(rc);
                return rc;
            }
        }

        if (NULL == tmp) {
            ns_elem->data_seg = seg;
        } else {
            tmp->next = seg;
        }
        tmp = seg;
        ns_elem->num_data_seg++;
    }

    return PMIX_SUCCESS;
}

static int _put_ns_info_to_initial_segment(pmix_common_dstore_ctx_t *ds_ctx,
                                           const ns_map_data_t *ns_map, pmix_pshmem_seg_t *metaseg,
                                           pmix_pshmem_seg_t *dataseg)
{
    ns_seg_info_t elem;
    size_t num_elems;
    num_elems = *((size_t*)(_ESH_SESSION_sm_seg_last(ds_ctx->session_array,
                                                     ns_map->tbl_idx)->seg_info.seg_base_addr));
    pmix_dstore_seg_desc_t *last_seg = _ESH_SESSION_sm_seg_last(ds_ctx->session_array, ns_map->tbl_idx);
    pmix_status_t rc;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s", __FILE__, __LINE__, __func__));

    if (ds_ctx->max_ns_num == num_elems) {
        num_elems = 0;
        if (NULL == (last_seg = pmix_common_dstor_extend_segment(last_seg, ds_ctx->base_path, ns_map->name,
                                                                 ds_ctx->jobuid, ds_ctx->setjobuid))) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            return rc;
        }
        /* mark previous segment as full */
        size_t full = 1;
        memcpy((uint8_t*)(_ESH_SESSION_sm_seg_last(ds_ctx->session_array, ns_map->tbl_idx)->seg_info.seg_base_addr +
                          sizeof(size_t)), &full, sizeof(size_t));
        _ESH_SESSION_sm_seg_last(ds_ctx->session_array, ns_map->tbl_idx) = last_seg;
        memset(_ESH_SESSION_sm_seg_last(ds_ctx->session_array, ns_map->tbl_idx)->seg_info.seg_base_addr,
               0, ds_ctx->initial_segment_size);
    }
    memset(&elem.ns_map, 0, sizeof(elem.ns_map));
    pmix_strncpy(elem.ns_map.name, ns_map->name, sizeof(elem.ns_map.name)-1);
    elem.ns_map.tbl_idx = ns_map->tbl_idx;
    elem.num_meta_seg = 1;
    elem.num_data_seg = 1;
    memcpy((uint8_t*)(_ESH_SESSION_sm_seg_last(ds_ctx->session_array, ns_map->tbl_idx)->seg_info.seg_base_addr) +
           sizeof(size_t) * 2 + num_elems * sizeof(ns_seg_info_t), &elem, sizeof(ns_seg_info_t));
    num_elems++;
    memcpy((uint8_t*)(_ESH_SESSION_sm_seg_last(ds_ctx->session_array, ns_map->tbl_idx)->seg_info.seg_base_addr),
           &num_elems, sizeof(size_t));
    return PMIX_SUCCESS;
}

/* clients should sync local info with information from initial segment regularly */
static void _update_initial_segment_info(pmix_common_dstore_ctx_t *ds_ctx, const ns_map_data_t *ns_map)
{
    pmix_dstore_seg_desc_t *tmp;
    tmp = _ESH_SESSION_sm_seg_first(ds_ctx->session_array, ns_map->tbl_idx);

    PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s", __FILE__, __LINE__, __func__));

    /* go through all global segments */
    do {
        /* check if current segment was marked as full but no more next segment is in the chain */
        if (NULL == tmp->next && 1 == *((size_t*)((uint8_t*)(tmp->seg_info.seg_base_addr) + sizeof(size_t)))) {
            tmp->next = pmix_common_dstor_attach_new_segment(PMIX_DSTORE_INITIAL_SEGMENT, ds_ctx->base_path,
                                                             ns_map->name, tmp->id+1);
        }
        tmp = tmp->next;
    }
    while (NULL != tmp);
}

/* this function will be used by clients to get ns data from the initial segment and add them to the tracker list */
static ns_seg_info_t *_get_ns_info_from_initial_segment(pmix_common_dstore_ctx_t *ds_ctx,
                                                        const ns_map_data_t *ns_map)
{
    pmix_status_t rc;
    size_t i;
    pmix_dstore_seg_desc_t *tmp;
    ns_seg_info_t *elem, *cur_elem;
    elem = NULL;
    size_t num_elems;

    PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s", __FILE__, __LINE__, __func__));

    tmp = _ESH_SESSION_sm_seg_first(ds_ctx->session_array, ns_map->tbl_idx);

    rc = 1;
    /* go through all global segments */
    do {
        num_elems = *((size_t*)(tmp->seg_info.seg_base_addr));
        for (i = 0; i < num_elems; i++) {
            cur_elem = (ns_seg_info_t*)((uint8_t*)(tmp->seg_info.seg_base_addr) + sizeof(size_t) * 2 + i * sizeof(ns_seg_info_t));
            if (0 == (rc = strncmp(cur_elem->ns_map.name, ns_map->name, strlen(ns_map->name)+1))) {
                break;
            }
        }
        if (0 == rc) {
            elem = cur_elem;
            break;
        }
        tmp = tmp->next;
    }
    while (NULL != tmp);
    return elem;
}

static ns_track_elem_t *_get_track_elem_for_namespace(pmix_common_dstore_ctx_t *ds_ctx,
                                                      ns_map_data_t *ns_map)
{
    ns_track_elem_t *new_elem = NULL;
    size_t size = pmix_value_array_get_size(ds_ctx->ns_track_array);

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: nspace %s",
                         __FILE__, __LINE__, __func__, ns_map->name));

    /* check if this namespace is already being tracked to avoid duplicating data. */
    if (ns_map->track_idx >= 0) {
        if ((ns_map->track_idx + 1) > (int)size) {
            return NULL;
        }
        /* data for this namespace should be already stored in shared memory region. */
        /* so go and just put new data. */
        return pmix_value_array_get_item(ds_ctx->ns_track_array, ns_map->track_idx);
    }

    /* create shared memory regions for this namespace and store its info locally
     * to operate with address and detach/unlink afterwards. */
    if (NULL == (new_elem = pmix_value_array_get_item(ds_ctx->ns_track_array, size))) {
        return NULL;
    }
    PMIX_CONSTRUCT(new_elem, ns_track_elem_t);
    pmix_strncpy(new_elem->ns_map.name, ns_map->name, sizeof(new_elem->ns_map.name)-1);
    /* save latest track idx to info of nspace */
    ns_map->track_idx = size;

    return new_elem;
}

static rank_meta_info *_get_rank_meta_info(pmix_common_dstore_ctx_t *ds_ctx, pmix_rank_t rank, pmix_dstore_seg_desc_t *segdesc)
{
    size_t i;
    rank_meta_info *elem = NULL;
    pmix_dstore_seg_desc_t *tmp = segdesc;
    size_t num_elems, rel_offset;
    int id;
    rank_meta_info *cur_elem;

    size_t rcount = rank == PMIX_RANK_WILDCARD ? 0 : rank + 1;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s",
                         __FILE__, __LINE__, __func__));

    if (1 == ds_ctx->direct_mode) {
        /* do linear search to find the requested rank inside all meta segments
         * for this namespace. */
        /* go through all existing meta segments for this namespace */
        do {
            num_elems = *((size_t*)(tmp->seg_info.seg_base_addr));
            for (i = 0; i < num_elems; i++) {
                cur_elem = (rank_meta_info*)((uint8_t*)(tmp->seg_info.seg_base_addr) + sizeof(size_t) + i * sizeof(rank_meta_info));
                if (rcount == cur_elem->rank) {
                    elem = cur_elem;
                    break;
                }
            }
            tmp = tmp->next;
        }
        while (NULL != tmp && NULL == elem);
    } else {
        /* directly compute index of meta segment (id) and relative offset (rel_offset)
         * inside this segment for fast lookup a rank_meta_info object for the requested rank. */
        id = rcount/ds_ctx->max_meta_elems;
        rel_offset = (rcount % ds_ctx->max_meta_elems) * sizeof(rank_meta_info) + sizeof(size_t);
        /* go through all existing meta segments for this namespace.
         * Stop at id number if it exists. */
        while (NULL != tmp->next && 0 != id) {
            tmp = tmp->next;
            id--;
        }
        if (0 == id) {
            /* the segment is found, looking for data for the target rank. */
            elem = (rank_meta_info*)((uint8_t*)(tmp->seg_info.seg_base_addr) + rel_offset);
            if ( 0 == elem->offset) {
                /* offset can never be 0, it means that there is no data for this rank yet. */
                elem = NULL;
            }
        }
    }
    return elem;
}

static int set_rank_meta_info(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_info, rank_meta_info *rinfo)
{
    /* it's claimed that there is still no meta info for this rank stored */
    pmix_dstore_seg_desc_t *tmp;
    size_t num_elems, rel_offset;
    int id, count;
    rank_meta_info *cur_elem;

    if (!ns_info || !rinfo) {
        PMIX_ERROR_LOG(PMIX_ERROR);
        return PMIX_ERROR;
    }

    PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: nspace %s, add rank %lu offset %lu count %lu meta info",
                         __FILE__, __LINE__, __func__,
                         ns_info->ns_map.name, (unsigned long)rinfo->rank,
                         (unsigned long)rinfo->offset, (unsigned long)rinfo->count));

    tmp = ns_info->meta_seg;
    if (1 == ds_ctx->direct_mode) {
        /* get the last meta segment to put new rank_meta_info at the end. */
        while (NULL != tmp->next) {
            tmp = tmp->next;
        }
        num_elems = *((size_t*)(tmp->seg_info.seg_base_addr));
        if (ds_ctx->max_meta_elems <= num_elems) {
            PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                        "%s:%d:%s: extend meta segment for nspace %s",
                        __FILE__, __LINE__, __func__, ns_info->ns_map.name));
            /* extend meta segment, so create a new one */
            tmp = pmix_common_dstor_extend_segment(tmp, ds_ctx->base_path, ns_info->ns_map.name,
                                                   ds_ctx->jobuid, ds_ctx->setjobuid);
            if (NULL == tmp) {
                PMIX_ERROR_LOG(PMIX_ERROR);
                return PMIX_ERROR;
            }
            ns_info->num_meta_seg++;
            memset(tmp->seg_info.seg_base_addr, 0, sizeof(rank_meta_info));
            /* update number of meta segments for namespace in initial_segment */
            ns_seg_info_t *elem = _get_ns_info_from_initial_segment(ds_ctx, &ns_info->ns_map);
            if (NULL == elem) {
                PMIX_ERROR_LOG(PMIX_ERROR);
                return PMIX_ERROR;
            }
            if (ns_info->num_meta_seg != elem->num_meta_seg) {
                elem->num_meta_seg = ns_info->num_meta_seg;
            }
            num_elems = 0;
        }
        cur_elem = (rank_meta_info*)((uint8_t*)(tmp->seg_info.seg_base_addr) + sizeof(size_t) + num_elems * sizeof(rank_meta_info));
        memcpy(cur_elem, rinfo, sizeof(rank_meta_info));
        num_elems++;
        memcpy(tmp->seg_info.seg_base_addr, &num_elems, sizeof(size_t));
    } else {
        /* directly compute index of meta segment (id) and relative offset (rel_offset)
         * inside this segment for fast lookup a rank_meta_info object for the requested rank. */
        size_t rcount = rinfo->rank == PMIX_RANK_WILDCARD ? 0 : rinfo->rank + 1;
        id = rcount/ds_ctx->max_meta_elems;
        rel_offset = (rcount % ds_ctx->max_meta_elems) * sizeof(rank_meta_info) + sizeof(size_t);
        count = id;
        /* go through all existing meta segments for this namespace.
         * Stop at id number if it exists. */
        while (NULL != tmp->next && 0 != count) {
            tmp = tmp->next;
            count--;
        }
        /* if there is no segment with this id, then create all missing segments till the id number. */
        if ((int)ns_info->num_meta_seg < (id+1)) {
            while ((int)ns_info->num_meta_seg != (id+1)) {
                /* extend meta segment, so create a new one */
                tmp = pmix_common_dstor_extend_segment(tmp, ds_ctx->base_path, ns_info->ns_map.name,
                                                       ds_ctx->jobuid, ds_ctx->setjobuid);
                if (NULL == tmp) {
                    PMIX_ERROR_LOG(PMIX_ERROR);
                    return PMIX_ERROR;
                }
                memset(tmp->seg_info.seg_base_addr, 0, sizeof(rank_meta_info));
                ns_info->num_meta_seg++;
            }
            /* update number of meta segments for namespace in initial_segment */
            ns_seg_info_t *elem = _get_ns_info_from_initial_segment(ds_ctx, &ns_info->ns_map);
            if (NULL == elem) {
                PMIX_ERROR_LOG(PMIX_ERROR);
                return PMIX_ERROR;
            }
            if (ns_info->num_meta_seg != elem->num_meta_seg) {
                elem->num_meta_seg = ns_info->num_meta_seg;
            }
        }
        /* store rank_meta_info object by rel_offset. */
        cur_elem = (rank_meta_info*)((uint8_t*)(tmp->seg_info.seg_base_addr) + rel_offset);
        memcpy(cur_elem, rinfo, sizeof(rank_meta_info));
    }
    return PMIX_SUCCESS;
}

static uint8_t *_get_data_region_by_offset(pmix_common_dstore_ctx_t *ds_ctx, pmix_dstore_seg_desc_t *segdesc, size_t offset)
{
    pmix_dstore_seg_desc_t *tmp = segdesc;
    size_t rel_offset = offset;
    uint8_t *dataaddr = NULL;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s",
                         __FILE__, __LINE__, __func__));

    /* go through all existing data segments for this namespace */
    do {
        if (rel_offset >= ds_ctx->data_segment_size) {
            rel_offset -= ds_ctx->data_segment_size;
        } else {
            dataaddr = tmp->seg_info.seg_base_addr + rel_offset;
        }
        tmp = tmp->next;
    } while (NULL != tmp && NULL == dataaddr);

    return dataaddr;
}

static size_t get_free_offset(pmix_common_dstore_ctx_t *ds_ctx, pmix_dstore_seg_desc_t *data_seg)
{
    size_t offset;
    pmix_dstore_seg_desc_t *tmp;
    int id = 0;
    tmp = data_seg;
    /* first find the last data segment */
    while (NULL != tmp->next) {
        tmp = tmp->next;
        id++;
    }
    offset = *((size_t*)(tmp->seg_info.seg_base_addr));
    if (0 == offset) {
        /* this is the first created data segment, the first 8 bytes are used to place the free offset value itself */
        offset = sizeof(size_t);
    }
    return (id * ds_ctx->data_segment_size + offset);
}

static int put_empty_ext_slot(pmix_common_dstore_ctx_t *ds_ctx, pmix_dstore_seg_desc_t *dataseg)
{
    size_t global_offset, rel_offset, data_ended, val = 0;
    uint8_t *addr;
    pmix_status_t rc;

    global_offset = get_free_offset(ds_ctx, dataseg);
    rel_offset = global_offset % ds_ctx->data_segment_size;
    if (rel_offset + PMIX_DS_SLOT_SIZE(ds_ctx) > ds_ctx->data_segment_size) {
        PMIX_ERROR_LOG(PMIX_ERROR);
        return PMIX_ERROR;
    }
    addr = _get_data_region_by_offset(ds_ctx, dataseg, global_offset);
    PMIX_DS_PUT_KEY(rc, ds_ctx, addr, ESH_REGION_EXTENSION, (void*)&val, sizeof(size_t));
    if (rc != PMIX_SUCCESS) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    /* update offset at the beginning of current segment */
    data_ended = rel_offset + PMIX_DS_SLOT_SIZE(ds_ctx);
    addr = (uint8_t*)(addr - rel_offset);
    memcpy(addr, &data_ended, sizeof(size_t));
    return PMIX_SUCCESS;
}

static size_t put_data_to_the_end(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_info,
                                  pmix_dstore_seg_desc_t *dataseg, char *key, void *buffer, size_t size)
{
    size_t offset, id = 0;
    pmix_dstore_seg_desc_t *tmp;
    size_t global_offset, data_ended;
    uint8_t *addr;
    pmix_status_t rc;

    PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: key %s",
                         __FILE__, __LINE__, __func__, key));

    tmp = dataseg;
    while (NULL != tmp->next) {
        tmp = tmp->next;
        id++;
    }
    global_offset = get_free_offset(ds_ctx, dataseg);
    offset = global_offset % ds_ctx->data_segment_size;

    /* We should provide additional space at the end of segment to
     * place EXTENSION_SLOT to have an ability to enlarge data for this rank.*/
    if ((sizeof(size_t) + PMIX_DS_KEY_SIZE(ds_ctx, key, size) + PMIX_DS_SLOT_SIZE(ds_ctx)) >
            ds_ctx->data_segment_size) {
        /* this is an error case: segment is so small that cannot place evem a single key-value pair.
         * warn a user about it and fail. */
        offset = 0; /* offset cannot be 0 in normal case, so we use this value to indicate a problem. */
        pmix_output(0, "PLEASE set NS_DATA_SEG_SIZE to value which is larger when %lu.",
                    (unsigned long)(sizeof(size_t) + strlen(key) + 1 + sizeof(size_t) +
                                    size + PMIX_DS_SLOT_SIZE(ds_ctx)));
        return offset;
    }

    /* check the corner case that was observed at large scales:
     * https://github.com/pmix/master/pull/282#issuecomment-277454198
     *
     * if last time we stopped exactly on the border of the segment
     * new segment wasn't allocated to us but (global_offset % _data_segment_size) == 0
     * so if offset is 0 here - we need to allocate the segment as well
     */
    if ( (0 == offset) || ( (offset + PMIX_DS_KEY_SIZE(ds_ctx, key, size) +
                             PMIX_DS_SLOT_SIZE(ds_ctx)) > ds_ctx->data_segment_size) ) {
        id++;
        /* create a new data segment. */
        tmp = pmix_common_dstor_extend_segment(tmp, ds_ctx->base_path, ns_info->ns_map.name,
                                               ds_ctx->jobuid, ds_ctx->setjobuid);
        if (NULL == tmp) {
            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
            offset = 0; /* offset cannot be 0 in normal case, so we use this value to indicate a problem. */
            return offset;
        }
        ns_info->num_data_seg++;
        /* update_ns_info_in_initial_segment */
        ns_seg_info_t *elem = _get_ns_info_from_initial_segment(ds_ctx, &ns_info->ns_map);
        if (NULL == elem) {
            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
            offset = 0; /* offset cannot be 0 in normal case, so we use this value to indicate a problem. */
            return offset;
        }
        elem->num_data_seg++;
        offset = sizeof(size_t);
    }
    global_offset = offset + id * ds_ctx->data_segment_size;
    addr = (uint8_t*)(tmp->seg_info.seg_base_addr)+offset;
    PMIX_DS_PUT_KEY(rc, ds_ctx, addr, key, buffer, size);
    if (rc != PMIX_SUCCESS) {
        PMIX_ERROR_LOG(rc);
        return 0;
    }

    /* update offset at the beginning of current segment */
    data_ended = offset + PMIX_DS_KEY_SIZE(ds_ctx, key, size);
    addr = (uint8_t*)(tmp->seg_info.seg_base_addr);
    memcpy(addr, &data_ended, sizeof(size_t));
    PMIX_OUTPUT_VERBOSE((1, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: key %s, rel start offset %lu, rel end offset %lu, abs shift %lu size %lu",
                         __FILE__, __LINE__, __func__,
                         key, (unsigned long)offset,
                         (unsigned long)data_ended,
                         (unsigned long)(id * ds_ctx->data_segment_size),
                         (unsigned long)size));
    return global_offset;
}

static int pmix_sm_store(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_info,
                         pmix_rank_t rank, pmix_kval_t *kval, rank_meta_info **rinfo, int data_exist)
{
    size_t offset, size, kval_cnt;
    pmix_buffer_t buffer;
    pmix_status_t rc;
    pmix_dstore_seg_desc_t *datadesc;
    uint8_t *addr;

    PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: for rank %u, replace flag %d",
                         __FILE__, __LINE__, __func__, rank, data_exist));

    datadesc = ns_info->data_seg;
    /* pack value to the buffer */
    PMIX_CONSTRUCT(&buffer, pmix_buffer_t);
    PMIX_BFROPS_PACK(rc, _client_peer(ds_ctx), &buffer, kval->value, 1, PMIX_VALUE);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }
    size = buffer.bytes_used;

    if (0 == data_exist) {
        /* there is no data blob for this rank yet, so add it. */
        size_t free_offset;
        free_offset = get_free_offset(ds_ctx, datadesc);
        offset = put_data_to_the_end(ds_ctx, ns_info, datadesc, kval->key, buffer.base_ptr, size);
        if (0 == offset) {
            /* this is an error */
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
        /* if it's the first time when we put data for this rank, then *rinfo == NULL,
         * and even if segment was extended, and data was put into the next segment,
         * we don't need to extension slot at the end of previous segment.
         * If we try, we might overwrite other segments memory,
         * because previous segment is already full. */
        if (free_offset != offset && NULL != *rinfo) {
            /* here we compare previous free offset with the offset where we just put data.
             * It should be equal in the normal case. If it's not true, then it means that
             * segment was extended, and we put data to the next segment, so we now need to
             * put extension slot at the end of previous segment with a "reference" to a new_offset */
            addr = _get_data_region_by_offset(ds_ctx, datadesc, free_offset);
            PMIX_DS_PUT_KEY(rc, ds_ctx, addr, ESH_REGION_EXTENSION, (void*)&offset, sizeof(size_t));
            if (rc != PMIX_SUCCESS) {
                PMIX_ERROR_LOG(rc);
                return 0;
            }
        }
        if (NULL == *rinfo) {
            *rinfo = (rank_meta_info*)malloc(sizeof(rank_meta_info));
            (*rinfo)->rank = rank;
            (*rinfo)->offset = offset;
            (*rinfo)->count = 0;
        }
        (*rinfo)->count++;
    } else if (NULL != *rinfo) {
        /* there is data blob for this rank */
        addr = _get_data_region_by_offset(ds_ctx, datadesc, (*rinfo)->offset);
        if (NULL == addr) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
        /* go through previous data region and find key matches.
         * If one is found, then mark this kval as invalidated.
         * Then put a new empty offset to the next extension slot,
         * and add new kval by this offset.
         * no need to update meta info, it's still the same. */
        kval_cnt = (*rinfo)->count;
        int add_to_the_end = 1;
        while (0 < kval_cnt) {
            /* data is stored in the following format:
             * size_t size
             * key[ESH_KNAME_LEN(addr)]
             * byte buffer containing pmix_value, should be loaded to pmix_buffer_t and unpacked.
             * next kval pair
             * .....
             * extension slot which has key = EXTENSION_SLOT and a size_t value for offset to next data address for this process.
             */
            if(PMIX_DS_KEY_IS_EXTSLOT(ds_ctx, addr)) {
                memcpy(&offset, PMIX_DS_DATA_PTR(ds_ctx, addr), sizeof(size_t));
                if (0 < offset) {
                    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                                "%s:%d:%s: for rank %lu, replace flag %d %s is filled with %lu value",
                                __FILE__, __LINE__, __func__,
                                (unsigned long)rank, data_exist,
                                ESH_REGION_EXTENSION, (unsigned long)offset));
                    /* go to next item, updating address */
                    addr = _get_data_region_by_offset(ds_ctx, datadesc, offset);
                    if (NULL == addr) {
                        rc = PMIX_ERROR;
                        PMIX_ERROR_LOG(rc);
                        goto exit;
                    }
                } else {
                    /* should not be, we should be out of cycle when this happens */
                }
            } else if (0 == strncmp(PMIX_DS_KNAME_PTR(ds_ctx, addr), kval->key,
                                    PMIX_DS_KNAME_LEN(ds_ctx, kval->key))) {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %u, replace flag %d found target key %s",
                            __FILE__, __LINE__, __func__, rank, data_exist, kval->key));
                /* target key is found, compare value sizes */
                if (PMIX_DS_DATA_SIZE(ds_ctx, addr, PMIX_DS_DATA_PTR(ds_ctx, addr)) != size) {
                //if (1) { /* if we want to test replacing values for existing keys. */
                    /* invalidate current value and store another one at the end of data region. */
                    PMIX_DS_KEY_SET_INVALID(ds_ctx, addr);
                    /* decrementing count, it will be incremented back when we add a new value for this key at the end of region. */
                    (*rinfo)->count--;
                    kval_cnt--;
                    /* go to next item, updating address */
                    addr += PMIX_DS_KV_SIZE(ds_ctx, addr);
                    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                                "%s:%d:%s: for rank %u, replace flag %d mark key %s regions as invalidated. put new data at the end.",
                                __FILE__, __LINE__, __func__, rank, data_exist, kval->key));
                } else {
                    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                                "%s:%d:%s: for rank %u, replace flag %d replace data for key %s type %d in place",
                                __FILE__, __LINE__, __func__, rank, data_exist, kval->key, kval->value->type));
                    /* replace old data with new one. */
                    memset(PMIX_DS_DATA_PTR(ds_ctx, addr), 0,
                           PMIX_DS_DATA_SIZE(ds_ctx, addr, PMIX_DS_DATA_PTR(ds_ctx, addr)));
                    memcpy(PMIX_DS_DATA_PTR(ds_ctx, addr), buffer.base_ptr, size);
                    addr += PMIX_DS_KV_SIZE(ds_ctx, addr);
                    add_to_the_end = 0;
                    break;
                }
            } else {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %u, replace flag %d skip %s key, look for %s key",
                            __FILE__, __LINE__, __func__, rank, data_exist,
                            PMIX_DS_KNAME_PTR(ds_ctx, addr), kval->key));
                /* Skip it: key is "INVALIDATED" or key is valid but different from target one. */
                if (!PMIX_DS_KEY_IS_INVALID(ds_ctx, addr)) {
                    /* count only valid items */
                    kval_cnt--;
                }
                /* go to next item, updating address */
                addr += PMIX_DS_KV_SIZE(ds_ctx, addr);
            }
        }
        if (1 == add_to_the_end) {
            /* if we get here, it means that we want to add a new item for the target rank, or
             * we mark existing item with the same key as "invalidated" and want to add new item
             * for the same key. */
            size_t free_offset;
            (*rinfo)->count++;
            free_offset = get_free_offset(ds_ctx, datadesc);

            /*
             * Remove trailing extention slot if we are continuing
             * same ranks data.
             *
             * When keys are stored individually through _store_data_for_rank
             * an empty extention slot is placed every time.
             *
             * This is required because there is no information about whether or not the next key
             * will belong to the same rank.
             *
             * As the result EACH keys stored with _store_data_for_rank is
             * followed by extension slot. This slows down search and increases
             * the memory footprint.
             *
             * The following code tries to deal with such one-key-at-a-time
             * situation by:
             *  - checking if the last key-value for this rank is an extention
             *    slot
             *  - If this is the case - checks if this key-value pair is the
             *    last one at the moment and can be safely deleted.
             *  - if it is - current segment's offset pointer is decreased by
             *    the size of the extention slot key-value effectively removing
             *    it from the dstor
             */
            if (PMIX_DS_KEY_IS_EXTSLOT(ds_ctx, addr)){
                /* Find the last data segment */
                pmix_dstore_seg_desc_t *ldesc = datadesc;
                uint8_t *segstart;
                size_t offs_past_extslot = 0;
                size_t offs_cur_segment = 0;
                while (NULL != ldesc->next) {
                    ldesc = ldesc->next;
                }

                /* Calculate the offset of the end of the extension slot */
                offs_cur_segment = free_offset % ds_ctx->data_segment_size;
                segstart = ldesc->seg_info.seg_base_addr;
                offs_past_extslot = (addr + PMIX_DS_KV_SIZE(ds_ctx, addr)) - segstart;

                /* We can erase extension slot if:
                 *  - address of the ext slot belongs to the occupied part of the
                 *    last segment
                 *  - local offset within the segment is equal to the local
                 *    offset of the end of extension slot
                 */
                if( ( (addr > segstart) && (addr < (segstart + offs_cur_segment)) )
                     && (offs_cur_segment == offs_past_extslot) ) {
                    /* Calculate a new free offset that doesn't account this
                     * extension slot */
                    size_t new_offset = addr - segstart;
                    /* Rewrite segment's offset information to exclude
                     * extension slot */
                    memcpy(segstart, &new_offset, sizeof(size_t));
                    /* Recalculate free_offset */
                    free_offset = get_free_offset(ds_ctx, datadesc);
                }
            }

            /* add to the end */
            offset = put_data_to_the_end(ds_ctx, ns_info, datadesc, kval->key, buffer.base_ptr, size);
            if (0 == offset) {
                rc = PMIX_ERROR;
                PMIX_ERROR_LOG(rc);
                goto exit;
            }
            /* we just reached the end of data for the target rank, and there can be two cases:
             * (1) - we are in the middle of data segment; data for this rank is separated from
             * data for different ranks, and that's why next element is EXTENSION_SLOT.
             * We put new data to the end of data region and just update EXTENSION_SLOT value by new offset.
             */
            if (PMIX_DS_KEY_IS_EXTSLOT(ds_ctx, addr)) {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %u, replace flag %d %s should be filled with offset %lu value",
                            __FILE__, __LINE__, __func__, rank, data_exist, ESH_REGION_EXTENSION, offset));
                memcpy(PMIX_DS_DATA_PTR(ds_ctx, addr), &offset, sizeof(size_t));
            } else {
                /* (2) - we point to the first free offset, no more data is stored further in this segment.
                 * There is no EXTENSION_SLOT by this addr since we continue pushing data for the same rank,
                 * and there is no need to split it.
                 * But it's possible that we reached the end of current data region and just jumped to the new region
                 * to put new data, in that case free_offset != offset and we must put EXTENSION_SLOT by the current addr
                 * forcibly and store new offset in its value. */
                if (free_offset != offset) {
                    /* segment was extended, need to put extension slot by free_offset indicating new_offset */
                    PMIX_DS_PUT_KEY(rc, ds_ctx, addr, ESH_REGION_EXTENSION, (void*)&offset, sizeof(size_t));
                    if (rc != PMIX_SUCCESS) {
                        PMIX_ERROR_LOG(rc);
                        return 0;
                    }
                }
            }
            PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                        "%s:%d:%s: for rank %u, replace flag %d item not found ext slot empty, put key %s to the end",
                        __FILE__, __LINE__, __func__, rank, data_exist, kval->key));
        }
    }
exit:
    PMIX_DESTRUCT(&buffer);
    return rc;
}

static int _store_data_for_rank(pmix_common_dstore_ctx_t *ds_ctx, ns_track_elem_t *ns_info,
                                pmix_rank_t rank, pmix_buffer_t *buf)
{
    pmix_status_t rc;

    pmix_kval_t *kp;
    pmix_dstore_seg_desc_t *metadesc, *datadesc;
    int32_t cnt;

    rank_meta_info *rinfo = NULL;
    size_t num_elems, free_offset, new_free_offset;
    int data_exist;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: for rank %u", __FILE__, __LINE__, __func__, rank));

    metadesc = ns_info->meta_seg;
    datadesc = ns_info->data_seg;

    if (NULL == datadesc || NULL == metadesc) {
        rc = PMIX_ERR_BAD_PARAM;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    num_elems = *((size_t*)(metadesc->seg_info.seg_base_addr));
    data_exist = 0;
    /* when we don't use linear search (direct_mode == 0) we don't use num_elems field,
     * so anyway try to get rank_meta_info first. */
    if (0 < num_elems || 0 == ds_ctx->direct_mode) {
        /* go through all elements in meta segment and look for target rank. */
        rinfo = _get_rank_meta_info(ds_ctx, rank, metadesc);
        if (NULL != rinfo) {
            data_exist = 1;
        }
    }
    /* incoming buffer may contain several inner buffers for different scopes,
     * so unpack these buffers, and then unpack kvals from each modex buffer,
     * storing them in the shared memory dstore.
     */
    free_offset = get_free_offset(ds_ctx, datadesc);
    cnt = 1;
    kp = PMIX_NEW(pmix_kval_t);
    PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, buf, kp, &cnt, PMIX_KVAL);
    while(PMIX_SUCCESS == rc) {
        pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                            "pmix: unpacked key %s", kp->key);
        if (PMIX_SUCCESS != (rc = pmix_sm_store(ds_ctx, ns_info, rank, kp, &rinfo, data_exist))) {
            PMIX_ERROR_LOG(rc);
            if (NULL != rinfo) {
                free(rinfo);
            }
            return rc;
        }
        PMIX_RELEASE(kp); // maintain acctg - hash_store does a retain
        cnt = 1;
        kp = PMIX_NEW(pmix_kval_t);
        PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, buf, kp, &cnt, PMIX_KVAL);
    }

    PMIX_RELEASE(kp);

    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
        /* TODO: should we error-exit here? */
    } else {
        rc = PMIX_SUCCESS;
    }

    /* Check if new data was put at the end of data segment.
     * It's possible that old data just was replaced with new one,
     * in that case we don't reserve space for EXTENSION_SLOT, it's
     * already reserved.
     * */
    new_free_offset = get_free_offset(ds_ctx, datadesc);
    if (new_free_offset != free_offset) {
        /* Reserve space for EXTENSION_SLOT at the end of data blob.
         * We need it to split data for one rank from data for different
         * ranks and to allow extending data further.
         * We also put EXTENSION_SLOT at the end of each data segment, and
         * its value points to the beginning of next data segment.
         * */
        rc = put_empty_ext_slot(ds_ctx, ns_info->data_seg);
        if (PMIX_SUCCESS != rc) {
            if ((0 == data_exist) && NULL != rinfo) {
                free(rinfo);
            }
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }

    /* if this is the first data posted for this rank, then
     * update meta info for it */
    if (0 == data_exist) {
        set_rank_meta_info(ds_ctx, ns_info, rinfo);
        if (NULL != rinfo) {
            free(rinfo);
        }
    }

    return rc;
}

static inline ssize_t _get_univ_size(pmix_common_dstore_ctx_t *ds_ctx, const char *nspace)
{
    ssize_t nprocs = 0;
    pmix_value_t *val;
    int rc;

    rc = _dstore_fetch(ds_ctx, nspace, PMIX_RANK_WILDCARD, PMIX_UNIV_SIZE, &val);
    if( PMIX_SUCCESS != rc ) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    if( val->type != PMIX_UINT32 ){
        rc = PMIX_ERR_BAD_PARAM;
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    nprocs = (ssize_t)val->data.uint32;
    PMIX_VALUE_RELEASE(val);
    return nprocs;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_cache_job_info(pmix_common_dstore_ctx_t *ds_ctx,
                                struct pmix_namespace_t *ns,
                                pmix_info_t info[], size_t ninfo)
{
    return PMIX_SUCCESS;
}


pmix_common_dstore_ctx_t *pmix_common_dstor_init(const char *ds_name, pmix_info_t info[], size_t ninfo,
                                                 pmix_common_lock_callbacks_t *lock_cb,
                                                 pmix_common_dstore_file_cbs_t *file_cb)
{
    pmix_status_t rc;
    size_t n;
    char *dstor_tmpdir = NULL;
    size_t tbl_idx = 0;
    ns_map_data_t *ns_map = NULL;
    pmix_common_dstore_ctx_t *ds_ctx = NULL;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "pmix:gds:dstore init");

    ds_ctx = (pmix_common_dstore_ctx_t*) malloc(sizeof(*ds_ctx));
    if (NULL == ds_ctx) {
        PMIX_ERROR_LOG(PMIX_ERR_OUT_OF_RESOURCE);
        return NULL;
    }
    memset(ds_ctx, 0, sizeof(*ds_ctx));

    /* assign lock callbacks */
    ds_ctx->lock_cbs = lock_cb;
    ds_ctx->file_cbs = file_cb;

    /* open the pshmem and select the active plugins */
    if( PMIX_SUCCESS != (rc = pmix_mca_base_framework_open(&pmix_pshmem_base_framework, 0)) ) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    if( PMIX_SUCCESS != (rc = pmix_pshmem_base_select()) ) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    ds_ctx->jobuid = getuid();
    ds_ctx->setjobuid = 0;

    if (PMIX_SUCCESS != (rc = _esh_tbls_init(ds_ctx))) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    rc = pmix_pshmem.init();
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    _set_constants_from_env(ds_ctx);
    ds_ctx->ds_name = strdup(ds_name);

    /* find the temp dir */
    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        ds_ctx->session_map_search = (session_map_search_fn_t)_esh_session_map_search_server;

        /* scan incoming info for directives */
        if (NULL != info) {
            for (n=0; n < ninfo; n++) {
                if (0 == strcmp(PMIX_USERID, info[n].key)) {
                    ds_ctx->jobuid = info[n].value.data.uint32;
                    ds_ctx->setjobuid = 1;
                    continue;
                }
                if (0 == strcmp(PMIX_DSTPATH, info[n].key)) {
                    /* PMIX_DSTPATH is the way for RM to customize the
                     * place where shared memory files are placed.
                     * We need this for the following reasons:
                     * - disk usage: files can be relatively large and the system may
                     *   have a small common temp directory.
                     * - performance: system may have a fast IO device (i.e. burst buffer)
                     *   for the local usage.
                     *
                     * PMIX_DSTPATH has higher priority than PMIX_SERVER_TMPDIR
                     */
                    if( PMIX_STRING != info[n].value.type ){
                        rc = PMIX_ERR_BAD_PARAM;
                        PMIX_ERROR_LOG(rc);
                        goto err_exit;
                    }
                    dstor_tmpdir = (char*)info[n].value.data.string;
                    continue;
                }
                if (0 == strcmp(PMIX_SERVER_TMPDIR, info[n].key)) {
                    if( PMIX_STRING != info[n].value.type ){
                        rc = PMIX_ERR_BAD_PARAM;
                        PMIX_ERROR_LOG(rc);
                        goto err_exit;
                    }
                    if (NULL == dstor_tmpdir) {
                        dstor_tmpdir = (char*)info[n].value.data.string;
                    }
                    continue;
                }
            }
        }

        if (NULL == dstor_tmpdir) {
            if (NULL == (dstor_tmpdir = getenv("TMPDIR"))) {
                if (NULL == (dstor_tmpdir = getenv("TEMP"))) {
                    if (NULL == (dstor_tmpdir = getenv("TMP"))) {
                        dstor_tmpdir = "/tmp";
                    }
                }
            }
        }

        rc = asprintf(&ds_ctx->base_path, "%s/pmix_dstor_%s_%d", dstor_tmpdir,
                      ds_ctx->ds_name, getpid());
        if ((0 > rc) || (NULL == ds_ctx->base_path)) {
            rc = PMIX_ERR_OUT_OF_RESOURCE;
            PMIX_ERROR_LOG(rc);
            goto err_exit;
        }

        if (0 != mkdir(ds_ctx->base_path, 0770)) {
            if (EEXIST != errno) {
                rc = PMIX_ERROR;
                PMIX_ERROR_LOG(rc);
                goto err_exit;
            }
        }
        if (ds_ctx->setjobuid > 0) {
            if (chown(ds_ctx->base_path, (uid_t) ds_ctx->jobuid, (gid_t) -1) < 0){
                rc = PMIX_ERR_NO_PERMISSIONS;
                PMIX_ERROR_LOG(rc);
                goto err_exit;
            }
        }
        ds_ctx->session_map_search = _esh_session_map_search_server;
        return ds_ctx;
    }
    /* for clients */
    else {
        char *env_name = NULL;
        int ds_ver = 0;

        sscanf(ds_ctx->ds_name, "ds%d", &ds_ver);
        if (0 == ds_ver) {
            rc = PMIX_ERR_INIT;
            PMIX_ERROR_LOG(rc);
            goto err_exit;
        }
        if (0 > asprintf(&env_name, PMIX_DSTORE_VER_BASE_PATH_FMT, ds_ver)) {
             rc = PMIX_ERR_NOMEM;
             PMIX_ERROR_LOG(rc);
             goto err_exit;
        }
        dstor_tmpdir = getenv(env_name);
        free(env_name);

        if (NULL == dstor_tmpdir) {
            dstor_tmpdir = getenv(PMIX_DSTORE_ESH_BASE_PATH);
        }
        if (NULL == dstor_tmpdir){
            rc = PMIX_ERR_NOT_AVAILABLE; // simply disqualify ourselves
            goto err_exit;
        }
        if (NULL == (ds_ctx->base_path = strdup(dstor_tmpdir))) {
            rc = PMIX_ERR_OUT_OF_RESOURCE;
            PMIX_ERROR_LOG(rc);
            goto err_exit;
        }
        ds_ctx->session_map_search = _esh_session_map_search_client;
        /* init ds_ctx protect lock */
        if (0 != pthread_mutex_init(&ds_ctx->lock, NULL)) {
            rc = PMIX_ERR_INIT;
            PMIX_ERROR_LOG(rc);
            goto err_exit;
        }
    }

    rc = _esh_session_tbl_add(ds_ctx, &tbl_idx);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    char *nspace = NULL;
    /* if we don't see the required info, then we cannot init */
    if (NULL == (nspace = getenv("PMIX_NAMESPACE"))) {
        rc = PMIX_ERR_INVALID_NAMESPACE;
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }
    /* lock init */
    rc = ds_ctx->lock_cbs->init(&_ESH_SESSION_lock(ds_ctx->session_array, tbl_idx), ds_ctx->base_path, nspace, 1, ds_ctx->jobuid, ds_ctx->setjobuid);
    if (rc != PMIX_SUCCESS) {
        goto err_exit;
    }
    ns_map = _esh_session_map(ds_ctx, nspace, 0, tbl_idx);
    if (NULL == ns_map) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    if (PMIX_SUCCESS != (rc =_esh_session_init(ds_ctx, tbl_idx, ns_map, 1,
                                               ds_ctx->jobuid, ds_ctx->setjobuid))) {
        PMIX_ERROR_LOG(rc);
        goto err_exit;
    }

    return ds_ctx;
err_exit:
    pmix_common_dstor_finalize(ds_ctx);
    return NULL;
}

PMIX_EXPORT void pmix_common_dstor_finalize(pmix_common_dstore_ctx_t *ds_ctx)
{
    struct stat st = {0};
    pmix_status_t rc = PMIX_SUCCESS;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s", __FILE__, __LINE__, __func__));

    _esh_sessions_cleanup(ds_ctx);
    _esh_ns_map_cleanup(ds_ctx);
    _esh_ns_track_cleanup(ds_ctx);

    pmix_pshmem.finalize();

    if (NULL != ds_ctx->base_path){
        if(PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
            if (lstat(ds_ctx->base_path, &st) >= 0){
                if (PMIX_SUCCESS != (rc = _esh_dir_del(ds_ctx->base_path))) {
                    PMIX_ERROR_LOG(rc);
                }
            }
        }
        free(ds_ctx->base_path);
        ds_ctx->base_path = NULL;
    }
    if (NULL != ds_ctx->clients_peer) {
        PMIX_RELEASE(ds_ctx->clients_peer->nptr);
        PMIX_RELEASE(ds_ctx->clients_peer);
    }
    /* close the pshmem framework */
    if( PMIX_SUCCESS != (rc = pmix_mca_base_framework_close(&pmix_pshmem_base_framework)) ) {
        PMIX_ERROR_LOG(rc);
    }
    free(ds_ctx->ds_name);
    free(ds_ctx->base_path);
    free(ds_ctx);
}

static pmix_status_t _dstore_store_nolock(pmix_common_dstore_ctx_t *ds_ctx,
                                   ns_map_data_t *ns_map,
                                   pmix_rank_t rank,
                                   pmix_kval_t *kv)
{
    pmix_status_t rc = PMIX_SUCCESS;
    ns_track_elem_t *elem;
    pmix_buffer_t xfer;
    ns_seg_info_t ns_info;

    if (NULL == kv) {
        return PMIX_ERROR;
    }

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: for %s:%u",
                         __FILE__, __LINE__, __func__, ns_map->name, rank));

    /* First of all, we go through local track list (list of ns_track_elem_t structures)
     * and look for an element for the target namespace.
     * If it is there, then shared memory segments for it are created, so we take it.
     * Otherwise, create a new element, fill its fields, create corresponding meta
     * and data segments for this namespace, add it to the local track list,
     * and put this info (ns_seg_info_t) to the initial segment. If initial segment
     * if full, then extend it by creating a new one and mark previous one as full.
     * All this stuff is done inside _get_track_elem_for_namespace function.
     */

    elem = _get_track_elem_for_namespace(ds_ctx, ns_map);
    if (NULL == elem) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* If a new element was just created, we need to create corresponding meta and
     * data segments and update corresponding element's fields. */
    if (NULL == elem->meta_seg || NULL == elem->data_seg) {
        memset(&ns_info.ns_map, 0, sizeof(ns_info.ns_map));
        pmix_strncpy(ns_info.ns_map.name, ns_map->name, sizeof(ns_info.ns_map.name)-1);
        ns_info.ns_map.tbl_idx = ns_map->tbl_idx;
        ns_info.num_meta_seg = 1;
        ns_info.num_data_seg = 1;
        rc = _update_ns_elem(ds_ctx, elem, &ns_info);
        if (PMIX_SUCCESS != rc || NULL == elem->meta_seg || NULL == elem->data_seg) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }

        /* zero created shared memory segments for this namespace */
        memset(elem->meta_seg->seg_info.seg_base_addr, 0, ds_ctx->meta_segment_size);
        memset(elem->data_seg->seg_info.seg_base_addr, 0, ds_ctx->data_segment_size);

        /* put ns's shared segments info to the global meta segment. */
        rc = _put_ns_info_to_initial_segment(ds_ctx, ns_map, &elem->meta_seg->seg_info, &elem->data_seg->seg_info);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto exit;
        }
    }

    /* Now we know info about meta segment for this namespace. If meta segment
     * is not empty, then we look for data for the target rank. If they present, replace it. */
    PMIX_CONSTRUCT(&xfer, pmix_buffer_t);
    PMIX_LOAD_BUFFER(pmix_globals.mypeer, &xfer, kv->value->data.bo.bytes, kv->value->data.bo.size);

    rc = _store_data_for_rank(ds_ctx, elem, rank, &xfer);

    PMIX_DESTRUCT(&xfer);

    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

exit:
    return rc;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_store(pmix_common_dstore_ctx_t *ds_ctx,
                                const pmix_proc_t *proc,
                                pmix_scope_t scope,
                                pmix_kval_t *kv)
{
    pmix_status_t rc = PMIX_SUCCESS;
    ns_map_data_t *ns_map;
    pmix_kval_t *kv2;
    pmix_buffer_t tmp;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds: dstore store for key '%s' scope %d",
                        proc->nspace, proc->rank, kv->key, scope);

    if (PMIX_PROC_IS_CLIENT(pmix_globals.mypeer)) {
        rc = PMIX_ERR_NOT_SUPPORTED;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    kv2 = PMIX_NEW(pmix_kval_t);
    PMIX_VALUE_CREATE(kv2->value, 1);
    kv2->value->type = PMIX_BYTE_OBJECT;

    PMIX_CONSTRUCT(&tmp, pmix_buffer_t);

    PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &tmp, kv, 1, PMIX_KVAL);
    PMIX_UNLOAD_BUFFER(&tmp, kv2->value->data.bo.bytes, kv2->value->data.bo.size);

    if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, proc->nspace))) {
        rc = PMIX_ERROR;
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* set exclusive lock */
    rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, wr_lock);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    rc = _dstore_store_nolock(ds_ctx, ns_map, proc->rank, kv2);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

    /* unset lock */
    rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, wr_unlock);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

exit:
    PMIX_RELEASE(kv2);
    PMIX_DESTRUCT(&tmp);

    return rc;
}

static pmix_status_t _dstore_fetch(pmix_common_dstore_ctx_t *ds_ctx,
                                   const char *nspace, pmix_rank_t rank,
                                   const char *key, pmix_value_t **kvs)
{
    ns_seg_info_t *ns_info = NULL;
    pmix_status_t rc = PMIX_ERROR, lock_rc;
    ns_track_elem_t *elem;
    rank_meta_info *rinfo = NULL;
    size_t kval_cnt = 0;
    pmix_dstore_seg_desc_t *meta_seg, *data_seg;
    uint8_t *addr;
    pmix_buffer_t buffer;
    pmix_value_t val, *kval = NULL;
    uint32_t nprocs;
    pmix_rank_t cur_rank;
    ns_map_data_t *ns_map = NULL;
    bool all_ranks_found = true;
    bool key_found = false;
    pmix_info_t *info = NULL;
    size_t ninfo;
    size_t keyhash = 0;
    bool lock_is_set = false;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: for %s:%u look for key %s",
                         __FILE__, __LINE__, __func__, nspace, rank, key));

    if ((PMIX_RANK_UNDEF == rank) && (NULL == key)) {
        PMIX_OUTPUT_VERBOSE((7, pmix_gds_base_framework.framework_output,
                             "dstore: Does not support passed parameters"));
        rc = PMIX_ERR_BAD_PARAM;
        goto error;
    }

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: for %s:%u look for key %s",
                         __FILE__, __LINE__, __func__, nspace, rank, key));

    /* protect info of dstore segments before it will be updated */
    if (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        if (0 != (rc = pthread_mutex_lock(&ds_ctx->lock))) {
            goto error;
        }
        lock_is_set = true;
    }

    if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, nspace))) {
        /* This call is issued from the the client.
         * client must have the session, otherwise the error is fatal.
         */
        rc = PMIX_ERR_FATAL;
        goto error;
    }

    if (NULL == kvs) {
        rc = PMIX_ERR_FATAL;
        goto error;
    }

    if (PMIX_RANK_UNDEF == rank) {
        ssize_t _nprocs = _get_univ_size(ds_ctx, ns_map->name);
        if( 0 > _nprocs ){
            goto error;
        }
        nprocs = (size_t) _nprocs;
        cur_rank = 0;
    } else {
        nprocs = 1;
        cur_rank = rank;
    }

    /* grab shared lock */
    lock_rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, rd_lock);
    if (PMIX_SUCCESS != lock_rc) {
        /* Something wrong with the lock. The error is fatal */
        rc = lock_rc;
        goto error;
    }

    /* First of all, we go through all initial segments and look at their field.
     * If it's 1, then generate name of next initial segment incrementing id by one and attach to it.
     * We need this step to synchronize initial shared segments with our local track list.
     * Then we look for the target namespace in all initial segments.
     * If it is found, we get numbers of meta & data segments and
     * compare these numbers with the number of trackable meta & data
     * segments for this namespace in the local track list.
     * If the first number exceeds the last, or the local track list
     * doesn't track current namespace yet, then we update it (attach
     * to additional segments).
     */

    /* first update local information about initial segments. they can be extended, so then we need to attach to new segments. */
    _update_initial_segment_info(ds_ctx, ns_map);

    ns_info = _get_ns_info_from_initial_segment(ds_ctx, ns_map);
    if (NULL == ns_info) {
        /* no data for this namespace is found in the shared memory. */
        PMIX_OUTPUT_VERBOSE((7, pmix_gds_base_framework.framework_output,
                    "%s:%d:%s:  no data for ns %s is found in the shared memory.",
                    __FILE__, __LINE__, __func__, ns_map->name));
        rc = PMIX_ERR_PROC_ENTRY_NOT_FOUND;
        goto done;
    }

    /* get ns_track_elem_t object for the target namespace from the local track list. */
    elem = _get_track_elem_for_namespace(ds_ctx, ns_map);
    if (NULL == elem) {
        /* Shouldn't happen! */
        rc = PMIX_ERR_FATAL;
        PMIX_ERROR_LOG(rc);
        goto done;
    }

    /* need to update tracker:
     * attach to shared memory regions for this namespace and store its info locally
     * to operate with address and detach/unlink afterwards. */
    rc = _update_ns_elem(ds_ctx, elem, ns_info);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        goto done;
    }

    /* Now we have the data from meta segment for this namespace. */
    meta_seg = elem->meta_seg;
    data_seg = elem->data_seg;

    if( NULL != key ) {
        keyhash = PMIX_DS_KEY_HASH(ds_ctx, key);
    }

    /* all segment data updated, ctx lock may released */
    if (lock_is_set) {
        lock_is_set = false;
        if (0 != (rc = pthread_mutex_unlock(&ds_ctx->lock))) {
            goto error;
        }
    }

    while (nprocs--) {
        /* Get the rank meta info in the shared meta segment. */
        rinfo = _get_rank_meta_info(ds_ctx, cur_rank, meta_seg);
        if (NULL == rinfo) {
            PMIX_OUTPUT_VERBOSE((7, pmix_gds_base_framework.framework_output,
                        "%s:%d:%s:  no data for this rank is found in the shared memory. rank %u",
                        __FILE__, __LINE__, __func__, cur_rank));
            all_ranks_found = false;
            continue;
        }
        addr = _get_data_region_by_offset(ds_ctx, data_seg, rinfo->offset);
        if (NULL == addr) {
            /* This means that meta-info is broken - error is fatal */
            rc = PMIX_ERR_FATAL;
            PMIX_ERROR_LOG(rc);
            goto done;
        }
        kval_cnt = rinfo->count;

        /*  Initialize array for all keys of rank */
        if ((NULL == key) && (kval_cnt > 0)) {
            kval = (pmix_value_t*)malloc(sizeof(pmix_value_t));
            if (NULL == kval) {
                rc = PMIX_ERR_NOMEM;
                goto done;
            }
            PMIX_VALUE_CONSTRUCT(kval);

            ninfo = kval_cnt;
            PMIX_INFO_CREATE(info, ninfo);
            if (NULL == info) {
                rc = PMIX_ERR_NOMEM;
                goto done;
            }

            kval->type = PMIX_DATA_ARRAY;
            kval->data.darray = (pmix_data_array_t*)malloc(sizeof(pmix_data_array_t));
            if (NULL == kval->data.darray) {
                rc = PMIX_ERR_NOMEM;
                goto done;
            }
            kval->data.darray->type = PMIX_INFO;
            kval->data.darray->size = ninfo;
            kval->data.darray->array = info;
            *kvs = kval;
        }

        rc = PMIX_SUCCESS;
        while (0 < kval_cnt) {
            /* data is stored in the following format:
             * key_val_pair {
             *     size_t size;
             *     char key[KNAME_LEN(addr)];
             *     byte_t byte[size]; // should be loaded to pmix_buffer_t and unpacked.
             * };
             * segment_format {
             *     key_val_pair kv_array[n];
             *     EXTENSION slot;
             * }
             * EXTENSION slot which has key = EXTENSION_SLOT and a size_t value for offset
             * to next data address for this process.
             */
            if (PMIX_DS_KEY_IS_INVALID(ds_ctx, addr)) {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %s:%u, skip %s region",
                            __FILE__, __LINE__, __func__, nspace, cur_rank, ESH_REGION_INVALIDATED));
                /* skip it
                 * go to next item, updating address */
                addr += PMIX_DS_KV_SIZE(ds_ctx, addr);
            } else if (PMIX_DS_KEY_IS_EXTSLOT(ds_ctx, addr)) {
                size_t offset;
                memcpy(&offset, PMIX_DS_DATA_PTR(ds_ctx, addr), sizeof(size_t));
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %s:%u, reached %s with %lu value",
                            __FILE__, __LINE__, __func__, nspace, cur_rank, ESH_REGION_EXTENSION, offset));
                if (0 < offset) {
                    /* go to next item, updating address */
                    addr = _get_data_region_by_offset(ds_ctx, data_seg, offset);
                    if (NULL == addr) {
                        /* This shouldn't happen - error is fatal */
                        rc = PMIX_ERR_FATAL;
                        PMIX_ERROR_LOG(rc);
                        goto done;
                    }
                } else {
                    /* no more data for this rank */
                    PMIX_OUTPUT_VERBOSE((7, pmix_gds_base_framework.framework_output,
                                "%s:%d:%s:  no more data for this rank is found in the shared memory. rank %u key %s not found",
                                __FILE__, __LINE__, __func__, cur_rank, key));
                    break;
                }
            } else if (NULL == key) {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %s:%u, found target key %s",
                            __FILE__, __LINE__, __func__, nspace, cur_rank, PMIX_DS_KNAME_PTR(ds_ctx, addr)));

                uint8_t *data_ptr = PMIX_DS_DATA_PTR(ds_ctx, addr);
                size_t data_size = PMIX_DS_DATA_SIZE(ds_ctx, addr, data_ptr);
                PMIX_CONSTRUCT(&buffer, pmix_buffer_t);
                PMIX_LOAD_BUFFER(_client_peer(ds_ctx), &buffer, data_ptr, data_size);
                int cnt = 1;
                /* unpack value for this key from the buffer. */
                PMIX_VALUE_CONSTRUCT(&val);
                PMIX_BFROPS_UNPACK(rc, _client_peer(ds_ctx), &buffer, &val, &cnt, PMIX_VALUE);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    goto done;
                }
                pmix_strncpy(info[kval_cnt - 1].key, PMIX_DS_KNAME_PTR(ds_ctx, addr),
                        PMIX_DS_KNAME_LEN(ds_ctx, addr));
                pmix_value_xfer(&info[kval_cnt - 1].value, &val);
                PMIX_VALUE_DESTRUCT(&val);
                buffer.base_ptr = NULL;
                buffer.bytes_used = 0;
                PMIX_DESTRUCT(&buffer);
                key_found = true;

                kval_cnt--;
                addr += PMIX_DS_KV_SIZE(ds_ctx, addr);
            } else if (PMIX_DS_KEY_MATCH(ds_ctx, addr, key, keyhash)) {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %s:%u, found target key %s",
                            __FILE__, __LINE__, __func__, nspace, cur_rank, key));
                /* target key is found, get value */
                uint8_t *data_ptr = PMIX_DS_DATA_PTR(ds_ctx, addr);
                size_t data_size = PMIX_DS_DATA_SIZE(ds_ctx, addr, data_ptr);
                PMIX_CONSTRUCT(&buffer, pmix_buffer_t);
                PMIX_LOAD_BUFFER(_client_peer(ds_ctx), &buffer, data_ptr, data_size);
                int cnt = 1;
                /* unpack value for this key from the buffer. */
                *kvs = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                PMIX_BFROPS_UNPACK(rc, _client_peer(ds_ctx), &buffer, (void*)*kvs, &cnt, PMIX_VALUE);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    goto done;
                }
                buffer.base_ptr = NULL;
                buffer.bytes_used = 0;
                PMIX_DESTRUCT(&buffer);
                key_found = true;
                goto done;
            } else {
                PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                            "%s:%d:%s: for rank %s:%u, skip key %s look for key %s",
                            __FILE__, __LINE__, __func__, nspace, cur_rank,
                            PMIX_DS_KNAME_PTR(ds_ctx, addr), key));
                /* go to next item, updating address */
                addr += PMIX_DS_KV_SIZE(ds_ctx, addr);
                kval_cnt--;
            }
        }

        if (PMIX_RANK_UNDEF == rank) {
            cur_rank++;
        }
    }

done:
    /* unset lock */
    lock_rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, rd_unlock);
    if (PMIX_SUCCESS != lock_rc) {
        PMIX_ERROR_LOG(lock_rc);
    }

    /* unset ds_ctx lock */
    if (lock_is_set) {
        pthread_mutex_unlock(&ds_ctx->lock);
    }

    if( rc != PMIX_SUCCESS ){
        if ((NULL == key) && (kval_cnt > 0)) {
            if( NULL != info ) {
                PMIX_INFO_FREE(info, ninfo);
            }
            if (NULL != kval) {
                PMIX_VALUE_RELEASE(kval);
            }
        }
        return rc;
    }

    if( key_found ){
        /* the key is found - nothing to do */
        return PMIX_SUCCESS;
    }

    if( !all_ranks_found ){
        /* Not all ranks was found - need to request
         * all of them and search again
         */
        rc = PMIX_ERR_PROC_ENTRY_NOT_FOUND;
        return rc;
    }
    rc = PMIX_ERR_NOT_FOUND;
    return rc;

error:
    if (lock_is_set) {
        pthread_mutex_unlock(&ds_ctx->lock);
    }
    PMIX_ERROR_LOG(rc);
    return rc;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_fetch(pmix_common_dstore_ctx_t *ds_ctx,
                                                    const pmix_proc_t *proc,
                                                    pmix_scope_t scope, bool copy,
                                                    const char *key,
                                                    pmix_info_t info[], size_t ninfo,
                                                    pmix_list_t *kvs)
{
    pmix_kval_t *kv;
    pmix_value_t *val;
    pmix_status_t rc = PMIX_SUCCESS;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "gds: dstore fetch `%s`", key == NULL ? "NULL" : key);

    rc = _dstore_fetch(ds_ctx, proc->nspace, proc->rank, key, &val);
    if (PMIX_SUCCESS == rc) {
        if( NULL == key ) {
            pmix_info_t *info;
            size_t n, ninfo;

            if (NULL == val->data.darray ||
                PMIX_INFO != val->data.darray->type ||
                0 == val->data.darray->size) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
                return PMIX_ERR_NOT_FOUND;
            }
            info = (pmix_info_t*)val->data.darray->array;
            ninfo = val->data.darray->size;

            for (n = 0; n < ninfo; n++){
                kv = PMIX_NEW(pmix_kval_t);
                if (NULL == kv) {
                    rc = PMIX_ERR_NOMEM;
                    PMIX_VALUE_RELEASE(val);
                    return rc;
                }
                kv->key = strdup(info[n].key);
                PMIX_VALUE_XFER(rc, kv->value, &info[n].value);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kv);
                    PMIX_VALUE_RELEASE(val);
                    return rc;
                }
                pmix_list_append(kvs, &kv->super);
            }

            return PMIX_SUCCESS;
        }
        /* just return the value */
        kv = PMIX_NEW(pmix_kval_t);
        if (NULL == kv) {
            PMIX_VALUE_RELEASE(val);
            return PMIX_ERR_NOMEM;
        }
        kv->key = strdup(key);
        kv->value = val;
        pmix_list_append(kvs, &kv->super);
    }
    return rc;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_setup_fork(pmix_common_dstore_ctx_t *ds_ctx, const char *base_path_env,
                                           const pmix_proc_t *peer, char ***env)
{
    pmix_status_t rc = PMIX_SUCCESS;
    ns_map_data_t *ns_map = NULL;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "gds: dstore setup fork");

    if (NULL == ds_ctx->session_map_search) {
        rc = PMIX_ERR_NOT_AVAILABLE;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, peer->nspace))) {
        rc = PMIX_ERR_NOT_AVAILABLE;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    if ((NULL == ds_ctx->base_path) || (strlen(ds_ctx->base_path) == 0)){
        rc = PMIX_ERR_NOT_AVAILABLE;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    if(PMIX_SUCCESS != (rc = pmix_setenv(base_path_env,
                                        _ESH_SESSION_path(ds_ctx->session_array, ns_map->tbl_idx),
                                         true, env))){
        PMIX_ERROR_LOG(rc);
    }

    return rc;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_add_nspace(pmix_common_dstore_ctx_t *ds_ctx,
                                const char *nspace, pmix_info_t info[], size_t ninfo)
{
    pmix_status_t rc = PMIX_SUCCESS;
    size_t tbl_idx=0;
    uid_t jobuid = ds_ctx->jobuid;
    char setjobuid = ds_ctx->setjobuid;
    size_t n;
    ns_map_data_t *ns_map = NULL;
    uint32_t local_size = 0;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "gds: dstore add nspace");

    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (0 == strcmp(PMIX_USERID, info[n].key)) {
                jobuid = info[n].value.data.uint32;
                setjobuid = 1;
                continue;
            }
            if (0 == strcmp(PMIX_LOCAL_SIZE, info[n].key)) {
                local_size = info[n].value.data.uint32;
                continue;
            }
        }
    }

    if (PMIX_SUCCESS != _esh_jobuid_tbl_search(ds_ctx, jobuid, &tbl_idx)) {

        rc = _esh_session_tbl_add(ds_ctx, &tbl_idx);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
        ns_map = _esh_session_map(ds_ctx, nspace, local_size, tbl_idx);
        if (NULL == ns_map) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            return rc;
        }

        if (PMIX_SUCCESS != (rc =_esh_session_init(ds_ctx, tbl_idx, ns_map,
                                                   local_size, jobuid, setjobuid))) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }
    else {
        ns_map = _esh_session_map(ds_ctx, nspace, local_size, tbl_idx);
        if (NULL == ns_map) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }

    /* lock init */
    ds_ctx->lock_cbs->init(&_ESH_SESSION_lock(ds_ctx->session_array, tbl_idx),
                           ds_ctx->base_path, nspace, local_size, ds_ctx->jobuid,
                           ds_ctx->setjobuid);
    if (NULL == _ESH_SESSION_lock(ds_ctx->session_array, tbl_idx)) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    return PMIX_SUCCESS;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_del_nspace(pmix_common_dstore_ctx_t *ds_ctx, const char* nspace)
{
    pmix_status_t rc = PMIX_SUCCESS;
    size_t map_idx, size;
    int in_use = 0;
    ns_map_data_t *ns_map_data = NULL;
    ns_map_t *ns_map;
    session_t *session_tbl = NULL;
    ns_track_elem_t *trk = NULL;
    int dstor_track_idx;
    size_t session_tbl_idx;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
        "%s:%d:%s delete nspace `%s`", __FILE__, __LINE__, __func__, nspace));

    if (NULL == (ns_map_data = ds_ctx->session_map_search(ds_ctx, nspace))) {
        rc = PMIX_ERR_NOT_AVAILABLE;
        return rc;
    }
    dstor_track_idx = ns_map_data->track_idx;
    session_tbl_idx = ns_map_data->tbl_idx;
    size = pmix_value_array_get_size(ds_ctx->ns_map_array);
    ns_map = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->ns_map_array, ns_map_t);

    for (map_idx = 0; map_idx < size; map_idx++){
        if (ns_map[map_idx].in_use &&
                        (ns_map[map_idx].data.tbl_idx == ns_map_data->tbl_idx)) {
            if (0 == strcmp(ns_map[map_idx].data.name, nspace)) {
                _esh_session_map_clean(ds_ctx, &ns_map[map_idx]);
                continue;
            }
            in_use++;
        }
    }

    /* A lot of nspaces may be using same session info
     * session record can only be deleted once all references are gone */
    if (!in_use) {
        session_tbl = PMIX_VALUE_ARRAY_GET_BASE(ds_ctx->session_array, session_t);
        PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                             "%s:%d:%s delete session for jobuid: %d",
                             __FILE__, __LINE__, __func__, session_tbl[session_tbl_idx].jobuid));
        size = pmix_value_array_get_size(ds_ctx->ns_track_array);
        if (size && (dstor_track_idx >= 0)) {
            if((dstor_track_idx + 1) > (int)size) {
                rc = PMIX_ERR_VALUE_OUT_OF_BOUNDS;
                PMIX_ERROR_LOG(rc);
                goto exit;
            }
            trk = pmix_value_array_get_item(ds_ctx->ns_track_array, dstor_track_idx);
            if (true == trk->in_use) {
                PMIX_DESTRUCT(trk);
                pmix_value_array_remove_item(ds_ctx->ns_track_array, dstor_track_idx);
            }
        }
        _esh_session_release(ds_ctx, session_tbl_idx);
     }
exit:
    return rc;
}

static inline int _my_client(const char *nspace, pmix_rank_t rank)
{
    pmix_peer_t *peer;
    int i;
    int local = 0;

    for (i = 0; i < pmix_server_globals.clients.size; i++) {
        if (NULL != (peer = (pmix_peer_t *)pmix_pointer_array_get_item(&pmix_server_globals.clients, i))) {
            if (0 == strcmp(peer->info->pname.nspace, nspace) && peer->info->pname.rank == rank) {
                local = 1;
                break;
            }
        }
    }

    return local;
}

/* this function is only called by the PMIx server when its
 * host has received data from some other peer. It therefore
 * always contains data solely from remote procs, and we
 * shall store it accordingly */
PMIX_EXPORT pmix_status_t pmix_common_dstor_store_modex(pmix_common_dstore_ctx_t *ds_ctx,
                                                            struct pmix_namespace_t *nspace,
                                                            pmix_list_t *cbs,
                                                            pmix_buffer_t *buf)
{
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_status_t rc1 = PMIX_SUCCESS;
    pmix_namespace_t *ns = (pmix_namespace_t*)nspace;
    ns_map_data_t *ns_map;

    if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, ns->nspace))) {
        rc = PMIX_ERROR;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    /* set exclusive lock */
    rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, wr_lock);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    rc = pmix_gds_base_store_modex(nspace, cbs, buf, (pmix_gds_base_store_modex_cb_fn_t)_dstor_store_modex_cb, ds_ctx);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
    }

    /* unset lock */
    rc1 = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, wr_unlock);
    if (PMIX_SUCCESS != rc1) {
        PMIX_ERROR_LOG(rc1);
        if (PMIX_SUCCESS == rc) {
            rc = rc1;
        }
    }

    return rc;
}

static pmix_status_t _dstor_store_modex_cb(pmix_common_dstore_ctx_t *ds_ctx,
                                                struct pmix_namespace_t *nspace,
                                                pmix_list_t *cbs,
                                                pmix_byte_object_t *bo)
{
    pmix_namespace_t *ns = (pmix_namespace_t*)nspace;
    pmix_status_t rc = PMIX_SUCCESS;
    int32_t cnt;
    pmix_buffer_t pbkt;
    pmix_proc_t proc;
    pmix_kval_t *kv;
    ns_map_data_t *ns_map;
    pmix_buffer_t tmp;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:dstore:store_modex for nspace %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        ns->nspace);

    /* NOTE: THE BYTE OBJECT DELIVERED HERE WAS CONSTRUCTED
     * BY A SERVER, AND IS THEREFORE PACKED USING THE SERVER'S
     * PEER OBJECT (WHICH IS REQUIRED TO BE THE SAME AS OUR OWN) */

    /* this is data returned via the PMIx_Fence call when
     * data collection was requested, so it only contains
     * REMOTE/GLOBAL data. The byte object contains
     * the rank followed by pmix_kval_t's. The list of callbacks
     * contains all local participants. */

    /* setup the byte object for unpacking */
    PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
    /* the next step unfortunately NULLs the byte object's
     * entries, so we need to ensure we restore them! */
    PMIX_LOAD_BUFFER(pmix_globals.mypeer, &pbkt, bo->bytes, bo->size);
    /* unload the proc that provided this data */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, &proc, &cnt, PMIX_PROC);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        bo->bytes = pbkt.base_ptr;
        bo->size = pbkt.bytes_used; // restore the incoming data
        pbkt.base_ptr = NULL;
        PMIX_DESTRUCT(&pbkt);
        return rc;
    }
    /* don't store blobs to the sm dstore from local clients */
    if (_my_client(proc.nspace, proc.rank)) {
        bo->bytes = pbkt.base_ptr;
        bo->size = pbkt.bytes_used; // restore the incoming data
        pbkt.base_ptr = NULL;
        PMIX_DESTRUCT(&pbkt);
        return PMIX_SUCCESS;
    }

    /* Prepare a buffer to be provided to the dstor store primitive */
    PMIX_CONSTRUCT(&tmp, pmix_buffer_t);

    /* unpack the remaining values until we hit the end of the buffer */
    cnt = 1;
    kv = PMIX_NEW(pmix_kval_t);
    PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, kv, &cnt, PMIX_KVAL);
    while (PMIX_SUCCESS == rc) {
        /* store this in the hash table */
        PMIX_GDS_STORE_KV(rc, pmix_globals.mypeer, &proc, PMIX_REMOTE, kv);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            bo->bytes = pbkt.base_ptr;
            bo->size = pbkt.bytes_used; // restore the incoming data
            pbkt.base_ptr = NULL;
            PMIX_DESTRUCT(&pbkt);
            return rc;
        }

        /* place the key to the to be provided to _dstore_store_nolock */
        PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &tmp, kv, 1, PMIX_KVAL);

        /* Release the kv to maintain accounting
         * as the hash increments the ref count */
        PMIX_RELEASE(kv);

        /* proceed to the next element */
        kv = PMIX_NEW(pmix_kval_t);
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, kv, &cnt, PMIX_KVAL);
    }

    /* Release the kv that didn't received the value
     * because input buffer was exhausted */
    PMIX_RELEASE(kv);
    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
    } else {
        rc = PMIX_SUCCESS;
    }

    /* Create a key-value pair with the buffer
     * to be passed to _dstore_store_nolock */
    kv = PMIX_NEW(pmix_kval_t);
    PMIX_VALUE_CREATE(kv->value, 1);
    kv->value->type = PMIX_BYTE_OBJECT;
    PMIX_UNLOAD_BUFFER(&tmp, kv->value->data.bo.bytes, kv->value->data.bo.size);

    /* Get the namespace map element for the process "proc" */
    if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, proc.nspace))) {
        rc = PMIX_ERROR;
        PMIX_ERROR_LOG(rc);
        bo->bytes = pbkt.base_ptr;
        bo->size = pbkt.bytes_used; // restore the incoming data
        pbkt.base_ptr = NULL;
        PMIX_DESTRUCT(&pbkt);
        return rc;
    }

    /* Store all keys at once */
    rc = _dstore_store_nolock(ds_ctx, ns_map, proc.rank, kv);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
    }

    /* Release all resources */
    PMIX_RELEASE(kv);
    PMIX_DESTRUCT(&tmp);

    /* Reset the input buffer */
    bo->bytes = pbkt.base_ptr;
    bo->size = pbkt.bytes_used;
    pbkt.base_ptr = NULL;
    PMIX_DESTRUCT(&pbkt);

    return rc;
}

static pmix_status_t _store_job_info(pmix_common_dstore_ctx_t *ds_ctx, ns_map_data_t *ns_map,
                                     pmix_proc_t *proc)
{
    pmix_cb_t cb;
    pmix_kval_t *kv;
    pmix_buffer_t buf;
    pmix_kval_t *kv2 = NULL, *kvp;
    pmix_status_t rc = PMIX_SUCCESS;

    PMIX_CONSTRUCT(&cb, pmix_cb_t);
    PMIX_CONSTRUCT(&buf, pmix_buffer_t);
    kvp = PMIX_NEW(pmix_kval_t);
    PMIX_VALUE_CREATE(kvp->value, 1);
    kvp->value->type = PMIX_BYTE_OBJECT;

    cb.proc = proc;
    cb.scope = PMIX_INTERNAL;
    cb.copy = false;

    PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
    if (PMIX_SUCCESS != rc) {
        if (rc == PMIX_ERR_PROC_ENTRY_NOT_FOUND) {
            /* there is no error if no data for job info */
            rc = PMIX_SUCCESS;
        }
        goto exit;
    }

    PMIX_LIST_FOREACH(kv, &cb.kvs, pmix_kval_t) {
      if ((PMIX_PROC_IS_V1(_client_peer(ds_ctx)) || PMIX_PROC_IS_V20(_client_peer(ds_ctx))) &&
           0 != strncmp("pmix.", kv->key, 4) &&
           kv->value->type == PMIX_DATA_ARRAY) {
            pmix_info_t *info;
            size_t size, i;
            info = kv->value->data.darray->array;
            size = kv->value->data.darray->size;

            for (i = 0; i < size; i++) {
                if (0 == strcmp(PMIX_LOCAL_PEERS, info[i].key)) {
                    kv2 = PMIX_NEW(pmix_kval_t);
                    kv2->key = strdup(kv->key);
                    PMIX_VALUE_XFER(rc, kv2->value, &info[i].value);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_RELEASE(kv2);
                        goto exit;
                    }
                    PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &buf, kv2, 1, PMIX_KVAL);
                    if (PMIX_SUCCESS != rc) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_RELEASE(kv2);
                        goto exit;
                    }
                    PMIX_RELEASE(kv2);
                }
            }
        } else {
            PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &buf, kv, 1, PMIX_KVAL);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                goto exit;
            }
        }
    }

    PMIX_UNLOAD_BUFFER(&buf, kvp->value->data.bo.bytes, kvp->value->data.bo.size);
    if (PMIX_SUCCESS != (rc = _dstore_store_nolock(ds_ctx, ns_map, proc->rank, kvp))) {
        PMIX_ERROR_LOG(rc);
        goto exit;
    }

exit:
    PMIX_RELEASE(kvp);
    PMIX_DESTRUCT(&cb);
    PMIX_DESTRUCT(&buf);
    return rc;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_register_job_info(pmix_common_dstore_ctx_t *ds_ctx,
                                struct pmix_peer_t *pr,
                                pmix_buffer_t *reply)
{
    pmix_peer_t *peer = (pmix_peer_t*)pr;
    pmix_namespace_t *ns = peer->nptr;
    char *msg;
    pmix_status_t rc;
    pmix_proc_t proc;
    pmix_rank_t rank;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:dstore:register_job_info for peer [%s:%d]",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        peer->info->pname.nspace, peer->info->pname.rank);

    if (0 == ns->ndelivered) { // don't store twice
        ns_map_data_t *ns_map;

        _client_compat_save(ds_ctx, peer);
        pmix_strncpy(proc.nspace, ns->nspace, PMIX_MAX_NSLEN);
        proc.rank = PMIX_RANK_WILDCARD;
        if (NULL == (ns_map = ds_ctx->session_map_search(ds_ctx, proc.nspace))) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            return rc;
        }

        /* set exclusive lock */
        rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, wr_lock);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }

        rc = _store_job_info(ds_ctx, ns_map, &proc);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }

        for (rank=0; rank < ns->nprocs; rank++) {
            proc.rank = rank;
            rc = _store_job_info(ds_ctx, ns_map, &proc);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                return rc;
            }
        }
        /* unset lock */
        rc = _ESH_LOCK(ds_ctx, ns_map->tbl_idx, wr_unlock);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    }

    /* answer to client */
    msg = ns->nspace;
    PMIX_BFROPS_PACK(rc, peer, reply, &msg, 1, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    return rc;
}

PMIX_EXPORT pmix_status_t pmix_common_dstor_store_job_info(pmix_common_dstore_ctx_t *ds_ctx,
                                const char *nspace,
                                pmix_buffer_t *job_data)
{
    pmix_status_t rc = PMIX_SUCCESS;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%u] pmix:gds:dstore store job info for nspace %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank, nspace);

    /* check buf data */
    if ((NULL == job_data) || (0 == job_data->bytes_used)) {
        rc = PMIX_ERR_BAD_PARAM;
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    return rc;
}

static void _client_compat_save(pmix_common_dstore_ctx_t *ds_ctx, pmix_peer_t *peer)
{
    pmix_namespace_t *nptr = NULL;

    if (NULL == ds_ctx->clients_peer) {
        ds_ctx->clients_peer = PMIX_NEW(pmix_peer_t);
        nptr = PMIX_NEW(pmix_namespace_t);
        ds_ctx->clients_peer->nptr = nptr;
    }
    ds_ctx->clients_peer->nptr->compat = peer->nptr->compat;
    ds_ctx->clients_peer->proc_type = peer->proc_type;
}

static inline pmix_peer_t * _client_peer(pmix_common_dstore_ctx_t *ds_ctx)
{
    if (NULL == ds_ctx->clients_peer) {
        return pmix_globals.mypeer;
    }
    return ds_ctx->clients_peer;
}
