/*
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2018      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_GDS_DS_BASE_H_
#define PMIX_GDS_DS_BASE_H_

#include <pthread.h>
#include <src/include/pmix_config.h>
#include <pmix_common.h>

#include "src/include/pmix_globals.h"
#include "src/class/pmix_list.h"
#include "src/mca/gds/gds.h"
#include "src/mca/pshmem/pshmem.h"
#include "src/mca/common/dstore/dstore_file.h"

typedef void* pmix_common_dstor_lock_ctx_t;

typedef pmix_status_t (*pmix_common_dstor_lock_init_fn_t)(pmix_common_dstor_lock_ctx_t *ctx,
                                                const char *base_path,  const char *name,
                                                uint32_t local_size, uid_t uid, bool setuid);
typedef void (*pmix_common_dstor_lock_finalize_fn_t)(pmix_common_dstor_lock_ctx_t *ctx);
typedef pmix_status_t (*pmix_common_dstor_lock_rd_get_fn_t)(pmix_common_dstor_lock_ctx_t ctx);
typedef pmix_status_t (*pmix_common_dstor_lock_rd_rel_fn_t)(pmix_common_dstor_lock_ctx_t ctx);
typedef pmix_status_t (*pmix_common_dstor_lock_wr_get_fn_t)(pmix_common_dstor_lock_ctx_t ctx);
typedef pmix_status_t (*pmix_common_dstor_lock_wr_rel_fn_t)(pmix_common_dstor_lock_ctx_t ctx);

typedef struct {
    pmix_common_dstor_lock_init_fn_t init;
    pmix_common_dstor_lock_finalize_fn_t finalize;
    pmix_common_dstor_lock_rd_get_fn_t rd_lock;
    pmix_common_dstor_lock_rd_rel_fn_t rd_unlock;
    pmix_common_dstor_lock_wr_get_fn_t wr_lock;
    pmix_common_dstor_lock_wr_rel_fn_t wr_unlock;
} pmix_common_lock_callbacks_t;

typedef struct pmix_common_dstore_ctx_s pmix_common_dstore_ctx_t;

PMIX_EXPORT pmix_common_dstore_ctx_t *pmix_common_dstor_init(const char *ds_name, pmix_info_t info[], size_t ninfo,
                                                             pmix_common_lock_callbacks_t *lock_cb,
                                                             pmix_common_dstore_file_cbs_t *file_cb);
PMIX_EXPORT void pmix_common_dstor_finalize(pmix_common_dstore_ctx_t *ds_ctx);
PMIX_EXPORT pmix_status_t pmix_common_dstor_add_nspace(pmix_common_dstore_ctx_t *ds_ctx,
                                const char *nspace, pmix_info_t info[], size_t ninfo);
PMIX_EXPORT pmix_status_t pmix_common_dstor_del_nspace(pmix_common_dstore_ctx_t *ds_ctx, const char* nspace);
PMIX_EXPORT pmix_status_t pmix_common_dstor_setup_fork(pmix_common_dstore_ctx_t *ds_ctx, const char *base_path_env,
                                           const pmix_proc_t *peer, char ***env);
PMIX_EXPORT pmix_status_t pmix_common_dstor_cache_job_info(pmix_common_dstore_ctx_t *ds_ctx,
                                struct pmix_namespace_t *ns,
                                pmix_info_t info[], size_t ninfo);
PMIX_EXPORT pmix_status_t pmix_common_dstor_register_job_info(pmix_common_dstore_ctx_t *ds_ctx,
                                struct pmix_peer_t *pr,
                                pmix_buffer_t *reply);
PMIX_EXPORT pmix_status_t pmix_common_dstor_store_job_info(pmix_common_dstore_ctx_t *ds_ctx,
                                const char *nspace,
                                pmix_buffer_t *job_data);
PMIX_EXPORT pmix_status_t pmix_common_dstor_store(pmix_common_dstore_ctx_t *ds_ctx,
                                const pmix_proc_t *proc,
                                pmix_scope_t scope,
                                pmix_kval_t *kv);
PMIX_EXPORT pmix_status_t pmix_common_dstor_fetch(pmix_common_dstore_ctx_t *ds_ctx,
                                const pmix_proc_t *proc,
                                pmix_scope_t scope, bool copy,
                                const char *key,
                                pmix_info_t info[], size_t ninfo,
                                pmix_list_t *kvs);
PMIX_EXPORT pmix_status_t pmix_common_dstor_store_modex(pmix_common_dstore_ctx_t *ds_ctx,
                                struct pmix_namespace_t *nspace,
                                pmix_list_t *cbs,
                                pmix_buffer_t *buff);
#endif
