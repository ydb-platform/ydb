/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016-2018 IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2018 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>
#include <pmix_common.h>
#include "src/include/pmix_globals.h"
#include "src/util/error.h"
#include "src/mca/gds/base/base.h"
#include "src/util/argv.h"

#include "src/mca/common/dstore/dstore_common.h"
#include "gds_ds12_base.h"
#include "gds_ds12_lock.h"
#include "gds_ds12_file.h"
#include "src/mca/common/dstore/dstore_base.h"

static pmix_common_dstore_ctx_t *ds12_ctx;

static pmix_status_t ds12_init(pmix_info_t info[], size_t ninfo)
{
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_common_dstore_file_cbs_t *dstore_file_cbs = NULL;

    if (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        dstore_file_cbs = &pmix_ds20_file_module;
    }
    ds12_ctx = pmix_common_dstor_init("ds12", info, ninfo,
                                      &pmix_ds12_lock_module,
                                      dstore_file_cbs);
    if (NULL == ds12_ctx) {
        rc = PMIX_ERR_INIT;
    }

    return rc;
}

static void ds12_finalize(void)
{
    pmix_common_dstor_finalize(ds12_ctx);
}

static pmix_status_t ds12_assign_module(pmix_info_t *info, size_t ninfo,
                                        int *priority)
{
    size_t n, m;
    char **options;

    *priority = 20;
    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_GDS_MODULE, PMIX_MAX_KEYLEN)) {
                options = pmix_argv_split(info[n].value.data.string, ',');
                for (m=0; NULL != options[m]; m++) {
                    if (0 == strcmp(options[m], "ds12")) {
                        /* they specifically asked for us */
                        *priority = 100;
                        break;
                    }
                    if (0 == strcmp(options[m], "dstore")) {
                        /* they are asking for any dstore module - we
                         * take an intermediate priority in case another
                         * dstore is more modern than us */
                        *priority = 50;
                        break;
                    }
                }
                pmix_argv_free(options);
                break;
            }
        }
    }

    return PMIX_SUCCESS;
}

static pmix_status_t ds12_cache_job_info(struct pmix_namespace_t *ns,
                                pmix_info_t info[], size_t ninfo)
{
    return PMIX_SUCCESS;
}

static pmix_status_t ds12_register_job_info(struct pmix_peer_t *pr,
                                            pmix_buffer_t *reply)
{
    if (PMIX_PROC_IS_V1(pr)) {
        ds12_ctx->file_cbs = &pmix_ds12_file_module;
    } else {
        ds12_ctx->file_cbs = &pmix_ds20_file_module;
    }
    return pmix_common_dstor_register_job_info(ds12_ctx, pr, reply);
}

static pmix_status_t ds12_store_job_info(const char *nspace,  pmix_buffer_t *buf)
{
    return pmix_common_dstor_store_job_info(ds12_ctx, nspace, buf);
}

static pmix_status_t ds12_store(const pmix_proc_t *proc,
                                    pmix_scope_t scope,
                                    pmix_kval_t *kv)
{
    return pmix_common_dstor_store(ds12_ctx, proc, scope, kv);
}

/* this function is only called by the PMIx server when its
 * host has received data from some other peer. It therefore
 * always contains data solely from remote procs, and we
 * shall store it accordingly */
static pmix_status_t ds12_store_modex(struct pmix_namespace_t *nspace,
                                      pmix_list_t *cbs,
                                      pmix_buffer_t *buf)
{
    return pmix_common_dstor_store_modex(ds12_ctx, nspace, cbs, buf);
}

static pmix_status_t ds12_fetch(const pmix_proc_t *proc,
                                    pmix_scope_t scope, bool copy,
                                    const char *key,
                                    pmix_info_t info[], size_t ninfo,
                                    pmix_list_t *kvs)
{
    return pmix_common_dstor_fetch(ds12_ctx, proc, scope, copy, key, info, ninfo, kvs);
}

static pmix_status_t ds12_setup_fork(const pmix_proc_t *peer, char ***env)
{
    return pmix_common_dstor_setup_fork(ds12_ctx, PMIX_DSTORE_ESH_BASE_PATH, peer, env);
}

static pmix_status_t ds12_add_nspace(const char *nspace,
                                pmix_info_t info[],
                                size_t ninfo)
{
    return pmix_common_dstor_add_nspace(ds12_ctx, nspace, info, ninfo);
}

static pmix_status_t ds12_del_nspace(const char* nspace)
{
    return pmix_common_dstor_del_nspace(ds12_ctx, nspace);
}

pmix_gds_base_module_t pmix_ds12_module = {
    .name = "ds12",
    .is_tsafe = false,
    .init = ds12_init,
    .finalize = ds12_finalize,
    .assign_module = ds12_assign_module,
    .cache_job_info = ds12_cache_job_info,
    .register_job_info = ds12_register_job_info,
    .store_job_info = ds12_store_job_info,
    .store = ds12_store,
    .store_modex = ds12_store_modex,
    .fetch = ds12_fetch,
    .setup_fork = ds12_setup_fork,
    .add_nspace = ds12_add_nspace,
    .del_nspace = ds12_del_nspace,
};

