/*
 * Copyright (c) 2015-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016-2018 IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <string.h>
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
#include "src/util/hash.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"
#include "src/mca/preg/preg.h"

#include "src/mca/gds/base/base.h"
#include "gds_hash.h"

static pmix_status_t hash_init(pmix_info_t info[], size_t ninfo);
static void hash_finalize(void);

static pmix_status_t hash_assign_module(pmix_info_t *info, size_t ninfo,
                                        int *priority);

static pmix_status_t hash_cache_job_info(struct pmix_namespace_t *ns,
                                         pmix_info_t info[], size_t ninfo);

static pmix_status_t hash_register_job_info(struct pmix_peer_t *pr,
                                            pmix_buffer_t *reply);

static pmix_status_t hash_store_job_info(const char *nspace,
                                        pmix_buffer_t *buf);

static pmix_status_t hash_store(const pmix_proc_t *proc,
                                pmix_scope_t scope,
                                pmix_kval_t *kv);

static pmix_status_t hash_store_modex(struct pmix_namespace_t *ns,
                                      pmix_list_t *cbs,
                                      pmix_buffer_t *buff);

static pmix_status_t _hash_store_modex(void * cbdata,
                                       struct pmix_namespace_t *ns,
                                       pmix_list_t *cbs,
                                       pmix_byte_object_t *bo);

static pmix_status_t hash_fetch(const pmix_proc_t *proc,
                                pmix_scope_t scope, bool copy,
                                const char *key,
                                pmix_info_t info[], size_t ninfo,
                                pmix_list_t *kvs);

static pmix_status_t setup_fork(const pmix_proc_t *peer, char ***env);

static pmix_status_t nspace_add(const char *nspace,
                                pmix_info_t info[],
                                size_t ninfo);

static pmix_status_t nspace_del(const char *nspace);

static pmix_status_t assemb_kvs_req(const pmix_proc_t *proc,
                              pmix_list_t *kvs,
                              pmix_buffer_t *bo,
                              void *cbdata);

static pmix_status_t accept_kvs_resp(pmix_buffer_t *buf);

pmix_gds_base_module_t pmix_hash_module = {
    .name = "hash",
    .is_tsafe = false,
    .init = hash_init,
    .finalize = hash_finalize,
    .assign_module = hash_assign_module,
    .cache_job_info = hash_cache_job_info,
    .register_job_info = hash_register_job_info,
    .store_job_info = hash_store_job_info,
    .store = hash_store,
    .store_modex = hash_store_modex,
    .fetch = hash_fetch,
    .setup_fork = setup_fork,
    .add_nspace = nspace_add,
    .del_nspace = nspace_del,
    .assemb_kvs_req = assemb_kvs_req,
    .accept_kvs_resp = accept_kvs_resp
};

typedef struct {
    pmix_list_item_t super;
    char *ns;
    pmix_namespace_t *nptr;
    pmix_hash_table_t internal;
    pmix_hash_table_t remote;
    pmix_hash_table_t local;
    bool gdata_added;
} pmix_hash_trkr_t;

static void htcon(pmix_hash_trkr_t *p)
{
    p->ns = NULL;
    p->nptr = NULL;
    PMIX_CONSTRUCT(&p->internal, pmix_hash_table_t);
    pmix_hash_table_init(&p->internal, 256);
    PMIX_CONSTRUCT(&p->remote, pmix_hash_table_t);
    pmix_hash_table_init(&p->remote, 256);
    PMIX_CONSTRUCT(&p->local, pmix_hash_table_t);
    pmix_hash_table_init(&p->local, 256);
    p->gdata_added = false;
}
static void htdes(pmix_hash_trkr_t *p)
{
    if (NULL != p->ns) {
        free(p->ns);
    }
    if (NULL != p->nptr) {
        PMIX_RELEASE(p->nptr);
    }
    pmix_hash_remove_data(&p->internal, PMIX_RANK_WILDCARD, NULL);
    PMIX_DESTRUCT(&p->internal);
    pmix_hash_remove_data(&p->remote, PMIX_RANK_WILDCARD, NULL);
    PMIX_DESTRUCT(&p->remote);
    pmix_hash_remove_data(&p->local, PMIX_RANK_WILDCARD, NULL);
    PMIX_DESTRUCT(&p->local);
}
static PMIX_CLASS_INSTANCE(pmix_hash_trkr_t,
                           pmix_list_item_t,
                           htcon, htdes);

static pmix_list_t myhashes;

static pmix_status_t hash_init(pmix_info_t info[], size_t ninfo)
{
    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "gds: hash init");

    PMIX_CONSTRUCT(&myhashes, pmix_list_t);
    return PMIX_SUCCESS;
}

static void hash_finalize(void)
{
    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "gds: hash finalize");

    PMIX_LIST_DESTRUCT(&myhashes);
}

static pmix_status_t hash_assign_module(pmix_info_t *info, size_t ninfo,
                                        int *priority)
{
    size_t n, m;
    char **options;

    *priority = 10;
    if (NULL != info) {
        for (n=0; n < ninfo; n++) {
            if (0 == strncmp(info[n].key, PMIX_GDS_MODULE, PMIX_MAX_KEYLEN)) {
                options = pmix_argv_split(info[n].value.data.string, ',');
                for (m=0; NULL != options[m]; m++) {
                    if (0 == strcmp(options[m], "hash")) {
                        /* they specifically asked for us */
                        *priority = 100;
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

static pmix_status_t store_map(pmix_hash_table_t *ht,
                               char **nodes, char **ppn)
{
    pmix_status_t rc;
    pmix_value_t *val;
    size_t m, n;
    pmix_info_t *iptr, *info;
    pmix_rank_t rank;
    bool updated;
    pmix_kval_t *kp2;
    char **procs;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:hash:store_map",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank);

    /* if the lists don't match, then that's wrong */
    if (pmix_argv_count(nodes) != pmix_argv_count(ppn)) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return PMIX_ERR_BAD_PARAM;
    }

    for (n=0; NULL != nodes[n]; n++) {
        /* check and see if we already have data for this node */
        val = NULL;
        rc = pmix_hash_fetch(ht, PMIX_RANK_WILDCARD, nodes[n], &val);
        if (PMIX_SUCCESS == rc && NULL != val) {
            /* already have some data. See if we have the list of local peers */
            if (PMIX_DATA_ARRAY != val->type ||
                NULL == val->data.darray ||
                PMIX_INFO != val->data.darray->type ||
                0 == val->data.darray->size) {
                /* something is wrong */
                PMIX_VALUE_RELEASE(val);
                PMIX_ERROR_LOG(PMIX_ERR_INVALID_VAL);
                return PMIX_ERR_INVALID_VAL;
            }
            iptr = (pmix_info_t*)val->data.darray->array;
            updated = false;
            for (m=0; m < val->data.darray->size; m++) {
                if (0 == strncmp(iptr[m].key, PMIX_LOCAL_PEERS, PMIX_MAX_KEYLEN)) {
                    /* we will update this entry */
                    if (NULL != iptr[m].value.data.string) {
                        free(iptr[m].value.data.string);
                    }
                    iptr[m].value.data.string = strdup(ppn[n]);
                    updated = true;
                    break;
                }
            }
            if (!updated) {
                /* append this entry to the current data */
                kp2 = PMIX_NEW(pmix_kval_t);
                if (NULL == kp2) {
                    return PMIX_ERR_NOMEM;
                }
                kp2->key = strdup(nodes[n]);
                kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                if (NULL == kp2->value) {
                    PMIX_RELEASE(kp2);
                    return PMIX_ERR_NOMEM;
                }
                kp2->value->type = PMIX_DATA_ARRAY;
                kp2->value->data.darray = (pmix_data_array_t*)malloc(sizeof(pmix_data_array_t));
                if (NULL == kp2->value->data.darray) {
                    PMIX_RELEASE(kp2);
                    return PMIX_ERR_NOMEM;
                }
                kp2->value->data.darray->type = PMIX_INFO;
                kp2->value->data.darray->size = val->data.darray->size + 1;
                PMIX_INFO_CREATE(info, kp2->value->data.darray->size);
                if (NULL == info) {
                    PMIX_RELEASE(kp2);
                    return PMIX_ERR_NOMEM;
                }
                /* copy the pre-existing data across */
                for (m=0; m < val->data.darray->size; m++) {
                    PMIX_INFO_XFER(&info[m], &iptr[m]);
                }
                PMIX_INFO_LOAD(&info[kp2->value->data.darray->size-1], PMIX_LOCAL_PEERS, ppn[n], PMIX_STRING);
                kp2->value->data.darray->array = info;
                if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    return rc;
                }
                PMIX_RELEASE(kp2);
            }
        } else {
            /* store the list as-is */
            kp2 = PMIX_NEW(pmix_kval_t);
            if (NULL == kp2) {
                return PMIX_ERR_NOMEM;
            }
            kp2->key = strdup(nodes[n]);
            kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
            if (NULL == kp2->value) {
                PMIX_RELEASE(kp2);
                return PMIX_ERR_NOMEM;
            }
            kp2->value->type = PMIX_DATA_ARRAY;
            kp2->value->data.darray = (pmix_data_array_t*)malloc(sizeof(pmix_data_array_t));
            if (NULL == kp2->value->data.darray) {
                PMIX_RELEASE(kp2);
                return PMIX_ERR_NOMEM;
            }
            kp2->value->data.darray->type = PMIX_INFO;
            PMIX_INFO_CREATE(info, 1);
            if (NULL == info) {
                PMIX_RELEASE(kp2);
                return PMIX_ERR_NOMEM;
            }
            PMIX_INFO_LOAD(&info[0], PMIX_LOCAL_PEERS, ppn[n], PMIX_STRING);
            kp2->value->data.darray->array = info;
            kp2->value->data.darray->size = 1;
            if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                return rc;
            }
            PMIX_RELEASE(kp2);
        }
        /* split the list of procs so we can store their
         * individual location data */
        procs = pmix_argv_split(ppn[n], ',');
        for (m=0; NULL != procs[m]; m++) {
            /* store the hostname for each proc */
            kp2 = PMIX_NEW(pmix_kval_t);
            kp2->key = strdup(PMIX_HOSTNAME);
            kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
            kp2->value->type = PMIX_STRING;
            kp2->value->data.string = strdup(nodes[n]);
            rank = strtol(procs[m], NULL, 10);
            if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, rank, kp2))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                pmix_argv_free(procs);
                return rc;
            }
            PMIX_RELEASE(kp2);  // maintain acctg
        }
        pmix_argv_free(procs);
    }

    /* store the comma-delimited list of nodes hosting
     * procs in this nspace in case someone using PMIx v2
     * requests it */
    kp2 = PMIX_NEW(pmix_kval_t);
    kp2->key = strdup(PMIX_NODE_LIST);
    kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
    kp2->value->type = PMIX_STRING;
    kp2->value->data.string = pmix_argv_join(nodes, ',');
    if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE(kp2);
        return rc;
    }
    PMIX_RELEASE(kp2);  // maintain acctg

    return PMIX_SUCCESS;
}

pmix_status_t hash_cache_job_info(struct pmix_namespace_t *ns,
                                  pmix_info_t info[], size_t ninfo)
{
    pmix_namespace_t *nptr = (pmix_namespace_t*)ns;
    pmix_hash_trkr_t *trk, *t;
    pmix_hash_table_t *ht;
    pmix_kval_t *kp2, *kvptr;
    pmix_info_t *iptr;
    char **nodes=NULL, **procs=NULL;
    uint8_t *tmp;
    pmix_rank_t rank;
    pmix_status_t rc=PMIX_SUCCESS;
    size_t n, j, size, len;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:hash:cache_job_info for nspace %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        nptr->nspace);

    /* find the hash table for this nspace */
    trk = NULL;
    PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(nptr->nspace, t->ns)) {
            trk = t;
            break;
        }
    }
    if (NULL == trk) {
        /* create a tracker as we will likely need it */
        trk = PMIX_NEW(pmix_hash_trkr_t);
        if (NULL == trk) {
            return PMIX_ERR_NOMEM;
        }
        PMIX_RETAIN(nptr);
        trk->nptr = nptr;
        trk->ns = strdup(nptr->nspace);
        pmix_list_append(&myhashes, &trk->super);
    }

    /* if there isn't any data, then be content with just
     * creating the tracker */
    if (NULL == info || 0 == ninfo) {
        return PMIX_SUCCESS;
    }

    /* cache the job info on the internal hash table for this nspace */
    ht = &trk->internal;
    for (n=0; n < ninfo; n++) {
        if (0 == strcmp(info[n].key, PMIX_NODE_MAP)) {
            /* store the node map itself since that is
             * what v3 uses */
            kp2 = PMIX_NEW(pmix_kval_t);
            kp2->key = strdup(PMIX_NODE_MAP);
            kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
            kp2->value->type = PMIX_STRING;
            kp2->value->data.string = strdup(info[n].value.data.string);
            if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                return rc;
            }
            PMIX_RELEASE(kp2);  // maintain acctg

            /* parse the regex to get the argv array of node names */
            if (PMIX_SUCCESS != (rc = pmix_preg.parse_nodes(info[n].value.data.string, &nodes))) {
                PMIX_ERROR_LOG(rc);
                goto release;
            }
            /* if we have already found the proc map, then parse
             * and store the detailed map */
            if (NULL != procs) {
                if (PMIX_SUCCESS != (rc = store_map(ht, nodes, procs))) {
                    PMIX_ERROR_LOG(rc);
                    goto release;
                }
            }
        } else if (0 == strcmp(info[n].key, PMIX_PROC_MAP)) {
            /* parse the regex to get the argv array containing proc ranks on each node */
            if (PMIX_SUCCESS != (rc = pmix_preg.parse_procs(info[n].value.data.string, &procs))) {
                PMIX_ERROR_LOG(rc);
                goto release;
            }
            /* if we have already recv'd the node map, then parse
             * and store the detailed map */
            if (NULL != nodes) {
                if (PMIX_SUCCESS != (rc = store_map(ht, nodes, procs))) {
                    PMIX_ERROR_LOG(rc);
                    goto release;
                }
            }
        } else if (0 == strcmp(info[n].key, PMIX_PROC_DATA)) {
            /* an array of data pertaining to a specific proc */
            if (PMIX_DATA_ARRAY != info[n].value.type) {
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                rc = PMIX_ERR_TYPE_MISMATCH;
                goto release;
            }
            size = info[n].value.data.darray->size;
            iptr = (pmix_info_t*)info[n].value.data.darray->array;
            /* first element of the array must be the rank */
            if (0 != strcmp(iptr[0].key, PMIX_RANK) ||
                PMIX_PROC_RANK != iptr[0].value.type) {
                rc = PMIX_ERR_TYPE_MISMATCH;
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                goto release;
            }
            rank = iptr[0].value.data.rank;
            /* cycle thru the values for this rank and store them */
            for (j=1; j < size; j++) {
                kp2 = PMIX_NEW(pmix_kval_t);
                if (NULL == kp2) {
                    rc = PMIX_ERR_NOMEM;
                    goto release;
                }
                kp2->key = strdup(iptr[j].key);
                PMIX_VALUE_XFER(rc, kp2->value, &iptr[j].value);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    goto release;
                }
                /* if the value contains a string that is longer than the
                 * limit, then compress it */
                if (PMIX_STRING_SIZE_CHECK(kp2->value)) {
                    if (pmix_util_compress_string(kp2->value->data.string, &tmp, &len)) {
                        if (NULL == tmp) {
                            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                            rc = PMIX_ERR_NOMEM;
                            goto release;
                        }
                        kp2->value->type = PMIX_COMPRESSED_STRING;
                        free(kp2->value->data.string);
                        kp2->value->data.bo.bytes = (char*)tmp;
                        kp2->value->data.bo.size = len;
                    }
                }
                /* store it in the hash_table */
                if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, rank, kp2))) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    goto release;
                }
                PMIX_RELEASE(kp2);  // maintain acctg
            }
        } else {
            /* just a value relating to the entire job */
            kp2 = PMIX_NEW(pmix_kval_t);
            if (NULL == kp2) {
                rc = PMIX_ERR_NOMEM;
                goto release;
            }
            kp2->key = strdup(info[n].key);
            PMIX_VALUE_XFER(rc, kp2->value, &info[n].value);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                goto release;
            }
            /* if the value contains a string that is longer than the
             * limit, then compress it */
            if (PMIX_STRING_SIZE_CHECK(kp2->value)) {
                if (pmix_util_compress_string(kp2->value->data.string, &tmp, &len)) {
                    if (NULL == tmp) {
                        rc = PMIX_ERR_NOMEM;
                        PMIX_ERROR_LOG(rc);
                        PMIX_RELEASE(kp2);
                        goto release;
                    }
                    kp2->value->type = PMIX_COMPRESSED_STRING;
                    free(kp2->value->data.string);
                    kp2->value->data.bo.bytes = (char*)tmp;
                    kp2->value->data.bo.size = len;
                }
            }
            if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                goto release;
            }
            PMIX_RELEASE(kp2);  // maintain acctg
            /* if this is the job size, then store it */
            if (0 == strncmp(info[n].key, PMIX_JOB_SIZE, PMIX_MAX_KEYLEN)) {
                nptr->nprocs = info[n].value.data.uint32;
            }
        }
    }

    /* now add any global data that was provided */
    if (!trk->gdata_added) {
        PMIX_LIST_FOREACH(kvptr, &pmix_server_globals.gdata, pmix_kval_t) {
            /* sadly, the data cannot simultaneously exist on two lists,
             * so we must make a copy of it here */
            kp2 = PMIX_NEW(pmix_kval_t);
            if (NULL == kp2) {
                rc = PMIX_ERR_NOMEM;
                goto release;
            }
            kp2->key = strdup(kvptr->key);
            PMIX_VALUE_XFER(rc, kp2->value, kvptr->value);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                goto release;
            }
            if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp2);
                break;
            }
            PMIX_RELEASE(kp2);  // maintain acctg
        }
        trk->gdata_added = true;
    }

  release:
    if (NULL != nodes) {
        pmix_argv_free(nodes);
    }
    if (NULL != procs) {
        pmix_argv_free(procs);
    }
    return rc;
}

static pmix_status_t register_info(pmix_peer_t *peer,
                                   pmix_namespace_t *ns,
                                   pmix_buffer_t *reply)
{
    pmix_hash_trkr_t *trk, *t;
    pmix_hash_table_t *ht;
    pmix_value_t *val, blob;
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_info_t *info;
    size_t ninfo, n;
    pmix_kval_t kv;
    pmix_buffer_t buf;
    pmix_rank_t rank;

    trk = NULL;
    PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(ns->nspace, t->ns)) {
            trk = t;
            break;
        }
    }
    if (NULL == trk) {
        return PMIX_ERR_INVALID_NAMESPACE;
    }
    /* the job data is stored on the internal hash table */
    ht = &trk->internal;

    /* fetch all values from the hash table tied to rank=wildcard */
    val = NULL;
    rc = pmix_hash_fetch(ht, PMIX_RANK_WILDCARD, NULL, &val);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        if (NULL != val) {
            PMIX_VALUE_RELEASE(val);
        }
        return rc;
    }

    if (NULL == val || NULL == val->data.darray ||
        PMIX_INFO != val->data.darray->type ||
        0 == val->data.darray->size) {
        return PMIX_ERR_NOT_FOUND;
    }
    info = (pmix_info_t*)val->data.darray->array;
    ninfo = val->data.darray->size;
    for (n=0; n < ninfo; n++) {
        kv.key = info[n].key;
        kv.value = &info[n].value;
        PMIX_BFROPS_PACK(rc, peer, reply, &kv, 1, PMIX_KVAL);
    }
    if (NULL != val) {
        PMIX_VALUE_RELEASE(val);
    }

    for (rank=0; rank < ns->nprocs; rank++) {
        val = NULL;
        rc = pmix_hash_fetch(ht, rank, NULL, &val);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            if (NULL != val) {
                PMIX_VALUE_RELEASE(val);
            }
            return rc;
        }
        if (NULL == val) {
            return PMIX_ERR_NOT_FOUND;
        }
        PMIX_CONSTRUCT(&buf, pmix_buffer_t);
        PMIX_BFROPS_PACK(rc, peer, &buf, &rank, 1, PMIX_PROC_RANK);

        info = (pmix_info_t*)val->data.darray->array;
        ninfo = val->data.darray->size;
        for (n=0; n < ninfo; n++) {
            kv.key = info[n].key;
            kv.value = &info[n].value;
            PMIX_BFROPS_PACK(rc, peer, &buf, &kv, 1, PMIX_KVAL);
        }
        kv.key = PMIX_PROC_BLOB;
        kv.value = &blob;
        blob.type = PMIX_BYTE_OBJECT;
        PMIX_UNLOAD_BUFFER(&buf, blob.data.bo.bytes, blob.data.bo.size);
        PMIX_BFROPS_PACK(rc, peer, reply, &kv, 1, PMIX_KVAL);
        PMIX_VALUE_DESTRUCT(&blob);
        PMIX_DESTRUCT(&buf);

        if (NULL != val) {
            PMIX_VALUE_RELEASE(val);
        }
    }
    return rc;
}

/* the purpose of this function is to pack the job-level
 * info stored in the pmix_namespace_t into a buffer and send
 * it to the given client */
static pmix_status_t hash_register_job_info(struct pmix_peer_t *pr,
                                            pmix_buffer_t *reply)
{
    pmix_peer_t *peer = (pmix_peer_t*)pr;
    pmix_namespace_t *ns = peer->nptr;
    char *msg;
    pmix_status_t rc;
    pmix_hash_trkr_t *trk, *t2;

    if (!PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        /* this function is only available on servers */
        PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
        return PMIX_ERR_NOT_SUPPORTED;
    }

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:hash:register_job_info for peer [%s:%d]",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        peer->info->pname.nspace, peer->info->pname.rank);

    /* first see if we already have processed this data
     * for another peer in this nspace so we don't waste
     * time doing it again */
    if (NULL != ns->jobbkt) {
        /* we have packed this before - can just deliver it */
        PMIX_BFROPS_COPY_PAYLOAD(rc, peer, reply, ns->jobbkt);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
        }
        /* now see if we have delivered it to all our local
         * clients for this nspace */
        if (ns->ndelivered == ns->nlocalprocs) {
            /* we have, so let's get rid of the packed
             * copy of the data */
            PMIX_RELEASE(ns->jobbkt);
            ns->jobbkt = NULL;
        }
        return rc;
    }

    /* setup a tracker for this nspace as we will likely
     * need it again */
    trk = NULL;
    PMIX_LIST_FOREACH(t2, &myhashes, pmix_hash_trkr_t) {
        if (ns == t2->nptr) {
            trk = t2;
            if (NULL == trk->ns) {
                trk->ns = strdup(ns->nspace);
            }
            break;
        }
    }
    if (NULL == trk) {
        trk = PMIX_NEW(pmix_hash_trkr_t);
        trk->ns = strdup(ns->nspace);
        PMIX_RETAIN(ns);
        trk->nptr = ns;
        pmix_list_append(&myhashes, &trk->super);
    }

    /* the job info for the specified nspace has
     * been given to us in the info array - pack
     * them for delivery */
    /* pack the name of the nspace */
    msg = ns->nspace;
    PMIX_BFROPS_PACK(rc, peer, reply, &msg, 1, PMIX_STRING);
    if (PMIX_SUCCESS != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    rc = register_info(peer, ns, reply);
    if (PMIX_SUCCESS == rc) {
        /* if we have more than one local client for this nspace,
         * save this packed object so we don't do this again */
        if (1 < ns->nlocalprocs) {
            PMIX_RETAIN(reply);
            ns->jobbkt = reply;
        }
    } else {
        PMIX_ERROR_LOG(rc);
    }

    return rc;
}

static pmix_status_t hash_store_job_info(const char *nspace,
                                         pmix_buffer_t *buf)
{
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_kval_t *kptr, *kp2, kv;
    pmix_value_t *val;
    int32_t cnt;
    size_t nnodes, len, n;
    uint32_t i, j;
    char **procs = NULL;
    uint8_t *tmp;
    pmix_byte_object_t *bo;
    pmix_buffer_t buf2;
    int rank;
    pmix_hash_trkr_t *htptr;
    pmix_hash_table_t *ht;
    char **nodelist = NULL;
    pmix_info_t *info, *iptr;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%u] pmix:gds:hash store job info for nspace %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank, nspace);

    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer) &&
        !PMIX_PROC_IS_LAUNCHER(pmix_globals.mypeer)) {
        /* this function is NOT available on servers */
        PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* check buf data */
    if ((NULL == buf) || (0 == buf->bytes_used)) {
        rc = PMIX_ERR_BAD_PARAM;
        PMIX_ERROR_LOG(rc);
        return rc;
    }

    /* see if we already have a hash table for this nspace */
    ht = NULL;
    PMIX_LIST_FOREACH(htptr, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(htptr->ns, nspace)) {
            ht = &htptr->internal;
            break;
        }
    }
    if (NULL == ht) {
        /* nope - create one */
        htptr = PMIX_NEW(pmix_hash_trkr_t);
        htptr->ns = strdup(nspace);
        pmix_list_append(&myhashes, &htptr->super);
        ht = &htptr->internal;
    }

    cnt = 1;
    kptr = PMIX_NEW(pmix_kval_t);
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, kptr, &cnt, PMIX_KVAL);
    while (PMIX_SUCCESS == rc) {
        pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                            "[%s:%u] pmix:gds:hash store job info working key %s",
                            pmix_globals.myid.nspace, pmix_globals.myid.rank, kptr->key);
        if (0 == strcmp(kptr->key, PMIX_PROC_BLOB)) {
            bo = &(kptr->value->data.bo);
            PMIX_CONSTRUCT(&buf2, pmix_buffer_t);
            PMIX_LOAD_BUFFER(pmix_client_globals.myserver, &buf2, bo->bytes, bo->size);
            /* start by unpacking the rank */
            cnt = 1;
            PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                               &buf2, &rank, &cnt, PMIX_PROC_RANK);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_DESTRUCT(&buf2);
                return rc;
            }
            /* unpack the blob and save the values for this rank */
            cnt = 1;
            kp2 = PMIX_NEW(pmix_kval_t);
            PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                               &buf2, kp2, &cnt, PMIX_KVAL);
            while (PMIX_SUCCESS == rc) {
                /* if the value contains a string that is longer than the
                 * limit, then compress it */
                if (PMIX_STRING_SIZE_CHECK(kp2->value)) {
                    if (pmix_util_compress_string(kp2->value->data.string, &tmp, &len)) {
                        if (NULL == tmp) {
                            PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                            rc = PMIX_ERR_NOMEM;
                            return rc;
                        }
                        kp2->value->type = PMIX_COMPRESSED_STRING;
                        free(kp2->value->data.string);
                        kp2->value->data.bo.bytes = (char*)tmp;
                        kp2->value->data.bo.size = len;
                    }
                }
                /* this is data provided by a job-level exchange, so store it
                 * in the job-level data hash_table */
                if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, rank, kp2))) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    PMIX_DESTRUCT(&buf2);
                    return rc;
                }
                PMIX_RELEASE(kp2); // maintain accounting
                cnt = 1;
                kp2 = PMIX_NEW(pmix_kval_t);
                PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                                   &buf2, kp2, &cnt, PMIX_KVAL);
            }
            /* cleanup */
            PMIX_DESTRUCT(&buf2);  // releases the original kptr data
            PMIX_RELEASE(kp2);
        } else if (0 == strcmp(kptr->key, PMIX_MAP_BLOB)) {
            /* transfer the byte object for unpacking */
            bo = &(kptr->value->data.bo);
            PMIX_CONSTRUCT(&buf2, pmix_buffer_t);
            PMIX_LOAD_BUFFER(pmix_client_globals.myserver, &buf2, bo->bytes, bo->size);
            /* start by unpacking the number of nodes */
            cnt = 1;
            PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                               &buf2, &nnodes, &cnt, PMIX_SIZE);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_DESTRUCT(&buf2);
                return rc;
            }
            /* unpack the list of procs on each node */
            for (i=0; i < nnodes; i++) {
                cnt = 1;
                PMIX_CONSTRUCT(&kv, pmix_kval_t);
                PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                                   &buf2, &kv, &cnt, PMIX_KVAL);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_DESTRUCT(&buf2);
                    PMIX_DESTRUCT(&kv);
                    return rc;
                }
                /* track the nodes in this nspace */
                pmix_argv_append_nosize(&nodelist, kv.key);
                /* save the list of peers for this node - but first
                 * check to see if we already have some data for this node */
                rc = pmix_hash_fetch(ht, PMIX_RANK_WILDCARD, kv.key, &val);
                if (PMIX_SUCCESS == rc) {
                    /* already have some data, so we need to add to it */
                    kp2 = PMIX_NEW(pmix_kval_t);
                    kp2->key = strdup(kv.key);
                    kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                    kp2->value->type = PMIX_DATA_ARRAY;
                    kp2->value->data.darray = (pmix_data_array_t*)malloc(sizeof(pmix_data_array_t));
                    if (NULL == kp2->value->data.darray) {
                        PMIX_DESTRUCT(&buf2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_RELEASE(kp2);
                        return PMIX_ERR_NOMEM;
                    }
                    kp2->value->data.darray->type = PMIX_INFO;
                    kp2->value->data.darray->size = val->data.darray->size + 1;
                    PMIX_INFO_CREATE(info, kp2->value->data.darray->size);
                    if (NULL == info) {
                        PMIX_DESTRUCT(&buf2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_RELEASE(kp2);
                        return PMIX_ERR_NOMEM;
                    }
                    iptr = (pmix_info_t*)val->data.darray->array;
                    /* copy the pre-existing data across */
                    for (n=0; n < val->data.darray->size; n++) {
                        PMIX_INFO_XFER(&info[n], &iptr[n]);
                    }
                    PMIX_INFO_LOAD(&info[kp2->value->data.darray->size-1], PMIX_LOCAL_PEERS, kv.value->data.string, PMIX_STRING);
                    kp2->value->data.darray->array = info;
                    if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_RELEASE(kp2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_DESTRUCT(&buf2);
                        return rc;
                    }
                    PMIX_RELEASE(kp2);  // maintain acctg
                } else {
                    /* nope - so add this by itself */
                    kp2 = PMIX_NEW(pmix_kval_t);
                    kp2->key = strdup(kv.key);
                    kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                    kp2->value->type = PMIX_DATA_ARRAY;
                    kp2->value->data.darray = (pmix_data_array_t*)malloc(sizeof(pmix_data_array_t));
                    if (NULL == kp2->value->data.darray) {
                        PMIX_DESTRUCT(&buf2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_RELEASE(kp2);
                        return PMIX_ERR_NOMEM;
                    }
                    kp2->value->data.darray->type = PMIX_INFO;
                    PMIX_INFO_CREATE(info, 1);
                    if (NULL == info) {
                        PMIX_DESTRUCT(&buf2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_RELEASE(kp2);
                        return PMIX_ERR_NOMEM;
                    }
                    PMIX_INFO_LOAD(&info[0], PMIX_LOCAL_PEERS, kv.value->data.string, PMIX_STRING);
                    kp2->value->data.darray->array = info;
                    kp2->value->data.darray->size = 1;
                    if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_RELEASE(kp2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_DESTRUCT(&buf2);
                        return rc;
                    }
                    PMIX_RELEASE(kp2);  // maintain acctg
                }
                /* split the list of procs so we can store their
                 * individual location data */
                procs = pmix_argv_split(kv.value->data.string, ',');
                for (j=0; NULL != procs[j]; j++) {
                    /* store the hostname for each proc - again, this is
                     * data obtained via a job-level exchange, so store it
                     * in the job-level data hash_table */
                    kp2 = PMIX_NEW(pmix_kval_t);
                    kp2->key = strdup(PMIX_HOSTNAME);
                    kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                    kp2->value->type = PMIX_STRING;
                    kp2->value->data.string = strdup(kv.key);
                    rank = strtol(procs[j], NULL, 10);
                    if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, rank, kp2))) {
                        PMIX_ERROR_LOG(rc);
                        PMIX_RELEASE(kp2);
                        PMIX_DESTRUCT(&kv);
                        PMIX_DESTRUCT(&buf2);
                        pmix_argv_free(procs);
                        return rc;
                    }
                    PMIX_RELEASE(kp2);  // maintain acctg
                }
                pmix_argv_free(procs);
                PMIX_DESTRUCT(&kv);
            }
            if (NULL != nodelist) {
                /* store the comma-delimited list of nodes hosting
                 * procs in this nspace */
                kp2 = PMIX_NEW(pmix_kval_t);
                kp2->key = strdup(PMIX_NODE_LIST);
                kp2->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                kp2->value->type = PMIX_STRING;
                kp2->value->data.string = pmix_argv_join(nodelist, ',');
                pmix_argv_free(nodelist);
                if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kp2))) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_RELEASE(kp2);
                    PMIX_DESTRUCT(&kv);
                    PMIX_DESTRUCT(&buf2);
                    return rc;
                }
                PMIX_RELEASE(kp2);  // maintain acctg
            }
            /* cleanup */
            PMIX_DESTRUCT(&buf2);
        } else {
            /* if the value contains a string that is longer than the
             * limit, then compress it */
            if (PMIX_STRING_SIZE_CHECK(kptr->value)) {
                if (pmix_util_compress_string(kptr->value->data.string, &tmp, &len)) {
                    if (NULL == tmp) {
                        PMIX_ERROR_LOG(PMIX_ERR_NOMEM);
                        rc = PMIX_ERR_NOMEM;
                        return rc;
                    }
                    kptr->value->type = PMIX_COMPRESSED_STRING;
                    free(kptr->value->data.string);
                    kptr->value->data.bo.bytes = (char*)tmp;
                    kptr->value->data.bo.size = len;
                }
            }
            pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                                "[%s:%u] pmix:gds:hash store job info storing key %s for WILDCARD rank",
                                pmix_globals.myid.nspace, pmix_globals.myid.rank, kptr->key);
            if (PMIX_SUCCESS != (rc = pmix_hash_store(ht, PMIX_RANK_WILDCARD, kptr))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kptr);
                return rc;
            }
        }
        PMIX_RELEASE(kptr);
        kptr = PMIX_NEW(pmix_kval_t);
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                           buf, kptr, &cnt, PMIX_KVAL);
    }
    /* need to release the leftover kptr */
    PMIX_RELEASE(kptr);

    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
    } else {
        rc = PMIX_SUCCESS;
    }
    return rc;
}

static pmix_status_t hash_store(const pmix_proc_t *proc,
                                pmix_scope_t scope,
                                pmix_kval_t *kv)
{
    pmix_hash_trkr_t *trk, *t;
    pmix_status_t rc;
    pmix_kval_t *kp;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:hash:hash_store for proc [%s:%d] key %s type %s scope %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        proc->nspace, proc->rank, kv->key,
                        PMIx_Data_type_string(kv->value->type), PMIx_Scope_string(scope));

    if (NULL == kv->key) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* find the hash table for this nspace */
    trk = NULL;
    PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(proc->nspace, t->ns)) {
            trk = t;
            break;
        }
    }
    if (NULL == trk) {
        /* create one */
        trk = PMIX_NEW(pmix_hash_trkr_t);
        trk->ns = strdup(proc->nspace);
        pmix_list_append(&myhashes, &trk->super);
    }

    /* see if the proc is me */
    if (proc->rank == pmix_globals.myid.rank &&
        0 == strncmp(proc->nspace, pmix_globals.myid.nspace, PMIX_MAX_NSLEN)) {
        if (PMIX_INTERNAL != scope) {
            /* always maintain a copy of my own info here to simplify
             * later retrieval */
            kp = PMIX_NEW(pmix_kval_t);
            if (NULL == kp) {
                return PMIX_ERR_NOMEM;
            }
            kp->key = strdup(kv->key);
            kp->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
            if (NULL == kp->value) {
                PMIX_RELEASE(kp);
                return PMIX_ERR_NOMEM;
            }
            PMIX_BFROPS_VALUE_XFER(rc, pmix_globals.mypeer, kp->value, kv->value);
            if (PMIX_SUCCESS != rc) {
                PMIX_RELEASE(kp);
                return rc;
            }
            if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->internal, proc->rank, kp))) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kp);
                return rc;
            }
            PMIX_RELEASE(kp);  // maintain accounting
        }
    }

    /* store it in the corresponding hash table */
    if (PMIX_INTERNAL == scope) {
        if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->internal, proc->rank, kv))) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    } else if (PMIX_REMOTE == scope) {
        if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->remote, proc->rank, kv))) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    } else if (PMIX_LOCAL == scope) {
        if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->local, proc->rank, kv))) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
    } else if (PMIX_GLOBAL == scope) {
        if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->remote, proc->rank, kv))) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
        /* a pmix_kval_t can only be on one list at a time, so we
         * have to duplicate it here */
        kp = PMIX_NEW(pmix_kval_t);
        if (NULL == kp) {
            return PMIX_ERR_NOMEM;
        }
        kp->key = strdup(kv->key);
        kp->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
        if (NULL == kp->value) {
            PMIX_RELEASE(kp);
            return PMIX_ERR_NOMEM;
        }
        PMIX_BFROPS_VALUE_XFER(rc, pmix_globals.mypeer, kp->value, kv->value);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(kp);
            return rc;
        }
        if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->local, proc->rank, kp))) {
            PMIX_ERROR_LOG(rc);
            PMIX_RELEASE(kp);
            return rc;
        }
        PMIX_RELEASE(kp);  // maintain accounting
    } else {
        return PMIX_ERR_BAD_PARAM;
    }

    return PMIX_SUCCESS;
}

/* this function is only called by the PMIx server when its
 * host has received data from some other peer. It therefore
 * always contains data solely from remote procs, and we
 * shall store it accordingly */
static pmix_status_t hash_store_modex(struct pmix_namespace_t *nspace,
                                      pmix_list_t *cbs,
                                      pmix_buffer_t *buf) {
    return pmix_gds_base_store_modex(nspace, cbs, buf, _hash_store_modex, NULL);
}

static pmix_status_t _hash_store_modex(void * cbdata,
                                       struct pmix_namespace_t *nspace,
                                       pmix_list_t *cbs,
                                       pmix_byte_object_t *bo)
{
    pmix_namespace_t *ns = (pmix_namespace_t*)nspace;
    pmix_hash_trkr_t *trk, *t;
    pmix_status_t rc = PMIX_SUCCESS;
    int32_t cnt;
    pmix_buffer_t pbkt;
    pmix_proc_t proc;
    pmix_kval_t *kv;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%d] gds:hash:store_modex for nspace %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        ns->nspace);

    /* find the hash table for this nspace */
    trk = NULL;
    PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(ns->nspace, t->ns)) {
            trk = t;
            break;
        }
    }
    if (NULL == trk) {
        /* create one */
        trk = PMIX_NEW(pmix_hash_trkr_t);
        trk->ns = strdup(ns->nspace);
        pmix_list_append(&myhashes, &trk->super);
    }

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
    /* unpack the remaining values until we hit the end of the buffer */
    cnt = 1;
    kv = PMIX_NEW(pmix_kval_t);
    PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, kv, &cnt, PMIX_KVAL);
    while (PMIX_SUCCESS == rc) {
        /* store this in the hash table */
        if (PMIX_SUCCESS != (rc = pmix_hash_store(&trk->remote, proc.rank, kv))) {
            PMIX_ERROR_LOG(rc);
            bo->bytes = pbkt.base_ptr;
            bo->size = pbkt.bytes_used; // restore the incoming data
            pbkt.base_ptr = NULL;
            PMIX_DESTRUCT(&pbkt);
            return rc;
        }
        PMIX_RELEASE(kv);  // maintain accounting as the hash increments the ref count
        /* continue along */
        kv = PMIX_NEW(pmix_kval_t);
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer, &pbkt, kv, &cnt, PMIX_KVAL);
    }
    PMIX_RELEASE(kv);  // maintain accounting
    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
    } else {
        rc = PMIX_SUCCESS;
    }
    bo->bytes = pbkt.base_ptr;
    bo->size = pbkt.bytes_used; // restore the incoming data
    pbkt.base_ptr = NULL;
    PMIX_DESTRUCT(&pbkt);
    return rc;
}


static pmix_status_t hash_fetch(const pmix_proc_t *proc,
                                pmix_scope_t scope, bool copy,
                                const char *key,
                                pmix_info_t qualifiers[], size_t nqual,
                                pmix_list_t *kvs)
{
    pmix_hash_trkr_t *trk, *t;
    pmix_status_t rc;
    pmix_value_t *val;
    pmix_kval_t *kv;
    pmix_info_t *info;
    size_t n, ninfo;
    pmix_hash_table_t *ht;

    pmix_output_verbose(2, pmix_gds_base_framework.framework_output,
                        "[%s:%u] pmix:gds:hash fetch %s for proc %s:%u on scope %s",
                        pmix_globals.myid.nspace, pmix_globals.myid.rank,
                        (NULL == key) ? "NULL" : key,
                        proc->nspace, proc->rank, PMIx_Scope_string(scope));

    /* if the rank is wildcard and the key is NULL, then
     * they are asking for a complete copy of the job-level
     * info for this nspace - retrieve it */
    if (NULL == key && PMIX_RANK_WILDCARD == proc->rank) {
        /* see if we have a tracker for this nspace - we will
         * if we already cached the job info for it */
        trk = NULL;
        PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
            if (0 == strcmp(proc->nspace, t->ns)) {
                trk = t;
                break;
            }
        }
        if (NULL == trk) {
            /* let the caller know */
            return PMIX_ERR_INVALID_NAMESPACE;
        }
        /* the job data is stored on the internal hash table */
        ht = &trk->internal;
        /* fetch all values from the hash table tied to rank=wildcard */
        val = NULL;
        rc = pmix_hash_fetch(ht, PMIX_RANK_WILDCARD, NULL, &val);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            if (NULL != val) {
                PMIX_VALUE_RELEASE(val);
            }
            return rc;
        }
        if (NULL == val) {
            return PMIX_ERR_NOT_FOUND;
        }
        /* the data is returned in a pmix_data_array_t of pmix_info_t
         * structs. cycle thru and transfer them to the list */
        if (PMIX_DATA_ARRAY != val->type ||
            NULL == val->data.darray ||
            PMIX_INFO != val->data.darray->type) {
            PMIX_ERROR_LOG(PMIX_ERR_INVALID_VAL);
            PMIX_VALUE_RELEASE(val);
            return PMIX_ERR_INVALID_VAL;
        }
        info = (pmix_info_t*)val->data.darray->array;
        ninfo = val->data.darray->size;
        for (n=0; n < ninfo; n++) {
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
        PMIX_VALUE_RELEASE(val);
        return PMIX_SUCCESS;
    }

    /* find the hash table for this nspace */
    trk = NULL;
    PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(proc->nspace, t->ns)) {
            trk = t;
            break;
        }
    }
    if (NULL == trk) {
        return PMIX_ERR_INVALID_NAMESPACE;
    }

    /* fetch from the corresponding hash table - note that
     * we always provide a copy as we don't support
     * shared memory */
    if (PMIX_INTERNAL == scope ||
        PMIX_SCOPE_UNDEF == scope ||
        PMIX_GLOBAL == scope ||
        PMIX_RANK_WILDCARD == proc->rank) {
        ht = &trk->internal;
    } else if (PMIX_LOCAL == scope ||
               PMIX_GLOBAL == scope) {
        ht = &trk->local;
    } else if (PMIX_REMOTE == scope) {
        ht = &trk->remote;
    } else {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return PMIX_ERR_BAD_PARAM;
    }

  doover:
    rc = pmix_hash_fetch(ht, proc->rank, key, &val);
    if (PMIX_SUCCESS == rc) {
        /* if the key was NULL, then all found keys will be
         * returned as a pmix_data_array_t in the value */
        if (NULL == key) {
            if (NULL == val->data.darray ||
                PMIX_INFO != val->data.darray->type ||
                0 == val->data.darray->size) {
                PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
                return PMIX_ERR_NOT_FOUND;
            }
            info = (pmix_info_t*)val->data.darray->array;
            ninfo = val->data.darray->size;
            for (n=0; n < ninfo; n++) {
                kv = PMIX_NEW(pmix_kval_t);
                if (NULL == kv) {
                    PMIX_VALUE_RELEASE(val);
                    return PMIX_ERR_NOMEM;
                }
                kv->key = strdup(info[n].key);
                kv->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
                if (NULL == kv->value) {
                    PMIX_VALUE_RELEASE(val);
                    PMIX_RELEASE(kv);
                    return PMIX_ERR_NOMEM;
                }
                PMIX_BFROPS_VALUE_XFER(rc, pmix_globals.mypeer,
                                       kv->value, &info[n].value);
                if (PMIX_SUCCESS != rc) {
                    PMIX_ERROR_LOG(rc);
                    PMIX_VALUE_RELEASE(val);
                    PMIX_RELEASE(kv);
                    return rc;
                }
                pmix_list_append(kvs, &kv->super);
            }
            PMIX_VALUE_RELEASE(val);
            if (PMIX_GLOBAL == scope && ht == &trk->local) {
                /* need to do this again for the remote data */
                ht = &trk->remote;
                goto doover;
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
    } else {
        if (PMIX_GLOBAL == scope ||
            PMIX_SCOPE_UNDEF == scope) {
            if (ht == &trk->internal) {
                /* need to also try the local data */
                ht = &trk->local;
                goto doover;
            } else if (ht == &trk->local) {
                /* need to also try the remote data */
                ht = &trk->remote;
                goto doover;
            }
        }
    }

    return rc;
}

static pmix_status_t setup_fork(const pmix_proc_t *proc, char ***env)
{
    /* we don't need to add anything */
    return PMIX_SUCCESS;
}

static pmix_status_t nspace_add(const char *nspace,
                                pmix_info_t info[],
                                size_t ninfo)
{
    /* we don't need to do anything here */
    return PMIX_SUCCESS;
}

static pmix_status_t nspace_del(const char *nspace)
{
    pmix_hash_trkr_t *t;

    /* find the hash table for this nspace */
    PMIX_LIST_FOREACH(t, &myhashes, pmix_hash_trkr_t) {
        if (0 == strcmp(nspace, t->ns)) {
            /* release it */
            pmix_list_remove_item(&myhashes, &t->super);
            PMIX_RELEASE(t);
            break;
        }
    }
    return PMIX_SUCCESS;
}

static pmix_status_t assemb_kvs_req(const pmix_proc_t *proc,
                              pmix_list_t *kvs,
                              pmix_buffer_t *buf,
                              void *cbdata)
{
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_server_caddy_t *cd = (pmix_server_caddy_t*)cbdata;
    pmix_kval_t *kv;

    if (!PMIX_PROC_IS_V1(cd->peer)) {
        PMIX_BFROPS_PACK(rc, cd->peer, buf, proc, 1, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            return rc;
        }
    }
    PMIX_LIST_FOREACH(kv, kvs, pmix_kval_t) {
        PMIX_BFROPS_PACK(rc, cd->peer, buf, kv, 1, PMIX_KVAL);
        if (PMIX_SUCCESS != rc) {
            return rc;
        }
    }
    return rc;
}

static pmix_status_t accept_kvs_resp(pmix_buffer_t *buf)
{
    pmix_status_t rc = PMIX_SUCCESS;
    int32_t cnt;
    pmix_byte_object_t bo;
    pmix_buffer_t pbkt;
    pmix_kval_t *kv;
    pmix_proc_t proct;

    /* the incoming payload is provided as a set of packed
     * byte objects, one for each rank. A pmix_proc_t is the first
     * entry in the byte object. If the rank=PMIX_RANK_WILDCARD,
     * then that byte object contains job level info
     * for the provided nspace. Otherwise, the byte
     * object contains the pmix_kval_t's that were "put" by the
     * referenced process */
    cnt = 1;
    PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                       buf, &bo, &cnt, PMIX_BYTE_OBJECT);
    while (PMIX_SUCCESS == rc) {
        /* setup the byte object for unpacking */
        PMIX_CONSTRUCT(&pbkt, pmix_buffer_t);
        PMIX_LOAD_BUFFER(pmix_client_globals.myserver,
                         &pbkt, bo.bytes, bo.size);
        /* unpack the id of the providing process */
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                           &pbkt, &proct, &cnt, PMIX_PROC);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            return rc;
        }
        cnt = 1;
        kv = PMIX_NEW(pmix_kval_t);
        PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                           &pbkt, kv, &cnt, PMIX_KVAL);
        while (PMIX_SUCCESS == rc) {
            /* let the GDS component for this peer store it - if
             * the kval contains shmem connection info, then the
             * component will know what to do about it (or else
             * we selected the wrong component for this peer!) */

            PMIX_GDS_STORE_KV(rc, pmix_globals.mypeer, &proct, PMIX_INTERNAL, kv);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                PMIX_RELEASE(kv);
                PMIX_DESTRUCT(&pbkt);
                return rc;
            }
            PMIX_RELEASE(kv);  // maintain accounting
            /* get the next one */
            kv = PMIX_NEW(pmix_kval_t);
            cnt = 1;
            PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                               &pbkt, kv, &cnt, PMIX_KVAL);
        }
        PMIX_RELEASE(kv);  // maintain accounting
        if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
            PMIX_ERROR_LOG(rc);
            PMIX_DESTRUCT(&pbkt);
            return rc;
        }
        PMIX_DESTRUCT(&pbkt);
        /* get the next one */
        cnt = 1;
        PMIX_BFROPS_UNPACK(rc, pmix_client_globals.myserver,
                           buf, &bo, &cnt, PMIX_BYTE_OBJECT);
    }
    if (PMIX_ERR_UNPACK_READ_PAST_END_OF_BUFFER != rc) {
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    return rc;
}
