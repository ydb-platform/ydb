/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2018 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2016-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2018      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_GDS_H
#define PMIX_GDS_H

#include <src/include/pmix_config.h>


#include <pmix_common.h>
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/mca/bfrops/bfrops_types.h"


/* The client dictates the GDS module that will be used to interact
 * with the server - this module is stored in pmix_globals.mypeer->compat.gds
 * Because that is a long address to keep typing out, convenience macros
 * are provided for when that module is to be used in an operation.
 *
 * However, an application can open any number of GDS modules for
 * purposes other than exchanging info with the server. For example,
 * an application may wish to utilize a DHT module for its own
 * peer-to-peer data sharing. Thus, the public and private interfaces
 * are deliberately designed to be generic. The macros should make
 * things easier for the typical internal operations
 *
 * NOTE: ALTHOUGH SOME GDS COMPONENTS MAY UTILIZE THEIR OWN INTERNAL
 * PROGRESS THREADS, THE GDS IS NOT GUARANTEED TO BE THREAD-SAFE.
 * GDS FUNCTIONS SHOULD THEREFORE ALWAYS BE CALLED IN A THREAD-SAFE
 * CONDITION - E.G., FROM WITHIN AN EVENT
 */

BEGIN_C_DECLS
/* forward declaration */
struct pmix_peer_t;
struct pmix_namespace_t;

/* backdoor to base verbosity */
PMIX_EXPORT extern int pmix_gds_base_output;

/**
 * Initialize the module. Returns an error if the module cannot
 * run, success if it can.
 */
typedef pmix_status_t (*pmix_gds_base_module_init_fn_t)(pmix_info_t info[], size_t ninfo);

/**
 * Finalize the module. Tear down any allocated storage, disconnect
 * from any system support.
 */
typedef void (*pmix_gds_base_module_fini_fn_t)(void);

/**
 * Assign a module per the requested directives. Modules should
 * review the provided directives to determine if they can support
 * the request. Modules are "scanned" in component priority order
 * and given an opportunity to respond. If a module offers itself,
 * it will provide a priority (which can be based on the directives
 * and therefore different from the component priority). The highest
 * returned priority received from a responder will be selected
 * and a pointer to its module returned */
typedef pmix_status_t (*pmix_gds_base_assign_module_fn_t)(pmix_info_t *info,
                                                          size_t ninfo,
                                                          int *priority);

/* SERVER FN: assemble the keys buffer for server answer */
typedef pmix_status_t (*pmix_gds_base_module_assemb_kvs_req_fn_t)(const pmix_proc_t *proc,
                                                            pmix_list_t *kvs,
                                                            pmix_buffer_t *buf,
                                                            void *cbdata);

/* define a macro for server keys answer based on peer */
#define PMIX_GDS_ASSEMB_KVS_REQ(s, p, r, k, b, c)                       \
    do {                                                                \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds;             \
        (s) = PMIX_SUCCESS;                                             \
        if (NULL != _g->assemb_kvs_req) {                               \
            pmix_output_verbose(1, pmix_gds_base_output,                \
                                "[%s:%d] GDS ASSEMBLE REQ WITH %s",     \
                                __FILE__, __LINE__, _g->name);          \
            (s) = _g->assemb_kvs_req(r, k, b, (void*)c);                \
        }                                                               \
    } while(0)


/* CLIENT FN: unpack buffer and key processing */
typedef pmix_status_t (*pmix_gds_base_module_accept_kvs_resp_fn_t)(pmix_buffer_t *buf);

/* define a macro for client key processing from a server response based on peer */
#define PMIX_GDS_ACCEPT_KVS_RESP(s, p, b)                                   \
    do {                                                                    \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds;                 \
        (s) = PMIX_SUCCESS;                                                 \
        if (NULL != _g->accept_kvs_resp) {                                  \
            pmix_output_verbose(1, pmix_gds_base_output,                    \
                                "[%s:%d] GDS ACCEPT RESP WITH %s",          \
                                __FILE__, __LINE__, _g->name);              \
            (s) = _g->accept_kvs_resp(b);                                   \
        }                                                                   \
    } while (0)


/* SERVER FN: cache job-level info in the server's GDS until client
 * procs connect and we discover which GDS module to use for them.
 * Note that this is essentially the same function as store_job_info,
 * only we don't have packed data on the server side, and don't want
 * to incur the overhead of packing it just to unpack it in the function.
 */
typedef pmix_status_t (*pmix_gds_base_module_cache_job_info_fn_t)(struct pmix_namespace_t *ns,
                                                                  pmix_info_t info[], size_t ninfo);

/* define a convenience macro for caching job info */
#define PMIX_GDS_CACHE_JOB_INFO(s, p, n, i, ni)                             \
    do {                                                                    \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds;                 \
        pmix_output_verbose(1, pmix_gds_base_output,                        \
                            "[%s:%d] GDS CACHE JOB INFO WITH %s",           \
                            __FILE__, __LINE__, _g->name);                  \
       (s) = _g->cache_job_info((struct pmix_namespace_t*)(n), (i), (ni));     \
    } while(0)

/* register job-level info - this is provided as a special function
 * to allow for optimization. Called solely by the server. We cannot
 * prepare the job-level info provided at PMIx_Register_nspace, because
 * we don't know the GDS component to use for that application until
 * a local client contacts us. Thus, the module is required to process
 * the job-level info cached in the pmix_namespace_t for this job and
 * do whatever is necessary to support the client, packing any required
 * return message into the provided buffer.
 *
 * This function will be called once for each local client of
 * a given nspace. PMIx assumes that all peers of a given nspace
 * will use the same GDS module. Thus, the module is free to perform
 * any relevant optimizations (e.g., packing the data only once and
 * then releasing the cached buffer once all local clients have
 * been serviced, or storing it once in shared memory and simply
 * returning the shared memory rendezvous information for subsequent
 * calls).
 *
 * Info provided in the reply buffer will be given to the "store_job_info"
 * API of the GDS module on the client. Since this should match the
 * module used by the server, each module has full knowledge and control
 * over what is in the reply buffer.
 *
 * The pmix_peer_t of the requesting client is provided here so that
 * the module can access the job-level info cached on the corresponding
 * pmix_namespace_t pointed to by the pmix_peer_t
 */
typedef pmix_status_t (*pmix_gds_base_module_register_job_info_fn_t)(struct pmix_peer_t *pr,
                                                                     pmix_buffer_t *reply);

/* define a convenience macro for registering job info for
 * a given peer */
#define PMIX_GDS_REGISTER_JOB_INFO(s, p, b)                         \
    do {                                                            \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds;         \
        pmix_output_verbose(1, pmix_gds_base_output,                \
                            "[%s:%d] GDS REG JOB INFO WITH %s",     \
                            __FILE__, __LINE__, _g->name);          \
        (s) = _g->register_job_info((struct pmix_peer_t*)(p), b);   \
    } while(0)


/* update job-level info - this is provided as a special function
 * to allow for optimization. Called solely by the client. The buffer
 * provided to this API is the same one given to the server by the
 * corresponding "register_job_info" function
 */
typedef pmix_status_t (*pmix_gds_base_module_store_job_info_fn_t)(const char *nspace,
                                                                  pmix_buffer_t *buf);

/* define a convenience macro for storing job info based on peer */
#define PMIX_GDS_STORE_JOB_INFO(s, p, n, b)                         \
    do {                                                            \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds;         \
        pmix_output_verbose(1, pmix_gds_base_output,                \
                            "[%s:%d] GDS STORE JOB INFO WITH %s",   \
                            __FILE__, __LINE__, _g->name);          \
        (s) = _g->store_job_info(n, b);                             \
    } while(0)


/**
* store key/value pair - these will either be values committed by the peer
* and transmitted to the server, or values stored locally by the peer.
* The format of the data depends on the GDS module. Note that data stored
* with PMIX_INTERNAL scope should be stored solely within the process and
* is never shared.
*
* @param peer   pointer to pmix_peer_t object of the peer that
*               provided the data
*
* @param proc   the proc that the data describes
*
* @param scope  scope of the data
*
* @param kv     key/value pair.
*
* @return PMIX_SUCCESS on success.
*/
typedef pmix_status_t (*pmix_gds_base_module_store_fn_t)(const pmix_proc_t *proc,
                                                         pmix_scope_t scope,
                                                         pmix_kval_t *kv);

/* define a convenience macro for storing key-val pairs based on peer */
#define PMIX_GDS_STORE_KV(s, p, pc, sc, k)                  \
    do {                                                    \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds; \
        pmix_output_verbose(1, pmix_gds_base_output,        \
                            "[%s:%d] GDS STORE KV WITH %s", \
                            __FILE__, __LINE__, _g->name);  \
        (s) = _g->store(pc, sc, k);                         \
    } while(0)


/**
 * unpack and store a data "blob" from a peer so that the individual
 * elements can later be retrieved. This is an optimization path to
 * avoid repeatedly storing pmix_kval_t's for multiple local procs
 * from the same nspace.
 *
 * ranks - a list of pmix_rank_info_t for the local ranks from this
 *         nspace - this is to be used to filter the cbs list
 *
 * cbs - a list of pmix_server_caddy_t's that contain the pmix_peer_t
 *       pointers of the local participants. The list can be used to
 *       identify those participants corresponding to this nspace
 *       (and thus, GDS component)
 *
 * bo - pointer to the byte object containing the data
 *
 */
typedef pmix_status_t (*pmix_gds_base_module_store_modex_fn_t)(struct pmix_namespace_t *ns,
                                                               pmix_list_t *cbs,
                                                               pmix_buffer_t *buff);

/**
 * define a convenience macro for storing modex byte objects
 *
 * r - return status code
 *
 * n - pointer to the pmix_namespace_t this blob is to be stored for
 *
 * l - pointer to pmix_list_t containing pmix_server_caddy_t objects
 *     of the local_cbs of the collective tracker
 *
 * b - pointer to pmix_byte_object_t containing the data
 */
#define PMIX_GDS_STORE_MODEX(r, n, l, b)  \
    do {                                                                    \
        pmix_output_verbose(1, pmix_gds_base_output,                        \
                            "[%s:%d] GDS STORE MODEX WITH %s",              \
                            __FILE__, __LINE__, (n)->compat.gds->name);     \
        (r) = (n)->compat.gds->store_modex((struct pmix_namespace_t*)n, l, b); \
    } while (0)

/**
* fetch value corresponding to provided key from within the defined
* scope. A NULL key returns all values committed by the given peer
* for that scope.
*
* @param proc    namespace and rank whose info is being requested
*
* @param key     key.
*
* @param scope   scope of the data to be considered
*
* @param copy    true if the caller _requires_ a copy of the data. This
*                is used when the requestor is off-node. If
*                set to false, then the GDS component can provide
*                either a copy of the data, or shmem contact info
*                to the location of the data
*
* @param info    array of pmix_info_t the caller provided as
*                qualifiers to guide the request
*
* @param ninfo   number of elements in the info array
*
* @param kvs     pointer to a list that will be populated with the
*                returned pmix_kval_t data
*
* @return       PMIX_SUCCESS on success.
*
* Note: all available job-level data for a given nspace can be fetched
* by passing a proc with rank=PMIX_RANK_WILDCARD and a NULL key. Similarly,
* passing a NULL key for a non-wildcard rank will return all data "put"
* by that rank. Scope is ignored for job-level data requests.
*
* When a specific rank if provided with a NULL key, then data for only
* that rank is returned. If the scope is PMIX_LOCAL, then the returned
* data shall include only data that was specifically "put" to local scope,
* plus any data that was put to PMIX_GLOBAL scope. Similarly, a scope of
* PMIX_REMOTE will return data that was "put" to remote scope, plus
* any data that was put to PMIX_GLOBAL scope. A scope of PMIX_GLOBAL
* will return LOCAL, REMOTE, and GLOBAL data.
*
* Data stored with PMIX_INTERNAL scope can be retrieved with that scope.
*/
typedef pmix_status_t (*pmix_gds_base_module_fetch_fn_t)(const pmix_proc_t *proc,
                                                         pmix_scope_t scope, bool copy,
                                                         const char *key,
                                                         pmix_info_t info[], size_t ninfo,
                                                         pmix_list_t *kvs);

/* define a convenience macro for fetch key-val pairs based on peer,
 * passing a pmix_cb_t containing all the required info */
#define PMIX_GDS_FETCH_KV(s, p, c)      \
    do {                                                    \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds; \
        pmix_output_verbose(1, pmix_gds_base_output,        \
                            "[%s:%d] GDS FETCH KV WITH %s", \
                            __FILE__, __LINE__, _g->name);  \
        (s) = _g->fetch((c)->proc, (c)->scope, (c)->copy,   \
                        (c)->key, (c)->info, (c)->ninfo,    \
                        &(c)->kvs);                         \
    } while(0)


/**
* Add any envars to a peer's environment that the module needs
* to communicate. The API stub will rotate across all active modules, giving
* each a chance to contribute
*
* @return PMIX_SUCCESS on success.
*/
typedef pmix_status_t (*pmix_gds_base_module_setup_fork_fn_t)(const pmix_proc_t *proc,
                                                              char ***env);

/**
* Define a new nspace in the GDS
*
* @param nspace   namespace string
*
* @return PMIX_SUCCESS on success.
*/
typedef pmix_status_t (*pmix_gds_base_module_add_nspace_fn_t)(const char *nspace,
                                                              pmix_info_t info[],
                                                              size_t ninfo);

/* define a convenience macro for add_nspace based on peer */
#define PMIX_GDS_ADD_NSPACE(s, n, i, ni)                    \
    do {                                                    \
        pmix_gds_base_active_module_t *_g;                  \
        pmix_status_t _s = PMIX_SUCCESS;                    \
        (s) = PMIX_SUCCESS;                                 \
        pmix_output_verbose(1, pmix_gds_base_output,        \
                            "[%s:%d] GDS ADD NSPACE %s",    \
                            __FILE__, __LINE__, (n));       \
        PMIX_LIST_FOREACH(_g, &pmix_gds_globals.actives,    \
                          pmix_gds_base_active_module_t) {  \
            if (NULL != _g->module->add_nspace) {           \
                _s = _g->module->add_nspace(n, i, ni);      \
            }                                               \
            if (PMIX_SUCCESS != _s) {                       \
                (s) = PMIX_ERROR;                           \
            }                                               \
        }                                                   \
    } while(0)


/**
* Delete nspace and its associated data
*
* @param nspace   namespace string
*
* @return PMIX_SUCCESS on success.
*/
typedef pmix_status_t (*pmix_gds_base_module_del_nspace_fn_t)(const char* nspace);

/* define a convenience macro for del_nspace based on peer */
#define PMIX_GDS_DEL_NSPACE(s, n)                           \
    do {                                                    \
        pmix_gds_base_active_module_t *_g;                  \
        pmix_status_t _s = PMIX_SUCCESS;                    \
        (s) = PMIX_SUCCESS;                                 \
        pmix_output_verbose(1, pmix_gds_base_output,        \
                            "[%s:%d] GDS DEL NSPACE %s",    \
                            __FILE__, __LINE__, (n));       \
        PMIX_LIST_FOREACH(_g, &pmix_gds_globals.actives,    \
                          pmix_gds_base_active_module_t) {  \
            if (NULL != _g->module->del_nspace) {           \
                _s = _g->module->del_nspace(n);             \
            }                                               \
            if (PMIX_SUCCESS != _s) {                       \
                (s) = PMIX_ERROR;                           \
            }                                               \
        }                                                   \
    } while(0)

/* define a convenience macro for is_tsafe for fetch operation */
#define PMIX_GDS_FETCH_IS_TSAFE(s, p)                       \
    do {                                                    \
        pmix_gds_base_module_t *_g = (p)->nptr->compat.gds; \
        pmix_output_verbose(1, pmix_gds_base_output,        \
                "[%s:%d] GDS FETCH IS THREAD SAFE WITH %s", \
                            __FILE__, __LINE__, _g->name);  \
        if (true == _g->is_tsafe) {                         \
            (s) = PMIX_SUCCESS;                             \
        } else {                                            \
            (s) = PMIX_ERR_NOT_SUPPORTED;                   \
        }                                                   \
} while(0)

/**
* structure for gds modules
*/
typedef struct {
    const char *name;
    const bool is_tsafe;
    pmix_gds_base_module_init_fn_t                  init;
    pmix_gds_base_module_fini_fn_t                  finalize;
    pmix_gds_base_assign_module_fn_t                assign_module;
    pmix_gds_base_module_cache_job_info_fn_t        cache_job_info;
    pmix_gds_base_module_register_job_info_fn_t     register_job_info;
    pmix_gds_base_module_store_job_info_fn_t        store_job_info;
    pmix_gds_base_module_store_fn_t                 store;
    pmix_gds_base_module_store_modex_fn_t           store_modex;
    pmix_gds_base_module_fetch_fn_t                 fetch;
    pmix_gds_base_module_setup_fork_fn_t            setup_fork;
    pmix_gds_base_module_add_nspace_fn_t            add_nspace;
    pmix_gds_base_module_del_nspace_fn_t            del_nspace;
    pmix_gds_base_module_assemb_kvs_req_fn_t        assemb_kvs_req;
    pmix_gds_base_module_accept_kvs_resp_fn_t       accept_kvs_resp;

} pmix_gds_base_module_t;

/* NOTE: there is no public GDS interface structure - all access is
 * done directly to/from an assigned module */

/* define the component structure */
struct pmix_gds_base_component_t {
    pmix_mca_base_component_t                       base;
    pmix_mca_base_component_data_t                  data;
    int                                             priority;
};
typedef struct pmix_gds_base_component_t pmix_gds_base_component_t;


/*
 * Macro for use in components that are of type gds
 */
#define PMIX_GDS_BASE_VERSION_1_0_0 \
    PMIX_MCA_BASE_VERSION_1_0_0("gds", 1, 0, 0)

END_C_DECLS

#endif
