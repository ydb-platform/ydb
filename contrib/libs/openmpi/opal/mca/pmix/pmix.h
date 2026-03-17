/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_PMIX_H
#define OPAL_PMIX_H

#include "opal_config.h"
#include "opal/types.h"

#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif

#include "opal/mca/mca.h"
#include "opal/mca/event/event.h"
#include "opal/dss/dss.h"
#include "opal/runtime/opal.h"
#include "opal/dss/dss.h"
#include "opal/util/error.h"
#include "opal/util/proc.h"
#include "opal/hash_string.h"

#include "opal/mca/pmix/pmix_types.h"
#include "opal/mca/pmix/pmix_server.h"

BEGIN_C_DECLS

/* provide access to the framework verbose output without
 * exposing the entire base */
extern int opal_pmix_verbose_output;
extern bool opal_pmix_collect_all_data;
extern bool opal_pmix_base_async_modex;
extern int opal_pmix_base_exchange(opal_value_t *info,
                                   opal_pmix_pdata_t *pdat,
                                   int timeout);

/*
 * Count the fash for the the external RM
 */
#define OPAL_HASH_JOBID( str, hash ){               \
    OPAL_HASH_STR( str, hash );                     \
    hash &= ~(0x8000);                              \
}

/**
 * Provide a simplified macro for sending data via modex
 * to other processes. The macro requires four arguments:
 *
 * r - the integer return status from the modex op
 * sc - the PMIX scope of the data
 * s - the key to tag the data being posted
 * d - pointer to the data object being posted
 * t - the type of the data
 */
#define OPAL_MODEX_SEND_VALUE(r, sc, s, d, t)                            \
    do {                                                                 \
        opal_value_t _kv;                                                \
        OBJ_CONSTRUCT(&(_kv), opal_value_t);                             \
        _kv.key = (s);                                                   \
        if (OPAL_SUCCESS != ((r) = opal_value_load(&(_kv), (d), (t)))) { \
            OPAL_ERROR_LOG((r));                                         \
        } else {                                                         \
            if (OPAL_SUCCESS != ((r) = opal_pmix.put(sc, &(_kv)))) {     \
                OPAL_ERROR_LOG((r));                                     \
            }                                                            \
        }                                                                \
        /* opal_value_load makes a copy of the data, so release it */    \
        _kv.key = NULL;                                                  \
        OBJ_DESTRUCT(&(_kv));                                            \
    } while(0);

/**
 * Provide a simplified macro for sending data via modex
 * to other processes. The macro requires four arguments:
 *
 * r - the integer return status from the modex op
 * sc - the PMIX scope of the data
 * s - the key to tag the data being posted
 * d - the data object being posted
 * sz - the number of bytes in the data object
 */
#define OPAL_MODEX_SEND_STRING(r, sc, s, d, sz)                  \
    do {                                                         \
        opal_value_t _kv;                                        \
        OBJ_CONSTRUCT(&(_kv), opal_value_t);                     \
        _kv.key = (s);                                           \
        _kv.type = OPAL_BYTE_OBJECT;                             \
        _kv.data.bo.bytes = (uint8_t*)(d);                       \
        _kv.data.bo.size = (sz);                                 \
        if (OPAL_SUCCESS != ((r) = opal_pmix.put(sc, &(_kv)))) { \
            OPAL_ERROR_LOG((r));                                 \
        }                                                        \
        _kv.data.bo.bytes = NULL; /* protect the data */         \
        _kv.key = NULL;  /* protect the key */                   \
        OBJ_DESTRUCT(&(_kv));                                    \
    } while(0);

/**
 * Provide a simplified macro for sending data via modex
 * to other processes. The macro requires four arguments:
 *
 * r - the integer return status from the modex op
 * sc - the PMIX scope of the data
 * s - the MCA component that is posting the data
 * d - the data object being posted
 * sz - the number of bytes in the data object
 */
#define OPAL_MODEX_SEND(r, sc, s, d, sz)                        \
    do {                                                        \
        char *_key;                                             \
        _key = mca_base_component_to_string((s));               \
        OPAL_MODEX_SEND_STRING((r), (sc), _key, (d), (sz));     \
        free(_key);                                             \
    } while(0);

/**
 * Provide a simplified macro for retrieving modex data
 * from another process when we don't want the PMIx module
 * to request it from the server if not found:
 *
 * r - the integer return status from the modex op (int)
 * s - string key (char*)
 * p - pointer to the opal_process_name_t of the proc that posted
 *     the data (opal_process_name_t*)
 * d - pointer to a location wherein the data object
 *     is to be returned
 * t - the expected data type
 */
#define OPAL_MODEX_RECV_VALUE_OPTIONAL(r, s, p, d, t)                                    \
    do {                                                                                 \
        opal_value_t *_kv, *_info;                                                       \
        opal_list_t _ilist;                                                              \
        OPAL_OUTPUT_VERBOSE((1, opal_pmix_verbose_output,                                \
                            "%s[%s:%d] MODEX RECV VALUE OPTIONAL FOR PROC %s KEY %s",    \
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),                          \
                            __FILE__, __LINE__,                                          \
                            OPAL_NAME_PRINT(*(p)), (s)));                                \
        OBJ_CONSTRUCT(&(_ilist), opal_list_t);                                           \
        _info = OBJ_NEW(opal_value_t);                                                   \
        _info->key = strdup(OPAL_PMIX_OPTIONAL);                                         \
        _info->type = OPAL_BOOL;                                                         \
        _info->data.flag = true;                                                         \
        opal_list_append(&(_ilist), &(_info)->super);                                    \
        if (OPAL_SUCCESS == ((r) = opal_pmix.get((p), (s), &(_ilist), &(_kv)))) {        \
            if (NULL == _kv) {                                                           \
                (r) = OPAL_ERR_NOT_FOUND;                                                \
            } else {                                                                     \
                (r) = opal_value_unload(_kv, (void**)(d), (t));                          \
                OBJ_RELEASE(_kv);                                                        \
            }                                                                            \
        }                                                                                \
        OPAL_LIST_DESTRUCT(&(_ilist));                                                   \
    } while(0);

/**
 * Provide a simplified macro for retrieving modex data
 * from another process when we want the PMIx module
 * to request it from the server if not found, but do not
 * want the server to go find it if the server doesn't
 * already have it:
 *
 * r - the integer return status from the modex op (int)
 * s - string key (char*)
 * p - pointer to the opal_process_name_t of the proc that posted
 *     the data (opal_process_name_t*)
 * d - pointer to a location wherein the data object
 *     is to be returned
 * t - the expected data type
 */
#define OPAL_MODEX_RECV_VALUE_IMMEDIATE(r, s, p, d, t)                                   \
    do {                                                                                 \
        opal_value_t *_kv, *_info;                                                       \
        opal_list_t _ilist;                                                              \
        opal_output_verbose(1, opal_pmix_verbose_output,                                \
                            "%s[%s:%d] MODEX RECV VALUE IMMEDIATE FOR PROC %s KEY %s",   \
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),                          \
                            __FILE__, __LINE__,                                          \
                            OPAL_NAME_PRINT(*(p)), (s));                                \
        OBJ_CONSTRUCT(&(_ilist), opal_list_t);                                           \
        _info = OBJ_NEW(opal_value_t);                                                   \
        _info->key = strdup(OPAL_PMIX_IMMEDIATE);                                        \
        _info->type = OPAL_BOOL;                                                         \
        _info->data.flag = true;                                                         \
        opal_list_append(&(_ilist), &(_info)->super);                                    \
        if (OPAL_SUCCESS == ((r) = opal_pmix.get((p), (s), &(_ilist), &(_kv)))) {        \
            if (NULL == _kv) {                                                           \
                (r) = OPAL_ERR_NOT_FOUND;                                                \
            } else {                                                                     \
                (r) = opal_value_unload(_kv, (void**)(d), (t));                          \
                OBJ_RELEASE(_kv);                                                        \
            }                                                                            \
        }                                                                                \
        OPAL_LIST_DESTRUCT(&(_ilist));                                                   \
    } while(0);

/**
 * Provide a simplified macro for retrieving modex data
 * from another process:
 *
 * r - the integer return status from the modex op (int)
 * s - string key (char*)
 * p - pointer to the opal_process_name_t of the proc that posted
 *     the data (opal_process_name_t*)
 * d - pointer to a location wherein the data object
 *     is to be returned
 * t - the expected data type
 */
#define OPAL_MODEX_RECV_VALUE(r, s, p, d, t)                                    \
    do {                                                                        \
        opal_value_t *_kv;                                                      \
        OPAL_OUTPUT_VERBOSE((1, opal_pmix_verbose_output,                       \
                            "%s[%s:%d] MODEX RECV VALUE FOR PROC %s KEY %s",    \
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),                 \
                            __FILE__, __LINE__,                                 \
                            OPAL_NAME_PRINT(*(p)), (s)));                       \
        if (OPAL_SUCCESS == ((r) = opal_pmix.get((p), (s), NULL, &(_kv)))) {    \
            if (NULL == _kv) {                                                  \
                (r) = OPAL_ERR_NOT_FOUND;                                       \
            } else {                                                            \
                (r) = opal_value_unload(_kv, (void**)(d), (t));                 \
                OBJ_RELEASE(_kv);                                               \
           }                                                                    \
        }                                                                       \
    } while(0);

/**
 * Provide a simplified macro for retrieving modex data
 * from another process:
 *
 * r - the integer return status from the modex op (int)
 * s - string key (char*)
 * p - pointer to the opal_process_name_t of the proc that posted
 *     the data (opal_process_name_t*)
 * d - pointer to a location wherein the data object
 *     it to be returned (char**)
 * sz - pointer to a location wherein the number of bytes
 *     in the data object can be returned (size_t)
 */
#define OPAL_MODEX_RECV_STRING(r, s, p, d, sz)                                  \
    do {                                                                        \
        opal_value_t *_kv;                                                      \
        OPAL_OUTPUT_VERBOSE((1, opal_pmix_verbose_output,                       \
                            "%s[%s:%d] MODEX RECV STRING FOR PROC %s KEY %s",   \
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),                 \
                            __FILE__, __LINE__,                                 \
                            OPAL_NAME_PRINT(*(p)), (s)));                       \
        if (OPAL_SUCCESS == ((r) = opal_pmix.get((p), (s), NULL, &(_kv)))) {    \
            if (NULL == _kv) {                                                  \
                *(sz) = 0;                                                      \
                (r) = OPAL_ERR_NOT_FOUND;                                       \
            } else {                                                            \
                *(d) = _kv->data.bo.bytes;                                      \
                *(sz) = _kv->data.bo.size;                                      \
                _kv->data.bo.bytes = NULL; /* protect the data */               \
                OBJ_RELEASE(_kv);                                               \
            }                                                                   \
        } else {                                                                \
            *(sz) = 0;                                                          \
            (r) = OPAL_ERR_NOT_FOUND;                                           \
        }                                                                       \
    } while(0);

/**
 * Provide a simplified macro for retrieving modex data
 * from another process:
 *
 * r - the integer return status from the modex op (int)
 * s - the MCA component that posted the data (mca_base_component_t*)
 * p - pointer to the opal_process_name_t of the proc that posted
 *     the data (opal_process_name_t*)
 * d - pointer to a location wherein the data object
 *     it to be returned (char**)
 * sz - pointer to a location wherein the number of bytes
 *     in the data object can be returned (size_t)
 */
#define OPAL_MODEX_RECV(r, s, p, d, sz)                                 \
    do {                                                                \
        char *_key;                                                     \
        _key = mca_base_component_to_string((s));                       \
        OPAL_OUTPUT_VERBOSE((1, opal_pmix_verbose_output,               \
                            "%s[%s:%d] MODEX RECV FOR PROC %s KEY %s",  \
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),         \
                            __FILE__, __LINE__,                         \
                            OPAL_NAME_PRINT(*(p)), _key));              \
        if (NULL == _key) {                                             \
            OPAL_ERROR_LOG(OPAL_ERR_OUT_OF_RESOURCE);                   \
            (r) = OPAL_ERR_OUT_OF_RESOURCE;                             \
        } else {                                                        \
            OPAL_MODEX_RECV_STRING((r), _key, (p), (d), (sz));          \
            free(_key);                                                 \
        }                                                               \
    } while(0);

/**
 * Provide a macro for accessing a base function that exchanges
 * data values between two procs using the PMIx Publish/Lookup
 * APIs */
 #define OPAL_PMIX_EXCHANGE(r, i, p, t)                          \
    do {                                                         \
        OPAL_OUTPUT_VERBOSE((1, opal_pmix_verbose_output,        \
                            "%s[%s:%d] EXCHANGE %s WITH %s",     \
                            OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),  \
                            __FILE__, __LINE__,                  \
                            (i)->key, (p)->value.key));          \
        (r) = opal_pmix_base_exchange((i), (p), (t));            \
    } while(0);


/************************************************************
 *                       CLIENT APIs                        *
 ************************************************************/

/* Initialize the PMIx client
 * When called the client will check for the required connection
 * information of the local server and will establish the connection.
 * If the information is not found, or the server connection fails, then
 * an appropriate error constant will be returned.
 */
typedef int (*opal_pmix_base_module_init_fn_t)(opal_list_t *ilist);

/* Finalize the PMIx client, closing the connection to the local server.
 * An error code will be returned if, for some reason, the connection
 * cannot be closed. */
typedef int (*opal_pmix_base_module_fini_fn_t)(void);

/* Returns _true_ if the PMIx client has been successfully initialized,
 * returns _false_ otherwise. Note that the function only reports the
 * internal state of the PMIx client - it does not verify an active
 * connection with the server, nor that the server is functional. */
typedef int (*opal_pmix_base_module_initialized_fn_t)(void);

/* Request that the provided list of opal_namelist_t procs be aborted, returning the
 * provided _status_ and printing the provided message. A _NULL_
 * for the proc list indicates that all processes in the caller's
 * nspace are to be aborted.
 *
 * The response to this request is somewhat dependent on the specific resource
 * manager and its configuration (e.g., some resource managers will
 * not abort the application if the provided _status_ is zero unless
 * specifically configured to do so), and thus lies outside the control
 * of PMIx itself. However, the client will inform the RM of
 * the request that the application be aborted, regardless of the
 * value of the provided _status_.
 *
 * Passing a _NULL_ msg parameter is allowed. Note that race conditions
 * caused by multiple processes calling PMIx_Abort are left to the
 * server implementation to resolve with regard to which status is
 * returned and what messages (if any) are printed.
 */
typedef int (*opal_pmix_base_module_abort_fn_t)(int status, const char *msg,
                                                opal_list_t *procs);

/* Push all previously _PMIx_Put_ values to the local PMIx server.
 * This is an asynchronous operation - the library will immediately
 * return to the caller while the data is transmitted to the local
 * server in the background */
typedef int (*opal_pmix_base_module_commit_fn_t)(void);

/* Execute a blocking barrier across the processes identified in the
 * specified list of opal_namelist_t. Passing a _NULL_ pointer
 * indicates that the barrier is to span all processes in the client's
 * namespace. Each provided opal_namelist_t can pass PMIX_RANK_WILDCARD to
 * indicate that all processes in the given jobid are
 * participating.
 *
 * The _collect_data_ parameter is passed to the server to indicate whether
 * or not the barrier operation is to return the _put_ data from all
 * participating processes. A value of _false_ indicates that the callback
 * is just used as a release and no data is to be returned at that time. A
 * value of _true_ indicates that all _put_ data is to be collected by the
 * barrier. Returned data is locally cached so that subsequent calls to _PMIx_Get_
 * can be serviced without communicating to/from the server, but at the cost
 * of increased memory footprint
 */
typedef int (*opal_pmix_base_module_fence_fn_t)(opal_list_t *procs, int collect_data);

/* Fence_nb */
/* Non-blocking version of PMIx_Fence. Note that the function will return
 * an error if a _NULL_ callback function is given. */
typedef int (*opal_pmix_base_module_fence_nb_fn_t)(opal_list_t *procs, int collect_data,
                                                   opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Push a value into the client's namespace. The client library will cache
 * the information locally until _PMIx_Commit_ is called. The provided scope
 * value is passed to the local PMIx server, which will distribute the data
 * as directed. */
typedef int (*opal_pmix_base_module_put_fn_t)(opal_pmix_scope_t scope,
                                              opal_value_t *val);

/* Retrieve information for the specified _key_ as published by the rank
 * and jobid i the provided opal_process_name, and subject to any provided
 * constraints, returning a pointer to the value in the given address.
 *
 * This is a blocking operation - the caller will block until
 * the specified data has been _PMIx_Put_ by the specified rank. The caller is
 * responsible for freeing all memory associated with the returned value when
 * no longer required. */
typedef int (*opal_pmix_base_module_get_fn_t)(const opal_process_name_t *proc,
                                              const char *key, opal_list_t *info,
                                              opal_value_t **val);

/* Retrieve information for the specified _key_ as published by the given rank
 * and jobid in the opal_process_name_t, and subject to any provided
 * constraints. This is a non-blocking operation - the
 * callback function will be executed once the specified data has been _PMIx_Put_
 * by the specified proc and retrieved by the local server. */
typedef int (*opal_pmix_base_module_get_nb_fn_t)(const opal_process_name_t *proc,
                                                 const char *key, opal_list_t *info,
                                                 opal_pmix_value_cbfunc_t cbfunc, void *cbdata);

/* Publish the given data to the "universal" nspace
 * for lookup by others subject to the provided scope.
 * Note that the keys must be unique within the specified
 * scope or else an error will be returned (first published
 * wins). Attempts to access the data by procs outside of
 * the provided scope will be rejected.
 *
 * Note: Some host environments may support user/group level
 * access controls on the information in addition to the scope.
 * These can be specified in the info array using the appropriately
 * defined keys.
 *
 * The persistence parameter instructs the server as to how long
 * the data is to be retained, within the context of the scope.
 * For example, data published within _PMIX_NAMESPACE_ will be
 * deleted along with the namespace regardless of the persistence.
 * However, data published within PMIX_USER would be retained if
 * the persistence was set to _PMIX_PERSIST_SESSION_ until the
 * allocation terminates.
 *
 * The blocking form will block until the server confirms that the
 * data has been posted and is available. The non-blocking form will
 * return immediately, executing the callback when the server confirms
 * availability of the data */
typedef int (*opal_pmix_base_module_publish_fn_t)(opal_list_t *info);
typedef int (*opal_pmix_base_module_publish_nb_fn_t)(opal_list_t *info,
                                                     opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Lookup information published by another process within the
 * specified scope. A scope of _PMIX_SCOPE_UNDEF_ requests that
 * the search be conducted across _all_ namespaces. The "data"
 * parameter consists of an array of pmix_pdata_t struct with the
 * keys specifying the requested information. Data will be returned
 * for each key in the associated info struct - any key that cannot
 * be found will return with a data type of "PMIX_UNDEF". The function
 * will return SUCCESS if _any_ values can be found, so the caller
 * must check each data element to ensure it was returned.
 *
 * The proc field in each pmix_pdata_t struct will contain the
 * nspace/rank of the process that published the data.
 *
 * Note: although this is a blocking function, it will _not_ wait
 * for the requested data to be published. Instead, it will block
 * for the time required by the server to lookup its current data
 * and return any found items. Thus, the caller is responsible for
 * ensuring that data is published prior to executing a lookup, or
 * for retrying until the requested data is found */
typedef int (*opal_pmix_base_module_lookup_fn_t)(opal_list_t *data,
                                                 opal_list_t *info);

/* Non-blocking form of the _PMIx_Lookup_ function. Data for
 * the provided NULL-terminated keys array will be returned
 * in the provided callback function. The _wait_ parameter
 * is used to indicate if the caller wishes the callback to
 * wait for _all_ requested data before executing the callback
 * (_true_), or to callback once the server returns whatever
 * data is immediately available (_false_) */
typedef int (*opal_pmix_base_module_lookup_nb_fn_t)(char **keys, opal_list_t *info,
                                                    opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata);

/* Unpublish data posted by this process using the given keys
 * within the specified scope. The function will block until
 * the data has been removed by the server. A value of _NULL_
 * for the keys parameter instructs the server to remove
 * _all_ data published by this process within the given scope */
typedef int (*opal_pmix_base_module_unpublish_fn_t)(char **keys, opal_list_t *info);

/* Non-blocking form of the _PMIx_Unpublish_ function. The
 * callback function will be executed once the server confirms
 * removal of the specified data. A value of _NULL_
 * for the keys parameter instructs the server to remove
 * _all_ data published by this process within the given scope  */
typedef int (*opal_pmix_base_module_unpublish_nb_fn_t)(char **keys, opal_list_t *info,
                                                       opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Spawn a new job. The spawned applications are automatically
 * connected to the calling process, and their assigned namespace
 * is returned in the nspace parameter - a _NULL_ value in that
 * location indicates that the caller doesn't wish to have the
 * namespace returned. Behavior of individual resource managers
 * may differ, but it is expected that failure of any application
 * process to start will result in termination/cleanup of _all_
 * processes in the newly spawned job and return of an error
 * code to the caller */
typedef int (*opal_pmix_base_module_spawn_fn_t)(opal_list_t *job_info,
                                                opal_list_t *apps,
                                                opal_jobid_t *jobid);

/* Non-blocking form of the _PMIx_Spawn_ function. The callback
 * will be executed upon launch of the specified applications,
 * or upon failure to launch any of them. */
typedef int (*opal_pmix_base_module_spawn_nb_fn_t)(opal_list_t *job_info,
                                                   opal_list_t *apps,
                                                   opal_pmix_spawn_cbfunc_t cbfunc,
                                                   void *cbdata);

/* Record the specified processes as "connected". Both blocking and non-blocking
 * versions are provided. This means that the resource manager should treat the
 * failure of any process in the specified group as a reportable event, and take
 * appropriate action. Note that different resource managers may respond to
 * failures in different manners.
 *
 * The list is to be provided as opal_namelist_t objects
 *
 * The callback function is to be called once all participating processes have
 * called connect. The server is required to return any job-level info for the
 * connecting processes that might not already have - i.e., if the connect
 * request involves procs from different nspaces, then each proc shall receive
 * the job-level info from those nspaces other than their own.
 *
 * Note: a process can only engage in _one_ connect operation involving the identical
 * set of ranges at a time. However, a process _can_ be simultaneously engaged
 * in multiple connect operations, each involving a different set of ranges */
typedef int (*opal_pmix_base_module_connect_fn_t)(opal_list_t *procs);

typedef int (*opal_pmix_base_module_connect_nb_fn_t)(opal_list_t *procs,
                                                     opal_pmix_op_cbfunc_t cbfunc,
                                                     void *cbdata);

/* Disconnect a previously connected set of processes. An error will be returned
 * if the specified set of procs was not previously "connected". As above, a process
 * may be involved in multiple simultaneous disconnect operations. However, a process
 * is not allowed to reconnect to a set of procs that has not fully completed
 * disconnect - i.e., you have to fully disconnect before you can reconnect to the
 * _same_ group of processes. */
typedef int (*opal_pmix_base_module_disconnect_fn_t)(opal_list_t *procs);

typedef int (*opal_pmix_base_module_disconnect_nb_fn_t)(opal_list_t *procs,
                                                        opal_pmix_op_cbfunc_t cbfunc,
                                                        void *cbdata);

/* Given a node name, return an array of processes within the specified jobid
 * on that node. If the jobid is OPAL_JOBID_WILDCARD, then all processes on the node will
 * be returned. If the specified node does not currently host any processes,
 * then the returned list will be empty.
 */
typedef int (*opal_pmix_base_module_resolve_peers_fn_t)(const char *nodename,
                                                        opal_jobid_t jobid,
                                                        opal_list_t *procs);


/* Given a jobid, return the list of nodes hosting processes within
 * that jobid. The returned string will contain a comma-delimited list
 * of nodenames. The caller is responsible for releasing the string
 * when done with it */
typedef int (*opal_pmix_base_module_resolve_nodes_fn_t)(opal_jobid_t jobid, char **nodelist);


/************************************************************
 *                       SERVER APIs                        *
 *                                                          *
 * These are calls that go down (or "south") from the ORTE  *
 * daemon into the PMIx server library                      *
 ************************************************************/

/* Initialize the server support library - must pass the callback
 * module for the server to use, plus any attributes we want to
 * pass down to it */
typedef int (*opal_pmix_base_module_server_init_fn_t)(opal_pmix_server_module_t *module,
                                                      opal_list_t *info);

/* Finalize the server support library */
typedef int (*opal_pmix_base_module_server_finalize_fn_t)(void);

/* given a semicolon-separated list of input values, generate
 * a regex that can be passed down to the client for parsing.
 * The caller is responsible for free'ing the resulting
 * string
 *
 * If values have leading zero's, then that is preserved. You
 * have to add back any prefix/suffix for node names
 * odin[009-015,017-023,076-086]
 *
 *     "pmix:odin[009-015,017-023,076-086]"
 *
 * Note that the "pmix" at the beginning of each regex indicates
 * that the PMIx native parser is to be used by the client for
 * parsing the provided regex. Other parsers may be supported - see
 * the pmix_client.h header for a list.
 */
typedef int (*opal_pmix_base_module_generate_regex_fn_t)(const char *input, char **regex);

/* The input is expected to consist of a comma-separated list
 * of ranges. Thus, an input of:
 *     "1-4;2-5;8,10,11,12;6,7,9"
 * would generate a regex of
 *     "[pmix:2x(3);8,10-12;6-7,9]"
 *
 * Note that the "pmix" at the beginning of each regex indicates
 * that the PMIx native parser is to be used by the client for
 * parsing the provided regex. Other parsers may be supported - see
 * the pmix_client.h header for a list.
 */
typedef int (*opal_pmix_base_module_generate_ppn_fn_t)(const char *input, char **ppn);

/* Setup the data about a particular nspace so it can
 * be passed to any child process upon startup. The PMIx
 * connection procedure provides an opportunity for the
 * host PMIx server to pass job-related info down to a
 * child process. This might include the number of
 * processes in the job, relative local ranks of the
 * processes within the job, and other information of
 * use to the process. The server is free to determine
 * which, if any, of the supported elements it will
 * provide - defined values are provided in pmix_common.h.
 *
 * NOTE: the server must register ALL nspaces that will
 * participate in collective operations with local processes.
 * This means that the server must register an nspace even
 * if it will not host any local procs from within that
 * nspace IF any local proc might at some point perform
 * a collective operation involving one or more procs from
 * that nspace. This is necessary so that the collective
 * operation can know when it is locally complete.
 *
 * The caller must also provide the number of local procs
 * that will be launched within this nspace. This is required
 * for the PMIx server library to correctly handle collectives
 * as a collective operation call can occur before all the
 * procs have been started */
typedef int (*opal_pmix_base_module_server_register_nspace_fn_t)(opal_jobid_t jobid,
                                                                 int nlocalprocs,
                                                                 opal_list_t *info,
                                                                 opal_pmix_op_cbfunc_t cbfunc,
                                                                 void *cbdata);

/* Deregister an nspace. Instruct the PMIx server to purge
 * all info relating to the provided jobid so that memory
 * can be freed. Note that the server will automatically
 * purge all info relating to any clients it has from
 * this nspace */
typedef void (*opal_pmix_base_module_server_deregister_nspace_fn_t)(opal_jobid_t jobid,
                                                                    opal_pmix_op_cbfunc_t cbfunc,
                                                                    void *cbdata);

/* Register a client process with the PMIx server library. The
 * expected user ID and group ID of the child process helps the
 * server library to properly authenticate clients as they connect
 * by requiring the two values to match.
 *
 * The host server can also, if it desires, provide an object
 * it wishes to be returned when a server function is called
 * that relates to a specific process. For example, the host
 * server may have an object that tracks the specific client.
 * Passing the object to the library allows the library to
 * return that object when the client calls "finalize", thus
 * allowing the host server to access the object without
 * performing a lookup. */
typedef int (*opal_pmix_base_module_server_register_client_fn_t)(const opal_process_name_t *proc,
                                                                 uid_t uid, gid_t gid,
                                                                 void *server_object,
                                                                 opal_pmix_op_cbfunc_t cbfunc,
                                                                 void *cbdata);

/* Deregister a client. Instruct the PMIx server to purge
 * all info relating to the provided client so that memory
 * can be freed. As per above note, the server will automatically
 * free all client-related data when the nspace is deregistered,
 * so there is no need to call this function during normal
 * finalize operations. Instead, this is provided for use
 * during exception operations */
typedef void (*opal_pmix_base_module_server_deregister_client_fn_t)(const opal_process_name_t *proc,
                                                                    opal_pmix_op_cbfunc_t cbfunc,
                                                                    void *cbdata);

/* Setup the environment of a child process to be forked
 * by the host so it can correctly interact with the PMIx
 * server. The PMIx client needs some setup information
 * so it can properly connect back to the server. This function
 * will set appropriate environmental variables for this purpose. */
typedef int (*opal_pmix_base_module_server_setup_fork_fn_t)(const opal_process_name_t *proc, char ***env);

/* Define a function by which the host server can request modex data
 * from the local PMIx server. This is used to support the direct modex
 * operation - i.e., where data is cached locally on each PMIx
 * server for its own local clients, and is obtained on-demand
 * for remote requests. Upon receiving a request from a remote
 * server, the host server will call this function to pass the
 * request into the PMIx server. The PMIx server will return a blob
 * (once it becomes available) via the cbfunc - the host
 * server shall send the blob back to the original requestor */
typedef int (*opal_pmix_base_module_server_dmodex_request_fn_t)(const opal_process_name_t *proc,
                                                                opal_pmix_modex_cbfunc_t cbfunc,
                                                                void *cbdata);

/* Report an event to a process for notification via any
 * registered event handler. The handler registration can be
 * called by both the server and the client application. On the
 * server side, the handler is used to report events detected
 * by PMIx to the host server for handling. On the client side,
 * the handler is used to notify the process of events
 * reported by the server - e.g., the failure of another process.
 *
 * This function allows the host server to direct the server
 * convenience library to notify all registered local procs of
 * an event. The event can be local, or anywhere in the cluster.
 * The status indicates the event being reported.
 *
 * The source parameter informs the handler of the source that
 * generated the event. This will be NULL if the event came
 * from the external resource manager.
 *
 * The info array contains any further info the RM can and/or chooses
 * to provide.
 *
 * The callback function will be called upon completion of the
 * notify_event function's actions. Note that any messages will
 * have been queued, but may not have been transmitted by this
 * time. Note that the caller is required to maintain the input
 * data until the callback function has been executed if this
 * function returns OPAL_SUCCESS! */
typedef int (*opal_pmix_base_module_server_notify_event_fn_t)(int status,
                                                              const opal_process_name_t *source,
                                                              opal_list_t *info,
                                                              opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* push IO to local clients */
typedef int (*opal_pmix_base_module_server_push_io_fn_t)(const opal_process_name_t *source,
                                                         opal_pmix_iof_channel_t channel,
                                                         unsigned char *data, size_t nbytes);

/* define a callback function for the setup_application API. The returned info
 * array is owned by the PMIx server library and will be free'd when the
 * provided cbfunc is called. */
typedef void (*opal_pmix_setup_application_cbfunc_t)(int status,
                                                     opal_list_t *info,
                                                     void *provided_cbdata,
                                                     opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Provide a function by which we can request
 * any application-specific environmental variables prior to
 * launch of an application. For example, network libraries may
 * opt to provide security credentials for the application. This
 * is defined as a non-blocking operation in case network
 * libraries need to perform some action before responding. The
 * returned env will be distributed along with the application */
typedef int (*opal_pmix_server_setup_application_fn_t)(opal_jobid_t jobid,
                                                       opal_list_t *info,
                                                       opal_pmix_setup_application_cbfunc_t cbfunc, void *cbdata);

/* Provide a function by which the local PMIx server can perform
 * any application-specific operations prior to spawning local
 * clients of a given application. For example, a network library
 * might need to setup the local driver for "instant on" addressing.
 */
typedef int (*opal_pmix_server_setup_local_support_fn_t)(opal_jobid_t jobid,
                                                         opal_list_t *info,
                                                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata);


/************************************************************
 *                         TOOL APIs                        *
 ************************************************************/
/* Initialize the PMIx tool support
 * When called the library will check for the required connection
 * information of the local server and will establish the connection.
 * The connection info can be provided either in the environment or
 * in the list of attributes. If the information is not found, or the
 * server connection fails, then an appropriate error constant will
 * be returned.
 */
typedef int (*opal_pmix_base_module_tool_init_fn_t)(opal_list_t *ilist);

/* Finalize the PMIx tool support */
typedef int (*opal_pmix_base_module_tool_fini_fn_t)(void);


/************************************************************
 *                       UTILITY APIs                       *
 ************************************************************/

/* get the version of the embedded library */
typedef const char* (*opal_pmix_base_module_get_version_fn_t)(void);

/* Register an event handler to report event. Three types of events
 * can be reported:
 *
 * (a) those that occur within the client library, but are not
 *     reportable via the API itself (e.g., loss of connection to
 *     the server). These events typically occur during behind-the-scenes
 *     non-blocking operations.
 *
 * (b) job-related events such as the failure of another process in
 *     the job or in any connected job, impending failure of hardware
 *     within the job's usage footprint, etc.
 *
 * (c) system notifications that are made available by the local
 *     administrators
 *
 * By default, only events that directly affect the process and/or
 * any process to which it is connected (via the PMIx_Connect call)
 * will be reported. Options to modify that behavior can be provided
 * in the info array
 *
 * Both the client application and the resource manager can register
 * event handlers for specific events. PMIx client/server calls the registered
 * event handler upon receiving event notify notification (via PMIx_Notify_event)
 * from the other end (Resource Manager/Client application).
 *
 * Multiple event handlers can be registered for different events. PMIX returns
 * a size_t reference to each register handler in the callback fn. The caller
 * must retain the reference in order to deregister the evhandler.
 * Modification of the notification behavior can be accomplished by
 * deregistering the current evhandler, and then registering it
 * using a new set of info values.
 *
 * A NULL for event_codes indicates registration as a default event handler
 *
 * See pmix_types.h for a description of the notification function */
typedef void (*opal_pmix_base_module_register_fn_t)(opal_list_t *event_codes,
                                                    opal_list_t *info,
                                                    opal_pmix_notification_fn_t evhandler,
                                                    opal_pmix_evhandler_reg_cbfunc_t cbfunc,
                                                    void *cbdata);

/* deregister the evhandler
 * evhandler_ref is the reference returned by PMIx for the evhandler
 * to pmix_evhandler_reg_cbfunc_t */
typedef void (*opal_pmix_base_module_deregister_fn_t)(size_t evhandler,
                                                      opal_pmix_op_cbfunc_t cbfunc,
                                                      void *cbdata);

/* Report an event for notification via any
 * registered evhandler. On the PMIx
 * server side, this is used to report events detected
 * by PMIx to the host server for handling and/or distribution.
 *
 * The client application can also call this function to notify the
 * resource manager of an event it detected. It can specify the
 * range over which that notification should occur.
 *
 * The info array contains any further info the caller can and/or chooses
 * to provide.
 *
 * The callback function will be called upon completion of the
 * notify_event function's actions. Note that any messages will
 * have been queued, but may not have been transmitted by this
 * time. Note that the caller is required to maintain the input
 * data until the callback function has been executed if it
 * returns OPAL_SUCCESS!
*/
typedef int (*opal_pmix_base_module_notify_event_fn_t)(int status,
                                                       const opal_process_name_t *source,
                                                       opal_pmix_data_range_t range,
                                                       opal_list_t *info,
                                                       opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* store data internally, but don't push it out to be shared - this is
 * intended solely for storage of info on other procs that comes thru
 * a non-PMIx channel (e.g., may be computed locally) but is desired
 * to be available via a PMIx_Get call */
typedef int (*opal_pmix_base_module_store_fn_t)(const opal_process_name_t *proc,
                                                opal_value_t *val);

/* retrieve the nspace corresponding to a given jobid */
typedef const char* (*opal_pmix_base_module_get_nspace_fn_t)(opal_jobid_t jobid);

/* register a jobid-to-nspace pair */
typedef void (*opal_pmix_base_module_register_jobid_fn_t)(opal_jobid_t jobid, const char *nspace);

/* query information from the system */
typedef void (*opal_pmix_base_module_query_fn_t)(opal_list_t *queries,
                                                 opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* log data to the system */
typedef void (*opal_pmix_base_log_fn_t)(opal_list_t *info,
                                        opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* allocation */
typedef int (*opal_pmix_base_alloc_fn_t)(opal_pmix_alloc_directive_t directive,
                                         opal_list_t *info,
                                         opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* job control */
typedef int (*opal_pmix_base_job_control_fn_t)(opal_list_t *targets,
                                               opal_list_t *directives,
                                               opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* monitoring */
typedef int (*opal_pmix_base_process_monitor_fn_t)(opal_list_t *monitor,
                                                   opal_list_t *directives,
                                                   opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* register cleanup */
typedef int (*opal_pmix_base_register_cleanup_fn_t)(char *path, bool directory, bool ignore, bool jobscope);

typedef bool (*opal_pmix_base_legacy_get_fn_t)(void);

/*
 * the standard public API data structure
 */
typedef struct {
    opal_pmix_base_legacy_get_fn_t                          legacy_get;
    /* client APIs */
    opal_pmix_base_module_init_fn_t                         init;
    opal_pmix_base_module_fini_fn_t                         finalize;
    opal_pmix_base_module_initialized_fn_t                  initialized;
    opal_pmix_base_module_abort_fn_t                        abort;
    opal_pmix_base_module_commit_fn_t                       commit;
    opal_pmix_base_module_fence_fn_t                        fence;
    opal_pmix_base_module_fence_nb_fn_t                     fence_nb;
    opal_pmix_base_module_put_fn_t                          put;
    opal_pmix_base_module_get_fn_t                          get;
    opal_pmix_base_module_get_nb_fn_t                       get_nb;
    opal_pmix_base_module_publish_fn_t                      publish;
    opal_pmix_base_module_publish_nb_fn_t                   publish_nb;
    opal_pmix_base_module_lookup_fn_t                       lookup;
    opal_pmix_base_module_lookup_nb_fn_t                    lookup_nb;
    opal_pmix_base_module_unpublish_fn_t                    unpublish;
    opal_pmix_base_module_unpublish_nb_fn_t                 unpublish_nb;
    opal_pmix_base_module_spawn_fn_t                        spawn;
    opal_pmix_base_module_spawn_nb_fn_t                     spawn_nb;
    opal_pmix_base_module_connect_fn_t                      connect;
    opal_pmix_base_module_connect_nb_fn_t                   connect_nb;
    opal_pmix_base_module_disconnect_fn_t                   disconnect;
    opal_pmix_base_module_disconnect_nb_fn_t                disconnect_nb;
    opal_pmix_base_module_resolve_peers_fn_t                resolve_peers;
    opal_pmix_base_module_resolve_nodes_fn_t                resolve_nodes;
    opal_pmix_base_module_query_fn_t                        query;
    opal_pmix_base_log_fn_t                                 log;
    opal_pmix_base_alloc_fn_t                               allocate;
    opal_pmix_base_job_control_fn_t                         job_control;
    opal_pmix_base_process_monitor_fn_t                     monitor;
    opal_pmix_base_register_cleanup_fn_t                    register_cleanup;
    /* server APIs */
    opal_pmix_base_module_server_init_fn_t                  server_init;
    opal_pmix_base_module_server_finalize_fn_t              server_finalize;
    opal_pmix_base_module_generate_regex_fn_t               generate_regex;
    opal_pmix_base_module_generate_ppn_fn_t                 generate_ppn;
    opal_pmix_base_module_server_register_nspace_fn_t       server_register_nspace;
    opal_pmix_base_module_server_deregister_nspace_fn_t     server_deregister_nspace;
    opal_pmix_base_module_server_register_client_fn_t       server_register_client;
    opal_pmix_base_module_server_deregister_client_fn_t     server_deregister_client;
    opal_pmix_base_module_server_setup_fork_fn_t            server_setup_fork;
    opal_pmix_base_module_server_dmodex_request_fn_t        server_dmodex_request;
    opal_pmix_base_module_server_notify_event_fn_t          server_notify_event;
    opal_pmix_base_module_server_push_io_fn_t               server_iof_push;
    opal_pmix_server_setup_application_fn_t                 server_setup_application;
    opal_pmix_server_setup_local_support_fn_t               server_setup_local_support;
    /* tool APIs */
    opal_pmix_base_module_tool_init_fn_t                    tool_init;
    opal_pmix_base_module_tool_fini_fn_t                    tool_finalize;
    /* Utility APIs */
    opal_pmix_base_module_get_version_fn_t                  get_version;
    opal_pmix_base_module_register_fn_t                     register_evhandler;
    opal_pmix_base_module_deregister_fn_t                   deregister_evhandler;
    opal_pmix_base_module_notify_event_fn_t                 notify_event;
    opal_pmix_base_module_store_fn_t                        store_local;
    opal_pmix_base_module_get_nspace_fn_t                   get_nspace;
    opal_pmix_base_module_register_jobid_fn_t               register_jobid;
} opal_pmix_base_module_t;

typedef struct {
    mca_base_component_t                      base_version;
    mca_base_component_data_t                 base_data;
    int priority;
} opal_pmix_base_component_t;

/*
 * Macro for use in components that are of type pmix
 */
#define OPAL_PMIX_BASE_VERSION_2_0_0 \
    OPAL_MCA_BASE_VERSION_2_1_0("pmix", 2, 0, 0)

/* Global structure for accessing store functions */
OPAL_DECLSPEC extern opal_pmix_base_module_t opal_pmix;  /* holds base function pointers */

END_C_DECLS

#endif
