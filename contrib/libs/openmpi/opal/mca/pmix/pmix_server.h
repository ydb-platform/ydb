/*
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_PMIX_SERVER_H
#define OPAL_PMIX_SERVER_H

#include "opal_config.h"
#include "opal/types.h"

#include "opal/mca/pmix/pmix_types.h"

BEGIN_C_DECLS

/****    SERVER FUNCTION-SHIPPED APIs    ****/
/* NOTE: for performance purposes, the host server is required to
 * return as quickly as possible from all functions. Execution of
 * the function is thus to be done asynchronously so as to allow
 * the server support library to handle multiple client requests
 * as quickly and scalably as possible.
 *
 * ALL data passed to the host server functions is "owned" by the
 * server support library and MUST NOT be free'd. Data returned
 * by the host server via callback function is owned by the host
 * server, which is free to release it upon return from the callback */


/* Notify the host server that a client connected to us */
typedef int (*opal_pmix_server_client_connected_fn_t)(opal_process_name_t *proc,
                                                      void* server_object,
                                                      opal_pmix_op_cbfunc_t cbfunc,
                                                      void *cbdata);

/* Notify the host server that a client called pmix.finalize - note
 * that the client will be in a blocked state until the host server
 * executes the callback function, thus allowing the server support
 * library to release the client */
typedef int (*opal_pmix_server_client_finalized_fn_t)(opal_process_name_t *proc, void* server_object,
                                                      opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* A local client called pmix.abort - note that the client will be in a blocked
 * state until the host server executes the callback function, thus
 * allowing the server support library to release the client. The
 * list of procs_to_abort indicates which processes are to be terminated. A NULL
 * indicates that all procs in the client's nspace are to be terminated */
typedef int (*opal_pmix_server_abort_fn_t)(opal_process_name_t *proc, void *server_object,
                                           int status, const char msg[],
                                           opal_list_t *procs_to_abort,
                                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* At least one client called either pmix.fence or pmix.fence_nb. In either case,
 * the host server will be called via a non-blocking function to execute
 * the specified operation once all participating local procs have
 * contributed. All processes in the specified list are required to participate
 * in the fence[_nb] operation. The callback is to be executed once each daemon
 * hosting at least one participant has called the host server's fencenb function.
 *
 * The list of opal_value_t includes any directives from the user regarding
 * how the fence is to be executed (e.g., timeout limits).
 *
 * The provided data is to be collectively shared with all host
 * servers involved in the fence operation, and returned in the modex
 * cbfunc. A _NULL_ data value indicates that the local procs had
 * no data to contribute */
typedef int (*opal_pmix_server_fencenb_fn_t)(opal_list_t *procs, opal_list_t *info,
                                             char *data, size_t ndata,
                                             opal_pmix_modex_cbfunc_t cbfunc, void *cbdata);

/* Used by the PMIx server to request its local host contact the
 * PMIx server on the remote node that hosts the specified proc to
 * obtain and return a direct modex blob for that proc
 *
 * The list of opal_value_t includes any directives from the user regarding
 * how the operation is to be executed (e.g., timeout limits).
 */
typedef int (*opal_pmix_server_dmodex_req_fn_t)(opal_process_name_t *proc, opal_list_t *info,
                                                opal_pmix_modex_cbfunc_t cbfunc, void *cbdata);


/* Publish data per the PMIx API specification. The callback is to be executed
 * upon completion of the operation. The host server is not required to guarantee
 * support for the requested scope - i.e., the server does not need to return an
 * error if the data store doesn't support scope-based isolation. However, the
 * server must return an error (a) if the key is duplicative within the storage
 * scope, and (b) if the server does not allow overwriting of published info by
 * the original publisher - it is left to the discretion of the host server to
 * allow info-key-based flags to modify this behavior. The persist flag indicates
 * how long the server should retain the data. The nspace/rank of the publishing
 * process is also provided and is expected to be returned on any subsequent
 * lookup request */
typedef int (*opal_pmix_server_publish_fn_t)(opal_process_name_t *proc,
                                             opal_list_t *info,
                                             opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Lookup published data. The host server will be passed a NULL-terminated array
 * of string keys along with the scope within which the data is expected to have
 * been published. The host server is not required to guarantee support for all
 * PMIx-defined scopes, but should only search data stores within the specified
 * scope within the context of the corresponding "publish" API. The wait flag
 * indicates whether the server should wait for all data to become available
 * before executing the callback function, or should callback with whatever
 * data is immediately available.
 *
 * The list of opal_value_t includes any directives from the user regarding
 * how the operation is to be executed (e.g., timeout limits, whether the
 * lookup should wait until data appears).
 */
typedef int (*opal_pmix_server_lookup_fn_t)(opal_process_name_t *proc, char **keys,
                                            opal_list_t *info,
                                            opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata);

/* Delete data from the data store. The host server will be passed a NULL-terminated array
 * of string keys along with the scope within which the data is expected to have
 * been published. The callback is to be executed upon completion of the delete
 * procedure */
typedef int (*opal_pmix_server_unpublish_fn_t)(opal_process_name_t *proc, char **keys,
                                               opal_list_t *info,
                                               opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Spawn a set of applications/processes as per the PMIx API. Note that
 * applications are not required to be MPI or any other programming model.
 * Thus, the host server cannot make any assumptions as to their required
 * support. The callback function is to be executed once all processes have
 * been started. An error in starting any application or process in this
 * request shall cause all applications and processes in the request to
 * be terminated, and an error returned to the originating caller */
typedef int (*opal_pmix_server_spawn_fn_t)(opal_process_name_t *requestor,
                                           opal_list_t *job_info, opal_list_t *apps,
                                           opal_pmix_spawn_cbfunc_t cbfunc, void *cbdata);

/* Record the specified processes as "connected". This means that the resource
 * manager should treat the failure of any process in the specified group as
 * a reportable event, and take appropriate action. The callback function is
 * to be called once all participating processes have called connect. Note that
 * a process can only engage in *one* connect operation involving the identical
 * set of procs at a time. However, a process *can* be simultaneously engaged
 * in multiple connect operations, each involving a different set of procs
 *
 *  The list of opal_value_t includes any directives from the user regarding
 * how the operation is to be executed (e.g., timeout limits).
 */
typedef int (*opal_pmix_server_connect_fn_t)(opal_list_t *procs, opal_list_t *info,
                                             opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Disconnect a previously connected set of processes. An error should be returned
 * if the specified set of procs was not previously "connected". As above, a process
 * may be involved in multiple simultaneous disconnect operations. However, a process
 * is not allowed to reconnect to a set of ranges that has not fully completed
 * disconnect - i.e., you have to fully disconnect before you can reconnect to the
 * same group of processes.
 *
 * The list of opal_value_t includes any directives from the user regarding
 * how the operation is to be executed (e.g., timeout limits).
 */
typedef int (*opal_pmix_server_disconnect_fn_t)(opal_list_t *procs, opal_list_t *info,
                                                opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Register to receive notifications for the specified events. The resource
 * manager may have access to events beyond process failure. In cases where
 * the client application requests to be notified of such events, the request
 * will be passed to the PMIx server, which in turn shall pass the request to
 * the resource manager. The list of opal_value_t will provide the OPAL
 * error codes corresponding to the desired events */
 typedef int (*opal_pmix_server_register_events_fn_t)(opal_list_t *info,
                                                      opal_pmix_op_cbfunc_t cbfunc,
                                                      void *cbdata);

/* Deregister from the specified events. The list of opal_value_t will provide the OPAL
 * error codes corresponding to the desired events */
 typedef int (*opal_pmix_server_deregister_events_fn_t)(opal_list_t *info,
                                                        opal_pmix_op_cbfunc_t cbfunc,
                                                        void *cbdata);

/* Notify  the specified processes of an event generated either by
  * the PMIx server itself, or by one of its local clients. The RTE
  * is requested to pass the notification to each PMIx server that
  * hosts one or more of the specified processes */
typedef int (*opal_pmix_server_notify_fn_t)(int code, opal_process_name_t *source,
                                            opal_list_t *info,
                                            opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Query the RTE for information - the list is composed of opal_pmix_query_t items */
typedef int (*opal_pmix_server_query_fn_t)(opal_process_name_t *requestor,
                                           opal_list_t *queries,
                                           opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* Register that a tool has connected to the server, and request
 * that the tool be assigned a jobid for further interactions.
 * The optional opal_value_t list can be used to pass qualifiers for
 * the connection request:
 *
 * (a) OPAL_PMIX_USERID - effective userid of the tool
 * (b) OPAL_PMIX_GRPID - effective groupid of the tool
 * (c) OPAL_PMIX_FWD_STDOUT - forward any stdout to this tool
 * (d) OPAL_PMIX_FWD_STDERR - forward any stderr to this tool
 * (e) OPAL_PMIX_FWD_STDIN - forward stdin from this tool to any
 *     processes spawned on its behalf
 */
typedef void (*opal_pmix_server_tool_connection_fn_t)(opal_list_t *info,
                                                      opal_pmix_tool_connection_cbfunc_t cbfunc,
                                                      void *cbdata);

/* Log data on behalf of the client */
typedef void (*opal_pmix_server_log_fn_t)(opal_process_name_t *requestor,
                                          opal_list_t *info,
                                          opal_list_t *directives,
                                          opal_pmix_op_cbfunc_t cbfunc,
                                          void *cbdata);


/* Callback function for incoming connection requests from
 * local clients */
typedef void (*opal_pmix_connection_cbfunc_t)(int incoming_sd);

/* Register a socket the host server can monitor for connection
 * requests, harvest them, and then call our internal callback
 * function for further processing. A listener thread is essential
 * to efficiently harvesting connection requests from large
 * numbers of local clients such as occur when running on large
 * SMPs. The host server listener is required to call accept
 * on the incoming connection request, and then passing the
 * resulting socket to the provided cbfunc. A NULL for this function
 * will cause the internal PMIx server to spawn its own listener
 * thread */
typedef int (*opal_pmix_server_listener_fn_t)(int listening_sd,
                                              opal_pmix_connection_cbfunc_t cbfunc);

/* Request allocation modifications on behalf of a client */
typedef int (*opal_pmix_server_alloc_fn_t)(const opal_process_name_t *client,
                                           opal_pmix_alloc_directive_t directive,
                                           opal_list_t *data,
                                           opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* Execute a job control action on behalf of a client */
typedef int (*opal_pmix_server_job_control_fn_t)(const opal_process_name_t *requestor,
                                                 opal_list_t *targets, opal_list_t *directives,
                                                 opal_pmix_info_cbfunc_t cbfunc, void *cbdata);

/* we do not provide a monitoring capability */

/* Request forwarding of specified IO channels to the local PMIx server
 * for distribution to local clients */
typedef int (*opal_pmix_server_iof_pull_fn_t)(opal_list_t *sources,
                                              opal_list_t *directives,
                                              opal_pmix_iof_channel_t channels,
                                              opal_pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Entry point for pushing forwarded IO to clients/tools */
typedef int (*opal_pmix_server_iof_push_fn_t)(const opal_process_name_t *source,
                                              opal_pmix_iof_channel_t channel,
                                              unsigned char *data, size_t nbytes);

typedef struct opal_pmix_server_module_1_0_0_t {
    opal_pmix_server_client_connected_fn_t      client_connected;
    opal_pmix_server_client_finalized_fn_t      client_finalized;
    opal_pmix_server_abort_fn_t                 abort;
    opal_pmix_server_fencenb_fn_t               fence_nb;
    opal_pmix_server_dmodex_req_fn_t            direct_modex;
    opal_pmix_server_publish_fn_t               publish;
    opal_pmix_server_lookup_fn_t                lookup;
    opal_pmix_server_unpublish_fn_t             unpublish;
    opal_pmix_server_spawn_fn_t                 spawn;
    opal_pmix_server_connect_fn_t               connect;
    opal_pmix_server_disconnect_fn_t            disconnect;
    opal_pmix_server_register_events_fn_t       register_events;
    opal_pmix_server_deregister_events_fn_t     deregister_events;
    opal_pmix_server_notify_fn_t                notify_event;
    opal_pmix_server_query_fn_t                 query;
    opal_pmix_server_tool_connection_fn_t       tool_connected;
    opal_pmix_server_log_fn_t                   log;
    opal_pmix_server_listener_fn_t              listener;
    opal_pmix_server_alloc_fn_t                 allocate;
    opal_pmix_server_job_control_fn_t           job_control;
    opal_pmix_server_iof_pull_fn_t              iof_pull;
    opal_pmix_server_iof_push_fn_t              iof_push;
} opal_pmix_server_module_t;


END_C_DECLS

#endif
