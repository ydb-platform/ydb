/*
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer listed
 *   in this license in the documentation and/or other materials
 *   provided with the distribution.
 *
 * - Neither the name of the copyright holders nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * The copyright holders provide no reassurances that the source code
 * provided does not infringe any patent, copyright, or any other
 * intellectual property rights of third parties.  The copyright holders
 * disclaim any liability to any recipient for claims brought against
 * recipient by any third party for infringement of that parties
 * intellectual property rights.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * $HEADER$
 *
 * PMIx provides a "function-shipping" approach to support for
 * implementing the server-side of the protocol. This method allows
 * resource managers to implement the server without being burdened
 * with PMIx internal details. Accordingly, each PMIx API is mirrored
 * here in a function call to be provided by the server. When a
 * request is received from the client, the corresponding server function
 * will be called with the information.
 *
 * Any functions not supported by the RM can be indicated by a NULL for
 * the function pointer. Client calls to such functions will have a
 * "not supported" error returned.
 */

#ifndef PMIx_SERVER_API_H
#define PMIx_SERVER_API_H

/* Structure and constant definitions */
#include <pmix_common.h>

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

/****    SERVER FUNCTION-SHIPPED APIs    ****/
/* NOTE: for performance purposes, the host server is required to
 * return as quickly as possible from all functions. Execution of
 * the function is thus to be done asynchronously so as to allow
 * the PMIx server support library to handle multiple client requests
 * as quickly and scalably as possible.
 *
 * ALL data passed to the host server functions is "owned" by the
 * PMIX server support library and MUST NOT be free'd. Data returned
 * by the host server via callback function is owned by the host
 * server, which is free to release it upon return from the callback */

/* Notify the host server that a client connected to us - note
 * that the client will be in a blocked state until the host server
 * executes the callback function, thus allowing the PMIx server support
 * library to release the client */
typedef pmix_status_t (*pmix_server_client_connected_fn_t)(const pmix_proc_t *proc, void* server_object,
                                                           pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Notify the host server that a client called PMIx_Finalize - note
 * that the client will be in a blocked state until the host server
 * executes the callback function, thus allowing the PMIx server support
 * library to release the client */
typedef pmix_status_t (*pmix_server_client_finalized_fn_t)(const pmix_proc_t *proc, void* server_object,
                                                           pmix_op_cbfunc_t cbfunc, void *cbdata);

/* A local client called PMIx_Abort - note that the client will be in a blocked
 * state until the host server executes the callback function, thus
 * allowing the PMIx server support library to release the client. The
 * array of procs indicates which processes are to be terminated. A NULL
 * indicates that all procs in the client's nspace are to be terminated */
typedef pmix_status_t (*pmix_server_abort_fn_t)(const pmix_proc_t *proc, void *server_object,
                                                int status, const char msg[],
                                                pmix_proc_t procs[], size_t nprocs,
                                                pmix_op_cbfunc_t cbfunc, void *cbdata);

/* At least one client called either PMIx_Fence or PMIx_Fence_nb. In either case,
 * the host server will be called via a non-blocking function to execute
 * the specified operation once all participating local procs have
 * contributed. All processes in the specified array are required to participate
 * in the Fence[_nb] operation. The callback is to be executed once each daemon
 * hosting at least one participant has called the host server's fencenb function.
 *
 * The provided data is to be collectively shared with all PMIx
 * servers involved in the fence operation, and returned in the modex
 * cbfunc. A _NULL_ data value indicates that the local procs had
 * no data to contribute.
 *
 * The array of info structs is used to pass user-requested options to the server.
 * This can include directives as to the algorithm to be used to execute the
 * fence operation. The directives are optional _unless_ the _mandatory_ flag
 * has been set - in such cases, the host RM is required to return an error
 * if the directive cannot be met. */
typedef pmix_status_t (*pmix_server_fencenb_fn_t)(const pmix_proc_t procs[], size_t nprocs,
                                                  const pmix_info_t info[], size_t ninfo,
                                                  char *data, size_t ndata,
                                                  pmix_modex_cbfunc_t cbfunc, void *cbdata);


/* Used by the PMIx server to request its local host contact the
 * PMIx server on the remote node that hosts the specified proc to
 * obtain and return a direct modex blob for that proc.
 *
 * The array of info structs is used to pass user-requested options to the server.
 * This can include a timeout to preclude an indefinite wait for data that
 * may never become available. The directives are optional _unless_ the _mandatory_ flag
 * has been set - in such cases, the host RM is required to return an error
 * if the directive cannot be met. */
typedef pmix_status_t (*pmix_server_dmodex_req_fn_t)(const pmix_proc_t *proc,
                                                     const pmix_info_t info[], size_t ninfo,
                                                     pmix_modex_cbfunc_t cbfunc, void *cbdata);


/* Publish data per the PMIx API specification. The callback is to be executed
 * upon completion of the operation. The default data range is expected to be
 * PMIX_SESSION, and the default persistence PMIX_PERSIST_SESSION. These values
 * can be modified by including the respective pmix_info_t struct in the
 * provided array.
 *
 * Note that the host server is not required to guarantee support for any specific
 * range - i.e., the server does not need to return an error if the data store
 * doesn't support range-based isolation. However, the server must return an error
 * (a) if the key is duplicative within the storage range, and (b) if the server
 * does not allow overwriting of published info by the original publisher - it is
 * left to the discretion of the host server to allow info-key-based flags to modify
 * this behavior.
 *
 * The persistence indicates how long the server should retain the data.
 *
 * The identifier of the publishing process is also provided and is expected to
 * be returned on any subsequent lookup request */
typedef pmix_status_t (*pmix_server_publish_fn_t)(const pmix_proc_t *proc,
                                                  const pmix_info_t info[], size_t ninfo,
                                                  pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Lookup published data. The host server will be passed a NULL-terminated array
 * of string keys.
 *
 * The array of info structs is used to pass user-requested options to the server.
 * This can include a wait flag to indicate that the server should wait for all
 * data to become available before executing the callback function, or should
 * immediately callback with whatever data is available. In addition, a timeout
 * can be specified on the wait to preclude an indefinite wait for data that
 * may never be published. */
typedef pmix_status_t (*pmix_server_lookup_fn_t)(const pmix_proc_t *proc, char **keys,
                                                 const pmix_info_t info[], size_t ninfo,
                                                 pmix_lookup_cbfunc_t cbfunc, void *cbdata);

/* Delete data from the data store. The host server will be passed a NULL-terminated array
 * of string keys, plus potential directives such as the data range within which the
 * keys should be deleted. The callback is to be executed upon completion of the delete
 * procedure */
typedef pmix_status_t (*pmix_server_unpublish_fn_t)(const pmix_proc_t *proc, char **keys,
                                                    const pmix_info_t info[], size_t ninfo,
                                                    pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Spawn a set of applications/processes as per the PMIx API. Note that
 * applications are not required to be MPI or any other programming model.
 * Thus, the host server cannot make any assumptions as to their required
 * support. The callback function is to be executed once all processes have
 * been started. An error in starting any application or process in this
 * request shall cause all applications and processes in the request to
 * be terminated, and an error returned to the originating caller.
 *
 * Note that a timeout can be specified in the job_info array to indicate
 * that failure to start the requested job within the given time should
 * result in termination to avoid hangs */
typedef pmix_status_t (*pmix_server_spawn_fn_t)(const pmix_proc_t *proc,
                                                const pmix_info_t job_info[], size_t ninfo,
                                                const pmix_app_t apps[], size_t napps,
                                                pmix_spawn_cbfunc_t cbfunc, void *cbdata);

/* Record the specified processes as "connected". This means that the resource
 * manager should treat the failure of any process in the specified group as
 * a reportable event, and take appropriate action. The callback function is
 * to be called once all participating processes have called connect. Note that
 * a process can only engage in *one* connect operation involving the identical
 * set of procs at a time. However, a process *can* be simultaneously engaged
 * in multiple connect operations, each involving a different set of procs
 *
 * Note also that this is a collective operation within the client library, and
 * thus the client will be blocked until all procs participate. Thus, the info
 * array can be used to pass user directives, including a timeout.
 * The directives are optional _unless_ the _mandatory_ flag
 * has been set - in such cases, the host RM is required to return an error
 * if the directive cannot be met. */
typedef pmix_status_t (*pmix_server_connect_fn_t)(const pmix_proc_t procs[], size_t nprocs,
                                                  const pmix_info_t info[], size_t ninfo,
                                                  pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Disconnect a previously connected set of processes. An error should be returned
 * if the specified set of procs was not previously "connected". As above, a process
 * may be involved in multiple simultaneous disconnect operations. However, a process
 * is not allowed to reconnect to a set of ranges that has not fully completed
 * disconnect - i.e., you have to fully disconnect before you can reconnect to the
 * same group of processes.
  *
 * Note also that this is a collective operation within the client library, and
 * thus the client will be blocked until all procs participate. Thus, the info
 * array can be used to pass user directives, including a timeout.
 * The directives are optional _unless_ the _mandatory_ flag
 * has been set - in such cases, the host RM is required to return an error
 * if the directive cannot be met. */
typedef pmix_status_t (*pmix_server_disconnect_fn_t)(const pmix_proc_t procs[], size_t nprocs,
                                                     const pmix_info_t info[], size_t ninfo,
                                                     pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Register to receive notifications for the specified events. The resource
 * manager is _required_ to pass along to the local PMIx server all events
 * that directly relate to a registered namespace. However, the RM may have
 * access to events beyond those - e.g., environmental events. The PMIx server
 * will register to receive environmental events that match specific PMIx
 * event codes. If the host RM supports such notifications, it will need to
 * translate its own internal event codes to fit into a corresponding PMIx event
 * code - any specific info beyond that can be passed in via the pmix_info_t
 * upon notification.
 *
 * The info array included in this API is reserved for possible future directives
 * to further steer notification.
 */
 typedef pmix_status_t (*pmix_server_register_events_fn_t)(pmix_status_t *codes, size_t ncodes,
                                                           const pmix_info_t info[], size_t ninfo,
                                                           pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Deregister to receive notifications for the specified environmental events
 * for which the PMIx server has previously registered. The host RM remains
 * required to notify of any job-related events */
 typedef pmix_status_t (*pmix_server_deregister_events_fn_t)(pmix_status_t *codes, size_t ncodes,
                                                             pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Notify the specified processes of an event generated either by
 * the PMIx server itself, or by one of its local clients. The process
 * generating the event is provided in the source parameter. */
typedef pmix_status_t (*pmix_server_notify_event_fn_t)(pmix_status_t code,
                                                       const pmix_proc_t *source,
                                                       pmix_data_range_t range,
                                                       pmix_info_t info[], size_t ninfo,
                                                       pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Callback function for incoming connection requests from
 * local clients */
typedef void (*pmix_connection_cbfunc_t)(int incoming_sd, void *cbdata);

/* Register a socket the host server can monitor for connection
 * requests, harvest them, and then call our internal callback
 * function for further processing. A listener thread is essential
 * to efficiently harvesting connection requests from large
 * numbers of local clients such as occur when running on large
 * SMPs. The host server listener is required to call accept
 * on the incoming connection request, and then passing the
 * resulting soct to the provided cbfunc. A NULL for this function
 * will cause the internal PMIx server to spawn its own listener
 * thread */
typedef pmix_status_t (*pmix_server_listener_fn_t)(int listening_sd,
                                                   pmix_connection_cbfunc_t cbfunc,
                                                   void *cbdata);

/* Query information from the resource manager. The query will include
 * the nspace/rank of the proc that is requesting the info, an
 * array of pmix_query_t describing the request, and a callback
 * function/data for the return. */
typedef pmix_status_t (*pmix_server_query_fn_t)(pmix_proc_t *proct,
                                                pmix_query_t *queries, size_t nqueries,
                                                pmix_info_cbfunc_t cbfunc,
                                                void *cbdata);

/* Callback function for incoming tool connections - the host
 * RM shall provide an nspace/rank for the connecting tool. We
 * assume that a rank=0 will be the normal assignment, but allow
 * for the future possibility of a parallel set of tools
 * connecting, and thus each proc requiring a rank*/
typedef void (*pmix_tool_connection_cbfunc_t)(pmix_status_t status,
                                              pmix_proc_t *proc, void *cbdata);

/* Register that a tool has connected to the server, and request
 * that the tool be assigned an nspace/rank for further interactions.
 * The optional pmix_info_t array can be used to pass qualifiers for
 * the connection request:
 *
 * (a) PMIX_USERID - effective userid of the tool
 * (b) PMIX_GRPID - effective groupid of the tool
 * (c) PMIX_FWD_STDOUT - forward any stdout to this tool
 * (d) PMIX_FWD_STDERR - forward any stderr to this tool
 * (e) PMIX_FWD_STDIN - forward stdin from this tool to any
 *     processes spawned on its behalf
 */
typedef void (*pmix_server_tool_connection_fn_t)(pmix_info_t *info, size_t ninfo,
                                                 pmix_tool_connection_cbfunc_t cbfunc,
                                                 void *cbdata);

/* Log data on behalf of a client. Calls to the host thru this
 * function must _NOT_ call the PMIx_Log API as this will
 * trigger an infinite loop. Instead, the implementation must
 * perform one of three operations:
 *
 * (a) transfer the data+directives to a "gateway" server
 *     where they can be logged. Gateways are designated
 *     servers on nodes (typically service nodes) where
 *     centralized logging is supported. The data+directives
 *     may be passed to the PMIx_Log API once arriving at
 *     that destination.
 *
 * (b) transfer the data to a logging channel outside of
 *     PMIx, but directly supported by the host
 *
 * (c) return an error to the caller indicating that the
 *     requested action is not supported
 */
typedef void (*pmix_server_log_fn_t)(const pmix_proc_t *client,
                                     const pmix_info_t data[], size_t ndata,
                                     const pmix_info_t directives[], size_t ndirs,
                                     pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Request allocation modifications on behalf of a client */
typedef pmix_status_t (*pmix_server_alloc_fn_t)(const pmix_proc_t *client,
                                                pmix_alloc_directive_t directive,
                                                const pmix_info_t data[], size_t ndata,
                                                pmix_info_cbfunc_t cbfunc, void *cbdata);

/* Execute a job control action on behalf of a client */
typedef pmix_status_t (*pmix_server_job_control_fn_t)(const pmix_proc_t *requestor,
                                                      const pmix_proc_t targets[], size_t ntargets,
                                                      const pmix_info_t directives[], size_t ndirs,
                                                      pmix_info_cbfunc_t cbfunc, void *cbdata);

/* Request that a client be monitored for activity */
typedef pmix_status_t (*pmix_server_monitor_fn_t)(const pmix_proc_t *requestor,
                                                  const pmix_info_t *monitor, pmix_status_t error,
                                                  const pmix_info_t directives[], size_t ndirs,
                                                  pmix_info_cbfunc_t cbfunc, void *cbdata);

/* Request a credential from the host SMS
 * Input values include:
 *
 * proc - pointer to a pmix_proc_t identifier of the process on whose behalf
 *        the request is being made (i.e., the client originating the request)
 *
 * directives - an array of pmix_info_t structures containing directives pertaining
 *              to the request. This will typically include any pmix_info_t structs
 *              passed by the requesting client, but may also include directives
 *              required by (or available from) the PMIx server implementation - e.g.,
 *              the effective user and group ID's of the requesting process.
 *
 * ndirs - number of pmix_info_t structures in the directives array
 *
 * cbfunc - the pmix_credential_cbfunc_t function to be called upon completion
 *          of the request
 *
 * cbdata - pointer to an object to be returned when cbfunc is called
 *
 * Returned values:
 * PMIX_SUCCESS - indicates that the request is being processed by the host system
 *                management stack. The response will be coming in the provided
 *                callback function.
 *
 * Any other value indicates an appropriate error condition. The callback function
 * will _not_ be called in such cases.
 */
typedef pmix_status_t (*pmix_server_get_cred_fn_t)(const pmix_proc_t *proc,
                                                   const pmix_info_t directives[], size_t ndirs,
                                                   pmix_credential_cbfunc_t cbfunc, void *cbdata);

/* Request validation of a credential from the host SMS
 * Input values include:
 *
 * proc - pointer to a pmix_proc_t identifier of the process on whose behalf
 *        the request is being made (i.e., the client issuing the request)
 *
 * cred - pointer to a pmix_byte_object_t containing the provided credential
 *
 * directives - an array of pmix_info_t structures containing directives pertaining
 *              to the request. This will typically include any pmix_info_t structs
 *              passed by the requesting client, but may also include directives
 *              used by the PMIx server implementation
 *
 * ndirs - number of pmix_info_t structures in the directives array
 *
 * cbfunc - the pmix_validation_cbfunc_t function to be called upon completion
 *          of the request
 *
 * cbdata - pointer to an object to be returned when cbfunc is called
 *
 * Returned values:
 * PMIX_SUCCESS - indicates that the request is being processed by the host system
 *                management stack. The response will be coming in the provided
 *                callback function.
 *
 * Any other value indicates an appropriate error condition. The callback function
 * will _not_ be called in such cases.
 */
typedef pmix_status_t (*pmix_server_validate_cred_fn_t)(const pmix_proc_t *proc,
                                                        const pmix_byte_object_t *cred,
                                                        const pmix_info_t directives[], size_t ndirs,
                                                        pmix_validation_cbfunc_t cbfunc, void *cbdata);

/* Request the specified IO channels be forwarded from the given array of procs.
 * The function shall return PMIX_SUCCESS once the host RM accepts the request for
 * processing, or a PMIx error code if the request itself isn't correct or supported.
 * The callback function shall be called when the request has been processed,
 * returning either PMIX_SUCCESS to indicate that IO shall be forwarded as requested,
 * or some appropriate error code if the request has been denied.
 *
 * NOTE: STDIN is not supported in this call! The forwarding of stdin is a "push"
 * process - procs cannot request that it be "pulled" from some other source
 *
 * procs - array of process identifiers whose IO is being requested.
 *
 * nprocs - size of the procs array
 *
 * directives - array of key-value attributes further defining the request. This
 *              might include directives on buffering and security credentials for
 *              access to protected channels
 *
 * ndirs - size of the directives array
 *
 * channels - bitmask identifying the channels to be forwarded
 *
 * cbfunc - callback function when the IO forwarding has been setup
 *
 * cbdata - object to be returned in cbfunc
 *
 * This call serves as a registration with the host RM for the given IO channels from
 * the specified procs - the host RM is expected to ensure that this local PMIx server
 * is on the distribution list for the channel/proc combination
 */
typedef pmix_status_t (*pmix_server_iof_fn_t)(const pmix_proc_t procs[], size_t nprocs,
                                              const pmix_info_t directives[], size_t ndirs,
                                              pmix_iof_channel_t channels,
                                              pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Passes stdin to the host RM for transmission to specified recipients. The host RM is
 * responsible for forwarding the data to all PMIx servers that host the specified
 * target.
 *
 * source - pointer to the identifier of the process whose stdin is being provided
 *
 * targets - array of process identifiers to which the data is to be delivered. Note
 *           that a WILDCARD rank indicates that all procs in the given nspace are
 *           to receive a copy of the data
 *
 * ntargets - number of procs in the targets array
 *
 * directives - array of key-value attributes further defining the request. This
 *              might include directives on buffering and security credentials for
 *              access to protected channels
 *
 * ndirs - size of the directives array
 *
 * bo - pointer to a byte object containing the stdin data
 *
 * cbfunc - callback function when the data has been forwarded
 *
 * cbdata - object to be returned in cbfunc
 *
 */

typedef pmix_status_t (*pmix_server_stdin_fn_t)(const pmix_proc_t *source,
                                                const pmix_proc_t targets[], size_t ntargets,
                                                const pmix_info_t directives[], size_t ndirs,
                                                const pmix_byte_object_t *bo,
                                                pmix_op_cbfunc_t cbfunc, void *cbdata);


typedef struct pmix_server_module_2_0_0_t {
    /* v1x interfaces */
    pmix_server_client_connected_fn_t   client_connected;
    pmix_server_client_finalized_fn_t   client_finalized;
    pmix_server_abort_fn_t              abort;
    pmix_server_fencenb_fn_t            fence_nb;
    pmix_server_dmodex_req_fn_t         direct_modex;
    pmix_server_publish_fn_t            publish;
    pmix_server_lookup_fn_t             lookup;
    pmix_server_unpublish_fn_t          unpublish;
    pmix_server_spawn_fn_t              spawn;
    pmix_server_connect_fn_t            connect;
    pmix_server_disconnect_fn_t         disconnect;
    pmix_server_register_events_fn_t    register_events;
    pmix_server_deregister_events_fn_t  deregister_events;
    pmix_server_listener_fn_t           listener;
    /* v2x interfaces */
    pmix_server_notify_event_fn_t       notify_event;
    pmix_server_query_fn_t              query;
    pmix_server_tool_connection_fn_t    tool_connected;
    pmix_server_log_fn_t                log;
    pmix_server_alloc_fn_t              allocate;
    pmix_server_job_control_fn_t        job_control;
    pmix_server_monitor_fn_t            monitor;
    /* v3x interfaces */
    pmix_server_get_cred_fn_t           get_credential;
    pmix_server_validate_cred_fn_t      validate_credential;
    pmix_server_iof_fn_t                iof_pull;
    pmix_server_stdin_fn_t              push_stdin;
} pmix_server_module_t;

/****    HOST RM FUNCTIONS FOR INTERFACE TO PMIX SERVER    ****/

/* Initialize the server support library, and provide a
 * pointer to a pmix_server_module_t structure
 * containing the caller's callback functions. The
 * array of pmix_info_t structs is used to pass
 * additional info that may be required by the server
 * when initializing - e.g., a user/group ID to set
 * on the rendezvous file for the Unix Domain Socket. It
 * also may include the PMIX_SERVER_TOOL_SUPPORT key, thereby
 * indicating that the daemon is willing to accept connection
 * requests from tools */
PMIX_EXPORT pmix_status_t PMIx_server_init(pmix_server_module_t *module,
                                           pmix_info_t info[], size_t ninfo);

/* Finalize the server support library. If internal comm is
 * in-use, the server will shut it down at this time. All
 * memory usage is released */
PMIX_EXPORT pmix_status_t PMIx_server_finalize(void);

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
PMIX_EXPORT pmix_status_t PMIx_generate_regex(const char *input, char **regex);

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
PMIX_EXPORT pmix_status_t PMIx_generate_ppn(const char *input, char **ppn);

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
PMIX_EXPORT pmix_status_t PMIx_server_register_nspace(const pmix_nspace_t nspace, int nlocalprocs,
                                          pmix_info_t info[], size_t ninfo,
                                          pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Deregister an nspace and purge all objects relating to
 * it, including any client info from that nspace. This is
 * intended to support persistent PMIx servers by providing
 * an opportunity for the host RM to tell the PMIx server
 * library to release all memory for a completed job */
PMIX_EXPORT void PMIx_server_deregister_nspace(const pmix_nspace_t nspace,
                                               pmix_op_cbfunc_t cbfunc, void *cbdata);

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
PMIX_EXPORT pmix_status_t PMIx_server_register_client(const pmix_proc_t *proc,
                                                      uid_t uid, gid_t gid,
                                                      void *server_object,
                                                      pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Deregister a client and purge all data relating to it. The
 * deregister_nspace API will automatically delete all client
 * info for that nspace - this API is therefore intended solely
 * for use in exception cases */
PMIX_EXPORT void PMIx_server_deregister_client(const pmix_proc_t *proc,
                                               pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Setup the environment of a child process to be forked
 * by the host so it can correctly interact with the PMIx
 * server. The PMIx client needs some setup information
 * so it can properly connect back to the server. This function
 * will set appropriate environmental variables for this purpose. */
PMIX_EXPORT pmix_status_t PMIx_server_setup_fork(const pmix_proc_t *proc, char ***env);

/* Define a callback function the PMIx server will use to return
 * direct modex requests to the host server. The PMIx server
 * will free the data blob upon return from the response fn */
typedef void (*pmix_dmodex_response_fn_t)(pmix_status_t status,
                                          char *data, size_t sz,
                                          void *cbdata);

/* Define a function by which the host server can request modex data
 * from the local PMIx server. This is used to support the direct modex
 * operation - i.e., where data is cached locally on each PMIx
 * server for its own local clients, and is obtained on-demand
 * for remote requests. Upon receiving a request from a remote
 * server, the host server will call this function to pass the
 * request into the PMIx server. The PMIx server will return a blob
 * (once it becomes available) via the cbfunc - the host
 * server shall send the blob back to the original requestor */
PMIX_EXPORT pmix_status_t PMIx_server_dmodex_request(const pmix_proc_t *proc,
                                                     pmix_dmodex_response_fn_t cbfunc,
                                                     void *cbdata);

/* define a callback function for the setup_application API. The returned info
 * array is owned by the PMIx server library and will be free'd when the
 * provided cbfunc is called. */
typedef void (*pmix_setup_application_cbfunc_t)(pmix_status_t status,
                                                pmix_info_t info[], size_t ninfo,
                                                void *provided_cbdata,
                                                pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Provide a function by which the resource manager can request
 * any application-specific environmental variables, resource
 * assignments, and/or other data prior to launch of an application.
 * For example, network libraries may opt to provide security
 * credentials for the application. This is defined as a non-blocking
 * operation in case network libraries need to perform some action
 * before responding. Any returned env will be distributed along
 * with the application */
PMIX_EXPORT pmix_status_t PMIx_server_setup_application(const pmix_nspace_t nspace,
                                                        pmix_info_t info[], size_t ninfo,
                                                        pmix_setup_application_cbfunc_t cbfunc, void *cbdata);

/* Provide a function by which the local PMIx server can perform
 * any application-specific operations prior to spawning local
 * clients of a given application. For example, a network library
 * might need to setup the local driver for "instant on" addressing.
 * Data provided in the info array will be stored in the job-info
 * region for the nspace. Operations included in the info array
 * will be cached until the server calls PMIx_server_setup_fork,
 * thereby indicating that local clients of this nspace will exist.
 * Operations indicated by the provided data will only be executed
 * for the first local client - i.e., they will only be executed
 * once for a given nspace
 */
PMIX_EXPORT pmix_status_t PMIx_server_setup_local_support(const pmix_nspace_t nspace,
                                                          pmix_info_t info[], size_t ninfo,
                                                          pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Provide a function by which the host RM can pass forwarded IO
 * to the local PMIx server for distribution to its clients. The
 * PMIx server is responsible for determining which of its clients
 * have actually registered for the provided data
 *
 * Parameters include:
 *
 * source - the process that provided the data being forwarded
 *
 * channel - the IOF channel (stdin, stdout, etc.)
 *
 * bo - a byte object containing the data
 *
 * info - an optional array of metadata describing the data, including
 *        attributes such as PMIX_IOF_COMPLETE to indicate that the
 *        source channel has been closed
 *
 * ninfo - number of elements in the info array
 *
 * cbfunc - a callback function to be executed once the provided data
 *          is no longer required. The host RM is required to retain
 *          the byte object until the callback is executed, or a
 *          non-success status is returned by the function
 *
 * cbdata - object pointer to be returned in the callback function
 */
PMIX_EXPORT pmix_status_t PMIx_server_IOF_deliver(const pmix_proc_t *source,
                                                  pmix_iof_channel_t channel,
                                                  const pmix_byte_object_t *bo,
                                                  const pmix_info_t info[], size_t ninfo,
                                                  pmix_op_cbfunc_t cbfunc, void *cbdata);

/* Collect inventory of local resources. This is a non-blocking
 * API as it may involve somewhat lengthy operations to obtain
 * the requested information. Servers designated as "gateways"
 * and whose plugins support collection of infrastructure info
 * (e.g., switch and fabric topology, connectivity maps) shall
 * return that information - plugins on non-gateway servers
 * shall only return the node-local inventory. */
PMIX_EXPORT pmix_status_t PMIx_server_collect_inventory(pmix_info_t directives[], size_t ndirs,
                                                        pmix_info_cbfunc_t cbfunc, void *cbdata);

/* Deliver collected inventory for archiving by the corresponding
 * plugins. Typically executed on a "gateway" associated with the
 * system scheduler to enable use of inventory information by the
 * the scheduling algorithm. May also be used on compute nodes to
 * store a broader picture of the system for access by applications,
 * if desired */
PMIX_EXPORT pmix_status_t PMIx_server_deliver_inventory(pmix_info_t info[], size_t ninfo,
                                                        pmix_info_t directives[], size_t ndirs,
                                                        pmix_op_cbfunc_t cbfunc, void *cbdata);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif
