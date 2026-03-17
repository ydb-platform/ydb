/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Runtime Messaging Layer (RML) Communication Interface
 *
 * The Runtime Messaging Layer (RML) provices basic point-to-point
 * communication between ORTE processes.  The system is available for
 * most architectures, with some exceptions (the Cray XT3/XT4, for example).
 */


#ifndef ORTE_MCA_RML_RML_H_
#define ORTE_MCA_RML_RML_H_

#include "orte_config.h"
#include "orte/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "orte/mca/mca.h"
#include "opal/mca/crs/crs.h"
#include "opal/mca/crs/base/base.h"
#include "orte/mca/routed/routed.h"

#include "orte/mca/rml/rml_types.h"

BEGIN_C_DECLS


/* ******************************************************************** */

/* forward declare */
struct orte_rml_base_module_t;
struct orte_rml_component_t;

typedef struct {
    opal_object_t super;
    orte_process_name_t name;
    opal_buffer_t data;
    bool active;
} orte_rml_recv_cb_t;
OBJ_CLASS_DECLARATION(orte_rml_recv_cb_t);

/* Provide a generic callback function to release buffers
 * following a non-blocking send as this happens all over
 * the code base
 */
ORTE_DECLSPEC void orte_rml_send_callback(int status, orte_process_name_t* sender,
                                          opal_buffer_t* buffer, orte_rml_tag_t tag,
                                          void* cbdata);

ORTE_DECLSPEC void orte_rml_recv_callback(int status, orte_process_name_t* sender,
                                          opal_buffer_t *buffer,
                                          orte_rml_tag_t tag, void *cbdata);

/* ******************************************************************** */
/*                     RML CALLBACK FUNCTION DEFINITIONS                */

/**
 * Funtion prototype for callback from non-blocking iovec send and recv
 *
 * Funtion prototype for callback from non-blocking iovec send and recv.
 * On send, the iovec pointer will be the same pointer passed to
 * send_nb and count will equal the count given to send.
 *
 * On recv, the iovec pointer will be the address of a single iovec
 * allocated and owned by the RML, not the process receiving the
 * callback. Ownership of the data block can be transferred by setting
 * a user variable to point to the data block, and setting the
 * iovec->iov_base pointer to NULL.
 *
 * @note The parameter in/out parameters are relative to the user's callback
 * function.
 *
 * @param[in] status  Completion status
 * @param[in] peer    Opaque name of peer process
 * @param[in] msg     Pointer to the array of iovec that was sent
 *                    or to a single iovec that has been recvd
 * @param[in] count   Number of iovecs in the array
 * @param[in] tag     User defined tag for matching send/recv
 * @param[in] cbdata  User data passed to send_nb()
 */
typedef void (*orte_rml_callback_fn_t)(int status,
                                       orte_process_name_t* peer,
                                       struct iovec* msg,
                                       int count,
                                       orte_rml_tag_t tag,
                                       void* cbdata);


/**
 * Funtion prototype for callback from non-blocking buffer send and receive
 *
 * Function prototype for callback from non-blocking buffer send and
 * receive. On send, the buffer will be the same pointer passed to
 * send_buffer_nb. On receive, the buffer will be allocated and owned
 * by the RML, not the process receiving the callback.
 *
 * @note The parameter in/out parameters are relative to the user's callback
 * function.
 *
 * @param[in] status  Completion status
 * @param[in] peer    Name of peer process
 * @param[in] buffer  Message buffer
 * @param[in] tag     User defined tag for matching send/recv
 * @param[in] cbdata  User data passed to send_buffer_nb() or recv_buffer_nb()
 */
typedef void (*orte_rml_buffer_callback_fn_t)(int status,
                                              orte_process_name_t* peer,
                                              struct opal_buffer_t* buffer,
                                              orte_rml_tag_t tag,
                                              void* cbdata);

/**
 * Function prototype for exception callback
 *
 * Function prototype for callback triggered when a communication error is detected.
 *
 * @note The parameter in/out parameters are relative to the user's callback
 * function.
 *
 * @param[in] peer      Name of peer process
 * @param[in] exception Description of the error causing the exception
 */
typedef void (*orte_rml_exception_callback_t)(orte_process_name_t* peer,
                                              orte_rml_exception_t exception);


/* ******************************************************************** */
/*                 RML INTERNAL MODULE API DEFINITION                   */


/**
 * "Ping" another process to determine availability
 *
 * Ping another process to determine if it is available.  This
 * function only verifies that the process is alive and will allow a
 * connection to the local process.  It does *not* qualify as
 * establishing communication with the remote process, as required by
 * the note for set_contact_info().
 *
 * @param[in] contact_info The contact info string for the remote process
 * @param[in] tv           Timeout after which the ping should be failed
 *
 * @retval ORTE_SUCESS The process is available and will allow connections
 *                     from the local process
 * @retval ORTE_ERROR  An unspecified error occurred during the update
 */
typedef int (*orte_rml_module_ping_fn_t)(struct orte_rml_base_module_t *mod,
                                         const char* contact_info,
                                         const struct timeval* tv);


/**
 * Send an iovec non-blocking message
 *
 * Send an array of iovecs to the specified peer.  The call
 * will return immediately, although the iovecs may not be modified
 * until the completion callback is triggered.  The iovecs *may* be
 * passed to another call to send_nb before the completion callback is
 * triggered.  The callback being triggered does not give any
 * indication of remote completion.
 *
 * @param[in] peer   Name of receiving process
 * @param[in] msg    Pointer to an array of iovecs to be sent
 * @param[in] count  Number of iovecs in array
 * @param[in] tag    User defined tag for matching send/recv
 * @param[in] cbfunc Callback function on message comlpetion
 * @param[in] cbdata User data to provide during completion callback
 *
 * @retval ORTE_SUCCESS The message was successfully started
 * @retval ORTE_ERR_BAD_PARAM One of the parameters was invalid
 * @retval ORTE_ERR_ADDRESSEE_UNKNOWN Contact information for the
 *                    receiving process is not available
 * @retval ORTE_ERROR  An unspecified error occurred
 */
typedef int (*orte_rml_module_send_nb_fn_t)(struct orte_rml_base_module_t *mod,
                                            orte_process_name_t* peer,
                                            struct iovec* msg,
                                            int count,
                                            orte_rml_tag_t tag,
                                            orte_rml_callback_fn_t cbfunc,
                                            void* cbdata);


/**
 * Send a buffer non-blocking message
 *
 * Send a buffer to the specified peer.  The call
 * will return immediately, although the buffer may not be modified
 * until the completion callback is triggered.  The buffer *may* be
 * passed to another call to send_nb before the completion callback is
 * triggered.  The callback being triggered does not give any
 * indication of remote completion.
 *
 * @param[in] peer   Name of receiving process
 * @param[in] buffer Pointer to buffer to be sent
 * @param[in] tag    User defined tag for matching send/recv
 * @param[in] cbfunc Callback function on message comlpetion
 * @param[in] cbdata User data to provide during completion callback
 *
 * @retval ORTE_SUCCESS The message was successfully started
 * @retval ORTE_ERR_BAD_PARAM One of the parameters was invalid
 * @retval ORTE_ERR_ADDRESSEE_UNKNOWN Contact information for the
 *                    receiving process is not available
 * @retval ORTE_ERROR  An unspecified error occurred
 */
typedef int (*orte_rml_module_send_buffer_nb_fn_t)(struct orte_rml_base_module_t *mod,
                                                   orte_process_name_t* peer,
                                                   struct opal_buffer_t* buffer,
                                                   orte_rml_tag_t tag,
                                                   orte_rml_buffer_callback_fn_t cbfunc,
                                                   void* cbdata);

/**
 * Purge the RML/OOB of contact info and pending messages
 * to/from a specified process. Used when a process aborts
 * and is to be restarted
 */
typedef void (*orte_rml_module_purge_fn_t)(orte_process_name_t *peer);



/**
 * RML internal module interface - these will be implemented by all RML components
 */
typedef struct orte_rml_base_module_t {
    /* pointer to the parent component for this module */
    struct orte_rml_component_t                 *component;
    /* the routed module to be used */
    char                                        *routed;
    /** Ping process for connectivity check */
    orte_rml_module_ping_fn_t                    ping;

    /** Send non-blocking iovec message */
    orte_rml_module_send_nb_fn_t                 send_nb;

    /** Send non-blocking buffer message */
    orte_rml_module_send_buffer_nb_fn_t          send_buffer_nb;

    /** Purge information */
    orte_rml_module_purge_fn_t                   purge;
} orte_rml_base_module_t;


/* ******************************************************************** */
/*                 RML PUBLIC MODULE API DEFINITION                     */

/** Open conduit - call each component and see if they can provide a
 *  conduit that can satisfy all these attributes - return the conduit id
 *  (a negative value indicates error)
 */
typedef orte_rml_conduit_t (*orte_rml_API_open_conduit_fn_t)(opal_list_t *attributes);

/**
 * Close a conduit - allow the component to cleanup.
 */
typedef void (*orte_rml_API_close_conduit_fn_t)(orte_rml_conduit_t id);

/**
 * Query the library to provide all the supported interfaces/transport
 * providers in the current node/system.
 *
 * @param[out] List of providers and their attributes.
*/
typedef int (*orte_rml_API_query_transports_fn_t)(opal_list_t *transports);

/* query the routed module for a given conduit */
typedef char* (*orte_rml_API_query_routed_fn_t)(orte_rml_conduit_t id);

/**
 * "Ping" another process to determine availability
 *
 * Ping another process to determine if it is available.  This
 * function only verifies that the process is alive and will allow a
 * connection to the local process.  It does *not* qualify as
 * establishing communication with the remote process, as required by
 * the note for set_contact_info().
 *
 * @param[in] contact_info The contact info string for the remote process
 * @param[in] tv           Timeout after which the ping should be failed
 *
 * @retval ORTE_SUCESS The process is available and will allow connections
 *                     from the local process
 * @retval ORTE_ERROR  An unspecified error occurred during the update
 */
typedef int (*orte_rml_API_ping_fn_t)(orte_rml_conduit_t conduit_id,
                                      const char* contact_info,
                                      const struct timeval* tv);


/**
 * Send an iovec non-blocking message
 *
 * Send an array of iovecs to the specified peer.  The call
 * will return immediately, although the iovecs may not be modified
 * until the completion callback is triggered.  The iovecs *may* be
 * passed to another call to send_nb before the completion callback is
 * triggered.  The callback being triggered does not give any
 * indication of remote completion.
 *
 * @param[in] peer   Name of receiving process
 * @param[in] msg    Pointer to an array of iovecs to be sent
 * @param[in] count  Number of iovecs in array
 * @param[in] tag    User defined tag for matching send/recv
 * @param[in] cbfunc Callback function on message comlpetion
 * @param[in] cbdata User data to provide during completion callback
 *
 * @retval ORTE_SUCCESS The message was successfully started
 * @retval ORTE_ERR_BAD_PARAM One of the parameters was invalid
 * @retval ORTE_ERR_ADDRESSEE_UNKNOWN Contact information for the
 *                    receiving process is not available
 * @retval ORTE_ERROR  An unspecified error occurred
 */
typedef int (*orte_rml_API_send_nb_fn_t)(orte_rml_conduit_t conduit_id,
                                         orte_process_name_t* peer,
                                         struct iovec* msg,
                                         int count,
                                         orte_rml_tag_t tag,
                                         orte_rml_callback_fn_t cbfunc,
                                         void* cbdata);


/**
 * Send a buffer non-blocking message
 *
 * Send a buffer to the specified peer.  The call
 * will return immediately, although the buffer may not be modified
 * until the completion callback is triggered.  The buffer *may* be
 * passed to another call to send_nb before the completion callback is
 * triggered.  The callback being triggered does not give any
 * indication of remote completion.
 *
 * @param[in] peer   Name of receiving process
 * @param[in] buffer Pointer to buffer to be sent
 * @param[in] tag    User defined tag for matching send/recv
 * @param[in] cbfunc Callback function on message comlpetion
 * @param[in] cbdata User data to provide during completion callback
 *
 * @retval ORTE_SUCCESS The message was successfully started
 * @retval ORTE_ERR_BAD_PARAM One of the parameters was invalid
 * @retval ORTE_ERR_ADDRESSEE_UNKNOWN Contact information for the
 *                    receiving process is not available
 * @retval ORTE_ERROR  An unspecified error occurred
 */
typedef int (*orte_rml_API_send_buffer_nb_fn_t)(orte_rml_conduit_t conduit_id,
                                                orte_process_name_t* peer,
                                                struct opal_buffer_t* buffer,
                                                orte_rml_tag_t tag,
                                                orte_rml_buffer_callback_fn_t cbfunc,
                                                void* cbdata);

/**
 * Purge the RML/OOB of contact info and pending messages
 * to/from a specified process. Used when a process aborts
 * and is to be restarted
 */
typedef void (*orte_rml_API_purge_fn_t)(orte_process_name_t *peer);

/**
 * Receive an iovec non-blocking message
 *
 * @param[in]  peer    Peer process or ORTE_NAME_WILDCARD for wildcard receive
 * @param[in]  tag     User defined tag for matching send/recv
 * @param[in] persistent Boolean flag indicating whether or not this is a one-time recv
 * @param[in] cbfunc   Callback function on message comlpetion
 * @param[in] cbdata   User data to provide during completion callback
 */
typedef void (*orte_rml_API_recv_nb_fn_t)(orte_process_name_t* peer,
                                          orte_rml_tag_t tag,
                                          bool persistent,
                                          orte_rml_callback_fn_t cbfunc,
                                          void* cbdata);


/**
 * Receive a buffer non-blocking message
 *
 * @param[in]  peer    Peer process or ORTE_NAME_WILDCARD for wildcard receive
 * @param[in]  tag     User defined tag for matching send/recv
 * @param[in] persistent Boolean flag indicating whether or not this is a one-time recv
 * @param[in] cbfunc   Callback function on message comlpetion
 * @param[in] cbdata   User data to provide during completion callback
 */
typedef void (*orte_rml_API_recv_buffer_nb_fn_t)(orte_process_name_t* peer,
                                                 orte_rml_tag_t tag,
                                                 bool persistent,
                                                 orte_rml_buffer_callback_fn_t cbfunc,
                                                 void* cbdata);


/**
 * Cancel a posted non-blocking receive
 *
 * Attempt to cancel a posted non-blocking receive.
 *
 * @param[in] peer    Peer process or ORTE_NAME_WILDCARD, exactly as passed
 *                    to the non-blocking receive call
 * @param[in] tag     Posted receive tag
 */
typedef void (*orte_rml_API_recv_cancel_fn_t)(orte_process_name_t* peer,
                                              orte_rml_tag_t tag);

/**
 * RML API interface
 */
typedef struct {
    /** Open Conduit **/
    orte_rml_API_open_conduit_fn_t              open_conduit;

    /** Shutdown the conduit and clean up resources */
    orte_rml_API_close_conduit_fn_t             close_conduit;

    /** Ping process for connectivity check */
    orte_rml_API_ping_fn_t                      ping;

    /** Send non-blocking iovec message */
    orte_rml_API_send_nb_fn_t           send_nb;

    /** Send non-blocking buffer message */
    orte_rml_API_send_buffer_nb_fn_t    send_buffer_nb;

    /** Receive non-blocking iovec message */
    orte_rml_API_recv_nb_fn_t                   recv_nb;

    /** Receive non-blocking buffer message */
    orte_rml_API_recv_buffer_nb_fn_t            recv_buffer_nb;

    /** Cancel posted non-blocking receive */
    orte_rml_API_recv_cancel_fn_t               recv_cancel;

    /** Purge information */
    orte_rml_API_purge_fn_t                     purge;

    /** Query information of transport in system */
    orte_rml_API_query_transports_fn_t          query_transports;

    /* get the routed module for a given conduit */
    orte_rml_API_query_routed_fn_t              get_routed;
} orte_rml_base_API_t;

/** Interface for RML communication */
ORTE_DECLSPEC extern orte_rml_base_API_t orte_rml;

/* ******************************************************************** */
/*                     RML COMPONENT DEFINITION                         */

/**
 * RML open_conduit
 *
 * Create an instance (module) of the given RML component.  Upon
 * returning, the module data structure should be fully populated and
 * all functions should be usable and will have the conduit information.
 *
 * @param[in] opal_list_t of all attributes requested for the conduit.
 *            Each attribute will be key-value.
 *  [TODO] put in examples of the key-value here.
 * @return Exactly one module created by the call to the component's
 * initialization function should be returned.  The module structure
 * should be fully populated, and the priority should be set to a
 * reasonable value.
 *
 * @retval NULL An error occurred and initialization did not occur
 * @retval non-NULL The module was successfully initialized
 */
typedef orte_rml_base_module_t* (*orte_rml_component_open_conduit_fn_t)(opal_list_t *attributes);

/**
 * Query the library to provide all the supported interfaces/transport
 * providers in the current node/system.
 *
 */
typedef orte_rml_pathway_t* (*orte_rml_component_query_transports_fn_t)(void);

/** Close conduit - allow the specific component to
 *  cleanup the module for this conduit
 */
typedef void (*orte_rml_module_close_conduit_fn_t)(orte_rml_base_module_t *mod);

/**
 * RML component interface
 *
 * Component interface for the RML framework.  A public instance of
 * this structure, called mca_rml_[component name]_component, must
 * exist in any RML component.
 */
typedef struct orte_rml_component_t {
    /* Base component description */
    mca_base_component_t                        base;
    /* Base component data block */
    mca_base_component_data_t                   data;
    /* Component priority */
    int                                         priority;
    /* Component interface functions */
    orte_rml_component_open_conduit_fn_t        open_conduit;
    orte_rml_component_query_transports_fn_t    query_transports;
    orte_rml_module_close_conduit_fn_t          close_conduit;
} orte_rml_component_t;



/* ******************************************************************** */


/** Macro for use in components that are of type rml */
#define ORTE_RML_BASE_VERSION_3_0_0 \
    ORTE_MCA_BASE_VERSION_2_1_0("rml", 3, 0, 0)


/* ******************************************************************** */


END_C_DECLS

#endif
