/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Data packing subsystem.
 */

#ifndef PMIX_PTL_H_
#define PMIX_PTL_H_

#include <src/include/pmix_config.h>

#include <src/include/types.h>

#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"
#include "src/mca/bfrops/bfrops_types.h"

#include "ptl_types.h"

BEGIN_C_DECLS

/* forward declaration */
struct pmix_peer_t;

/* The overall objective of this framework is to provide transport
 * options by which a server can communicate with a client:
 *
 * (a) across different versions of the library - e.g., when the
 *     connection handshake changes.
 *
 * (b) using different transports as necessitated by different
 *     environments.
 *
 * This is a mult-select framework - i.e., multiple components
 * are selected and "active" at the same time. The intent is
 * to have one component for each use-case, with the
 * expectation that the community will do its best not to revise
 * communications in manners that expand components to support (a).
 * Thus, new variations should be rare, and only a few components
 * will exist.
 *
 * The framework itself reflects the fact that any given peer
 * will utilize only one messaging method.
 * Thus, once a peer is identified, it will pass its version string
 * to this framework's "assign_module" function, which will then
 * pass it to each component until one returns a module capable of
 * processing the given version. This module is then "attached" to
 * the pmix_peer_t object so it can be used for all subsequent
 * communication to/from that peer.
 *
 * Accordingly, there are two levels of APIs defined for this
 * framework:
 *
 * (a) component level - these allow for init/finalize of the
 *     component, and assignment of a module to a given peer
 *     based on the version that peer is using
 *
 * (b) module level - implement send/recv/etc. Note that the
 *     module only needs to provide those functions that differ
 *     from the base functions - they don't need to duplicate
 *     all that code!
 */

/****    MODULE INTERFACE DEFINITION    ****/

/* initialize an active plugin - note that servers may have
 * multiple active plugins, while clients can only have one */
typedef pmix_status_t (*pmix_ptl_init_fn_t)(void);

/* finalize an active plugin */
typedef void (*pmix_ptl_finalize_fn_t)(void);

/* (TWO-WAY) send a message to the peer, and get a response delivered
 * to the specified callback function. The buffer will be free'd
 * at the completion of the send, and the cbfunc will be called
 * when the corresponding reply is received */
typedef pmix_status_t (*pmix_ptl_send_recv_fn_t)(struct pmix_peer_t *peer,
                                                 pmix_buffer_t *bfr,
                                                 pmix_ptl_cbfunc_t cbfunc,
                                                 void *cbdata);

/* (ONE-WAY) send a message to the peer. The buffer will be free'd
 * at the completion of the send */
typedef pmix_status_t (*pmix_ptl_send_fn_t)(struct pmix_peer_t *peer,
                                            pmix_buffer_t *bfr,
                                            pmix_ptl_tag_t tag);

/* (ONE-WAY) register a persistent recv */
typedef pmix_status_t (*pmix_ptl_recv_fn_t)(struct pmix_peer_t *peer,
                                            pmix_ptl_cbfunc_t cbfunc,
                                            pmix_ptl_tag_t tag);

/* Cancel a persistent recv */
typedef pmix_status_t (*pmix_ptl_cancel_fn_t)(struct pmix_peer_t *peer,
                                              pmix_ptl_tag_t tag);

/* connect to a peer - this is a blocking function
 * to establish a connection to a peer. It assigns
 * the corresponding module to the peer's compat
 * structure for future use */
typedef pmix_status_t (*pmix_ptl_connect_to_peer_fn_t)(struct pmix_peer_t *peer,
                                                       pmix_info_t info[], size_t ninfo);


/**
 * Base structure for a PTL module
 */
struct pmix_ptl_module_t {
    pmix_ptl_init_fn_t                  init;
    pmix_ptl_finalize_fn_t              finalize;
    pmix_ptl_send_recv_fn_t             send_recv;
    pmix_ptl_send_fn_t                  send;
    pmix_ptl_recv_fn_t                  recv;
    pmix_ptl_cancel_fn_t                cancel;
    pmix_ptl_connect_to_peer_fn_t       connect_to_peer;
};
typedef struct pmix_ptl_module_t pmix_ptl_module_t;


/*****    MACROS FOR EXECUTING PTL FUNCTIONS    *****/
#define PMIX_PTL_SEND_RECV(r, p, b, c, d)                                               \
    do {                                                                                \
        if ((p)->finalized) {                                                           \
            (r) = PMIX_ERR_UNREACH;                                                     \
        } else {                                                                        \
            (r) = (p)->nptr->compat.ptl->send_recv((struct pmix_peer_t*)(p), b, c, d);  \
        }                                                                               \
    } while(0)

#define PMIX_PTL_SEND_ONEWAY(r, p, b, t)                                        \
    do {                                                                        \
        if ((p)->finalized) {                                                   \
            (r) = PMIX_ERR_UNREACH;                                             \
        } else {                                                                \
            (r) = (p)->nptr->compat.ptl->send((struct pmix_peer_t*)(p), b, t);  \
        }                                                                       \
    } while(0)

#define PMIX_PTL_RECV(r, p, c, t)      \
    (r) = (p)->nptr->compat.ptl->recv((struct pmix_peer_t*)(p), c, t)

#define PMIX_PTL_CANCEL(r, p, t)                        \
    (r) = (p)->nptr->compat.ptl->cancel((struct pmix_peer_t*)(p), t)

extern pmix_status_t pmix_ptl_base_connect_to_peer(struct pmix_peer_t* peer,
                                                   pmix_info_t info[], size_t ninfo);


/****    COMPONENT STRUCTURE DEFINITION    ****/

/* define a component-level API for establishing a
 * communication rendezvous point for local procs. Each active component
 * would be given an opportunity to register a listener with the
 * PTL base, and/or to establish their own method for handling
 * connection requests. The component sets the need_listener flag
 * to true if a listener thread is required - otherwise, it does _not_
 * modify this parameter */
typedef pmix_status_t (*pmix_ptl_base_setup_listener_fn_t)(pmix_info_t info[], size_t ninfo,
                                                           bool *need_listener);

/* define a component-level API for obtaining any envars that are to
 * be passed to client procs upon fork */
typedef pmix_status_t (*pmix_ptl_base_setup_fork_fn_t)(const pmix_proc_t *proc, char ***env);

/*
 * the standard component data structure
 */
struct pmix_ptl_base_component_t {
    pmix_mca_base_component_t                       base;
    pmix_mca_base_component_data_t                  data;
    int                                             priority;
    char*                                           uri;
    pmix_ptl_base_setup_listener_fn_t               setup_listener;
    pmix_ptl_base_setup_fork_fn_t                   setup_fork;

};
typedef struct pmix_ptl_base_component_t pmix_ptl_base_component_t;


/*
 * Macro for use in components that are of type ptl
 */
#define PMIX_PTL_BASE_VERSION_1_0_0 \
    PMIX_MCA_BASE_VERSION_1_0_0("ptl", 1, 0, 0)

END_C_DECLS

#endif /* PMIX_PTL_H */
