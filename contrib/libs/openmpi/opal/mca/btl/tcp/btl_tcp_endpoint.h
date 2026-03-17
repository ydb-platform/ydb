/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_BTL_TCP_ENDPOINT_H
#define MCA_BTL_TCP_ENDPOINT_H

#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "btl_tcp_frag.h"
#include "btl_tcp.h"
BEGIN_C_DECLS

#define MCA_BTL_TCP_ENDPOINT_CACHE 1
#define MCA_BTL_TCP_MAGIC_STRING_LENGTH 16
/**
 * State of TCP endpoint connection.
 */

typedef enum {
    MCA_BTL_TCP_CONNECTING = 0,
    MCA_BTL_TCP_CONNECT_ACK,
    MCA_BTL_TCP_CLOSED,
    MCA_BTL_TCP_FAILED,
    MCA_BTL_TCP_CONNECTED
} mca_btl_tcp_state_t;

/**
 * An abstraction that represents a connection to a endpoint process.
 * An instance of mca_btl_base_endpoint_t is associated w/ each process
 * and BTL pair at startup. However, connections to the endpoint
 * are established dynamically on an as-needed basis:
 */

struct mca_btl_base_endpoint_t {
    opal_list_item_t                super;
    struct mca_btl_tcp_module_t*    endpoint_btl;          /**< BTL instance that created this connection */
    struct mca_btl_tcp_proc_t*      endpoint_proc;         /**< proc structure corresponding to endpoint */
    struct mca_btl_tcp_addr_t*      endpoint_addr;         /**< address of endpoint */
    int                             endpoint_sd;           /**< socket connection to endpoint */
    int                             endpoint_sd_next;      /**< deadlock avoidance: socket connection to endpoint to set once the endpoint_sd has been correctly closed */
#if MCA_BTL_TCP_ENDPOINT_CACHE
    char*                           endpoint_cache;        /**< cache for the recv (reduce the number of recv syscall) */
    char*                           endpoint_cache_pos;    /**< current position in the cache */
    size_t                          endpoint_cache_length; /**< length of the data in the cache */
#endif  /* MCA_BTL_TCP_ENDPOINT_CACHE */
    struct mca_btl_tcp_frag_t*      endpoint_send_frag;    /**< current send frag being processed */
    struct mca_btl_tcp_frag_t*      endpoint_recv_frag;    /**< current recv frag being processed */
    mca_btl_tcp_state_t             endpoint_state;        /**< current state of the connection */
    uint32_t                        endpoint_retries;      /**< number of connection retries attempted */
    opal_list_t                     endpoint_frags;        /**< list of pending frags to send */
    opal_mutex_t                    endpoint_send_lock;    /**< lock for concurrent access to endpoint state */
    opal_mutex_t                    endpoint_recv_lock;    /**< lock for concurrent access to endpoint state */
    opal_event_t                    endpoint_accept_event;   /**< event for async processing of accept requests */
    opal_event_t                    endpoint_send_event;   /**< event for async processing of send frags */
    opal_event_t                    endpoint_recv_event;   /**< event for async processing of recv frags */
    bool                            endpoint_nbo;          /**< convert headers to network byte order? */
};

typedef struct mca_btl_base_endpoint_t mca_btl_base_endpoint_t;
typedef mca_btl_base_endpoint_t  mca_btl_tcp_endpoint_t;
OBJ_CLASS_DECLARATION(mca_btl_tcp_endpoint_t);

/* Magic socket handshake string */
extern const char mca_btl_tcp_magic_id_string[MCA_BTL_TCP_MAGIC_STRING_LENGTH];

typedef struct { 
    opal_process_name_t guid;
    char magic_id[MCA_BTL_TCP_MAGIC_STRING_LENGTH];
} mca_btl_tcp_endpoint_hs_msg_t;

void mca_btl_tcp_set_socket_options(int sd);
void mca_btl_tcp_endpoint_close(mca_btl_base_endpoint_t*);
int  mca_btl_tcp_endpoint_send(mca_btl_base_endpoint_t*, struct mca_btl_tcp_frag_t*);
void mca_btl_tcp_endpoint_accept(mca_btl_base_endpoint_t*, struct sockaddr*, int);
void mca_btl_tcp_endpoint_shutdown(mca_btl_base_endpoint_t*);

/*
 * Diagnostics: change this to "1" to enable the function
 * mca_btl_tcp_endpoint_dump(), below
 */
#define WANT_PEER_DUMP 0

#if OPAL_ENABLE_DEBUG && WANT_PEER_DUMP
#define MCA_BTL_TCP_ENDPOINT_DUMP(LEVEL, ENDPOINT, INFO, MSG) mca_btl_tcp_endpoint_dump((LEVEL), __FILE__, __LINE__, __func__, (ENDPOINT), (INFO), (MSG))
void mca_btl_tcp_endpoint_dump(int level, const char* fname, int lineno, const char* funcname,
                               mca_btl_base_endpoint_t* btl_endpoint, bool full_info, const char* msg);
#else
#define MCA_BTL_TCP_ENDPOINT_DUMP(LEVEL, ENDPOINT, INFO, MSG)
#endif  /* OPAL_ENABLE_DEBUG && WANT_PEER_DUMP */

END_C_DECLS

#endif
