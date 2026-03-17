/* -*- C -*-
 *
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc.  All rights reserved.
 * Copyright (c) 2014-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#ifndef PMIX_PTL_BASE_H_
#define PMIX_PTL_BASE_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* for struct timeval */
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "src/class/pmix_pointer_array.h"
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_framework.h"

#include "src/include/pmix_globals.h"
#include "src/mca/ptl/ptl.h"


 BEGIN_C_DECLS

/*
 * MCA Framework
 */
PMIX_EXPORT extern pmix_mca_base_framework_t pmix_ptl_base_framework;
/**
 * PTL select function
 *
 * Cycle across available components and construct the list
 * of active modules
 */
PMIX_EXPORT pmix_status_t pmix_ptl_base_select(void);

/**
 * Track an active component
 */
struct pmix_ptl_base_active_t {
    pmix_list_item_t super;
    pmix_status_t pri;
    pmix_ptl_base_component_t *component;
    pmix_ptl_module_t *module;
};
typedef struct pmix_ptl_base_active_t pmix_ptl_base_active_t;
PMIX_CLASS_DECLARATION(pmix_ptl_base_active_t);


/* framework globals */
struct pmix_ptl_globals_t {
    pmix_list_t actives;
    bool initialized;
    pmix_list_t posted_recvs;     // list of pmix_ptl_posted_recv_t
    pmix_list_t unexpected_msgs;
    int stop_thread[2];
    bool listen_thread_active;
    pmix_list_t listeners;
    uint32_t current_tag;
    size_t max_msg_size;
};
typedef struct pmix_ptl_globals_t pmix_ptl_globals_t;

PMIX_EXPORT extern pmix_ptl_globals_t pmix_ptl_globals;

/* API stubs */
PMIX_EXPORT pmix_status_t pmix_ptl_base_set_notification_cbfunc(pmix_ptl_cbfunc_t cbfunc);
PMIX_EXPORT char* pmix_ptl_base_get_available_modules(void);
PMIX_EXPORT pmix_ptl_module_t* pmix_ptl_base_assign_module(void);
PMIX_EXPORT pmix_status_t pmix_ptl_base_connect_to_peer(struct pmix_peer_t *peer,
                                                        pmix_info_t info[], size_t ninfo);

PMIX_EXPORT pmix_status_t pmix_ptl_base_register_recv(struct pmix_peer_t *peer,
                                                      pmix_ptl_cbfunc_t cbfunc,
                                                      pmix_ptl_tag_t tag);
PMIX_EXPORT pmix_status_t pmix_ptl_base_cancel_recv(struct pmix_peer_t *peer,
                                                    pmix_ptl_tag_t tag);

PMIX_EXPORT pmix_status_t pmix_ptl_base_start_listening(pmix_info_t *info, size_t ninfo);
PMIX_EXPORT void pmix_ptl_base_stop_listening(void);
PMIX_EXPORT pmix_status_t pmix_ptl_base_setup_fork(const pmix_proc_t *proc, char ***env);

/* base support functions */
PMIX_EXPORT void pmix_ptl_base_send(int sd, short args, void *cbdata);
PMIX_EXPORT void pmix_ptl_base_send_recv(int sd, short args, void *cbdata);
PMIX_EXPORT void pmix_ptl_base_send_handler(int sd, short flags, void *cbdata);
PMIX_EXPORT void pmix_ptl_base_recv_handler(int sd, short flags, void *cbdata);
PMIX_EXPORT void pmix_ptl_base_process_msg(int fd, short flags, void *cbdata);
PMIX_EXPORT pmix_status_t pmix_ptl_base_set_nonblocking(int sd);
PMIX_EXPORT pmix_status_t pmix_ptl_base_set_blocking(int sd);
PMIX_EXPORT pmix_status_t pmix_ptl_base_send_blocking(int sd, char *ptr, size_t size);
PMIX_EXPORT pmix_status_t pmix_ptl_base_recv_blocking(int sd, char *data, size_t size);
PMIX_EXPORT pmix_status_t pmix_ptl_base_connect(struct sockaddr_storage *addr,
                                                pmix_socklen_t len, int *fd);
PMIX_EXPORT void pmix_ptl_base_connection_handler(int sd, short args, void *cbdata);
PMIX_EXPORT pmix_status_t pmix_ptl_base_send_connect_ack(int sd);
PMIX_EXPORT pmix_status_t pmix_ptl_base_recv_connect_ack(int sd);
PMIX_EXPORT void pmix_ptl_base_lost_connection(pmix_peer_t *peer, pmix_status_t err);


END_C_DECLS

#endif
