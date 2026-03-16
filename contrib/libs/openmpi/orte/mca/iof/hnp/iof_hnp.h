/*
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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2016      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * The hnp IOF component is used in HNP processes only.  It is the
 * "hub" for all IOF activity, meaning that *all* IOF traffic is
 * routed to the hnp component, and this component figures out where
 * it is supposed to go from there.  Specifically: there is *no*
 * direct proxy-to-proxy IOF communication.  If a proxy/orted wants to
 * get a stream from another proxy/orted, the stream will go
 * proxy/orted -> HNP -> proxy/orted.
 *
 * The hnp IOF component does two things: 1. forward fragments between
 * file descriptors and streams, and 2. maintain forwarding tables to
 * "route" incoming fragments to outgoing destinations (both file
 * descriptors and other published streams).
 *
 */

#ifndef ORTE_IOF_HNP_H
#define ORTE_IOF_HNP_H

#include "orte_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif  /* HAVE_SYS_TYPES_H */
#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif  /* HAVE_SYS_UIO_H */
#ifdef HAVE_NET_UIO_H
#include <net/uio.h>
#endif  /* HAVE_NET_UIO_H */

#include "orte/mca/iof/iof.h"
#include "orte/mca/iof/base/base.h"


BEGIN_C_DECLS

/**
 * IOF HNP Component
 */
struct orte_iof_hnp_component_t {
    orte_iof_base_component_t super;
    opal_list_t procs;
    orte_iof_read_event_t *stdinev;
    opal_event_t stdinsig;
};
typedef struct orte_iof_hnp_component_t orte_iof_hnp_component_t;

ORTE_MODULE_DECLSPEC extern orte_iof_hnp_component_t mca_iof_hnp_component;
extern orte_iof_base_module_t orte_iof_hnp_module;

void orte_iof_hnp_recv(int status, orte_process_name_t* sender,
                       opal_buffer_t* buffer, orte_rml_tag_t tag,
                       void* cbdata);

void orte_iof_hnp_read_local_handler(int fd, short event, void *cbdata);
void orte_iof_hnp_stdin_cb(int fd, short event, void *cbdata);
bool orte_iof_hnp_stdin_check(int fd);

int orte_iof_hnp_send_data_to_endpoint(orte_process_name_t *host,
                                       orte_process_name_t *target,
                                       orte_iof_tag_t tag,
                                       unsigned char *data, int numbytes);

END_C_DECLS

#endif
