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
 * Copyright (c) 2006-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2010-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_OOB_TCP_COMMON_H_
#define _MCA_OOB_TCP_COMMON_H_

#include "orte_config.h"

#include "oob_tcp.h"
#include "oob_tcp_peer.h"

ORTE_MODULE_DECLSPEC void orte_oob_tcp_set_socket_options(int sd);
ORTE_MODULE_DECLSPEC char* mca_oob_tcp_state_print(mca_oob_tcp_state_t state);
ORTE_MODULE_DECLSPEC mca_oob_tcp_peer_t* mca_oob_tcp_peer_lookup(const orte_process_name_t *name);
#endif /* _MCA_OOB_TCP_COMMON_H_ */
