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
 * Copyright (c) 2007      Cisco Systems, Inc.   All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
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
 * The orted IOF component is used in daemons.  It is used
 * to orted all IOF actions back to the "hnp" IOF component (i.e., the
 * IOF component that runs in the HNP).  The orted IOF component is
 * loaded in an orted and then tied to the stdin, stdout,
 * and stderr streams of created child processes via pipes.  The orted
 * IOF component in the orted then acts as the relay between the
 * stdin/stdout/stderr pipes and the IOF component in the HNP.
 * This design allows us to manipulate stdin/stdout/stderr from before
 * main() in the child process.
 *
 * Much of the intelligence of this component is actually contained in
 * iof_base_endpoint.c (reading and writing to local file descriptors,
 * setting up events based on file descriptors, etc.).
 *
 * A non-blocking OOB receive is posted at the initialization of this
 * component to receive all messages from the HNP (e.g., data
 * fragments from streams, ACKs to fragments).
 *
 * Flow control is employed on a per-stream basis to ensure that
 * SOURCEs don't overwhelm SINK resources (E.g., send an entire input
 * file to an orted before the target process has read any of it).
 *
 */
#ifndef ORTE_IOF_ORTED_H
#define ORTE_IOF_ORTED_H

#include "orte_config.h"

#include "opal/class/opal_list.h"

#include "orte/mca/rml/rml_types.h"

#include "orte/mca/iof/iof.h"

BEGIN_C_DECLS

/**
 * IOF ORTED Component
 */
struct orte_iof_orted_component_t {
    orte_iof_base_component_t super;
    opal_list_t procs;
    bool xoff;
};
typedef struct orte_iof_orted_component_t orte_iof_orted_component_t;

ORTE_MODULE_DECLSPEC extern orte_iof_orted_component_t mca_iof_orted_component;
extern orte_iof_base_module_t orte_iof_orted_module;

void orte_iof_orted_recv(int status, orte_process_name_t* sender,
                         opal_buffer_t* buffer, orte_rml_tag_t tag,
                         void* cbdata);

void orte_iof_orted_read_handler(int fd, short event, void *data);
void orte_iof_orted_send_xonxoff(orte_iof_tag_t tag);

END_C_DECLS

#endif
