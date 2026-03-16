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
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * The tool IOF component is used in tools. It is used
 * to interface to the HNP to request forwarding of stdout/err/diag
 * from any combination of procs, and to forward stdin from the
 * tool to a specified proc provided the user has allowed that
 * functionality.
 *
 * Flow control is employed on a per-stream basis to ensure that
 * SOURCEs don't overwhelm SINK resources (E.g., send an entire input
 * file to an orted before the target process has read any of it).
 *
 */
#ifndef ORTE_IOF_TOOL_H
#define ORTE_IOF_TOOL_H

#include "orte_config.h"
#include "orte/mca/iof/iof.h"

BEGIN_C_DECLS

struct orte_iof_tool_component_t {
    orte_iof_base_component_t super;
    bool closed;
};
typedef struct orte_iof_tool_component_t orte_iof_tool_component_t;

ORTE_MODULE_DECLSPEC extern orte_iof_tool_component_t mca_iof_tool_component;
extern orte_iof_base_module_t orte_iof_tool_module;

void orte_iof_tool_recv(int status, orte_process_name_t* sender,
                         opal_buffer_t* buffer, orte_rml_tag_t tag,
                         void* cbdata);

END_C_DECLS

#endif
