/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Data server for OpenRTE
 */
#ifndef ORTE_DATA_SERVER_H
#define ORTE_DATA_SERVER_H

#include "orte_config.h"
#include "orte/types.h"

#include "opal/dss/dss_types.h"
#include "orte/mca/rml/rml_types.h"

BEGIN_C_DECLS

#define ORTE_PMIX_PUBLISH_CMD           0x01
#define ORTE_PMIX_LOOKUP_CMD            0x02
#define ORTE_PMIX_UNPUBLISH_CMD         0x03
#define ORTE_PMIX_PURGE_PROC_CMD        0x04

/* provide hooks to startup and finalize the data server */
ORTE_DECLSPEC int orte_data_server_init(void);
ORTE_DECLSPEC void orte_data_server_finalize(void);

/* provide hook for the non-blocking receive */
ORTE_DECLSPEC void orte_data_server(int status, orte_process_name_t* sender,
                                    opal_buffer_t* buffer, orte_rml_tag_t tag,
                                    void* cbdata);

END_C_DECLS

#endif /* ORTE_DATA_SERVER_H */
