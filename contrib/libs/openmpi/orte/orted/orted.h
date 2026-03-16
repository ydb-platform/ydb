/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
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

#ifndef ORTED_H
#define ORTED_H

#include "orte_config.h"
#include "orte/types.h"

#include <time.h>

#include "opal/dss/dss_types.h"
#include "opal/class/opal_pointer_array.h"
#include "orte/mca/rml/rml_types.h"

BEGIN_C_DECLS

/* main orted routine */
ORTE_DECLSPEC int orte_daemon(int argc, char *argv[]);

/* orted communication functions */
ORTE_DECLSPEC void orte_daemon_recv(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                      void* cbdata);

/* direct cmd processing entry points */
ORTE_DECLSPEC void orte_daemon_cmd_processor(int fd, short event, void *data);
ORTE_DECLSPEC int orte_daemon_process_commands(orte_process_name_t* sender,
                                               opal_buffer_t *buffer,
                                               orte_rml_tag_t tag);

END_C_DECLS

/* Local function */
int send_to_local_applications(opal_pointer_array_t *dead_names);

#endif /* ORTED_H */
