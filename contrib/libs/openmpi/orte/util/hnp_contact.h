/*
 *
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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * $Id: ompi_universe_setup_file I/O functions $
 *
 */

#ifndef ORTE_HNP_CONTACT_H
#define ORTE_HNP_CONTACT_H

#include "orte_config.h"
#include "orte/types.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "opal/class/opal_list.h"

BEGIN_C_DECLS

/* define a structure that can be used to create
* a list of HNP names and contact info. This is
* generally only useful for tools that want to
* contact an HNP
*/
typedef struct {
    /* Base object */
    opal_list_item_t super;
    /* process name for HNP */
    orte_process_name_t name;
    /* RML contact uri */
    char *rml_uri;
    /* pid */
    pid_t pid;
} orte_hnp_contact_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_hnp_contact_t);

ORTE_DECLSPEC int orte_write_hnp_contact_file(char *filename);

ORTE_DECLSPEC int orte_read_hnp_contact_file(char *filename, orte_hnp_contact_t *hnp, bool connect);

ORTE_DECLSPEC int orte_list_local_hnps(opal_list_t *hnps, bool connect);

END_C_DECLS
#endif
