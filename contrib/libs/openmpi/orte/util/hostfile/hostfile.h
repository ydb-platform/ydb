/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
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
/**
 * @file
 *
 * Resource Discovery (Hostfile)
 */
#ifndef ORTE_UTIL_HOSTFILE_H
#define ORTE_UTIL_HOSTFILE_H

#include "orte_config.h"

#include "opal/class/opal_list.h"


BEGIN_C_DECLS

ORTE_DECLSPEC int orte_util_add_hostfile_nodes(opal_list_t *nodes,
                                               char *hostfile);

ORTE_DECLSPEC int orte_util_filter_hostfile_nodes(opal_list_t *nodes,
                                                  char *hostfile,
                                                  bool remove);

ORTE_DECLSPEC int orte_util_get_ordered_host_list(opal_list_t *nodes,
                                                  char *hostfile);

END_C_DECLS

#endif
