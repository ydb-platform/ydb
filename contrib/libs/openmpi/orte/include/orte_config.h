/* -*- c -*-
 *
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
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * Function: - OS, CPU and compiler dependent configuration
 */

#ifndef ORTE_CONFIG_H
#define ORTE_CONFIG_H

#include "opal_config.h"

#define ORTE_IDENT_STRING OPAL_IDENT_STRING

#  if OPAL_C_HAVE_VISIBILITY
#    define ORTE_DECLSPEC           __opal_attribute_visibility__("default")
#    define ORTE_MODULE_DECLSPEC    __opal_attribute_visibility__("default")
#  else
#    define ORTE_DECLSPEC
#    define ORTE_MODULE_DECLSPEC
#  endif

#endif
