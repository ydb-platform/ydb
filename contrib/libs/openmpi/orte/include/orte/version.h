/*
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
 * Copyright (c) 2011 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * This file should be included by any file that needs full
 * version information for the ORTE project
 */

#ifndef ORTE_VERSIONS_H
#define ORTE_VERSIONS_H

#define ORTE_MAJOR_VERSION 4
#define ORTE_MINOR_VERSION 0
#define ORTE_RELEASE_VERSION 1
#define ORTE_GREEK_VERSION ""
#define ORTE_WANT_REPO_REV @ORTE_WANT_REPO_REV@
#define ORTE_REPO_REV "v4.0.1"
#ifdef ORTE_VERSION
/* If we included version.h, we want the real version, not the
   stripped (no-r number) verstion */
#undef ORTE_VERSION
#endif
#define ORTE_VERSION "4.0.1"

#endif
