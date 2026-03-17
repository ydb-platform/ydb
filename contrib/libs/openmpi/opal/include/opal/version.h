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
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * This file should be included by any file that needs full
 * version information for the OPAL project
 */

#ifndef OPAL_VERSIONS_H
#define OPAL_VERSIONS_H

#define OPAL_MAJOR_VERSION 4
#define OPAL_MINOR_VERSION 0
#define OPAL_RELEASE_VERSION 1
#define OPAL_GREEK_VERSION ""
#define OPAL_WANT_REPO_REV @OPAL_WANT_REPO_REV@
#define OPAL_REPO_REV "v4.0.1"
#ifdef OPAL_VERSION
/* If we included version.h, we want the real version, not the
   stripped (no-r number) verstion */
#undef OPAL_VERSION
#endif
#define OPAL_VERSION "4.0.1"
#define OPAL_CONFIGURE_CLI " \'--disable-static\' \'--prefix=/var/empty/openmpi-4.0.1\' \'--disable-io-romio\' \'--disable-mpi-fortran\' \'--disable-oshmem-fortran\' \'--enable-cxx-exceptions\' \'--enable-ipv6\' \'--enable-mca-static\' \'--enable-mpi-cxx\' \'--with-cuda=/var/empty/cudatoolkit-9.2.148.1\' \'--with-dl=dlopen\' \'--without-memory-manager\'"

#endif
