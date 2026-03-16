/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2016 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * MPI portion of debugger support
 */

#ifndef OMPI_DEBUGGERS_H
#define OMPI_DEBUGGERS_H

#include "ompi_config.h"

BEGIN_C_DECLS

/**
 * Setup the magic constants so that the debugger can find the DLL
 * necessary for understanding the queues and other structures.
 */
extern void ompi_debugger_setup_dlls(void);

/**
 * Flag debugger will set when an application may proceed past
 * MPI_INIT.  This needs to live in ompi_debuggers.c so that it's
 * compiled with -g, but is needed by the runtime framework for
 * startup
 */
OMPI_DECLSPEC extern volatile int MPIR_debug_gate;

/**
 * Flag debugger will set if application is being debugged.  This
 * needs to live in ompi_debuggers.c so that it's compiled with -g,
 * but is needed by the runtime framework for startup.
 */
OMPI_DECLSPEC extern volatile int MPIR_being_debugged;

END_C_DECLS

#endif /* OMPI_DEBUGGERS_H */
