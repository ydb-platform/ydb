/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2013-2014 Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/util/show_help.h"

#include "ompi/runtime/params.h"
#include "ompi/runtime/mpiruntime.h"

static char *ompi_mpi_dynamics_disabled_msg = "Enabled";


void ompi_mpi_dynamics_disable(const char *msg)
{
    assert(msg);

    ompi_mpi_dynamics_enabled = false;
    ompi_mpi_dynamics_disabled_msg = strdup(msg);
}

bool ompi_mpi_dynamics_is_enabled(const char *function)
{
    if (ompi_mpi_dynamics_enabled) {
        return true;
    }

    opal_show_help("help-mpi-api.txt",
                   "MPI function not supported",
                   true,
                   function,
                   ompi_mpi_dynamics_disabled_msg);
    return false;
}

void ompi_mpi_dynamics_finalize(void)
{
    // If dynamics were disabled, then we have a message to free
    if (!ompi_mpi_dynamics_enabled) {
        free(ompi_mpi_dynamics_disabled_msg);
        ompi_mpi_dynamics_disabled_msg = NULL;
    }
}
