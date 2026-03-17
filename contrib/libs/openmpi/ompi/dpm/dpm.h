/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013-2015 Intel, Inc. All rights reserved
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Dynamic Process Management Interface
 *
 */

#ifndef OMPI_DPM_H
#define OMPI_DPM_H

#include "ompi_config.h"

#include "ompi/info/info.h"
#include "ompi/communicator/communicator.h"

BEGIN_C_DECLS

/*
 * Initialize the DPM system
 */
int ompi_dpm_init(void);

/*
 * Connect/accept communications
 */
int ompi_dpm_connect_accept(ompi_communicator_t *comm, int root,
                            const char *port, bool send_first,
                            ompi_communicator_t **newcomm);

/**
 * Executes internally a disconnect on all dynamic communicators
 * in case the user did not disconnect them.
 */
int ompi_dpm_disconnect(ompi_communicator_t *comm);

/*
 * Dynamically spawn processes
 */
int ompi_dpm_spawn(int count, char const *array_of_commands[],
                   char **array_of_argv[],
                   const int array_of_maxprocs[],
                   const MPI_Info array_of_info[],
                   const char *port_name);

/*
 * This routine checks, whether an application has been spawned
 * by another MPI application, or has been independently started.
 * If it has been spawned, it establishes the parent communicator.
 * Since the routine has to communicate, it should be among the last
 * steps in MPI_Init, to be sure that everything is already set up.
 */
int ompi_dpm_dyn_init(void);

/*
 * Interface for mpi_finalize to call to ensure dynamically spawned procs
 * collectively finalize
 */
int ompi_dpm_dyn_finalize(void);

/* this routine counts the number of different jobids of the processes
   given in a certain communicator. If there is more than one jobid,
   we mark the communicator as 'dynamic'. This is especially relevant
   for the MPI_Comm_disconnect *and* for MPI_Finalize, where we have
   to wait for all still connected processes.
*/
void ompi_dpm_mark_dyncomm(ompi_communicator_t *comm);

/*
 * Define a rendezvous point for a dynamically spawned job
 */
int ompi_dpm_open_port(char *port_name);

/*
 * Unpublish the rendezvous point
 */
int ompi_dpm_close_port(const char *port_name);

/*
 * Finalize the DPM
 */
int ompi_dpm_finalize(void);

END_C_DECLS

#endif /* OMPI_DPM_H */
