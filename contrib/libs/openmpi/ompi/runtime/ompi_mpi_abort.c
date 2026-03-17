/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2010-2011 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2017      FUJITSU LIMITED.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <errno.h>

#include "opal/mca/backtrace/backtrace.h"
#include "opal/util/error.h"
#include "opal/runtime/opal_params.h"

#include "ompi/communicator/communicator.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/runtime/params.h"
#include "ompi/debuggers/debuggers.h"
#include "ompi/errhandler/errcode.h"

static bool have_been_invoked = false;


/*
 * Local helper function to build an array of all the procs in a
 * communicator, excluding this process.
 *
 * Killing a just the indicated peers must be implemented for
 * MPI_Abort() to work according to the standard language for
 * a 'high-quality' implementation.
 *
 * It would be nifty if we could differentiate between the
 * abort scenarios (but we don't, currently):
 *      - MPI_Abort()
 *      - MPI_ERRORS_ARE_FATAL
 *      - Victim of MPI_Abort()
 */
static void try_kill_peers(ompi_communicator_t *comm,
                           int errcode)
{
    int nprocs;
    ompi_process_name_t *procs;

    nprocs = ompi_comm_size(comm);
    /* ompi_comm_remote_size() returns 0 if not an intercomm, so
       this is safe */
    nprocs += ompi_comm_remote_size(comm);

    procs = (ompi_process_name_t*) calloc(nprocs, sizeof(ompi_process_name_t));
    if (NULL == procs) {
        /* quick clean orte and get out */
        ompi_rte_abort(errno, "Abort: unable to alloc memory to kill procs");
    }

    /* put all the local group procs in the abort list */
    int rank, i, count;
    rank = ompi_comm_rank(comm);
    for (count = i = 0; i < ompi_comm_size(comm); ++i) {
        if (rank == i) {
            /* Don't include this process in the array */
            --nprocs;
        } else {
            assert(count <= nprocs);
            procs[count++] =
                *OMPI_CAST_RTE_NAME(&ompi_group_get_proc_ptr(comm->c_remote_group, i, true)->super.proc_name);
        }
    }

    /* if requested, kill off remote group procs too */
    for (i = 0; i < ompi_comm_remote_size(comm); ++i) {
        assert(count <= nprocs);
        procs[count++] =
            *OMPI_CAST_RTE_NAME(&ompi_group_get_proc_ptr(comm->c_remote_group, i, true)->super.proc_name);
    }

    if (nprocs > 0) {
        ompi_rte_abort_peers(procs, nprocs, errcode);
    }

    /* We could fall through here if ompi_rte_abort_peers() fails, or
       if (nprocs == 0).  Either way, tidy up and let the caller
       handle it. */
    free(procs);
}

int
ompi_mpi_abort(struct ompi_communicator_t* comm,
               int errcode)
{
    char *host, hostname[OPAL_MAXHOSTNAMELEN];
    pid_t pid = 0;

    /* Protection for recursive invocation */
    if (have_been_invoked) {
        return OMPI_SUCCESS;
    }
    have_been_invoked = true;

    /* If MPI is initialized, we know we have a runtime nodename, so
       use that.  Otherwise, call gethostname. */
    if (ompi_rte_initialized) {
        host = ompi_process_info.nodename;
    } else {
        gethostname(hostname, sizeof(hostname));
        host = hostname;
    }
    pid = getpid();

    /* Should we print a stack trace?  Not aggregated because they
       might be different on all processes. */
    if (opal_abort_print_stack) {
        char **messages;
        int len, i;

        if (OPAL_SUCCESS == opal_backtrace_buffer(&messages, &len)) {
            for (i = 0; i < len; ++i) {
                fprintf(stderr, "[%s:%05d] [%d] func:%s\n", host, (int) pid,
                        i, messages[i]);
                fflush(stderr);
            }
            free(messages);
        } else {
            /* This will print an message if it's unable to print the
               backtrace, so we don't need an additional "else" clause
               if opal_backtrace_print() is not supported. */
            opal_backtrace_print(stderr, NULL, 1);
        }
    }

    /* Wait for a while before aborting */
    opal_delay_abort();

    /* If the RTE isn't setup yet/any more, then don't even try
       killing everyone.  Sorry, Charlie... */
    int32_t state = ompi_mpi_state;
    if (!ompi_rte_initialized) {
        fprintf(stderr, "[%s:%05d] Local abort %s completed successfully, but am not able to aggregate error messages, and not able to guarantee that all other processes were killed!\n",
                host, (int) pid,
                state >= OMPI_MPI_STATE_FINALIZE_STARTED ?
                "after MPI_FINALIZE started" : "before MPI_INIT completed");
        _exit(errcode == 0 ? 1 : errcode);
    }

    /* If OMPI is initialized and we have a non-NULL communicator,
       then try to kill just that set of processes */
    if (state >= OMPI_MPI_STATE_INIT_COMPLETED &&
        state < OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT &&
        NULL != comm) {
        try_kill_peers(comm, errcode);
    }

    /* We can fall through to here in a few cases:

       1. The attempt to kill just a subset of peers via
          try_kill_peers() failed (e.g., as of July 2014, ORTE does
          returns NOT_IMPLENTED from orte_rte_abort_peers()).
       2. MPI wasn't initialized, was already finalized, or we got a
          NULL communicator.

       In all of these cases, the only sensible thing left to do is to
       kill the entire job.  Wah wah. */
    ompi_rte_abort(errcode, NULL);

    /* Does not return */
}
