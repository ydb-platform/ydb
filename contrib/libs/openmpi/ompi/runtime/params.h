/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2013      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_RUNTIME_PARAMS_H
#define OMPI_RUNTIME_PARAMS_H

#include "ompi_config.h"

BEGIN_C_DECLS

/*
 * Global variables
 */

/**
 * Whether or not to check the parameters of top-level MPI API
 * functions or not.
 *
 * This variable should never be checked directly; the macro
 * MPI_PARAM_CHECK should be used instead.  This allows multiple
 * levels of MPI function parameter checking:
 *
 * #- Disable all parameter checking at configure/compile time
 * #- Enable all parameter checking at configure/compile time
 * #- Disable all parameter checking at run time
 * #- Enable all parameter checking at run time
 *
 * Hence, the MPI_PARAM_CHECK macro will either be "0", "1", or
 * "ompi_mpi_param_check".
 */
OMPI_DECLSPEC extern bool ompi_mpi_param_check;

/**
 * Whether or not to check for MPI handle leaks during MPI_FINALIZE.
 * If enabled, each MPI handle type will display a summary of the
 * handles that are still allocated during MPI_FINALIZE.
 *
 * This is good debugging for user applications to find out if they
 * are inadvertantly orphaning MPI handles.
 */
OMPI_DECLSPEC extern bool ompi_debug_show_handle_leaks;

/**
 * If > 0, show that many MPI_ALLOC_MEM leaks during MPI_FINALIZE.  If
 * enabled, memory that was returned via MPI_ALLOC_MEM but was never
 * freed via MPI_FREE_MEM will be displayed during MPI_FINALIZE.
 *
 * This is good debugging for user applications to find out if they
 * are inadvertantly orphaning MPI "special" memory.
 */
OMPI_DECLSPEC extern int ompi_debug_show_mpi_alloc_mem_leaks;

/**
 * Whether or not to actually free MPI handles when their
 * corresponding destructor is invoked.  If enabled, Open MPI will not
 * free handles, but will rather simply mark them as "freed".  Any
 * attempt to use them will result in an MPI exception.
 *
 * This is good debugging for user applications to find out if they
 * are inadvertantly using MPI handles after they have been freed.
 */
OMPI_DECLSPEC extern bool ompi_debug_no_free_handles;

/**
 * Whether or not to print MCA parameters on MPI_INIT
 *
 * This is good debugging for user applications to see exactly which
 * MCA parameters are being used in the current program execution.
 */
OMPI_DECLSPEC extern bool ompi_mpi_show_mca_params;

/**
 * Whether or not to print the MCA parameters to a file or to stdout
 *
 * If this argument is set then it is used when parameters are dumped
 * when the mpi_show_mca_params is set.
 */
OMPI_DECLSPEC extern char * ompi_mpi_show_mca_params_file;

/**
 * Whether an MPI_ABORT should print out a stack trace or not.
 */
OMPI_DECLSPEC extern bool ompi_mpi_abort_print_stack;

/**
 * Whether  MPI_ABORT  should  print  out an  identifying  message
 * (e.g., hostname  and PID)  and loop waiting  for a  debugger to
 * attach.  The value of the integer is how many seconds to wait:
 *
 * 0 = do not print the message and do not loop
 * negative value = print the message and loop forever
 * positive value = print the message and delay for that many seconds
 */
OMPI_DECLSPEC extern int ompi_mpi_abort_delay;

/**
 * Whether sparse MPI group storage formats are supported or not.
 */
OMPI_DECLSPEC extern bool ompi_have_sparse_group_storage;

/**
 * Whether sparse MPI group storage formats should be used or not.
 */
OMPI_DECLSPEC extern bool ompi_use_sparse_group_storage;

/**
 * Cutoff point for calling add_procs for all processes
 */
OMPI_DECLSPEC extern uint32_t ompi_add_procs_cutoff;

/**
 * Whether anything in the code base has disabled MPI dynamic process
 * functionality or not
 */
OMPI_DECLSPEC extern bool ompi_mpi_dynamics_enabled;

/* EXPERIMENTAL: do not perform an RTE barrier at the end of MPI_Init */
OMPI_DECLSPEC extern bool ompi_async_mpi_init;

/* EXPERIMENTAL: do not perform an RTE barrier at the beginning of MPI_Finalize */
OMPI_DECLSPEC extern bool ompi_async_mpi_finalize;

/**
 * A comma delimited list of SPC counters to turn on or 'attach'.  To turn
 * all counters on, the string can be simply "all".  An empty string will
 * keep all counters turned off.
 */
OMPI_DECLSPEC extern char * ompi_mpi_spc_attach_string;

/**
 * A boolean value that determines whether or not to dump the SPC counter
 * values in MPI_Finalize.  A value of true dumps the counters and false does not.
 */
OMPI_DECLSPEC extern bool ompi_mpi_spc_dump_enabled;


/**
 * Register MCA parameters used by the MPI layer.
 *
 * @returns OMPI_SUCCESS
 *
 * Registers several MCA parameters and initializes corresponding
 * global variables to the values obtained from the MCA system.
 */
OMPI_DECLSPEC int ompi_mpi_register_params(void);

/**
 * Display all MCA parameters used
 *
 * @returns OMPI_SUCCESS
 *
 * Displays in key = value format
 */
int ompi_show_all_mca_params(int32_t, int, char *);

END_C_DECLS

#endif /* OMPI_RUNTIME_PARAMS_H */
