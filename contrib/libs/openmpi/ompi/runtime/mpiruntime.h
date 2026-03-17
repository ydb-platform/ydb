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
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      University of Houston.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Interface into the MPI portion of the Open MPI Run Time Environment
 */

#ifndef OMPI_MPI_MPIRUNTIME_H
#define OMPI_MPI_MPIRUNTIME_H

#include "ompi_config.h"

#include "opal/class/opal_list.h"
#include "opal/class/opal_hash_table.h"
#include "opal/threads/mutex.h"

BEGIN_C_DECLS

/** forward type declaration */
struct ompi_communicator_t;
/** forward type declaration */
struct opal_thread_t;
/** forward type declaration */
struct ompi_predefined_datatype_t;

/* Global variables and symbols for the MPI layer */

/** Mutex to protect all the _init and _finalize variables */
OMPI_DECLSPEC extern opal_mutex_t ompi_mpi_bootstrap_mutex;
/** Did MPI start to initialize? */
OMPI_DECLSPEC extern volatile int32_t ompi_mpi_state;
/** Has the RTE been initialized? */
OMPI_DECLSPEC extern volatile bool ompi_rte_initialized;

/** Do we have multiple threads? */
OMPI_DECLSPEC extern bool ompi_mpi_thread_multiple;
/** Thread level requested to \c MPI_Init_thread() */
OMPI_DECLSPEC extern int ompi_mpi_thread_requested;
/** Thread level provided by Open MPI */
OMPI_DECLSPEC extern int ompi_mpi_thread_provided;
/** Identifier of the main thread */
OMPI_DECLSPEC extern struct opal_thread_t *ompi_mpi_main_thread;

/*
 * State of the MPI runtime.
 *
 * Atomically set/read in the ompi_mpi_state global variable (for
 * functions such as MPI_INITIALIZED and MPI_FINALIZED).
 */
typedef enum {
    OMPI_MPI_STATE_NOT_INITIALIZED = 0,

    OMPI_MPI_STATE_INIT_STARTED,
    OMPI_MPI_STATE_INIT_COMPLETED,

    /* The PAST_COMM_SELF_DESTRUCT state is needed because attribute
       callbacks that are invoked during the very beginning of
       MPI_FINALIZE are supposed to return FALSE if they call
       MPI_FINALIZED.  Hence, we need to distinguish between "We've
       started MPI_FINALIZE" and "We're far enough in MPI_FINALIZE
       that we now need to return TRUE from MPI_FINALIZED." */
    OMPI_MPI_STATE_FINALIZE_STARTED,
    OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT,
    OMPI_MPI_STATE_FINALIZE_COMPLETED
} ompi_mpi_state_t;

/*
 * These variables are for the MPI F03 bindings (F03 must bind Fortran
 * varaiables to symbols; it cannot bind Fortran variables to the
 * address of a C variable).
 */

OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_character_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_logical_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_logical1_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_logical2_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_logical4_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_logical8_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_integer_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_integer1_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_integer2_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_integer4_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_integer8_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_integer16_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_real_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_real4_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_real8_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_real16_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_dblprec_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_cplex_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_complex8_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_complex16_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_complex32_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_dblcplex_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_2real_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_2dblprec_addr;
OMPI_DECLSPEC extern struct ompi_predefined_datatype_t *ompi_mpi_2integer_addr;

OMPI_DECLSPEC extern struct ompi_status_public_t *ompi_mpi_status_ignore_addr;
OMPI_DECLSPEC extern struct ompi_status_public_t *ompi_mpi_statuses_ignore_addr;

/** Bitflags to be used for the modex exchange for the various thread
 *  levels. Required to support heterogeneous environments */
#define OMPI_THREADLEVEL_SINGLE_BF     0x00000001
#define OMPI_THREADLEVEL_FUNNELED_BF   0x00000002
#define OMPI_THREADLEVEL_SERIALIZED_BF 0x00000004
#define OMPI_THREADLEVEL_MULTIPLE_BF   0x00000008

#define OMPI_THREADLEVEL_SET_BITFLAG(threadlevelin,threadlevelout) { \
    if ( MPI_THREAD_SINGLE == threadlevelin ) {                 \
        threadlevelout |= OMPI_THREADLEVEL_SINGLE_BF;           \
    } else if ( MPI_THREAD_FUNNELED == threadlevelin ) {        \
        threadlevelout |= OMPI_THREADLEVEL_FUNNELED_BF;         \
    } else if ( MPI_THREAD_SERIALIZED == threadlevelin ) {      \
        threadlevelout |= OMPI_THREADLEVEL_SERIALIZED_BF;       \
    } else if ( MPI_THREAD_MULTIPLE == threadlevelin ) {       \
        threadlevelout |= OMPI_THREADLEVEL_MULTIPLE_BF;         \
    }}


#define OMPI_THREADLEVEL_IS_MULTIPLE(threadlevel) (threadlevel & OMPI_THREADLEVEL_MULTIPLE_BF)

/** In ompi_mpi_init: a list of all memory associated with calling
    MPI_REGISTER_DATAREP so that we can free it during
    MPI_FINALIZE. */
OMPI_DECLSPEC extern opal_list_t ompi_registered_datareps;

/** In ompi_mpi_init: the lists of Fortran 90 mathing datatypes.
 * We need these lists and hashtables in order to satisfy the new
 * requirements introduced in MPI 2-1 Sect. 10.2.5,
 * MPI_TYPE_CREATE_F90_xxxx, page 295, line 47.
 */
extern opal_hash_table_t ompi_mpi_f90_integer_hashtable;
extern opal_hash_table_t ompi_mpi_f90_real_hashtable;
extern opal_hash_table_t ompi_mpi_f90_complex_hashtable;

/** version string of ompi */
OMPI_DECLSPEC extern const char ompi_version_string[];

/**
 * Determine the thread level
 *
 * @param requested Thread support that is requested (IN)
 * @param provided Thread support that is provided (OUT)
 */
void ompi_mpi_thread_level(int requested, int *provided);

/**
 * Initialize the Open MPI MPI environment
 *
 * @param argc argc, typically from main() (IN)
 * @param argv argv, typically from main() (IN)
 * @param requested Thread support that is requested (IN)
 * @param provided Thread support that is provided (OUT)
 * @param reinit_ok Return successfully (with no error) if someone has
 * already called ompi_mpi_init().
 *
 * @returns MPI_SUCCESS if successful
 * @returns Error code if unsuccessful
 *
 * Intialize all support code needed for MPI applications.  This
 * function should only be called by MPI applications (including
 * singletons).  If this function is called, ompi_init() and
 * ompi_rte_init() should *not* be called.
 *
 * It is permissable to pass in (0, NULL) for (argc, argv).
 */
int ompi_mpi_init(int argc, char **argv, int requested, int *provided,
                  bool reinit_ok);

/**
 * Finalize the Open MPI MPI environment
 *
 * @returns MPI_SUCCESS if successful
 * @returns Error code if unsuccessful
 *
 * Should be called after all MPI functionality is complete (usually
 * during MPI_FINALIZE).
 */
int ompi_mpi_finalize(void);

/**
 * Abort the processes of comm
 */
OMPI_DECLSPEC int ompi_mpi_abort(struct ompi_communicator_t* comm,
                                 int errcode);

/**
 * Do a preconnect of MPI connections (i.e., force connections to
 * be made if they will be made).
 */
int ompi_init_preconnect_mpi(void);

/**
 * Called to disable MPI dynamic process support.  It should be called
 * by transports and/or environments where MPI dynamic process
 * functionality cannot be supported, and provide a string indicating
 * why the functionality is disabled (because it will be shown in a
 * user help message).  For example, "<TRANSPORT> does not support MPI
 * dynamic process functionality."
 *
 * This first-order functionality is fairly coarse-grained and simple:
 * it presents a friendly show-help message to tell users why their
 * MPI dynamic process functionality failed (vs. a potentially-cryptic
 * network or hardware failure message).
 *
 * Someone may choose to implement a more fine-grained approach in the
 * future.
 */
void ompi_mpi_dynamics_disable(const char *msg);

/**
 * Called by the MPI dynamic process functions (e.g., MPI_Comm_spawn)
 * to see if MPI dynamic process support is enabled.  If it's not,
 * this function will opal_show_help() a message and return false.
 */
bool ompi_mpi_dynamics_is_enabled(const char *function);

/**
 * Clean up memory / resources by the MPI dynamics process
 * functionality checker
 */
void ompi_mpi_dynamics_finalize(void);

END_C_DECLS

#endif /* OMPI_MPI_MPIRUNTIME_H */
