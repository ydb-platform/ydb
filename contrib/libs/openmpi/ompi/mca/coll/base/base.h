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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

/** @file
 * MCA coll base framework public interface functions.
 *
 * These functions are normally invoked by the back-ends of:
 *
 * - The back-ends of MPI_Init() and MPI_Finalize()
 * - Communicator constructors (e.g., MPI_Comm_split()) and
 *   destructors (e.g., MPI_Comm_free())
 * - The laminfo command
 */

#ifndef MCA_COLL_BASE_H
#define MCA_COLL_BASE_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/class/opal_list.h"
#include "opal/mca/base/base.h"

/*
 * Global functions for MCA overall collective open and close
 */

BEGIN_C_DECLS

/**
 * Create list of available coll components.
 *
 * @param allow_multi_user_threads Will be set to true if any of the
 * available components will allow multiple user threads
 * @param have_hidden_threads Will be set to true if any of the
 * available components have hidden threads.
 *
 * @retval OMPI_SUCCESS If one or more coll components are available.
 * @retval OMPI_ERROR If no coll components are found to be available.
 *
 * This function is invoked during ompi_mpi_init() to query all
 * successfully opened coll components and create a list of all
 * available coll components.
 *
 * This function traverses the (internal global variable) framework components
 * list and queries each component to see if it ever might want to run during
 * this MPI process.  If a component does not want to run it is closed and
 * removed from the framework components list.
 */
int mca_coll_base_find_available(bool enable_progress_threads,
                                 bool enable_mpi_threads);

/**
 * Select an available component for a new communicator.
 *
 * @param comm Communicator that the component will be selected for.
 * @param preferred The component that is preferred for this
 * communicator (or NULL).
 *
 * @return OMPI_SUCCESS Upon success.
 * @return OMPI_ERROR Upon failure.
 *
 * Note that the types of the parameters have "struct" in them
 * (e.g., ompi_communicator_t" vs. a plain "ompi_communicator_t") to
 * avoid an include file loop.  All similar types (e.g., "struct
 * ompi_communicator_t *", "ompi_communicator_t *", and "MPI_Comm")
 * are all typedef'ed to be the same, so the fact that we use struct
 * here in the prototype is ok.
 *
 * This function is invoked when a new communicator is created and a
 * coll component needs to be selected for it.  It should be invoked
 * near the end of the communicator creation process such that
 * almost everything else is functional on the communicator (e.g.,
 * point-to-point communication).
 *
 * Note that new communicators may be created as a result of
 * invoking this function.  Specifically: this function is called in
 * the depths of communicator creation, but during the execution of
 * this function, new communicators may be created, and therefore
 * communicator creation functions may be re-entered (albiet with
 * different arguments).
 */
int mca_coll_base_comm_select(struct ompi_communicator_t *comm);

/**
 * Finalize a coll component on a specific communicator.
 *
 * @param comm The communicator that is being destroyed.
 *
 * @retval OMPI_SUCCESS Always.
 *
 * Note that the type of the parameter is only a "struct
 * ompi_communicator_t" (vs. a plain "ompi_communicator_t") to avoid
 * an include file loop.  The types "struct ompi_communicator_t *",
 * "ompi_communicator_t *", and "MPI_Comm" are all typedef'ed to be
 * the same, so the fact that we use struct here in the prototype is
 * ok.
 *
 * This function is invoked near the beginning of the destruction of
 * a communicator.  It finalizes the coll component associated with the
 * communicator (e.g., allowing the component to clean up and free any
 * resources allocated for that communicator).  Note that similar to
 * mca_coll_base_select(), as result of this function, other
 * communicators may also be destroyed.
 */
int mca_coll_base_comm_unselect(struct ompi_communicator_t *comm);

/*
 * Globals
 */
OMPI_DECLSPEC extern mca_base_framework_t ompi_coll_base_framework;

END_C_DECLS
#endif /* MCA_BASE_COLL_H */
