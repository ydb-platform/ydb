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
 */

#ifndef OMPI_ERRHANDLER_PREDEFINED_H
#define OMPI_ERRHANDLER_PREDEFINED_H

#include "ompi_config.h"

struct ompi_communicator_t;
struct ompi_file_t;
struct ompi_win_t;

/**
 * Handler function for MPI_ERRORS_ARE_FATAL
 */
OMPI_DECLSPEC void ompi_mpi_errors_are_fatal_comm_handler(struct ompi_communicator_t **comm,
					    int *error_code, ...);
OMPI_DECLSPEC void ompi_mpi_errors_are_fatal_file_handler(struct ompi_file_t **file,
					    int *error_code, ...);
OMPI_DECLSPEC void ompi_mpi_errors_are_fatal_win_handler(struct ompi_win_t **win,
					    int *error_code, ...);

/**
 * Handler function for MPI_ERRORS_RETURN
 */
OMPI_DECLSPEC void ompi_mpi_errors_return_comm_handler(struct ompi_communicator_t **comm,
                                   int *error_code, ...);
OMPI_DECLSPEC void ompi_mpi_errors_return_file_handler(struct ompi_file_t **file,
                                   int *error_code, ...);
OMPI_DECLSPEC void ompi_mpi_errors_return_win_handler(struct ompi_win_t **win,
                                   int *error_code, ...);


#endif /* OMPI_ERRHANDLER_PREDEFINED_H */
