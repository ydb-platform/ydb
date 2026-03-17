/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_CXX_COMM_GLUE_H)
#define OMPI_CXX_COMM_GLUE_H

#include "ompi_config.h"
#include <stdlib.h>

#include "mpi.h"

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

typedef struct ompi_cxx_intercept_file_extra_state_t {
    void *read_fn_cxx;
    void *write_fn_cxx;
    void *extent_fn_cxx;
    void *extra_state_cxx;
} ompi_cxx_intercept_file_extra_state_t;

enum ompi_cxx_communicator_type_t {
  OMPI_CXX_COMM_TYPE_UNKNOWN,
  OMPI_CXX_COMM_TYPE_INTRACOMM,
  OMPI_CXX_COMM_TYPE_INTERCOMM,
  OMPI_CXX_COMM_TYPE_CART,
  OMPI_CXX_COMM_TYPE_GRAPH,
};
typedef enum ompi_cxx_communicator_type_t ompi_cxx_communicator_type_t;

/* need to declare this error handler here */
struct ompi_predefined_errhandler_t;
extern struct ompi_predefined_errhandler_t ompi_mpi_errors_throw_exceptions;

/**
 * C++ invocation function signature
 */
typedef void (ompi_cxx_dummy_fn_t) (void);

ompi_cxx_communicator_type_t ompi_cxx_comm_get_type (MPI_Comm comm);

int ompi_cxx_errhandler_invoke_comm (MPI_Comm comm, int ret, const char *message);

int ompi_cxx_attr_create_keyval_comm (MPI_Comm_copy_attr_function *copy_fn,
                                      MPI_Comm_delete_attr_function* delete_fn, int *keyval, void *extra_state,
                                      int flags, void *bindings_extra_state);
int ompi_cxx_attr_create_keyval_win (MPI_Win_copy_attr_function *copy_fn,
                                      MPI_Win_delete_attr_function* delete_fn, int *keyval, void *extra_state,
                                      int flags, void *bindings_extra_state);
int ompi_cxx_attr_create_keyval_type (MPI_Type_copy_attr_function *copy_fn,
                                      MPI_Type_delete_attr_function* delete_fn, int *keyval, void *extra_state,
                                      int flags, void *bindings_extra_state);

void ompi_mpi_cxx_comm_errhandler_invoke (MPI_Comm *mpi_comm, int *err,
                                          const char *message, void *comm_fn);
void ompi_mpi_cxx_win_errhandler_invoke (MPI_Win *mpi_comm, int *err,
                                         const char *message, void *win_fn);
int ompi_cxx_errhandler_invoke_file (MPI_File file, int ret, const char *message);
void ompi_mpi_cxx_file_errhandler_invoke (MPI_File *mpi_comm, int *err,
                                          const char *message, void *file_fn);

MPI_Errhandler ompi_cxx_errhandler_create_comm (ompi_cxx_dummy_fn_t *fn);
MPI_Errhandler ompi_cxx_errhandler_create_win (ompi_cxx_dummy_fn_t *fn);
MPI_Errhandler ompi_cxx_errhandler_create_file (ompi_cxx_dummy_fn_t *fn);

ompi_cxx_intercept_file_extra_state_t
*ompi_cxx_new_intercept_state (void *read_fn_cxx, void *write_fn_cxx, void *extent_fn_cxx,
                               void *extra_state_cxx);

void ompi_cxx_errhandler_set_callbacks (struct ompi_errhandler_t *errhandler, MPI_Comm_errhandler_function *eh_comm_fn,
                                        ompi_file_errhandler_fn *eh_file_fn, MPI_Win_errhandler_function *eh_win_fn);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif /* OMPI_CXX_COMM_GLUE_H */
