/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/attribute/attribute.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/file/file.h"
#include "opal/class/opal_list.h"
#include "cxx_glue.h"

typedef struct ompi_cxx_intercept_file_extra_state_item_t {
    opal_list_item_t super;
    ompi_cxx_intercept_file_extra_state_t state;
} ompi_cxx_intercept_file_extra_state_item_t;

OBJ_CLASS_DECLARATION(ompi_cxx_intercept_file_extra_state_item_t);
OBJ_CLASS_INSTANCE(ompi_cxx_intercept_file_extra_state_item_t, opal_list_item_t,
                   NULL, NULL);

ompi_cxx_communicator_type_t ompi_cxx_comm_get_type (MPI_Comm comm)
{
    if (OMPI_COMM_IS_GRAPH(comm)) {
        return OMPI_CXX_COMM_TYPE_GRAPH;
    } else if (OMPI_COMM_IS_CART(comm)) {
        return OMPI_CXX_COMM_TYPE_CART;
    } else if (OMPI_COMM_IS_INTRA(comm)) {
        return OMPI_CXX_COMM_TYPE_INTRACOMM;
    } else if (OMPI_COMM_IS_INTER(comm)) {
        return OMPI_CXX_COMM_TYPE_INTERCOMM;
    }

    return OMPI_CXX_COMM_TYPE_UNKNOWN;
}

int ompi_cxx_errhandler_invoke_comm (MPI_Comm comm, int ret, const char *message)
{
    return OMPI_ERRHANDLER_INVOKE (comm, ret, message);
}

int ompi_cxx_errhandler_invoke_file (MPI_File file, int ret, const char *message)
{
    return OMPI_ERRHANDLER_INVOKE (file, ret, message);
}

int ompi_cxx_attr_create_keyval_comm (MPI_Comm_copy_attr_function *copy_fn,
                                      MPI_Comm_delete_attr_function* delete_fn, int *keyval, void *extra_state,
                                      int flags, void *bindings_extra_state)
{
    ompi_attribute_fn_ptr_union_t copy_fn_u = {.attr_communicator_copy_fn =
                                               (MPI_Comm_internal_copy_attr_function *) copy_fn};
    ompi_attribute_fn_ptr_union_t delete_fn_u = {.attr_communicator_delete_fn =
                                                 (MPI_Comm_delete_attr_function *) delete_fn};

    return ompi_attr_create_keyval (COMM_ATTR, copy_fn_u, delete_fn_u, keyval, extra_state, 0, bindings_extra_state);
}

int ompi_cxx_attr_create_keyval_win (MPI_Win_copy_attr_function *copy_fn,
                                      MPI_Win_delete_attr_function* delete_fn, int *keyval, void *extra_state,
                                      int flags, void *bindings_extra_state)
{
    ompi_attribute_fn_ptr_union_t copy_fn_u = {.attr_win_copy_fn =
                                               (MPI_Win_internal_copy_attr_function *) copy_fn};
    ompi_attribute_fn_ptr_union_t delete_fn_u = {.attr_win_delete_fn =
                                                 (MPI_Win_delete_attr_function *) delete_fn};

    return ompi_attr_create_keyval (WIN_ATTR, copy_fn_u, delete_fn_u, keyval, extra_state, 0, NULL);
}

int ompi_cxx_attr_create_keyval_type (MPI_Type_copy_attr_function *copy_fn,
                                      MPI_Type_delete_attr_function* delete_fn, int *keyval, void *extra_state,
                                      int flags, void *bindings_extra_state)
{
    ompi_attribute_fn_ptr_union_t copy_fn_u = {.attr_datatype_copy_fn =
                                               (MPI_Type_internal_copy_attr_function *) copy_fn};
    ompi_attribute_fn_ptr_union_t delete_fn_u = {.attr_datatype_delete_fn =
                                                 (MPI_Type_delete_attr_function *) delete_fn};

    return ompi_attr_create_keyval (TYPE_ATTR, copy_fn_u, delete_fn_u, keyval, extra_state, 0, NULL);
}

MPI_Errhandler ompi_cxx_errhandler_create_comm (ompi_cxx_dummy_fn_t *fn)
{
    ompi_errhandler_t *errhandler;
    errhandler = ompi_errhandler_create(OMPI_ERRHANDLER_TYPE_COMM,
                                        (ompi_errhandler_generic_handler_fn_t *) fn,
                                        OMPI_ERRHANDLER_LANG_CXX);
    errhandler->eh_cxx_dispatch_fn =
        (ompi_errhandler_cxx_dispatch_fn_t *) ompi_mpi_cxx_comm_errhandler_invoke;
    return errhandler;
}

MPI_Errhandler ompi_cxx_errhandler_create_win (ompi_cxx_dummy_fn_t *fn)
{
    ompi_errhandler_t *errhandler;
    errhandler = ompi_errhandler_create(OMPI_ERRHANDLER_TYPE_WIN,
                                        (ompi_errhandler_generic_handler_fn_t *) fn,
                                        OMPI_ERRHANDLER_LANG_CXX);
    errhandler->eh_cxx_dispatch_fn =
        (ompi_errhandler_cxx_dispatch_fn_t *) ompi_mpi_cxx_win_errhandler_invoke;
    return errhandler;
}

MPI_Errhandler ompi_cxx_errhandler_create_file (ompi_cxx_dummy_fn_t *fn)
{
    ompi_errhandler_t *errhandler;
    errhandler = ompi_errhandler_create(OMPI_ERRHANDLER_TYPE_FILE,
                                        (ompi_errhandler_generic_handler_fn_t *) fn,
                                        OMPI_ERRHANDLER_LANG_CXX);
    errhandler->eh_cxx_dispatch_fn =
        (ompi_errhandler_cxx_dispatch_fn_t *) ompi_mpi_cxx_file_errhandler_invoke;
    return errhandler;
}

ompi_cxx_intercept_file_extra_state_t
*ompi_cxx_new_intercept_state (void *read_fn_cxx, void *write_fn_cxx, void *extent_fn_cxx,
                               void *extra_state_cxx)
{
    ompi_cxx_intercept_file_extra_state_item_t *intercept;

    intercept = OBJ_NEW(ompi_cxx_intercept_file_extra_state_item_t);
    if (NULL == intercept) {
        return NULL;
    }

    opal_list_append(&ompi_registered_datareps, &intercept->super);
    intercept->state.read_fn_cxx = read_fn_cxx;
    intercept->state.write_fn_cxx = write_fn_cxx;
    intercept->state.extent_fn_cxx = extent_fn_cxx;
    intercept->state.extra_state_cxx = extra_state_cxx;

    return &intercept->state;
}

void ompi_cxx_errhandler_set_callbacks (struct ompi_errhandler_t *errhandler, MPI_Comm_errhandler_function *eh_comm_fn,
                                        ompi_file_errhandler_fn *eh_file_fn, MPI_Win_errhandler_function *eh_win_fn)
{
    errhandler->eh_comm_fn = eh_comm_fn;
    errhandler->eh_file_fn = eh_file_fn;
    errhandler->eh_win_fn = eh_win_fn;
}
