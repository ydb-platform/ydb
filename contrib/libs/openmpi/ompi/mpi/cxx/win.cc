// -*- c++ -*-
//
// Copyright (c) 2006-2016 Los Alamos National Security, LLC.  All rights
//                         reserved.
// Copyright (c) 2007-2008 Sun Microsystems, Inc.  All rights reserved.
// Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

// do not include ompi_config.h because it kills the free/malloc defines
#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mpi/cxx/mpicxx.h"
#include "cxx_glue.h"

void
MPI::Win::Free()
{
    (void) MPI_Win_free(&mpi_win);
}


// This function needs some internal OMPI types, so it's not inlined
MPI::Errhandler
MPI::Win::Create_errhandler(MPI::Win::Errhandler_function* function)
{
    return ompi_cxx_errhandler_create_win ((ompi_cxx_dummy_fn_t *) function);
}


int
MPI::Win::do_create_keyval(MPI_Win_copy_attr_function* c_copy_fn,
                                MPI_Win_delete_attr_function* c_delete_fn,
                                Copy_attr_function* cxx_copy_fn,
                                Delete_attr_function* cxx_delete_fn,
                                void* extra_state, int &keyval)
{
    int ret, count = 0;
    keyval_intercept_data_t *cxx_extra_state;

    // If both the callbacks are C, then do the simple thing -- no
    // need for all the C++ machinery.
    if (NULL != c_copy_fn && NULL != c_delete_fn) {
        ret = ompi_cxx_attr_create_keyval_win (c_copy_fn, c_delete_fn, &keyval,
                                               extra_state, 0, NULL);
        if (MPI_SUCCESS != ret) {
            return ompi_cxx_errhandler_invoke_comm (MPI_COMM_WORLD, ret,
                                                    "MPI::Win::Create_keyval");
        }
    }

    // If either callback is C++, then we have to use the C++
    // callbacks for both, because we have to generate a new
    // extra_state.  And since we only get one extra_state (i.e., we
    // don't get one extra_state for the copy callback and another
    // extra_state for the delete callback), we have to use the C++
    // callbacks for both (and therefore translate the C++-special
    // extra_state into the user's original extra_state).
    cxx_extra_state = (keyval_intercept_data_t*)
        malloc(sizeof(keyval_intercept_data_t));
    if (NULL == cxx_extra_state) {
        return ompi_cxx_errhandler_invoke_comm (MPI_COMM_WORLD, MPI_ERR_NO_MEM,
                                                "MPI::Win::Create_keyval");
    }
    cxx_extra_state->c_copy_fn = c_copy_fn;
    cxx_extra_state->cxx_copy_fn = cxx_copy_fn;
    cxx_extra_state->c_delete_fn = c_delete_fn;
    cxx_extra_state->cxx_delete_fn = cxx_delete_fn;
    cxx_extra_state->extra_state = extra_state;

    // Error check.  Must have exactly 2 non-NULL function pointers.
    if (NULL != c_copy_fn) {
        ++count;
    }
    if (NULL != c_delete_fn) {
        ++count;
    }
    if (NULL != cxx_copy_fn) {
        ++count;
    }
    if (NULL != cxx_delete_fn) {
        ++count;
    }
    if (2 != count) {
        free(cxx_extra_state);
        return ompi_cxx_errhandler_invoke_comm (MPI_COMM_WORLD, MPI_ERR_ARG,
                                                "MPI::Win::Create_keyval");
    }

    // We do not call MPI_Win_create_keyval() here because we need to
    // pass in a special destructor to the backend keyval creation
    // that gets invoked when the keyval's reference count goes to 0
    // and is finally destroyed (i.e., clean up some caching/lookup
    // data here in the C++ bindings layer).  This destructor is
    // *only* used in the C++ bindings, so it's not set by the C
    // MPI_Comm_create_keyval().  Hence, we do all the work here (and
    // ensure to set the destructor atomicly when the keyval is
    // created).

    ret = ompi_cxx_attr_create_keyval_win ((MPI_Win_copy_attr_function *) ompi_mpi_cxx_win_copy_attr_intercept,
                                           ompi_mpi_cxx_win_delete_attr_intercept, &keyval,
                                           cxx_extra_state, 0, NULL);
    if (OMPI_SUCCESS != ret) {
        return ompi_cxx_errhandler_invoke_comm (MPI_COMM_WORLD, ret,
                                                "MPI::Win::Create_keyval");
    }

    return MPI_SUCCESS;
}
