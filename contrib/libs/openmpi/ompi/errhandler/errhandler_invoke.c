/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/win/win.h"
#include "ompi/file/file.h"
#include "ompi/request/request.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"


int ompi_errhandler_invoke(ompi_errhandler_t *errhandler, void *mpi_object,
                           int object_type, int err_code, const char *message)
{
    MPI_Fint fortran_handle, fortran_err_code = OMPI_INT_2_FINT(err_code);
    ompi_communicator_t *comm;
    ompi_win_t *win;
    ompi_file_t *file;

    /* If we got no errorhandler, then just invoke errors_abort */
    if (NULL == errhandler) {
        ompi_mpi_errors_are_fatal_comm_handler(NULL, NULL, message);
	return err_code;
    }

    /* Figure out what kind of errhandler it is, figure out if it's
       fortran or C, and then invoke it */

    switch (object_type) {
    case OMPI_ERRHANDLER_TYPE_COMM:
        comm = (ompi_communicator_t *) mpi_object;
        switch (errhandler->eh_lang) {
        case OMPI_ERRHANDLER_LANG_C:
            errhandler->eh_comm_fn(&comm, &err_code, message, NULL);
            break;

        case OMPI_ERRHANDLER_LANG_CXX:
            errhandler->eh_cxx_dispatch_fn(&comm, &err_code, message,
                                           (ompi_errhandler_generic_handler_fn_t *)errhandler->eh_comm_fn);
            break;

        case OMPI_ERRHANDLER_LANG_FORTRAN:
            fortran_handle = OMPI_INT_2_FINT(comm->c_f_to_c_index);
            errhandler->eh_fort_fn(&fortran_handle, &fortran_err_code);
            err_code = OMPI_FINT_2_INT(fortran_err_code);
            break;
        }
        break;

    case OMPI_ERRHANDLER_TYPE_WIN:
        win = (ompi_win_t *) mpi_object;
        switch (errhandler->eh_lang) {
        case OMPI_ERRHANDLER_LANG_C:
            errhandler->eh_win_fn(&win, &err_code, message, NULL);
            break;

        case OMPI_ERRHANDLER_LANG_CXX:
            errhandler->eh_cxx_dispatch_fn(&win, &err_code, message,
                                           (ompi_errhandler_generic_handler_fn_t *)errhandler->eh_win_fn);
            break;

        case OMPI_ERRHANDLER_LANG_FORTRAN:
            fortran_handle = OMPI_INT_2_FINT(win->w_f_to_c_index);
            errhandler->eh_fort_fn(&fortran_handle, &fortran_err_code);
            err_code = OMPI_FINT_2_INT(fortran_err_code);
            break;
        }
        break;

    case OMPI_ERRHANDLER_TYPE_FILE:
        file = (ompi_file_t *) mpi_object;
        switch (errhandler->eh_lang) {
        case OMPI_ERRHANDLER_LANG_C:
            errhandler->eh_file_fn(&file, &err_code, message, NULL);
            break;

        case OMPI_ERRHANDLER_LANG_CXX:
            errhandler->eh_cxx_dispatch_fn(&file, &err_code, message,
                                           (ompi_errhandler_generic_handler_fn_t *)errhandler->eh_file_fn);
            break;

        case OMPI_ERRHANDLER_LANG_FORTRAN:
            fortran_handle = OMPI_INT_2_FINT(file->f_f_to_c_index);
            errhandler->eh_fort_fn(&fortran_handle, &fortran_err_code);
            err_code = OMPI_FINT_2_INT(fortran_err_code);
            break;
        }
        break;
    }

    /* All done */
    return err_code;
}


int ompi_errhandler_request_invoke(int count,
                                   struct ompi_request_t **requests,
                                   const char *message)
{
    int i, ec, type;
    ompi_mpi_object_t mpi_object;

    /* Find the *first* request that has an error -- that's the one
       that we'll invoke the exception on.  In an error condition, the
       request will not have been reset back to MPI_REQUEST_NULL, so
       there's no need to cache values from before we call
       ompi_request_test(). */
    for (i = 0; i < count; ++i) {
        if (MPI_REQUEST_NULL != requests[i] &&
            MPI_SUCCESS != requests[i]->req_status.MPI_ERROR) {
            break;
        }
    }
    /* If there were no errors, return SUCCESS */
    if (i >= count) {
        return MPI_SUCCESS;
    }

    ec = ompi_errcode_get_mpi_code(requests[i]->req_status.MPI_ERROR);
    mpi_object = requests[i]->req_mpi_object;
    type = requests[i]->req_type;

    /* Since errors on requests cause them to not be freed (until we
       can examine them here), go through and free all requests with
       errors.  We only invoke the exception on the *first* request
       that had an error. */
    for (; i < count; ++i) {
        if (MPI_REQUEST_NULL != requests[i] &&
            MPI_SUCCESS != requests[i]->req_status.MPI_ERROR) {
            /* Ignore the error -- what are we going to do?  We're
               already going to invoke an exception */
            ompi_request_free(&(requests[i]));
        }
    }

    /* Invoke the exception */
    switch (type) {
    case OMPI_REQUEST_PML:
        return ompi_errhandler_invoke(mpi_object.comm->error_handler,
                                      mpi_object.comm,
                                      mpi_object.comm->errhandler_type,
                                      ec, message);
        break;
    case OMPI_REQUEST_IO:
        return ompi_errhandler_invoke(mpi_object.file->error_handler,
                                      mpi_object.file,
                                      mpi_object.file->errhandler_type,
                                      ec, message);
        break;
    case OMPI_REQUEST_WIN:
        return ompi_errhandler_invoke(mpi_object.win->error_handler,
                                      mpi_object.win,
                                      mpi_object.win->errhandler_type,
                                      ec, message);
        break;
    default:
        /* Covers REQUEST_GEN, REQUEST_NULL, REQUEST_MAX */
        return ompi_errhandler_invoke(MPI_COMM_WORLD->error_handler,
                                      MPI_COMM_WORLD,
                                      MPI_COMM_WORLD->errhandler_type,
                                      ec, message);
        break;
    }
}
