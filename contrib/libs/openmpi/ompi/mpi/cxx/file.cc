// -*- c++ -*-
//
// Copyright (c) 2006-2016 Los Alamos National Security, LLC.  All rights
//                         reserved.
// Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
// $COPYRIGHT$
//
// Additional copyrights may follow
//
// $HEADER$
//

// Do not include ompi_config.h before mpi.h because it causes
// malloc/free problems due to setting OMPI_BUILDING to 1
#include "mpi.h"

#include "ompi/constants.h"
#include "ompi/mpi/cxx/mpicxx.h"
#include "cxx_glue.h"

void
MPI::File::Close()
{
    (void) MPI_File_close(&mpi_file);
}


MPI::Errhandler
MPI::File::Create_errhandler(MPI::File::Errhandler_function* function)
{
    return ompi_cxx_errhandler_create_file ((ompi_cxx_dummy_fn_t *) function);
}


//
// Infrastructure for MPI_REGISTER_DATAREP
//
// Similar to what we have to do in the F77 bindings: call the C
// MPI_Register_datarep function with "intercept" callback functions
// that conform to the C bindings.  In these intercepts, convert the
// arguments to C++ calling convertions, and then invoke the actual
// C++ callbacks.

// Data structure passed to the intercepts (see below).  It is an OPAL
// list_item_t so that we can clean this memory up during
// MPI_FINALIZE.

// Intercept function for read conversions
static int read_intercept_fn(void *userbuf, MPI_Datatype type_c, int count_c,
                             void *filebuf, MPI_Offset position_c,
                             void *extra_state)
{
    MPI::Datatype type_cxx(type_c);
    MPI::Offset position_cxx(position_c);
    ompi_cxx_intercept_file_extra_state_t *intercept_data =
        (ompi_cxx_intercept_file_extra_state_t*) extra_state;
    MPI::Datarep_conversion_function *read_fn_cxx =
        (MPI::Datarep_conversion_function *) intercept_data->read_fn_cxx;

    read_fn_cxx (userbuf, type_cxx, count_c, filebuf, position_cxx,
                 intercept_data->extra_state_cxx);
    return MPI_SUCCESS;
}

// Intercept function for write conversions
static int write_intercept_fn(void *userbuf, MPI_Datatype type_c, int count_c,
                             void *filebuf, MPI_Offset position_c,
                              void *extra_state)
{
    MPI::Datatype type_cxx(type_c);
    MPI::Offset position_cxx(position_c);
    ompi_cxx_intercept_file_extra_state_t *intercept_data =
        (ompi_cxx_intercept_file_extra_state_t*) extra_state;
    MPI::Datarep_conversion_function *write_fn_cxx =
        (MPI::Datarep_conversion_function *) intercept_data->write_fn_cxx;

    write_fn_cxx (userbuf, type_cxx, count_c, filebuf, position_cxx,
                  intercept_data->extra_state_cxx);
    return MPI_SUCCESS;
}

// Intercept function for extent calculations
static int extent_intercept_fn(MPI_Datatype type_c, MPI_Aint *file_extent_c,
                               void *extra_state)
{
    MPI::Datatype type_cxx(type_c);
    MPI::Aint file_extent_cxx(*file_extent_c);
    ompi_cxx_intercept_file_extra_state_t *intercept_data =
        (ompi_cxx_intercept_file_extra_state_t*) extra_state;
    MPI::Datarep_extent_function *extent_fn_cxx =
        (MPI::Datarep_extent_function *) intercept_data->extent_fn_cxx;

    extent_fn_cxx (type_cxx, file_extent_cxx, intercept_data->extra_state_cxx);
    *file_extent_c = file_extent_cxx;
    return MPI_SUCCESS;
}

// C++ bindings for MPI::Register_datarep
void
MPI::Register_datarep(const char* datarep,
                      Datarep_conversion_function* read_fn_cxx,
                      Datarep_conversion_function* write_fn_cxx,
                      Datarep_extent_function* extent_fn_cxx,
                      void* extra_state_cxx)
{
    ompi_cxx_intercept_file_extra_state_t *intercept;

    intercept = ompi_cxx_new_intercept_state ((void *) read_fn_cxx, (void *) write_fn_cxx,
                                              (void *) extent_fn_cxx, extra_state_cxx);
    if (NULL == intercept) {
        ompi_cxx_errhandler_invoke_file (MPI_FILE_NULL, OMPI_ERR_OUT_OF_RESOURCE,
                                         "MPI::Register_datarep");
        return;
    }

    (void)MPI_Register_datarep (const_cast<char*>(datarep), read_intercept_fn,
                                write_intercept_fn, extent_intercept_fn, intercept);
}


void
MPI::Register_datarep(const char* datarep,
                      MPI_Datarep_conversion_function* read_fn_c,
                      Datarep_conversion_function* write_fn_cxx,
                      Datarep_extent_function* extent_fn_cxx,
                      void* extra_state_cxx)
{
    ompi_cxx_intercept_file_extra_state_t *intercept;

    intercept = ompi_cxx_new_intercept_state (NULL, (void *) write_fn_cxx, (void *) extent_fn_cxx,
                                              extra_state_cxx);
    if (NULL == intercept) {
        ompi_cxx_errhandler_invoke_file (MPI_FILE_NULL, OMPI_ERR_OUT_OF_RESOURCE,
                                         "MPI::Register_datarep");
        return;
    }

    (void)MPI_Register_datarep (const_cast<char*>(datarep), read_fn_c, write_intercept_fn,
                                extent_intercept_fn, intercept);
}


void
MPI::Register_datarep(const char* datarep,
                      Datarep_conversion_function* read_fn_cxx,
                      MPI_Datarep_conversion_function* write_fn_c,
                      Datarep_extent_function* extent_fn_cxx,
                      void* extra_state_cxx)
{
    ompi_cxx_intercept_file_extra_state_t *intercept;

    intercept = ompi_cxx_new_intercept_state ((void *) read_fn_cxx, NULL, (void *) extent_fn_cxx,
                                              extra_state_cxx);
    if (NULL == intercept) {
        ompi_cxx_errhandler_invoke_file (MPI_FILE_NULL, OMPI_ERR_OUT_OF_RESOURCE,
                                         "MPI::Register_datarep");
        return;
    }

    (void)MPI_Register_datarep (const_cast<char*>(datarep), read_intercept_fn, write_fn_c,
                                extent_intercept_fn, intercept);
}


void
MPI::Register_datarep(const char* datarep,
                      MPI_Datarep_conversion_function* read_fn_c,
                      MPI_Datarep_conversion_function* write_fn_c,
                      Datarep_extent_function* extent_fn_cxx,
                      void* extra_state_cxx)
{
    ompi_cxx_intercept_file_extra_state_t *intercept;

    intercept = ompi_cxx_new_intercept_state (NULL, NULL, (void *) extent_fn_cxx, extra_state_cxx);
    if (NULL == intercept) {
        ompi_cxx_errhandler_invoke_file (MPI_FILE_NULL, OMPI_ERR_OUT_OF_RESOURCE,
                                         "MPI::Register_datarep");
        return;
    }

    (void)MPI_Register_datarep (const_cast<char*>(datarep), read_fn_c, write_fn_c,
                                extent_intercept_fn, intercept);
}


