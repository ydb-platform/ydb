/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2009 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2008-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_create_f90_integer = PMPI_Type_create_f90_integer
#endif
#define MPI_Type_create_f90_integer PMPI_Type_create_f90_integer
#endif

static const char FUNC_NAME[] = "MPI_Type_create_f90_integer";


int MPI_Type_create_f90_integer(int r, MPI_Datatype *newtype)

{
    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        /* Note: These functions accept negative integers for the p and r
         * arguments.  This is because for the SELECTED_INTEGER_KIND,
         * negative numbers are equivalent to zero values.  See section
         * 13.14.95 of the Fortran 95 standard. */
    }

    /**
     * With respect to the MPI standard, MPI-2.0 Sect. 10.2.5, MPI_TYPE_CREATE_F90_xxxx,
     * page 295, line 47 we handle this nicely by caching the values in a hash table.
     * However, as the value of might not always make sense, a little bit of optimization
     * might be a good idea. Therefore, first we try to see if we can handle the value
     * with some kind of default value, and if it's the case then we look into the
     * cache.
     */

    if      (r > 38) *newtype = &ompi_mpi_datatype_null.dt;
#if OMPI_HAVE_FORTRAN_INTEGER16
    else if (r > 18) *newtype = &ompi_mpi_long_long_int.dt;
#else
    else if (r > 18) *newtype = &ompi_mpi_datatype_null.dt;
#endif  /* OMPI_HAVE_F90_INTEGER16 */
#if SIZEOF_LONG > SIZEOF_INT
    else if (r >  9) *newtype = &ompi_mpi_long.dt;
#else
#if SIZEOF_LONG_LONG > SIZEOF_INT
    else if (r >  9) *newtype = &ompi_mpi_long_long_int.dt;
#else
    else if (r >  9) *newtype = &ompi_mpi_datatype_null.dt;
#endif  /* SIZEOF_LONG_LONG > SIZEOF_INT */
#endif  /* SIZEOF_LONG > SIZEOF_INT */
    else if (r >  4) *newtype = &ompi_mpi_int.dt;
    else if (r >  2) *newtype = &ompi_mpi_short.dt;
    else             *newtype = &ompi_mpi_byte.dt;

    if( *newtype != &ompi_mpi_datatype_null.dt ) {
        ompi_datatype_t* datatype;
        const int* a_i[1];
        int rc;

        if( OPAL_SUCCESS == opal_hash_table_get_value_uint32( &ompi_mpi_f90_integer_hashtable,
                                                              r, (void**)newtype ) ) {
            return MPI_SUCCESS;
        }
        /* Create the duplicate type corresponding to selected type, then
         * set the argument to be a COMBINER with the correct value of r
         * and add it to the hash table. */
        if (OMPI_SUCCESS != ompi_datatype_duplicate( *newtype, &datatype)) {
            OMPI_ERRHANDLER_RETURN (MPI_ERR_INTERN, MPI_COMM_WORLD,
                                    MPI_ERR_INTERN, FUNC_NAME );
        }
        /* Make sure the user is not allowed to free this datatype as specified
         * in the MPI standard.
         */
        datatype->super.flags |= OMPI_DATATYPE_FLAG_PREDEFINED;
        /* Mark the datatype as a special F90 convenience type */
        char *new_name;
        asprintf(&new_name, "COMBINER %s", (*newtype)->name);
        size_t max_len = MPI_MAX_OBJECT_NAME;
        strncpy(datatype->name, new_name, max_len - 1);
        datatype->name[max_len - 1] = '\0';
        free(new_name);

        a_i[0] = &r;
        ompi_datatype_set_args( datatype, 1, a_i, 0, NULL, 0, NULL, MPI_COMBINER_F90_INTEGER );

        rc = opal_hash_table_set_value_uint32( &ompi_mpi_f90_integer_hashtable, r, datatype );
        if (OMPI_SUCCESS != rc) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, rc, FUNC_NAME);
        }
        *newtype = datatype;
        return MPI_SUCCESS;
    }

    return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
}
