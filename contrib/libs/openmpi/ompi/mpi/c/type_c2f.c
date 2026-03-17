/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_c2f = PMPI_Type_c2f
#endif
#define MPI_Type_c2f PMPI_Type_c2f
#endif

static const char FUNC_NAME[] = "MPI_Type_c2f";


MPI_Fint MPI_Type_c2f(MPI_Datatype datatype)
{

    OPAL_CR_NOOP_PROGRESS();

    MEMCHECKER(
        memchecker_datatype(datatype);
    );

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (NULL == datatype) {
            return OMPI_INT_2_FINT(-1);
        }
    }

    /* If necessary add the datatype to the f2c translation table */
    if( -1 == datatype->d_f_to_c_index ) {
        datatype->d_f_to_c_index = opal_pointer_array_add(&ompi_datatype_f_to_c_table, datatype);
        /* We don't check for error as returning a negative value is considered as an error */
    }
    return OMPI_INT_2_FINT(datatype->d_f_to_c_index);
}
