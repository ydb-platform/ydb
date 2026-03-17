/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_C_BINDINGS_H
#define OMPI_C_BINDINGS_H

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/datatype/ompi_datatype.h"

/* This library needs to be here so that we can define
 * the OPAL_CR_* checks
 */
#include "opal/runtime/opal_cr.h"

BEGIN_C_DECLS

/* If compiling in the profile directory, then we don't have weak
   symbols and therefore we need the defines to map from MPI->PMPI.
   NOTE: pragma weak stuff is handled on a file-by-file basis; it
   doesn't work to simply list all of the pragmas in a top-level
   header file. */

/* These macros have to be used to check the corectness of the datatype depending on the
 * operations that we have to do with them. They can be used on all functions, not only
 * on the top level MPI functions, as they does not trigger the error handler. Is the user
 * responsability to do it.
 */
#define OMPI_CHECK_DATATYPE_FOR_SEND( RC, DDT, COUNT )                  \
    do {                                                                \
        /* (RC) = MPI_SUCCESS; */                                       \
        if( NULL == (DDT) || MPI_DATATYPE_NULL == (DDT) ) (RC) = MPI_ERR_TYPE; \
        else if( (COUNT) < 0 ) (RC) = MPI_ERR_COUNT;                    \
        else if( !opal_datatype_is_committed(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE; \
        else if( !opal_datatype_is_valid(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE;       \
    } while (0)

#define OMPI_CHECK_DATATYPE_FOR_RECV( RC, DDT, COUNT )                  \
    do {                                                                \
        /* (RC) = MPI_SUCCESS; */                                        \
        if( NULL == (DDT) || MPI_DATATYPE_NULL == (DDT) ) (RC) = MPI_ERR_TYPE; \
        else if( (COUNT) < 0 ) (RC) = MPI_ERR_COUNT;                    \
        else if( !opal_datatype_is_committed(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE;   \
        /* XXX Fix flags else if( ompi_datatype_is_overlapped((DDT)) ) (RC) = MPI_ERR_TYPE; */ \
        else if( !opal_datatype_is_valid(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE;       \
    } while (0)

#define OMPI_CHECK_DATATYPE_FOR_ONE_SIDED( RC, DDT, COUNT )                          \
    do {                                                                             \
        /*(RC) = MPI_SUCCESS; */                                                     \
        if( NULL == (DDT) || MPI_DATATYPE_NULL == (DDT) ) (RC) = MPI_ERR_TYPE;       \
        else if( (COUNT) < 0 ) (RC) = MPI_ERR_COUNT;                                 \
        else if( !opal_datatype_is_committed(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE; \
        else if( opal_datatype_is_overlapped(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE; \
        else if( !opal_datatype_is_valid(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE;     \
    } while(0)

#define OMPI_CHECK_DATATYPE_FOR_VIEW( RC, DDT, COUNT )                  \
    do {                                                                \
        /* (RC) = MPI_SUCCESS; */                                        \
        if( NULL == (DDT) || MPI_DATATYPE_NULL == (DDT) ) (RC) = MPI_ERR_TYPE; \
        else if( (COUNT) < 0 ) (RC) = MPI_ERR_COUNT;                    \
        else if( !opal_datatype_is_committed(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE;   \
        /* XXX Fix flags else if( ompi_datatype_is_overlapped((DDT)) ) (RC) = MPI_ERR_TYPE; */ \
        else if( !opal_datatype_is_valid(&((DDT)->super)) ) (RC) = MPI_ERR_TYPE;       \
        else if( !ompi_datatype_is_monotonic((DDT)) ) (RC) = MPI_ERR_TYPE;       \
    } while (0)


/* This macro has to be used to check the correctness of the user buffer depending on the datatype.
 * This macro expects that the DDT parameter is a valid pointer to an ompi datatype object.
 */
#define OMPI_CHECK_USER_BUFFER(RC, BUFFER, DDT, COUNT)                  \
    do {                                                                \
        if ( NULL == (BUFFER) && 0 < (COUNT) && MPI_SUCCESS == (RC) ) { \
            if ( (DDT)->super.flags & OPAL_DATATYPE_FLAG_PREDEFINED ) { \
                (RC) = MPI_ERR_BUFFER;                                  \
            } else {                                                    \
                size_t size = 0;                                        \
                ptrdiff_t true_lb       = 0;                            \
                ptrdiff_t true_extended = 0;                            \
                ompi_datatype_type_size((DDT), &size);                       \
                ompi_datatype_get_true_extent((DDT), &true_lb, &true_extended); \
                if ( 0 < size && 0 == true_lb ) {                       \
                    (RC) = MPI_ERR_BUFFER;                              \
                }                                                       \
            }                                                           \
        }                                                               \
    } while (0)

END_C_DECLS

#endif /* OMPI_C_BINDINGS_H */
