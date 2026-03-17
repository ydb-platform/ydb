/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
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
#include "ompi/attribute/attribute.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_create_keyval = PMPI_Type_create_keyval
#endif
#define MPI_Type_create_keyval PMPI_Type_create_keyval
#endif

static const char FUNC_NAME[] = "MPI_Type_create_keyval";


int MPI_Type_create_keyval(MPI_Type_copy_attr_function *type_copy_attr_fn,
                           MPI_Type_delete_attr_function *type_delete_attr_fn,
                           int *type_keyval,
                           void *extra_state)
{
    int ret;
    ompi_attribute_fn_ptr_union_t copy_fn;
    ompi_attribute_fn_ptr_union_t del_fn;

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if ((NULL == type_copy_attr_fn) || (NULL == type_delete_attr_fn) ||
            (NULL == type_keyval)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD,
                                          MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    copy_fn.attr_datatype_copy_fn = (MPI_Type_internal_copy_attr_function*)type_copy_attr_fn;
    del_fn.attr_datatype_delete_fn = type_delete_attr_fn;

    ret = ompi_attr_create_keyval(TYPE_ATTR, copy_fn, del_fn,
                                  type_keyval, extra_state, 0, NULL);
    OMPI_ERRHANDLER_RETURN(ret, MPI_COMM_WORLD, ret, FUNC_NAME);
}


