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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/communicator/communicator.h"
#include "ompi/file/file.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_File_get_info = PMPI_File_get_info
#endif
#define MPI_File_get_info PMPI_File_get_info
#endif

static const char FUNC_NAME[] = "MPI_File_get_info";


int MPI_File_get_info(MPI_File fh, MPI_Info *info_used)
{
    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == info_used) {
            return OMPI_ERRHANDLER_INVOKE(fh, MPI_ERR_INFO, FUNC_NAME);
        }
        if (ompi_file_invalid(fh)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM,
                                          FUNC_NAME);
        }
    }

    if (NULL == fh->super.s_info) {
/*
 * Setup any defaults if MPI_Win_set_info was never called
 */
        opal_infosubscribe_change_info(&fh->super, &MPI_INFO_NULL->super);
    }


    (*info_used) = OBJ_NEW(ompi_info_t);
    if (NULL == (*info_used)) {
       return OMPI_ERRHANDLER_INVOKE(fh, MPI_ERR_NO_MEM, FUNC_NAME);
    }
    opal_info_t *opal_info_used = &(*info_used)->super;

    opal_info_dup_mpistandard(fh->super.s_info, &opal_info_used);

    return OMPI_SUCCESS;
}
