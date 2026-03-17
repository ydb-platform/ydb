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
#include "ompi/info/info.h"
#include "ompi/communicator/communicator.h"
#include "opal/util/info_subscriber.h"
#include "ompi/file/file.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_File_set_info = PMPI_File_set_info
#endif
#define MPI_File_set_info PMPI_File_set_info
#endif

static const char FUNC_NAME[] = "MPI_File_set_info";


int MPI_File_set_info(MPI_File fh, MPI_Info info)
{
    int ret; 

    OPAL_CR_NOOP_PROGRESS();

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_file_invalid(fh)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_FILE, FUNC_NAME);
        }

	if (NULL == info || MPI_INFO_NULL == info ||
            ompi_info_is_freed(info)) {
            return OMPI_ERRHANDLER_INVOKE(fh, MPI_ERR_INFO,
                                          FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    ret = opal_infosubscribe_change_info(&fh->super, &info->super);

    OMPI_ERRHANDLER_RETURN(ret, fh, ret, FUNC_NAME);
}
