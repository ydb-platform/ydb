/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "base.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fs/fs.h"

#include <unistd.h>

/*
 *	file_set_size_ufs
 *
 *	Function:	- set_size of a file
 *	Accepts:	- same arguments as MPI_File_set_size()
 *	Returns:	- Success if size is set
 */
int mca_fs_base_file_set_size (ompio_file_t *fh,
                          OMPI_MPI_OFFSET_TYPE size)
{
    int err = 0;

    err = ftruncate(fh->fd, size);

    fh->f_comm->c_coll->coll_bcast (&err,
                                   1,
                                   MPI_INT,
                                   OMPIO_ROOT,
                                   fh->f_comm,
                                   fh->f_comm->c_coll->coll_bcast_module);
    if (-1 == err) {
        return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}
