/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
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
 *	file_get_size_ufs
 *
 *	Function:	- get_size of a file
 *	Accepts:	- same arguments as MPI_File_get_size()
 *	Returns:	- Success if size is retrieved
 */
int mca_fs_base_file_get_size (ompio_file_t *fh,
                               OMPI_MPI_OFFSET_TYPE *size)
{
    *size = lseek(fh->fd, 0, SEEK_END);
    if (-1 == *size) {
        perror ("lseek");
        return OMPI_ERROR;
    }

    if (-1 == (lseek(fh->fd, fh->f_offset, SEEK_SET))) {
        perror ("lseek");
        return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}
