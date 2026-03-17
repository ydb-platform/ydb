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

#include <fcntl.h>
#include <unistd.h>
#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fs/fs.h"

/*
 *	file_close_ufs
 *
 *	Function:	- closes a new file
 *	Accepts:	- file handle
 *	Returns:	- Success if file closed
 */
int mca_fs_base_file_close (ompio_file_t *fh)
{
    fh->f_comm->c_coll->coll_barrier (fh->f_comm,
                                     fh->f_comm->c_coll->coll_barrier_module);
    /*    close (*(int *)fh->fd);*/
    close (fh->fd);
    /*    if (NULL != fh->fd)
    {
        free (fh->fd);
        fh->fd = NULL;
        }*/
    return OMPI_SUCCESS;
}
