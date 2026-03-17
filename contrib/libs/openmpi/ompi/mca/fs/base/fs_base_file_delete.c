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
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * Copyright (c) 2018      DataDirect Networks. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "base.h"

#include <unistd.h>

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/fs/fs.h"

/*
 *	file_delete_ufs
 *
 *	Function:	- deletes a file
 *	Accepts:	- file name & info
 *	Returns:	- Success if file closed
 */
int mca_fs_base_file_delete (char* file_name,
                             struct opal_info_t *info)
{
    int ret;

    ret = unlink(file_name);

    if (0 > ret ) {
        if ( ENOENT == errno ) {
            return MPI_ERR_NO_SUCH_FILE;
        } else {
            opal_output (0, "mca_fs_base_file_delete: Could not remove file "
                            "%s errno = %d %s\n",
                            file_name, errno, strerror(errno));
            return MPI_ERR_ACCESS;
        }
    }

    return OMPI_SUCCESS;
}
