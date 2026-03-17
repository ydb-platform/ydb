/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013-2015 University of Houston. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include "sharedfp_lockedfile.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"

/*Use fcntl to lock the hidden file which stores the current position*/
#include <fcntl.h>
#include <unistd.h>

int mca_sharedfp_lockedfile_request_position(struct mca_sharedfp_base_data_t * sh,
                                             int bytes_requested,
                                             OMPI_MPI_OFFSET_TYPE *offset)
{
    int ret = OMPI_SUCCESS;
    /*file descriptor for the hidden file that we want to lock*/
    int fd;
    /* flock structure that is used to setup the desired fcntl operation */
    struct flock fl;
    OMPI_MPI_OFFSET_TYPE position = 0;
    /*buffer to use when reading from the locked file*/
    OMPI_MPI_OFFSET_TYPE buf;
    /*int count = 1;*/

    struct mca_sharedfp_lockedfile_data * lockedfile_data = sh->selected_module_data;
    int handle = lockedfile_data->handle;

    *offset = 0;

    /* get the saved file descriptor,
     * the hidden file was opened during sharedfp_lockedfile_file_open function */
    fd = handle;

    /*set up the flock structure*/
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 0;
    fl.l_pid    = getpid();


    /* Aquire an exclusive lock */
    if (fcntl(fd, F_SETLKW, &fl) == -1) {
        opal_output(0,"sharedfp_lockedfile_request_position: errorr acquiring lock: fcntl(%d,F_SETLKW,&fl)\n",fd);
        opal_output(0,"sharedfp_lockedfile_request_position: error(%i): %s", errno, strerror(errno));
        return OMPI_ERROR;
    }
    else{
	if ( mca_sharedfp_lockedfile_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_lockedfile_request_position: Success: acquired lock.for fd: %d\n",fd);
	}
    }

    /* read from the file */
    lseek ( fd, 0, SEEK_SET );
    read ( fd, &buf, sizeof(OMPI_MPI_OFFSET_TYPE));
    if ( mca_sharedfp_lockedfile_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_lockedfile_request_position: Read last_offset=%lld! ret=%d\n",buf, ret);
    }

    /* increment the position  */
    position = buf + bytes_requested;
    if ( mca_sharedfp_lockedfile_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_lockedfile_request_position: old_offset=%lld, bytes_requested=%d, new offset=%lld!\n",
                    buf,bytes_requested,position);
    }

    /* write to the file  */
    lseek ( fd, 0, SEEK_SET );
    write ( fd, &position, sizeof(OMPI_MPI_OFFSET_TYPE));

    /* unlock the file */
    if ( mca_sharedfp_lockedfile_verbose ) {
        opal_output(ompi_sharedfp_base_framework.framework_output,
                    "sharedfp_lockedfile_request_position: Releasing lock...");
    }

    /* NOTE: We thought we could reuse the flock struct
    ** and only reset the l_type parameter, but it gave
    ** an invalid argument error. On my machine the invalid
    ** argument was l_whence. I reset it, but I also decided to
    ** reset all of the other parameters to avoid further problems.
    */
    fl.l_type   = F_UNLCK;  /* set to unlock same region */
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 0;
    fl.l_pid    = getpid();

    if (fcntl(fd, F_SETLK, &fl) == -1) {
        opal_output(0,"sharedfp_lockedfile_request_position:failed to release lock for fd: %d\n",fd);
        opal_output(0,"error(%i): %s", errno, strerror(errno));
        return OMPI_ERROR;
    }
    else {
	if ( mca_sharedfp_lockedfile_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_lockedfile_request_position: released lock.for fd: %d\n",fd);
	}
    }

    /* return position  */
    *offset = buf;

    return ret;
}
