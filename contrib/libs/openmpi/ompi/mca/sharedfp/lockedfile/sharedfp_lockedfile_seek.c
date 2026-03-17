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
 * Copyright (c) 2013-2018 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

/*Use fcntl to lock the file which stores the current position*/
#include <fcntl.h>

int
mca_sharedfp_lockedfile_seek (ompio_file_t *fh,
                              OMPI_MPI_OFFSET_TYPE off, int whence)
{
    int ret = OMPI_SUCCESS;
    struct mca_sharedfp_base_data_t *sh = NULL;
    struct mca_sharedfp_lockedfile_data * lockedfile_data;
    int fd_lockedfilehandle;
    /* flock structure that is used to setup the desired fcntl operation */
    struct flock fl;
    OMPI_MPI_OFFSET_TYPE offset, end_position=0;

    if(fh->f_sharedfp_data==NULL){
	opal_output(ompi_sharedfp_base_framework.framework_output,
		    "sharedfp_lockedfile_seek: module not initialized\n");
        return OMPI_ERROR;
    }

    sh = fh->f_sharedfp_data;
    offset = off * fh->f_etype_size;

    if( 0 == fh->f_rank ){
        if ( MPI_SEEK_SET == whence ){
            /*don't need to read current value*/
            if(offset < 0){
                opal_output(0,"sharedfp_lockedfile_seek - MPI_SEEK_SET, offset must be > 0,"
                            " got offset=%lld.\n",offset);
                return OMPI_ERROR;
            }
        }
	else if ( MPI_SEEK_CUR == whence){
            OMPI_MPI_OFFSET_TYPE current_position;
            ret = mca_sharedfp_lockedfile_get_position(fh,&current_position);
            if ( OMPI_SUCCESS != ret ) {
                return OMPI_ERROR;
            }

            offset = current_position + offset;
            fflush(stdout);
            if(offset < 0){
                opal_output(0,"sharedfp_lockedfile_seek - MPI_SEEK_CUR, offset must be > 0, got offset=%lld.\n",offset);
                return OMPI_ERROR;
            }
        }
	else if( MPI_SEEK_END == whence ){
            mca_common_ompio_file_get_size( fh,&end_position);
            offset = end_position + offset;

            if ( offset < 0){
                opal_output(0,"sharedfp_lockedfile_seek - MPI_SEEK_CUR, offset must be > 0, got offset=%lld.\n",offset);
                return OMPI_ERROR;
            }
        }else{
            opal_output(0,"sharedfp_lockedfile_seek - whence=%i is not supported\n",whence);
            return OMPI_ERROR;
        }


        /* Set Shared file pointer  */
        lockedfile_data = sh->selected_module_data;
        fd_lockedfilehandle = lockedfile_data->handle;

	opal_output(ompi_sharedfp_base_framework.framework_output,
		    "sharedfp_lockedfile_seek: Aquiring lock...");

        /* set up the flock structure */
        fl.l_type   = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start  = 0;
        fl.l_len    = 0;
        fl.l_pid    = getpid();

        /* Aquire an exclusive lock */
        if ( fcntl(fd_lockedfilehandle, F_SETLKW, &fl) == -1) {
            opal_output(0, "Erorr acquiring lock: fcntl(%d,F_SETLKW,&fl)\n",fd_lockedfilehandle);
            opal_output(0,"error(%i): %s", errno, strerror(errno));
            return OMPI_ERROR;
        }
	else{
	    opal_output(ompi_sharedfp_base_framework.framework_output,
			"sharedfp_lockedfile_seek: Success! acquired lock.for fd: %d\n",fd_lockedfilehandle);
        }

        /*-- -----------------
	 *write to the file
	 *--------------------
	 */
	lseek ( fd_lockedfilehandle, 0, SEEK_SET);
        write ( fd_lockedfilehandle, &offset, sizeof(OMPI_MPI_OFFSET_TYPE));

        /*-------------------
	 * unlock the file
	 *--------------------
	 */
	if ( mca_sharedfp_lockedfile_verbose ) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
                        "sharedfp_lockedfile_seek: Releasing lock...");
	}
	fl.l_type   = F_UNLCK;  /* set to unlock same region */
        fl.l_whence = SEEK_SET;
        fl.l_start  = 0;
        fl.l_len    = 0;
        fl.l_pid    = getpid();

        if (fcntl(fd_lockedfilehandle, F_SETLK, &fl) == -1) {
            opal_output(0,"Failed to release lock for fd: %d\n",fd_lockedfilehandle);
            opal_output(0,"error(%i): %s", errno, strerror(errno));
            return OMPI_ERROR;
        }
	else{
	    opal_output(ompi_sharedfp_base_framework.framework_output,
			"sharedfp_lockedfile_seek: released lock.for fd: %d\n",fd_lockedfilehandle);
        }
    }

    fh->f_comm->c_coll->coll_barrier ( fh->f_comm , fh->f_comm->c_coll->coll_barrier_module );
    return OMPI_SUCCESS;
}
