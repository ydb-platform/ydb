/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics. Since linkers generally pull in symbols by object fules,
 * keeping these symbols as the only symbols in this file prevents
 * utility programs such as "ompi_info" from having to import entire
 * modules just to query their version and parameters
 */

#include "ompi_config.h"
#include "mpi.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/sharedfp/base/base.h"
#include "ompi/mca/sharedfp/lockedfile/sharedfp_lockedfile.h"

/* included so that we can test file locking availability */
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

/*
 * *******************************************************************
 * ************************ actions structure ************************
 * *******************************************************************
 */
 /* IMPORTANT: Update here when adding sharedfp component interface functions*/
static mca_sharedfp_base_module_1_0_0_t lockedfile =  {
    mca_sharedfp_lockedfile_module_init, /* initalise after being selected */
    mca_sharedfp_lockedfile_module_finalize, /* close a module on a communicator */
    mca_sharedfp_lockedfile_seek,
    mca_sharedfp_lockedfile_get_position,
    mca_sharedfp_lockedfile_read,
    mca_sharedfp_lockedfile_read_ordered,
    mca_sharedfp_lockedfile_read_ordered_begin,
    mca_sharedfp_lockedfile_read_ordered_end,
    mca_sharedfp_lockedfile_iread,
    mca_sharedfp_lockedfile_write,
    mca_sharedfp_lockedfile_write_ordered,
    mca_sharedfp_lockedfile_write_ordered_begin,
    mca_sharedfp_lockedfile_write_ordered_end,
    mca_sharedfp_lockedfile_iwrite,
    mca_sharedfp_lockedfile_file_open,
    mca_sharedfp_lockedfile_file_close
};
/*
 * *******************************************************************
 * ************************* structure ends **************************
 * *******************************************************************
 */

int mca_sharedfp_lockedfile_component_init_query(bool enable_progress_threads,
                                                 bool enable_mpi_threads)
{
    /* Nothing to do */

    return OMPI_SUCCESS;
}

struct mca_sharedfp_base_module_1_0_0_t * mca_sharedfp_lockedfile_component_file_query(ompio_file_t *fh, int *priority) {
    struct flock lock;
    int fd, err;
    /*char *filename;*/
    char filename[256];
    int rank;
    bool has_file_lock_support=false;

    *priority = mca_sharedfp_lockedfile_priority;

    /*get the rank of this process*/
    rank = ompi_comm_rank ( fh->f_comm);

    /*test, and update priority*/
    /*
     * This test tests to see if fcntl returns success when asked to
     * establish a file lock.  This test is intended for use on file systems
     * such as NFS that may not implement file locks.  The locked file algorithm makes use
     * of file locks to implement the shared file pointer operations, and will not work
     * properly if file locks are not available.
     *
     * This is a simple test and has at least two limitations:
     *
     * 1. Some implementations of NFS are known to return success for
     * setting a file lock when in fact no lock has been set.  This
     * test will not detect such erroneous implementations of NFS
     *
     * 2. Some implementations will hang (enter and wait indefinitately)
     * within the fcntl call.  This test will also hang in that case.
     * Under normal conditions, this test should only take a few seconds to
     * run.
     *
     * The test prints a message showing the success or failure of
     * setting the file lock if PRINT_TAG is set to true in sharedfp.h.
     * It also sets the priority to a non-zero value on success and
     * returns NULL on failure.  If there is a failure, the system routine
     * strerror is interogated in order to print the reason.
     */

    /* Set the filename. */
    /*data filename created by appending .locktest.$rank to the original filename*/
    sprintf(filename,"%s%s%d",fh->f_filename,".locktest.",rank);

    lock.l_type   = F_WRLCK;
    lock.l_start  = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len    = 100;
    lock.l_pid      = getpid();

    fd = open(filename, O_RDWR | O_CREAT, 0644);

    if ( -1 == fd ){
        opal_output(ompi_sharedfp_base_framework.framework_output,
		    "mca_sharedfp_lockedfile_component_file_query: error opening file %s %s", filename, strerror(errno));
        has_file_lock_support=false;
    }
    else{
        err = fcntl(fd, F_SETLKW, &lock);
	opal_output(ompi_sharedfp_base_framework.framework_output,
		    "mca_sharedfp_lockedfile_component_file_query: returned err=%d, for fd=%d\n",err,fd);

        if (err) {
            opal_output(ompi_sharedfp_base_framework.framework_output,
			"mca_sharedfp_lockedfile_component_file_query: Failed to set a file lock on %s %s\n", filename, strerror(errno) );
            opal_output(ompi_sharedfp_base_framework.framework_output,
			"err=%d, errno=%d, EOPNOTSUPP=%d, EINVAL=%d, ENOSYS=%d, EACCES=%d, EAGAIN=%d, EBADF=%d\n",
                        err, errno, EOPNOTSUPP, EINVAL, ENOSYS, EACCES, EAGAIN, EBADF);

            if (errno == EACCES || errno == EAGAIN) {
                opal_output(ompi_sharedfp_base_framework.framework_output,
			    "errno=EACCES || EAGAIN, Already locked by another process\n");
            }

        }
        else {
	    opal_output(ompi_sharedfp_base_framework.framework_output,
			"mca_sharedfp_lockedfile_component_file_query: fcntl claims success in setting a file lock on %s\n", filename );

            has_file_lock_support=true;
        }
        /* printf("err = %d, errno = %d\n", err, errno); */
        close(fd);
        unlink( filename );
    }
    /**priority=100;*/
    if(has_file_lock_support){
        return &lockedfile;
    }

    *priority = 0;
    /*module can not run!, return NULL to indicate that we are unable to run*/

    opal_output(ompi_sharedfp_base_framework.framework_output,
		"mca_sharedfp_lockedfile_component_file_query: Can not run!, file locking not supported\n");
    return NULL;
}

int mca_sharedfp_lockedfile_component_file_unquery (ompio_file_t *file)
{
    /* This function might be needed for some purposes later. for now it
    * does not have anything to do since there are no steps which need
    * to be undone if this module is not selected */

    return OMPI_SUCCESS;
}

int mca_sharedfp_lockedfile_module_init (ompio_file_t *file)
{
    return OMPI_SUCCESS;
}


int mca_sharedfp_lockedfile_module_finalize (ompio_file_t *file)
{
    return OMPI_SUCCESS;
}
