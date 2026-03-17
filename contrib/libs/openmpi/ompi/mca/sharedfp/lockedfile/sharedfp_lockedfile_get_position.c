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
 * Copyright (c) 2008-2013 University of Houston. All rights reserved.
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

int
mca_sharedfp_lockedfile_get_position(ompio_file_t *fh,
                                     OMPI_MPI_OFFSET_TYPE * offset)
{
    int ret = OMPI_SUCCESS;
    mca_sharedfp_base_module_t * shared_fp_base_module;
    struct mca_sharedfp_base_data_t *sh = NULL;

    if(fh->f_sharedfp_data==NULL){
	opal_output(ompi_sharedfp_base_framework.framework_output,
		    "sharedfp_lockedfile_get_position - opening the shared file pointer\n");
        shared_fp_base_module = fh->f_sharedfp;

        ret = shared_fp_base_module->sharedfp_file_open(fh->f_comm,
                                                        fh->f_filename,
                                                        fh->f_amode,
                                                        fh->f_info,
                                                        fh);
        if (ret != OMPI_SUCCESS) {
            opal_output(0,"sharedfp_lockedfile_write - error opening the shared file pointer\n");
            return ret;
        }
    }
    /*Retrieve the shared file data struct*/
    sh = fh->f_sharedfp_data;

    /*Requesting the offset to write 0 bytes,
     *returns the current offset w/o updating it
     */
    ret = mca_sharedfp_lockedfile_request_position(sh,0,offset);

    return ret;
}
