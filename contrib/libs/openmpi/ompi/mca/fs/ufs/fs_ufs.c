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
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
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
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/fs/ufs/fs_ufs.h"
#include "ompi/mca/fs/base/base.h"

/*
 * *******************************************************************
 * ************************ actions structure ************************
 * *******************************************************************
 */
static mca_fs_base_module_1_0_0_t ufs =  {
    mca_fs_ufs_module_init, /* initalise after being selected */
    mca_fs_ufs_module_finalize, /* close a module on a communicator */
    mca_fs_ufs_file_open,
    mca_fs_base_file_close,
    mca_fs_base_file_delete,
    mca_fs_base_file_set_size,
    mca_fs_base_file_get_size,
    mca_fs_base_file_sync
};
/*
 * *******************************************************************
 * ************************* structure ends **************************
 * *******************************************************************
 */

int mca_fs_ufs_component_init_query(bool enable_progress_threads,
                                      bool enable_mpi_threads)
{
    /* Nothing to do */

   return OMPI_SUCCESS;
}

struct mca_fs_base_module_1_0_0_t *
mca_fs_ufs_component_file_query (ompio_file_t *fh, int *priority)
{
    /* UFS can always be used, will however have a low priority */

   *priority = mca_fs_ufs_priority;
   if (0 == fh->f_fstype ) {
       fh->f_fstype = UFS;
   }

   return &ufs;
}

int mca_fs_ufs_component_file_unquery (ompio_file_t *file)
{
   /* This function might be needed for some purposes later. for now it
    * does not have anything to do since there are no steps which need
    * to be undone if this module is not selected */

   return OMPI_SUCCESS;
}

int mca_fs_ufs_module_init (ompio_file_t *file)
{
    /* Make sure the file type is not overwritten by the last queried
	 * component */
    file->f_fstype = UFS;
    return OMPI_SUCCESS;
}


int mca_fs_ufs_module_finalize (ompio_file_t *file)
{
    return OMPI_SUCCESS;
}
