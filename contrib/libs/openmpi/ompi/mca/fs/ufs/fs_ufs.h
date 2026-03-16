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
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FS_UFS_H
#define MCA_FS_UFS_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/common/ompio/common_ompio.h"

extern int mca_fs_ufs_priority;
extern int mca_fs_ufs_lock_algorithm;

#define FS_UFS_LOCK_AUTO        0
#define FS_UFS_LOCK_NEVER       1
#define FS_UFS_LOCK_ENTIRE_FILE 2
#define FS_UFS_LOCK_RANGES      3

BEGIN_C_DECLS

int mca_fs_ufs_component_init_query(bool enable_progress_threads,
                                        bool enable_mpi_threads);
struct mca_fs_base_module_1_0_0_t *
mca_fs_ufs_component_file_query (ompio_file_t *fh, int *priority);
int mca_fs_ufs_component_file_unquery (ompio_file_t *file);

int mca_fs_ufs_module_init (ompio_file_t *file);
int mca_fs_ufs_module_finalize (ompio_file_t *file);

OMPI_MODULE_DECLSPEC extern mca_fs_base_component_2_0_0_t mca_fs_ufs_component;
/*
 * ******************************************************************
 * ********* functions which are implemented in this module *********
 * ******************************************************************
 */

int mca_fs_ufs_file_open (struct ompi_communicator_t *comm,
                          const char *filename,
                          int amode,
                          struct opal_info_t *info,
                          ompio_file_t *fh);

int mca_fs_ufs_file_close (ompio_file_t *fh);

int mca_fs_ufs_file_delete (char *filename,
                            struct opal_info_t *info);

int mca_fs_ufs_file_set_size (ompio_file_t *fh,
                              OMPI_MPI_OFFSET_TYPE size);

int mca_fs_ufs_file_get_size (ompio_file_t *fh,
                              OMPI_MPI_OFFSET_TYPE *size);

int mca_fs_ufs_file_sync (ompio_file_t *fh);

int mca_fs_ufs_file_seek (ompio_file_t *fh,
                          OMPI_MPI_OFFSET_TYPE offset,
                          int whence);
/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */

END_C_DECLS

#endif /* MCA_FS_UFS_H */
