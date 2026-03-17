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
 * Copyright (c) 2013-2016 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_SHAREDFP_LOCKEDFILE_H
#define MCA_SHAREDFP_LOCKEDFILE_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/mca/common/ompio/common_ompio.h"


BEGIN_C_DECLS

int mca_sharedfp_lockedfile_component_init_query(bool enable_progress_threads,
                                                 bool enable_mpi_threads);
struct mca_sharedfp_base_module_1_0_0_t *
        mca_sharedfp_lockedfile_component_file_query (ompio_file_t *file, int *priority);
int mca_sharedfp_lockedfile_component_file_unquery (ompio_file_t *file);

int mca_sharedfp_lockedfile_module_init (ompio_file_t *file);
int mca_sharedfp_lockedfile_module_finalize (ompio_file_t *file);

extern int mca_sharedfp_lockedfile_priority;
extern int mca_sharedfp_lockedfile_verbose;

OMPI_MODULE_DECLSPEC extern mca_sharedfp_base_component_2_0_0_t mca_sharedfp_lockedfile_component;
/*
 * ******************************************************************
 * ********* functions which are implemented in this module *********
 * ******************************************************************
 */
/*IMPORANT: Update here when implementing functions from sharedfp API*/

int mca_sharedfp_lockedfile_seek (ompio_file_t *fh,
                                  OMPI_MPI_OFFSET_TYPE offset, int whence);
int mca_sharedfp_lockedfile_get_position (ompio_file_t *fh,
                                          OMPI_MPI_OFFSET_TYPE * offset);
int mca_sharedfp_lockedfile_file_open (struct ompi_communicator_t *comm,
                                       const char* filename,
                                       int amode,
                                       struct opal_info_t *info,
                                       ompio_file_t *fh);
int mca_sharedfp_lockedfile_file_close (ompio_file_t *fh);
int mca_sharedfp_lockedfile_read (ompio_file_t *fh,
                                  void *buf, int count, MPI_Datatype datatype, MPI_Status *status);
int mca_sharedfp_lockedfile_read_ordered (ompio_file_t *fh,
                                          void *buf, int count, struct ompi_datatype_t *datatype,
                                          ompi_status_public_t *status
                                          );
int mca_sharedfp_lockedfile_read_ordered_begin (ompio_file_t *fh,
                                                 void *buf,
                                                 int count,
                                                 struct ompi_datatype_t *datatype);
int mca_sharedfp_lockedfile_read_ordered_end (ompio_file_t *fh,
                                               void *buf,
                                               ompi_status_public_t *status);
int mca_sharedfp_lockedfile_iread (ompio_file_t *fh,
                                    void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_request_t **request);
int mca_sharedfp_lockedfile_write (ompio_file_t *fh,
                                   const void *buf,
                                   int count,
                                   struct ompi_datatype_t *datatype,
                                   ompi_status_public_t *status);
int mca_sharedfp_lockedfile_write_ordered (ompio_file_t *fh,
                                           const void *buf,
                                           int count,
                                           struct ompi_datatype_t *datatype,
                                           ompi_status_public_t *status);
int mca_sharedfp_lockedfile_write_ordered_begin (ompio_file_t *fh,
                                                 const void *buf,
                                                 int count,
                                                 struct ompi_datatype_t *datatype);
int mca_sharedfp_lockedfile_write_ordered_end (ompio_file_t *fh,
                                               const void *buf,
                                               ompi_status_public_t *status);
int mca_sharedfp_lockedfile_iwrite (ompio_file_t *fh,
                                    const void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_request_t **request);

/*--------------------------------------------------------------*
 *Structures and definitions only for this component
 *--------------------------------------------------------------*/

/*This structure will hang off of the mca_sharedfp_base_data_t's
 *selected_module_data attribute
 */
struct mca_sharedfp_lockedfile_data
{
    int handle;
    char* filename;
};

typedef struct mca_sharedfp_lockedfile_data lockedfile_data;


int mca_sharedfp_lockedfile_request_position (struct mca_sharedfp_base_data_t * sh,
                                              int bytes_requested,
                                              OMPI_MPI_OFFSET_TYPE * offset);
/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */

END_C_DECLS

#endif /* MCA_SHAREDFP_LOCKEDFILE_H */
