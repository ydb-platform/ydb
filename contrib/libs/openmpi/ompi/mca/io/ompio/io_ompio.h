/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2018 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_IO_OMPIO_H
#define MCA_IO_OMPIO_H

#include <fcntl.h>

#include "mpi.h"
#include "opal/class/opal_list.h"
#include "ompi/errhandler/errhandler.h"
#include "opal/threads/mutex.h"
#include "ompi/file/file.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/fcoll/fcoll.h"
#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/sharedfp/sharedfp.h"
#include "ompi/communicator/communicator.h"
#include "ompi/info/info.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/request/request.h"

#include "ompi/mca/common/ompio/common_ompio.h"

extern int mca_io_ompio_cycle_buffer_size;
extern int mca_io_ompio_bytes_per_agg;
extern int mca_io_ompio_num_aggregators;
extern int mca_io_ompio_record_offset_info;
extern int mca_io_ompio_grouping_option;
extern int mca_io_ompio_max_aggregators_ratio;
extern int mca_io_ompio_aggregators_cutoff_threshold;
extern int mca_io_ompio_overwrite_amode;
extern int mca_io_ompio_verbose_info_parsing;

OMPI_DECLSPEC extern int mca_io_ompio_coll_timing_info;

#define QUEUESIZE 2048

/*
 * General values
 */
#define OMPIO_PREALLOC_MAX_BUF_SIZE   33554432
#define OMPIO_DEFAULT_CYCLE_BUF_SIZE  536870912
#define OMPIO_TAG_GATHER              -100
#define OMPIO_TAG_GATHERV             -101
#define OMPIO_TAG_BCAST               -102
#define OMPIO_TAG_SCATTERV            -103

/* ACCESS MODES --- not needed.. just use MPI_MODE_... */
#define OMPIO_MODE_CREATE              1
#define OMPIO_MODE_RDONLY              2
#define OMPIO_MODE_WRONLY              4
#define OMPIO_MODE_RDWR                8
#define OMPIO_MODE_DELETE_ON_CLOSE    16
#define OMPIO_MODE_UNIQUE_OPEN        32
#define OMPIO_MODE_EXCL               64
#define OMPIO_MODE_APPEND            128
#define OMPIO_MODE_SEQUENTIAL        256

/*---------------------------*/

BEGIN_C_DECLS

OMPI_DECLSPEC extern mca_io_base_component_2_0_0_t mca_io_ompio_component;
/*
 * global variables, instantiated in module.c
 */
extern opal_mutex_t mca_io_ompio_mutex;
extern mca_io_base_module_2_0_0_t mca_io_ompio_module;
OMPI_DECLSPEC extern mca_io_base_component_2_0_0_t mca_io_ompio_component;

/*Used in extracting offset adj-matrix*/
typedef struct mca_io_ompio_offlen_array_t{
    OMPI_MPI_OFFSET_TYPE offset;
    MPI_Aint             length;
    int                  process_id;
}mca_io_ompio_offlen_array_t;


#include "ompi/mca/common/ompio/common_ompio.h"
#include "ompi/mca/common/ompio/common_ompio_aggregators.h"

/* functions to retrieve the number of aggregators and the size of the
   temporary buffer on aggregators from the fcoll modules */
OMPI_DECLSPEC int  mca_io_ompio_get_mca_parameter_value ( char *mca_parameter_name, int name_length);

/*
 * Function that sorts an io_array according to the offset by filling
 * up an array of the indices into the array (HEAP SORT)
 */
OMPI_DECLSPEC int ompi_io_ompio_sort_offlen (mca_io_ompio_offlen_array_t *io_array,
                                             int num_entries,
                                             int *sorted);


OMPI_DECLSPEC int ompi_io_ompio_generate_current_file_view (struct ompio_file_t *fh,
                                                            size_t max_data,
                                                            struct iovec **f_iov,
                                                            int *iov_count);

OMPI_DECLSPEC int ompi_io_ompio_generate_groups (ompio_file_t *fh,
                                                 int num_aggregators,
                                                 int *root,
                                                 int *procs_per_group,
						 int **ranks);

/*
 * ******************************************************************
 * ********* functions which are implemented in this module *********
 * ******************************************************************
 */

int mca_io_ompio_file_set_view (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE disp,
                                struct ompi_datatype_t *etype,
                                struct ompi_datatype_t *filetype,
                                const char *datarep,
                                struct opal_info_t *info);

int mca_io_ompio_file_get_view (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE *disp,
                                struct ompi_datatype_t **etype,
                                struct ompi_datatype_t **filetype,
                                char *datarep);
int mca_io_ompio_file_open (struct ompi_communicator_t *comm,
                            const char *filename,
                            int amode,
                            struct opal_info_t *info,
                            struct ompi_file_t *fh);
int mca_io_ompio_file_close (struct ompi_file_t *fh);
int mca_io_ompio_file_set_size (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE size);
int mca_io_ompio_file_preallocate (struct ompi_file_t *fh,
                                   OMPI_MPI_OFFSET_TYPE size);
int mca_io_ompio_file_get_size (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE * size);
int mca_io_ompio_file_get_amode (struct ompi_file_t *fh,
                                 int *amode);
int mca_io_ompio_file_sync (struct ompi_file_t *fh);
int mca_io_ompio_file_seek (struct ompi_file_t *fh,
                            OMPI_MPI_OFFSET_TYPE offet,
                            int whence);
/* Section 9.3 */
int mca_io_ompio_file_set_view (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE disp,
                                struct ompi_datatype_t *etype,
                                struct ompi_datatype_t *filetype,
                                const char *datarep,
                                struct opal_info_t *info);
int mca_io_ompio_file_get_view (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE *disp,
                                struct ompi_datatype_t **etype,
                                struct ompi_datatype_t **filetype,
                                char *datarep);

/* Section 9.4.2 */
int mca_io_ompio_file_read_at (struct ompi_file_t *fh,
                               OMPI_MPI_OFFSET_TYPE offset,
                               void *buf,
                               int count,
                               struct ompi_datatype_t *datatype,
                               ompi_status_public_t *status);
int mca_io_ompio_file_read_at_all (struct ompi_file_t *fh,
                                   OMPI_MPI_OFFSET_TYPE offset,
                                   void *buf,
                                   int count,
                                   struct ompi_datatype_t *datatype,
                                   ompi_status_public_t *status);
int mca_io_ompio_file_write_at (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE offset,
                                const void *buf,
                                int count,
                                struct ompi_datatype_t *datatype,
                                ompi_status_public_t *status);
int mca_io_ompio_file_write_at_all (struct ompi_file_t *fh,
                                    OMPI_MPI_OFFSET_TYPE offset,
                                    const void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_status_public_t *status);
int mca_io_ompio_file_iread_at (struct ompi_file_t *fh,
                                OMPI_MPI_OFFSET_TYPE offset,
                                void *buf,
                                int count,
                                struct ompi_datatype_t *datatype,
                                ompi_request_t **request);
int mca_io_ompio_file_iwrite_at (struct ompi_file_t *fh,
                                 OMPI_MPI_OFFSET_TYPE offset,
                                 const void *buf,
                                 int count,
                                 struct ompi_datatype_t *datatype,
                                 ompi_request_t **request);

/* Section 9.4.3 */
int mca_io_ompio_file_read (struct ompi_file_t *fh,
                            void *buf,
                            int count,
                            struct ompi_datatype_t *datatype,
                            ompi_status_public_t *status);
int mca_io_ompio_file_read_all (struct ompi_file_t *fh,
                                void *buf,
                                int count,
                                struct ompi_datatype_t *datatype,
                                ompi_status_public_t *status);
int mca_io_ompio_file_iread_all (ompi_file_t *fh,
				void *buf,
				int count,
				struct ompi_datatype_t *datatype,
				 ompi_request_t **request);
int mca_io_ompio_file_iread_at_all (ompi_file_t *fh,
				    OMPI_MPI_OFFSET_TYPE offset,
				    void *buf,
				    int count,
				    struct ompi_datatype_t *datatype,
				    ompi_request_t **request);

int mca_io_ompio_file_write (struct ompi_file_t *fh,
                             const void *buf,
                             int count,
                             struct ompi_datatype_t *datatype,
                             ompi_status_public_t *status);
int mca_io_ompio_file_write_all (struct ompi_file_t *fh,
                                 const void *buf,
                                 int count,
                                 struct ompi_datatype_t *datatype,
                                 ompi_status_public_t *status);
int mca_io_ompio_file_iwrite_all (ompi_file_t *fh,
				  const void *buf,
				  int count,
				  struct ompi_datatype_t *datatype,
				  ompi_request_t **request);
int mca_io_ompio_file_iwrite_at_all (ompi_file_t *fh,
				     OMPI_MPI_OFFSET_TYPE offset,
				     const void *buf,
				     int count,
				     struct ompi_datatype_t *datatype,
				     ompi_request_t **request);
int mca_io_ompio_file_iread (struct ompi_file_t *fh,
                             void *buf,
                             int count,
                             struct ompi_datatype_t *datatype,
                             ompi_request_t **request);
int mca_io_ompio_file_iwrite (struct ompi_file_t *fh,
                              const void *buf,
                              int count,
                              struct ompi_datatype_t *datatype,
                              ompi_request_t **request);
int mca_io_ompio_file_seek (struct ompi_file_t *fh,
                            OMPI_MPI_OFFSET_TYPE offset,
                            int whence);
int mca_io_ompio_file_get_position (struct ompi_file_t *fh,
                                    OMPI_MPI_OFFSET_TYPE *offset);
int mca_io_ompio_file_get_byte_offset (struct ompi_file_t *fh,
                                       OMPI_MPI_OFFSET_TYPE offset,
                                       OMPI_MPI_OFFSET_TYPE *disp);

/* Section 9.4.4 */
int mca_io_ompio_file_read_shared (struct ompi_file_t *fh,
                                   void *buf,
                                   int count,
                                   struct ompi_datatype_t *datatype,
                                   ompi_status_public_t *status);
int mca_io_ompio_file_write_shared (struct ompi_file_t *fh,
                                    const void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_status_public_t *status);
int mca_io_ompio_file_iread_shared (struct ompi_file_t *fh,
                                    void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_request_t **request);
int mca_io_ompio_file_iwrite_shared (struct ompi_file_t *fh,
                                     const void *buf,
                                     int count,
                                     struct ompi_datatype_t *datatype,
                                     ompi_request_t **request);
int mca_io_ompio_file_read_ordered (struct ompi_file_t *fh,
                                    void *buf,
                                    int count,
                                    struct ompi_datatype_t *datatype,
                                    ompi_status_public_t *status);
int mca_io_ompio_file_write_ordered (struct ompi_file_t *fh,
                                     const void *buf,
                                     int count,
                                     struct ompi_datatype_t *datatype,
                                     ompi_status_public_t *status);
int mca_io_ompio_file_seek_shared (struct ompi_file_t *fh,
                                   OMPI_MPI_OFFSET_TYPE offset,
                                   int whence);
int mca_io_ompio_file_get_position_shared (struct ompi_file_t *fh,
                                           OMPI_MPI_OFFSET_TYPE *offset);

/* Section 9.4.5 */
int mca_io_ompio_file_read_at_all_begin (struct ompi_file_t *fh,
                                         OMPI_MPI_OFFSET_TYPE offset,
                                         void *buf,
                                         int count,
                                         struct ompi_datatype_t *datatype);
int mca_io_ompio_file_read_at_all_end (struct ompi_file_t *fh,
                                       void *buf,
                                       ompi_status_public_t *status);
int mca_io_ompio_file_write_at_all_begin (struct ompi_file_t *fh,
                                          OMPI_MPI_OFFSET_TYPE offset,
                                          const void *buf,
                                          int count,
                                          struct ompi_datatype_t *datatype);
int mca_io_ompio_file_write_at_all_end (struct ompi_file_t *fh,
                                        const void *buf,
                                        ompi_status_public_t *status);
int mca_io_ompio_file_read_all_begin (struct ompi_file_t *fh,
                                      void *buf,
                                      int count,
                                      struct ompi_datatype_t *datatype);
int mca_io_ompio_file_read_all_end (struct ompi_file_t *fh,
                                    void *buf,
                                    ompi_status_public_t *status);
int mca_io_ompio_file_write_all_begin (struct ompi_file_t *fh,
                                       const void *buf,
                                       int count,
                                       struct ompi_datatype_t *datatype);
int mca_io_ompio_file_write_all_end (struct ompi_file_t *fh,
                                     const void *buf,
                                     ompi_status_public_t *status);
int mca_io_ompio_file_read_ordered_begin (struct ompi_file_t *fh,
                                          void *buf,
                                          int count,
                                          struct ompi_datatype_t *datatype);
int mca_io_ompio_file_read_ordered_end (struct ompi_file_t *fh,
                                        void *buf,
                                        ompi_status_public_t *status);
int mca_io_ompio_file_write_ordered_begin (struct ompi_file_t *fh,
                                           const void *buf,
                                           int count,
                                           struct ompi_datatype_t *datatype);
int mca_io_ompio_file_write_ordered_end (struct ompi_file_t *fh,
                                         const void *buf,
                                         struct ompi_status_public_t *status);

/* Section 9.5.1 */
int mca_io_ompio_file_get_type_extent (struct ompi_file_t *fh,
                                       struct ompi_datatype_t *datatype,
                                       MPI_Aint *extent);

/* Section 9.6.1 */
int mca_io_ompio_file_set_atomicity (struct ompi_file_t *fh,
                                     int flag);
int mca_io_ompio_file_get_atomicity (struct ompi_file_t *fh,
                                     int *flag);
int mca_io_ompio_file_sync (struct ompi_file_t *fh);
/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */


END_C_DECLS

#endif /* MCA_IO_OMPIO_H */
