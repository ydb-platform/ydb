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
 * Copyright (c) 2008-2016 University of Houston. All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      DataDirect Networks. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COMMON_OMPIO_H
#define MCA_COMMON_OMPIO_H

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

#define OMPIO_MIN(a, b) (((a) < (b)) ? (a) : (b))
#define OMPIO_MAX(a, b) (((a) < (b)) ? (b) : (a))

#define OMPIO_MCA_GET(fh, name) ((fh)->f_get_mca_parameter_value(#name, strlen(#name)+1))
#define OMPIO_MCA_PRINT_INFO(_fh,_infostr,_infoval, _msg ) {            \
    int _verbose = _fh->f_get_mca_parameter_value("verbose_info_parsing", strlen("verbose_info_parsing")); \
    if ( 1==_verbose && 0==_fh->f_rank ) printf("File: %s info: %s value %s %s\n", _fh->f_filename, _infostr, _infoval, _msg); \
    if ( 2==_verbose ) printf("File: %s info: %s value %s %s\n", _fh->f_filename, _infostr, _infoval, _msg); \
    }
    

/*
 * Flags
 */
#define OMPIO_CONTIGUOUS_MEMORY      0x00000001
#define OMPIO_UNIFORM_FVIEW          0x00000002
#define OMPIO_FILE_IS_OPEN           0x00000004
#define OMPIO_FILE_VIEW_IS_SET       0x00000008
#define OMPIO_CONTIGUOUS_FVIEW       0x00000010
#define OMPIO_AGGREGATOR_IS_SET      0x00000020
#define OMPIO_SHAREDFP_IS_SET        0x00000040
#define OMPIO_LOCK_ENTIRE_FILE       0x00000080
#define OMPIO_LOCK_NEVER             0x00000100
#define OMPIO_LOCK_NOT_THIS_OP       0x00000200


#define OMPIO_ROOT                    0

/*AGGREGATOR GROUPING DECISIONS*/
#define OMPIO_MERGE                     1
#define OMPIO_SPLIT                     2
#define OMPIO_RETAIN                    3

#define DATA_VOLUME                     1
#define UNIFORM_DISTRIBUTION            2
#define CONTIGUITY                      3
#define OPTIMIZE_GROUPING               4
#define SIMPLE                          5
#define NO_REFINEMENT                   6
#define SIMPLE_PLUS                     7

#define OMPIO_LOCK_ENTIRE_REGION  10
#define OMPIO_LOCK_SELECTIVE      11

#define OMPIO_FCOLL_WANT_TIME_BREAKDOWN 0
#define MCA_IO_DEFAULT_FILE_VIEW_SIZE 4*1024*1024

#define OMPIO_UNIFORM_DIST_THRESHOLD     0.5
#define OMPIO_CONTG_THRESHOLD        1048576
#define OMPIO_CONTG_FACTOR                 8
#define OMPIO_DEFAULT_STRIPE_SIZE    1048576
#define OMPIO_PROCS_PER_GROUP_TAG          0
#define OMPIO_PROCS_IN_GROUP_TAG           1
#define OMPIO_MERGE_THRESHOLD            0.5

#define OMPIO_PERM_NULL               -1
#define OMPIO_IOVEC_INITIAL_SIZE      100

enum ompio_fs_type
{
    NONE = 0,
    UFS = 1,
    PVFS2 = 2,
    LUSTRE = 3,
    PLFS = 4
};

typedef struct mca_common_ompio_io_array_t {
    void                 *memory_address;
    /* we need that of type OMPI_MPI_OFFSET_TYPE */
    void                 *offset;
    size_t               length;
    /*mca_common_ompio_server_t io_server;*/
} mca_common_ompio_io_array_t;


typedef struct mca_common_ompio_access_array_t{
    OMPI_MPI_OFFSET_TYPE *offsets;
    int *lens;
    MPI_Aint *mem_ptrs;
    int count;
} mca_common_ompio_access_array_t;


/* forward declaration to keep the compiler happy. */
struct ompio_file_t;
typedef int (*mca_common_ompio_generate_current_file_view_fn_t) (struct ompio_file_t *fh,
							         size_t max_data,
							         struct iovec **f_iov,
							         int *iov_count);

/* functions to retrieve the number of aggregators and the size of the
   temporary buffer on aggregators from the fcoll modules */
typedef int (*mca_common_ompio_get_mca_parameter_value_fn_t) ( char *mca_parameter_name, int name_length );


struct mca_common_ompio_print_queue;

/**
 * Back-end structure for MPI_File
 */
struct ompio_file_t {
    /* General parameters */
    int                    fd;
    struct ompi_file_t    *f_fh;     /* pointer back to the file_t structure */
    OMPI_MPI_OFFSET_TYPE   f_offset; /* byte offset of current position */
    OMPI_MPI_OFFSET_TYPE   f_disp;   /* file_view displacement */
    int                    f_rank;
    int                    f_size;
    int                    f_amode;
    int                    f_perm;
    ompi_communicator_t   *f_comm;
    const char            *f_filename;
    char                  *f_datarep;
    opal_convertor_t      *f_convertor;
    opal_info_t           *f_info;
    int32_t                f_flags;
    void                  *f_fs_ptr;
    int                    f_fs_block_size;
    int                    f_atomicity;
    size_t                 f_stripe_size;
    int                    f_stripe_count;
    size_t                 f_cc_size;
    int                    f_bytes_per_agg;
    enum ompio_fs_type     f_fstype;
    ompi_request_t        *f_split_coll_req;
    bool                   f_split_coll_in_use;
    /* Place for selected sharedfp module to hang it's data.
       Note: Neither f_sharedfp nor f_sharedfp_component seemed appropriate for this.
    */
    void                  *f_sharedfp_data;


    /* File View parameters */
    struct iovec     *f_decoded_iov;
    uint32_t          f_iov_count;
    ompi_datatype_t  *f_iov_type;
    size_t            f_position_in_file_view; /* in bytes */
    size_t            f_total_bytes; /* total bytes read/written within 1 Fview*/
    int               f_index_in_file_view;
    ptrdiff_t         f_view_extent;
    size_t            f_view_size;
    ompi_datatype_t  *f_etype;
    ompi_datatype_t  *f_filetype;
    ompi_datatype_t  *f_orig_filetype; /* the fileview passed by the user to us */
    size_t            f_etype_size;

    /* contains IO requests that needs to be read/written */
    mca_common_ompio_io_array_t *f_io_array;
    int                      f_num_of_io_entries;

    /* Hooks for modules to hang things */
    mca_base_component_t *f_fs_component;
    mca_base_component_t *f_fcoll_component;
    mca_base_component_t *f_fbtl_component;
    mca_base_component_t *f_sharedfp_component;

    /* structure of function pointers */
    mca_fs_base_module_t       *f_fs;
    mca_fcoll_base_module_t    *f_fcoll;
    mca_fbtl_base_module_t     *f_fbtl;
    mca_sharedfp_base_module_t *f_sharedfp;

    /* Timing information  */
    struct mca_common_ompio_print_queue *f_coll_write_time;
    struct mca_common_ompio_print_queue *f_coll_read_time;

    /*initial list of aggregators and groups*/
    int *f_init_aggr_list;
    int  f_init_num_aggrs;
    int  f_init_procs_per_group;
    int *f_init_procs_in_group;

    /* final of aggregators and groups*/
    int *f_aggr_list;
    int  f_num_aggrs;
    int *f_procs_in_group;
    int  f_procs_per_group;

    /* internal ompio functions required by fbtl and fcoll */
    mca_common_ompio_generate_current_file_view_fn_t f_generate_current_file_view;

    mca_common_ompio_get_mca_parameter_value_fn_t          f_get_mca_parameter_value;
};
typedef struct ompio_file_t ompio_file_t;

struct mca_common_ompio_data_t {
    ompio_file_t ompio_fh;
};
typedef struct mca_common_ompio_data_t mca_common_ompio_data_t;


#include "common_ompio_print_queue.h"
#include "common_ompio_aggregators.h"

OMPI_DECLSPEC int mca_common_ompio_file_write (ompio_file_t *fh, const void *buf,  int count,
                                               struct ompi_datatype_t *datatype, 
                                               ompi_status_public_t *status);

OMPI_DECLSPEC int mca_common_ompio_file_write_at (ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset,  const void *buf,
                                                  int count,  struct ompi_datatype_t *datatype, 
                                                  ompi_status_public_t *status);

OMPI_DECLSPEC int mca_common_ompio_file_iwrite (ompio_file_t *fh, const void *buf, int count,
                                                struct ompi_datatype_t *datatype, ompi_request_t **request);

OMPI_DECLSPEC int mca_common_ompio_file_iwrite_at (ompio_file_t *fh,  OMPI_MPI_OFFSET_TYPE offset,
                                                   const void *buf,  int count,  struct ompi_datatype_t *datatype,
                                                   ompi_request_t **request);

OMPI_DECLSPEC int mca_common_ompio_file_write_at_all (ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset, const void *buf,
                                                      int count, struct ompi_datatype_t *datatype, 
                                                      ompi_status_public_t *status);


OMPI_DECLSPEC int mca_common_ompio_file_iwrite_at_all (ompio_file_t *fp, OMPI_MPI_OFFSET_TYPE offset, const void *buf,
                                                       int count, struct ompi_datatype_t *datatype, ompi_request_t **request);

OMPI_DECLSPEC int mca_common_ompio_build_io_array ( ompio_file_t *fh, int index, int cycles,
                                                    size_t bytes_per_cycle, int max_data, uint32_t iov_count,
                                                    struct iovec *decoded_iov, int *ii, int *jj, size_t *tbw,
                                                    size_t *spc );


OMPI_DECLSPEC int mca_common_ompio_file_read (ompio_file_t *fh,  void *buf,  int count,
                                              struct ompi_datatype_t *datatype, ompi_status_public_t *status);

OMPI_DECLSPEC int mca_common_ompio_file_read_at (ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset,  void *buf,
                                                 int count, struct ompi_datatype_t *datatype, 
                                                 ompi_status_public_t * status);

OMPI_DECLSPEC int mca_common_ompio_file_iread (ompio_file_t *fh, void *buf, int count,
                                               struct ompi_datatype_t *datatype, ompi_request_t **request);

OMPI_DECLSPEC int mca_common_ompio_file_iread_at (ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset,
                                                  void *buf, int count, struct ompi_datatype_t *datatype,
                                                  ompi_request_t **request);

OMPI_DECLSPEC int mca_common_ompio_file_read_at_all (ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset,
                                                     void *buf, int count, struct ompi_datatype_t *datatype,
                                                     ompi_status_public_t * status);

OMPI_DECLSPEC int mca_common_ompio_file_iread_at_all (ompio_file_t *fp, OMPI_MPI_OFFSET_TYPE offset,
                                                      void *buf, int count, struct ompi_datatype_t *datatype,
                                                      ompi_request_t **request);

OMPI_DECLSPEC int mca_common_ompio_file_open (ompi_communicator_t *comm, const char *filename,
                                              int amode, opal_info_t *info,
                                              ompio_file_t *ompio_fh, bool use_sharedfp);

OMPI_DECLSPEC int mca_common_ompio_file_delete (const char *filename,
                                                struct opal_info_t *info);
OMPI_DECLSPEC int mca_common_ompio_create_incomplete_file_handle (const char *filename,
                                                                  ompio_file_t **fh);

OMPI_DECLSPEC int mca_common_ompio_file_close (ompio_file_t *ompio_fh);
OMPI_DECLSPEC int mca_common_ompio_file_get_size (ompio_file_t *ompio_fh, OMPI_MPI_OFFSET_TYPE *size);
OMPI_DECLSPEC int mca_common_ompio_file_get_position (ompio_file_t *fh,OMPI_MPI_OFFSET_TYPE *offset);
OMPI_DECLSPEC int mca_common_ompio_set_explicit_offset (ompio_file_t *fh, OMPI_MPI_OFFSET_TYPE offset);
OMPI_DECLSPEC int mca_common_ompio_set_file_defaults (ompio_file_t *fh);
OMPI_DECLSPEC int mca_common_ompio_set_view (ompio_file_t *fh,  OMPI_MPI_OFFSET_TYPE disp,
                                             ompi_datatype_t *etype,  ompi_datatype_t *filetype, const char *datarep,
                                             opal_info_t *info);
 

/*
 * Function that takes in a datatype and buffer, and decodes that datatype
 * into an iovec using the convertor_raw function
 */
OMPI_DECLSPEC int mca_common_ompio_decode_datatype (struct ompio_file_t *fh,
                                                    struct ompi_datatype_t *datatype,
                                                    int count,
                                                    const void *buf,
                                                    size_t *max_data,
                                                    struct iovec **iov,
                                                    uint32_t *iov_count);

OMPI_DECLSPEC int mca_common_ompio_set_callbacks(mca_common_ompio_generate_current_file_view_fn_t generate_current_file_view,
                                                 mca_common_ompio_get_mca_parameter_value_fn_t get_mca_parameter_value);
#endif /* MCA_COMMON_OMPIO_H */
