/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      University of Houston. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_IO_H
#define MCA_IO_H

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "ompi/request/request.h"
#include "ompi/info/info.h"

/*
 * Forward declaration for private data on io components and modules.
 */
struct ompi_file_t;
struct mca_io_base_file_t;
struct mca_io_base_delete_t;


/*
 * Forward declarations of things declared in this file
 */
struct mca_io_base_module_2_0_0_t;
union mca_io_base_modules_t;


/**
 * Version of IO component interface that we're using.
 *
 * The IO component is being designed to ensure that it can
 * simultaneously support multiple component versions in a single
 * executable.  This is because ROMIO will always be v2.x that
 * supports pretty much a 1-to-1 MPI-API-to-module-function mapping,
 * but we plan to have a v3.x series that will be "something
 * different" (as yet undefined).
 */
enum mca_io_base_version_t {
    MCA_IO_BASE_V_NONE,
    MCA_IO_BASE_V_2_0_0,

    MCA_IO_BASE_V_MAX
};
/**
 * Convenience typedef
 */
typedef enum mca_io_base_version_t mca_io_base_version_t;


/*
 * Macro for use in components that are of type io
 * 1-1 mapping of all MPI-IO functions
 */
#define MCA_IO_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("io", 2, 0, 0)

/*
 * Component
 */

struct mca_io_base_module_2_0_0_t;
typedef int (*mca_io_base_component_init_query_fn_t)
    (bool enable_progress_threads, bool enable_mpi_threads);
typedef const struct mca_io_base_module_2_0_0_t *
    (*mca_io_base_component_file_query_2_0_0_fn_t)
    (struct ompi_file_t *file, struct mca_io_base_file_t **private_data,
     int *priority);
typedef int (*mca_io_base_component_file_unquery_fn_t)
    (struct ompi_file_t *file, struct mca_io_base_file_t *private_data);

typedef int (*mca_io_base_component_file_delete_query_fn_t)
    (const char *filename, struct opal_info_t *info,
     struct mca_io_base_delete_t **private_data,
     bool *usable, int *priority);
typedef int (*mca_io_base_component_file_delete_select_fn_t)
    (const char *filename, struct opal_info_t *info,
     struct mca_io_base_delete_t *private_data);
typedef int (*mca_io_base_component_file_delete_unselect_fn_t)
    (const char *filename, struct opal_info_t *info,
     struct mca_io_base_delete_t *private_data);

typedef int (*mca_io_base_component_register_datarep_fn_t)(
                                              const char *,
                                              MPI_Datarep_conversion_function*,
                                              MPI_Datarep_conversion_function*,
                                              MPI_Datarep_extent_function*,
                                              void*);


/* IO component version and interface functions. */
struct mca_io_base_component_2_0_0_t {
    mca_base_component_t io_version;
    mca_base_component_data_t io_data;

    mca_io_base_component_init_query_fn_t io_init_query;
    mca_io_base_component_file_query_2_0_0_fn_t io_file_query;
    mca_io_base_component_file_unquery_fn_t io_file_unquery;

    mca_io_base_component_file_delete_query_fn_t io_delete_query;
    mca_io_base_component_file_delete_unselect_fn_t io_delete_unquery;
    mca_io_base_component_file_delete_select_fn_t io_delete_select;

    mca_io_base_component_register_datarep_fn_t io_register_datarep;
};
typedef struct mca_io_base_component_2_0_0_t mca_io_base_component_2_0_0_t;


/*
 * All component versions
 */
union mca_io_base_components_t {
    mca_io_base_component_2_0_0_t v2_0_0;
};
typedef union mca_io_base_components_t mca_io_base_components_t;


/*
 * Module v2.0.0
 */

typedef int (*mca_io_base_module_file_open_fn_t)
    (struct ompi_communicator_t *comm, const char *filename, int amode,
     struct opal_info_t *info, struct ompi_file_t *fh);
typedef int (*mca_io_base_module_file_close_fn_t)(struct ompi_file_t *fh);

typedef int (*mca_io_base_module_file_set_size_fn_t)
    (struct ompi_file_t *fh, MPI_Offset size);
typedef int (*mca_io_base_module_file_preallocate_fn_t)
    (struct ompi_file_t *fh, MPI_Offset size);
typedef int (*mca_io_base_module_file_get_size_fn_t)
    (struct ompi_file_t *fh, MPI_Offset *size);
typedef int (*mca_io_base_module_file_get_amode_fn_t)
    (struct ompi_file_t *fh, int *amode);

typedef int (*mca_io_base_module_file_set_view_fn_t)
    (struct ompi_file_t *fh, MPI_Offset disp, struct ompi_datatype_t *etype,
     struct ompi_datatype_t *filetype, const char *datarep,
     struct opal_info_t *info);
typedef int (*mca_io_base_module_file_get_view_fn_t)
    (struct ompi_file_t *fh, MPI_Offset *disp,
     struct ompi_datatype_t **etype, struct ompi_datatype_t **filetype,
     char *datarep);

typedef int (*mca_io_base_module_file_read_at_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_read_at_all_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_at_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, const void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_at_all_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, const void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_status_public_t *status);

typedef int (*mca_io_base_module_file_iread_at_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_request_t **request);
typedef int (*mca_io_base_module_file_iwrite_at_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, const void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_request_t **request);

typedef int (*mca_io_base_module_file_iread_at_all_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_request_t **request);
typedef int (*mca_io_base_module_file_iwrite_at_all_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, const void *buf,
     int count, struct ompi_datatype_t *datatype,
     struct ompi_request_t **request);

typedef int (*mca_io_base_module_file_read_fn_t)
    (struct ompi_file_t *fh, void *buf, int count, struct ompi_datatype_t *
     datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_read_all_fn_t)
    (struct ompi_file_t *fh, void *buf, int count, struct ompi_datatype_t *
     datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count, struct ompi_datatype_t *
     datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_all_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count, struct ompi_datatype_t *
     datatype, struct ompi_status_public_t *status);

typedef int (*mca_io_base_module_file_iread_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_request_t **request);
typedef int (*mca_io_base_module_file_iwrite_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_request_t **request);

typedef int (*mca_io_base_module_file_iread_all_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_request_t **request);
typedef int (*mca_io_base_module_file_iwrite_all_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_request_t **request);

typedef int (*mca_io_base_module_file_seek_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, int whence);
typedef int (*mca_io_base_module_file_get_position_fn_t)
    (struct ompi_file_t *fh, MPI_Offset *offset);
typedef int (*mca_io_base_module_file_get_byte_offset_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, MPI_Offset *disp);

typedef int (*mca_io_base_module_file_read_shared_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_shared_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_iread_shared_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_request_t **request);
typedef int (*mca_io_base_module_file_iwrite_shared_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_request_t **request);
typedef int (*mca_io_base_module_file_read_ordered_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_ordered_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_seek_shared_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, int whence);
typedef int (*mca_io_base_module_file_get_position_shared_fn_t)
    (struct ompi_file_t *fh, MPI_Offset *offset);

typedef int (*mca_io_base_module_file_read_at_all_begin_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, void *buf,
     int count, struct ompi_datatype_t *datatype);
typedef int (*mca_io_base_module_file_read_at_all_end_fn_t)
    (struct ompi_file_t *fh, void *buf, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_at_all_begin_fn_t)
    (struct ompi_file_t *fh, MPI_Offset offset, const void *buf,
     int count, struct ompi_datatype_t *datatype);
typedef int (*mca_io_base_module_file_write_at_all_end_fn_t)
    (struct ompi_file_t *fh, const void *buf, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_read_all_begin_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype);
typedef int (*mca_io_base_module_file_read_all_end_fn_t)
    (struct ompi_file_t *fh, void *buf, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_all_begin_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype);
typedef int (*mca_io_base_module_file_write_all_end_fn_t)
    (struct ompi_file_t *fh, const void *buf, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_read_ordered_begin_fn_t)
    (struct ompi_file_t *fh, void *buf, int count,
     struct ompi_datatype_t *datatype);
typedef int (*mca_io_base_module_file_read_ordered_end_fn_t)
    (struct ompi_file_t *fh, void *buf, struct ompi_status_public_t *status);
typedef int (*mca_io_base_module_file_write_ordered_begin_fn_t)
    (struct ompi_file_t *fh, const void *buf, int count,
     struct ompi_datatype_t *datatype);
typedef int (*mca_io_base_module_file_write_ordered_end_fn_t)
    (struct ompi_file_t *fh, const void *buf, struct ompi_status_public_t *status);

typedef int (*mca_io_base_module_file_get_type_extent_fn_t)
    (struct ompi_file_t *fh, struct ompi_datatype_t *datatype,
     MPI_Aint *extent);

typedef int (*mca_io_base_module_file_set_atomicity_fn_t)
    (struct ompi_file_t *fh, int flag);
typedef int (*mca_io_base_module_file_get_atomicity_fn_t)
    (struct ompi_file_t *fh, int *flag);
typedef int (*mca_io_base_module_file_sync_fn_t)(struct ompi_file_t *fh);

struct mca_io_base_module_2_0_0_t {

    /* Back-ends to MPI API calls (pretty much a 1-to-1 mapping) */

    mca_io_base_module_file_open_fn_t        io_module_file_open;
    mca_io_base_module_file_close_fn_t       io_module_file_close;

    mca_io_base_module_file_set_size_fn_t    io_module_file_set_size;
    mca_io_base_module_file_preallocate_fn_t io_module_file_preallocate;
    mca_io_base_module_file_get_size_fn_t    io_module_file_get_size;
    mca_io_base_module_file_get_amode_fn_t   io_module_file_get_amode;

    mca_io_base_module_file_set_view_fn_t    io_module_file_set_view;
    mca_io_base_module_file_get_view_fn_t    io_module_file_get_view;

    mca_io_base_module_file_read_at_fn_t     io_module_file_read_at;
    mca_io_base_module_file_read_at_all_fn_t io_module_file_read_at_all;
    mca_io_base_module_file_write_at_fn_t    io_module_file_write_at;
    mca_io_base_module_file_write_at_all_fn_t  io_module_file_write_at_all;

    mca_io_base_module_file_iread_at_fn_t      io_module_file_iread_at;
    mca_io_base_module_file_iwrite_at_fn_t     io_module_file_iwrite_at;
    mca_io_base_module_file_iread_at_all_fn_t  io_module_file_iread_at_all;
    mca_io_base_module_file_iwrite_at_all_fn_t io_module_file_iwrite_at_all;

    mca_io_base_module_file_read_fn_t        io_module_file_read;
    mca_io_base_module_file_read_all_fn_t    io_module_file_read_all;
    mca_io_base_module_file_write_fn_t       io_module_file_write;
    mca_io_base_module_file_write_all_fn_t   io_module_file_write_all;

    mca_io_base_module_file_iread_fn_t       io_module_file_iread;
    mca_io_base_module_file_iwrite_fn_t      io_module_file_iwrite;
    mca_io_base_module_file_iread_all_fn_t   io_module_file_iread_all;
    mca_io_base_module_file_iwrite_all_fn_t  io_module_file_iwrite_all;

    mca_io_base_module_file_seek_fn_t        io_module_file_seek;
    mca_io_base_module_file_get_position_fn_t io_module_file_get_position;
    mca_io_base_module_file_get_byte_offset_fn_t io_module_file_get_byte_offset;

    mca_io_base_module_file_read_shared_fn_t   io_module_file_read_shared;
    mca_io_base_module_file_write_shared_fn_t  io_module_file_write_shared;
    mca_io_base_module_file_iread_shared_fn_t  io_module_file_iread_shared;
    mca_io_base_module_file_iwrite_shared_fn_t io_module_file_iwrite_shared;
    mca_io_base_module_file_read_ordered_fn_t  io_module_file_read_ordered;
    mca_io_base_module_file_write_ordered_fn_t io_module_file_write_ordered;
    mca_io_base_module_file_seek_shared_fn_t   io_module_file_seek_shared;
    mca_io_base_module_file_get_position_shared_fn_t  io_module_file_get_position_shared;

    mca_io_base_module_file_read_at_all_begin_fn_t    io_module_file_read_at_all_begin;
    mca_io_base_module_file_read_at_all_end_fn_t      io_module_file_read_at_all_end;
    mca_io_base_module_file_write_at_all_begin_fn_t   io_module_file_write_at_all_begin;
    mca_io_base_module_file_write_at_all_end_fn_t     io_module_file_write_at_all_end;
    mca_io_base_module_file_read_all_begin_fn_t       io_module_file_read_all_begin;
    mca_io_base_module_file_read_all_end_fn_t         io_module_file_read_all_end;
    mca_io_base_module_file_write_all_begin_fn_t      io_module_file_write_all_begin;
    mca_io_base_module_file_write_all_end_fn_t        io_module_file_write_all_end;
    mca_io_base_module_file_read_ordered_begin_fn_t   io_module_file_read_ordered_begin;
    mca_io_base_module_file_read_ordered_end_fn_t     io_module_file_read_ordered_end;
    mca_io_base_module_file_write_ordered_begin_fn_t  io_module_file_write_ordered_begin;
    mca_io_base_module_file_write_ordered_end_fn_t    io_module_file_write_ordered_end;

    mca_io_base_module_file_get_type_extent_fn_t      io_module_file_get_type_extent;

    mca_io_base_module_file_set_atomicity_fn_t        io_module_file_set_atomicity;
    mca_io_base_module_file_get_atomicity_fn_t        io_module_file_get_atomicity;
    mca_io_base_module_file_sync_fn_t                 io_module_file_sync;
};
typedef struct mca_io_base_module_2_0_0_t mca_io_base_module_2_0_0_t;


/*
 * All module versions
 */
union mca_io_base_modules_t {
    mca_io_base_module_2_0_0_t v2_0_0;
};
typedef union mca_io_base_modules_t mca_io_base_modules_t;

#endif /* MCA_IO_H */
