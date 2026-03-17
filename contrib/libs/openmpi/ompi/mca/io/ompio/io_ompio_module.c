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
 * Copyright (c) 2008-2011 University of Houston. All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"

#include "mpi.h"
#include "opal/threads/mutex.h"
#include "ompi/mca/io/io.h"
#include "io_ompio.h"

/*
 * The OMPIO module operations
 */
mca_io_base_module_2_0_0_t mca_io_ompio_module = {

    mca_io_ompio_file_open,
    mca_io_ompio_file_close,

    mca_io_ompio_file_set_size,
    mca_io_ompio_file_preallocate,
    mca_io_ompio_file_get_size,
    mca_io_ompio_file_get_amode,

    mca_io_ompio_file_set_view,
    mca_io_ompio_file_get_view,

    /* Index IO operations */
    mca_io_ompio_file_read_at,
    mca_io_ompio_file_read_at_all,
    mca_io_ompio_file_write_at,
    mca_io_ompio_file_write_at_all,

    mca_io_ompio_file_iread_at,
    mca_io_ompio_file_iwrite_at,
    mca_io_ompio_file_iread_at_all,
    mca_io_ompio_file_iwrite_at_all,

    /* non-indexed IO operations */
    mca_io_ompio_file_read,
    mca_io_ompio_file_read_all,
    mca_io_ompio_file_write,
    mca_io_ompio_file_write_all,

    mca_io_ompio_file_iread,
    mca_io_ompio_file_iwrite,
    mca_io_ompio_file_iread_all,
    mca_io_ompio_file_iwrite_all,

    mca_io_ompio_file_seek,
    mca_io_ompio_file_get_position,
    mca_io_ompio_file_get_byte_offset,

    mca_io_ompio_file_read_shared,
    mca_io_ompio_file_write_shared,
    mca_io_ompio_file_iread_shared,
    mca_io_ompio_file_iwrite_shared,
    mca_io_ompio_file_read_ordered,
    mca_io_ompio_file_write_ordered,
    mca_io_ompio_file_seek_shared,
    mca_io_ompio_file_get_position_shared,

    /* Split IO operations */
    mca_io_ompio_file_read_at_all_begin,
    mca_io_ompio_file_read_at_all_end,
    mca_io_ompio_file_write_at_all_begin,
    mca_io_ompio_file_write_at_all_end,
    mca_io_ompio_file_read_all_begin,
    mca_io_ompio_file_read_all_end,
    mca_io_ompio_file_write_all_begin,
    mca_io_ompio_file_write_all_end,
    mca_io_ompio_file_read_ordered_begin,
    mca_io_ompio_file_read_ordered_end,
    mca_io_ompio_file_write_ordered_begin,
    mca_io_ompio_file_write_ordered_end,

    mca_io_ompio_file_get_type_extent,

    /* Sync/atomic IO operations */
    mca_io_ompio_file_set_atomicity,
    mca_io_ompio_file_get_atomicity,
    mca_io_ompio_file_sync
};
