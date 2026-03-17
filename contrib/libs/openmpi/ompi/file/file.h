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
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      University of Houston. All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_FILE_H
#define OMPI_FILE_H

#include "ompi_config.h"
#include "mpi.h"
#include "opal/class/opal_list.h"
#include "ompi/errhandler/errhandler.h"
#include "opal/threads/mutex.h"
#include "opal/util/info_subscriber.h"
#include "ompi/mca/io/io.h"

/*
 * Flags
 */
#define OMPI_FILE_ISCLOSED     0x00000001
#define OMPI_FILE_HIDDEN       0x00000002

BEGIN_C_DECLS

/**
 * Back-end structure for MPI_File
 */
struct ompi_file_t {
    /** Base of OBJ_* interface */
    opal_infosubscriber_t super;

    /** Communicator that this file was created with */
    struct ompi_communicator_t *f_comm;

    /** Filename that this file was created with */
    char *f_filename;

    /** Amode that this file was created with */
    int f_amode;

    /** Bit flags */
    int32_t f_flags;

    /** Index in Fortran <-> C translation array */
    int f_f_to_c_index;

    /** Error handler.  This field does not have the "f_" prefix so
        that the OMPI_ERRHDL_* macros can find it, regardless of
        whether it's a comm, window, or file. */
    struct ompi_errhandler_t *error_handler;

    /** Type of the error handler.  This field does not have the "f_"
        prefix for the same reason as the field error_handler. */
    ompi_errhandler_type_t errhandler_type;

    /** Indicate what version of the IO component we're using (this
        indicates what member to look at in the union, below) */
    mca_io_base_version_t f_io_version;

    /** Mutex to be used to protect access to the selected component
        on a per file-handle basis */
    opal_mutex_t f_lock;

    /** The selected component (note that this is a union) -- we need
        this to add and remove the component from the list of
        components currently in use by the io framework for
        progression porpoises. */
    mca_io_base_components_t f_io_selected_component;

    /** The selected module (note that this is a union) */
    mca_io_base_modules_t f_io_selected_module;

    /** Allow the selected module to cache data on the file */
    struct mca_io_base_file_t *f_io_selected_data;
};
/**
 * Convenience typedef
 */
typedef struct ompi_file_t ompi_file_t;

/**
 * Padded struct to maintain back compatibiltiy.
 * See ompi/communicator/communicator.h comments with struct ompi_communicator_t
 * for full explanation why we chose the following padding construct for predefines.
 */
#define PREDEFINED_FILE_PAD 1536

struct ompi_predefined_file_t {
    struct ompi_file_t file;
    char padding[PREDEFINED_FILE_PAD - sizeof(ompi_file_t)];
};
typedef struct ompi_predefined_file_t ompi_predefined_file_t;

/**
 * Back-end instances for MPI_FILE_NULL (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_file_t  ompi_mpi_file_null;
OMPI_DECLSPEC extern ompi_predefined_file_t  *ompi_mpi_file_null_addr;


/**
 * Fortran to C conversion table
 */
extern opal_pointer_array_t ompi_file_f_to_c_table;

/**
 * Initialize MPI_File handling.
 *
 * @retval OMPI_SUCCESS Always.
 *
 * Invoked during ompi_mpi_init().
 */
int ompi_file_init(void);

/**
 * Back-end to MPI_FILE_OPEN: create a file handle, select an io
 * component to use, and have that componet open the file.
 *
 * @param comm Communicator
 * @param filename String filename
 * @param amode Mode flags
 * @param info Info
 * @param fh Output file handle
 *
 * @retval OMPI_SUCCESS Upon success
 * @retval OMPI_ERR* Upon error
 *
 * Create a file handle and select an io module to be paired with
 * it.  There is a corresponding ompi_file_close() function; it
 * mainly calls OBJ_RELEASE() but also does some other error
 * handling as well.
 */
int ompi_file_open(struct ompi_communicator_t *comm, const char *filename,
                   int amode, struct opal_info_t *info,
                   ompi_file_t **fh);

/**
 * Atomicly set a name on a file handle.
 *
 * @param file MPI_File handle to set the name on
 * @param name NULL-terminated string to use
 *
 * @returns OMPI_SUCCESS Always.
 *
 * At most (MPI_MAX_OBJECT_NAME-1) characters will be copied over to
 * the file name's name.  This function is performed atomically -- a
 * lock is used to ensure that there are not multiple writers to the
 * name to ensure that we don't end up with an erroneous name (e.g.,
 * a name without a \0 at the end).  After invoking this function,
 * ompi_file_is_name_set() will return true.
 */
int ompi_file_set_name(ompi_file_t *file, char *name);

/**
 * Back-end to MPI_FILE_CLOSE: destroy an ompi_file_t handle and
 * close the file.
 *
 * @param file Pointer to ompi_file_t
 *
 * @returns OMPI_SUCCESS Always.
 *
 * This is the preferred mechanism for freeing an ompi_file_t.
 * Although the main action that it performs is OBJ_RELEASE(), it
 * also does some additional handling for error checking, etc.
 */
int ompi_file_close(ompi_file_t **file);

/**
 * Tear down MPI_File handling.
 *
 * @retval OMPI_SUCCESS Always.
 *
 * Invoked during ompi_mpi_finalize().
 */
int ompi_file_finalize(void);

/**
 * Check to see if an MPI_File handle is valid.
 *
 * @param file The MPI file handle
 *
 * @retval true If the file handle is not valid
 * @retval false If the file handle is valid
 *
 * This is a convenience function, mainly for error checking in
 * top-level MPI API functions.
 */
static inline bool ompi_file_invalid(ompi_file_t *file)
{
    return (NULL == file ||
            &ompi_mpi_file_null.file == file ||
            0 != (file->f_flags & OMPI_FILE_ISCLOSED));
}

END_C_DECLS

#endif /* OMPI_FILE_H */
