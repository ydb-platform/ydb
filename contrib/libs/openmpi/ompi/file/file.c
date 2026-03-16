/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2009 Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009-2012 Cisco Systems, Inc.  All rights reserved.
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

#include "ompi_config.h"

#include "ompi/communicator/communicator.h"
#include "ompi/file/file.h"
#include "opal/class/opal_list.h"
#include "opal/util/output.h"
#include "ompi/runtime/params.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/info/info.h"


opal_mutex_t ompi_mpi_file_bootstrap_mutex = OPAL_MUTEX_STATIC_INIT;


/*
 * Table for Fortran <-> C file handle conversion
 */
opal_pointer_array_t ompi_file_f_to_c_table = {{0}};

/*
 * MPI_FILE_NULL (_addr flavor is for F03 bindings)
 */
ompi_predefined_file_t  ompi_mpi_file_null = {{{{0}}}};
ompi_predefined_file_t  *ompi_mpi_file_null_addr = &ompi_mpi_file_null;


/*
 * Local functions
 */
static void file_constructor(ompi_file_t *obj);
static void file_destructor(ompi_file_t *obj);


/*
 * Class instance for ompi_file_t
 */
OBJ_CLASS_INSTANCE(ompi_file_t,
                   opal_infosubscriber_t,
                   file_constructor,
                   file_destructor);


/*
 * Initialize file handling bookeeping
 */
int ompi_file_init(void)
{
    /* Setup file array */

    OBJ_CONSTRUCT(&ompi_file_f_to_c_table, opal_pointer_array_t);
    if( OPAL_SUCCESS != opal_pointer_array_init(&ompi_file_f_to_c_table, 0,
                                                OMPI_FORTRAN_HANDLE_MAX, 16) ) {
        return OMPI_ERROR;
    }

    /* Setup MPI_FILE_NULL.  Note that it will have the default error
       handler of MPI_ERRORS_RETURN, per MPI-2:9.7 (p265).  */

    OBJ_CONSTRUCT(&ompi_mpi_file_null.file, ompi_file_t);
    ompi_mpi_file_null.file.f_comm = &ompi_mpi_comm_null.comm;
    OBJ_RETAIN(ompi_mpi_file_null.file.f_comm);
    ompi_mpi_file_null.file.f_f_to_c_index = 0;
    opal_pointer_array_set_item(&ompi_file_f_to_c_table, 0,
                                &ompi_mpi_file_null.file);

    /* All done */

    return OMPI_SUCCESS;
}


/*
 * Back end to MPI_FILE_OPEN
 */
int ompi_file_open(struct ompi_communicator_t *comm, const char *filename,
                   int amode, struct opal_info_t *info, ompi_file_t **fh)
{
    int ret;
    ompi_file_t *file;

    file = OBJ_NEW(ompi_file_t);
    if (NULL == file) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }


    /* Save the params */

    file->f_comm = comm;
    OBJ_RETAIN(comm);

    /* Copy the info for the info layer */
    file->super.s_info = OBJ_NEW(opal_info_t);
    if (info) {
        opal_info_dup(info, &(file->super.s_info));
    }

    file->f_amode = amode;
    file->f_filename = strdup(filename);
    if (NULL == file->f_filename) {
        OBJ_RELEASE(file);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* Create the mutex */
    OBJ_CONSTRUCT(&file->f_lock, opal_mutex_t);

    /* Select a module and actually open the file */

    if (OMPI_SUCCESS != (ret = mca_io_base_file_select(file, NULL))) {
        OBJ_RELEASE(file);
        return ret;
    }

    /* All done */

    *fh = file;
    return OMPI_SUCCESS;
}


/*
 * Back end to MPI_FILE_CLOSE.
 */
int ompi_file_close(ompi_file_t **file)
{

    OBJ_DESTRUCT(&(*file)->f_lock);

    (*file)->f_flags |= OMPI_FILE_ISCLOSED;
    OBJ_RELEASE(*file);
    *file = &ompi_mpi_file_null.file;

    return OMPI_SUCCESS;
}


/*
 * Shut down the MPI_File bookkeeping
 */
int ompi_file_finalize(void)
{
    int i, max;
    size_t num_unnamed;
    ompi_file_t *file;

    /* Release MPI_FILE_NULL.  Do this so that we don't get a bogus leak
       report on it.  Plus, it's statically allocated, so we don't want
     to call OBJ_RELEASE on it. */

    OBJ_DESTRUCT(&ompi_mpi_file_null.file);
    opal_pointer_array_set_item(&ompi_file_f_to_c_table, 0, NULL);

    /* Iterate through all the file handles and destroy them.  Note
       that this also takes care of destroying MPI_FILE_NULL. */

    max = opal_pointer_array_get_size(&ompi_file_f_to_c_table);
    for (num_unnamed = i = 0; i < max; ++i) {
        file = (ompi_file_t *)opal_pointer_array_get_item(&ompi_file_f_to_c_table, i);

        /* If the file was closed but still exists because the user
           told us to never free handles, then do an OBJ_RELEASE it
           and all is well.  Then get the value again and see if it's
           actually been freed. */

        if (NULL != file && ompi_debug_no_free_handles &&
            0 == (file->f_flags & OMPI_FILE_ISCLOSED)) {
            OBJ_RELEASE(file);
            file = (ompi_file_t *)opal_pointer_array_get_item(&ompi_file_f_to_c_table, i);
        }

        if (NULL != file) {

            /* If the user wanted warnings about MPI object leaks,
               print out a message */

            if (ompi_debug_show_handle_leaks) {
                ++num_unnamed;
            }

            OBJ_RELEASE(file);
        }
        /* Don't bother setting each element back down to NULL; it
           would just take a lot of thread locks / unlocks and since
           we're destroying everything, it isn't worth it */
    }
    if (num_unnamed > 0) {
        opal_output(0, "WARNING: %lu unnamed MPI_File handles still allocated at MPI_FINALIZE", (unsigned long)num_unnamed);
    }
    OBJ_DESTRUCT(&ompi_file_f_to_c_table);

    /* All done */

    return OMPI_SUCCESS;
}


/*
 * Constructor
 */
static void file_constructor(ompi_file_t *file)
{
    /* Initialize the MPI_FILE_OPEN params */

    file->f_comm = NULL;
    file->f_filename = NULL;
    file->f_amode = 0;

    /* Initialize flags */

    file->f_flags = 0;

    /* Initialize the fortran <--> C translation index */

    file->f_f_to_c_index = opal_pointer_array_add(&ompi_file_f_to_c_table,
                                                  file);

    /* Initialize the error handler.  Per MPI-2:9.7 (p265), the
       default error handler on file handles is the error handler on
       MPI_FILE_NULL, which starts out as MPI_ERRORS_RETURN (but can
       be changed by invoking MPI_FILE_SET_ERRHANDLER on
       MPI_FILE_NULL). */

    file->errhandler_type = OMPI_ERRHANDLER_TYPE_FILE;
    if (file != &ompi_mpi_file_null.file) {
        file->error_handler = ompi_mpi_file_null.file.error_handler;
    } else {
        file->error_handler = &ompi_mpi_errors_return.eh;
    }
    OBJ_RETAIN(file->error_handler);

    /* Initialize the module */

    file->f_io_version = MCA_IO_BASE_V_NONE;
    memset(&(file->f_io_selected_module), 0,
           sizeof(file->f_io_selected_module));
    file->f_io_selected_data = NULL;

    /* If the user doesn't want us to ever free it, then add an extra
       RETAIN here */

    if (ompi_debug_no_free_handles) {
        OBJ_RETAIN(&(file->super));
    }
}


/*
 * Destructor
 */
static void file_destructor(ompi_file_t *file)
{
    /* Finalize the module */

    switch (file->f_io_version) {
    case MCA_IO_BASE_V_2_0_0:
        file->f_io_selected_module.v2_0_0.io_module_file_close(file);
        break;
    default:
        /* Should never get here */
        break;
    }

    /* Finalize the data members */

    if (NULL != file->f_comm) {
        OBJ_RELEASE(file->f_comm);
#if OPAL_ENABLE_DEBUG
        file->f_comm = NULL;
#endif
    }

    if (NULL != file->f_filename) {
        free(file->f_filename);
#if OPAL_ENABLE_DEBUG
        file->f_filename = NULL;
#endif
    }

    if (NULL != file->error_handler) {
        OBJ_RELEASE(file->error_handler);
#if OPAL_ENABLE_DEBUG
        file->error_handler = NULL;
#endif
    }

    if (NULL != file->super.s_info) {
        OBJ_RELEASE(file->super.s_info);
#if OPAL_ENABLE_DEBUG
        file->super.s_info = NULL;
#endif
    }

    /* Reset the f_to_c table entry */

    if (MPI_UNDEFINED != file->f_f_to_c_index &&
        NULL != opal_pointer_array_get_item(&ompi_file_f_to_c_table,
                                            file->f_f_to_c_index)) {
        opal_pointer_array_set_item(&ompi_file_f_to_c_table,
                                    file->f_f_to_c_index, NULL);
    }
}
