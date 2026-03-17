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
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * MCA io base framework public interface functions.
 */

#ifndef MCA_IO_BASE_H
#define MCA_IO_BASE_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/class/opal_list.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/io/io.h"


/*
 * Global functions for MCA overall io open and close
 */

BEGIN_C_DECLS
    /**
     * Create list of available io components.
     *
     * @param allow_multi_user_threads Will be set to true if any of the
     * available components will allow multiple user threads
     * @param have_hidden_threads Will be set to true if any of the
     * available components have hidden threads.
     *
     * @retval OMPI_SUCCESS If one or more io components are available.
     * @retval OMPI_ERROR If no io components are found to be available.
     *
     * This function is invoked during ompi_mpi_init() to query all
     * successfully opened io components and create a list of all
     * available io components.
     *
     * This function traverses the (internal global variable)
     * mca_io_base_components_opened list and queries each component to see
     * if it ever might want to run during this MPI process.  It creates
     * another internal global variable list named
     * mca_io_base_components_available, consisting of a list of components
     * that are available for selection when file handles are created.
     * This variable should \em only be used by other io base
     * functions -- it is not considered a public interface member --
     * and is only mentioned here for completeness.
     */
    OMPI_DECLSPEC int mca_io_base_find_available(bool enable_progress_threads,
                                                 bool enable_mpi_threads);

    /**
     * Select an available component for a new file handle.
     *
     * @param file File Handle that the component will be selected for.
     * @param preferred The component that is preferred for this
     * file handle (or NULL).
     *
     * @return OMPI_SUCCESS Upon success.
     * @return OMPI_ERROR Upon failure.
     *
     * Note that the types of the parameters have "struct" in them
     * (e.g., ompi_file_t" vs. a plain "ompi_file_t") to
     * avoid an include file loop.  All similar types (e.g., "struct
     * ompi_file_t *", "ompi_file_t *", and "MPI_File")
     * are all typedef'ed to be the same, so the fact that we use struct
     * here in the prototype is ok.
     *
     * This function is invoked when a new file handle is created and a
     * io component needs to be selected for it.  It should be invoked
     * near the end of the file handle creation process such that
     * almost everything else is functional on the file handle.
     *
     * This function invokes the selection process for io components,
     * which works as follows:
     *
     * - If the \em preferred argument is NULL, the selection set is
     *   defined to be all the components found during
     *   mca_io_base_find_available().
     * - If \em preferred is not NULL, then the selection set is just
     *   that component.  (However, in this mode, we may make 2 passes
     *   through the selection process -- more on this below).
     * - All components in the selection set are queried to see if they
     *   want to run with that file handle.  All components that want to
     *   run are ranked by their priority and the highest priority
     *   component is selected.  All non-selected components have their
     *   "unquery" function invoked to let them know that they were not
     *   selected.
     * - The selected module will have its "init" function
     *   invoked to let it know that it was selected.  All unselected
     *   components will have their file_unselect function invoked.
     * - If we fall through this entire process and no component is
     *   selected \em and the \em preferred argument is not NULL, then
     *   run the entire process again as if the \em preferred argument
     *   was NULL (i.e., use the entire available set of components).
     *
     * At the end of this process, we'll either have a single
     * component/module pair that is selected and initialized for the
     * file handle, or no component was selected and an error is
     * returned up the stack.
     */
    OMPI_DECLSPEC int mca_io_base_file_select(struct ompi_file_t *file,
                                              mca_base_component_t *preferred);

    /**
     * Finalize a io component on a specific file handle.
     *
     * @param file The file handle that is being destroyed.
     *
     * @retval OMPI_SUCCESS Always.
     *
     * Note that the type of the parameter is only a "struct
     * ompi_file_t" (vs. a plain "ompi_file_t") to avoid an include file
     * loop.  The types "struct ompi_file_t *", "ompi_file_t *", and
     * "MPI_File" are all typedef'ed to be the same, so the fact that we
     * use struct here in the prototype is ok.
     *
     * This function is invoked near the beginning of the destruction of
     * a file handle.  It finalizes the io component associated with the
     * file handle (e.g., allowing the component to clean up and free any
     * resources allocated for that file handle).  Note that similar to
     * mca_io_base_select(), as result of this function, other
     * file handles may also be destroyed.
     */
    OMPI_DECLSPEC int mca_io_base_file_unselect(struct ompi_file_t *file);

    /**
     * Invoke a back-end component to delete a file.
     *
     * @param filename Name of the file to be deleted
     * @param info MPI_Info for additional information
     *
     * This function is a bit different than most other MPI_File_*
     * functions -- it does not take a MPI_File handle.  As such, this
     * function function invokes appropriate delete handlers on all
     * the available components (rather than some pre-selected
     * module).  See io.h for details.
     */
    OMPI_DECLSPEC int mca_io_base_delete(const char *filename,
                                         struct opal_info_t *info);

    OMPI_DECLSPEC int mca_io_base_register_datarep(const char *,
                                              MPI_Datarep_conversion_function*,
                                              MPI_Datarep_conversion_function*,
                                              MPI_Datarep_extent_function*,
                                              void*);

/*
 * Globals
 */

OMPI_DECLSPEC extern mca_base_framework_t ompi_io_base_framework;

END_C_DECLS
#endif /* MCA_BASE_IO_H */
