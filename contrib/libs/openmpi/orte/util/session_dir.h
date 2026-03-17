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
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file:
 *
 * Find and/or create Open MPI session directory.
 *
 * The orte_session_dir() function searches for a temporary directory
 * that is used by the Open MPI system for storing system-critical
 * information. For a given system and user, the function attempts to
 * find (or create, if not found and create is requested) a directory
 * that will be used to independently house information for multiple
 * universes, as the user creates them. Thus, the function pursues a
 * directory tree of the form:
 *
 * \par \em [prefix-dir] An absolute path that identifies a temporary
 * directory that is read-write-execute accessible to everyone. The
 * function first checks to see if the user has specified the [prefix]
 * directory on the command line. If so, then the function will use
 * that [prefix] if the access permissions are correct, or will return
 * an error condition if not - the function will not search for
 * alternative locations if the user provides the [prefix] name.
 *
 * \par If the [prefix] is not provided by the user, the function
 * searches for a suitable directory in a specific order, taking the
 * first option that meets the access permission requirement, using:
 * (a) the "OMPI_PREFIX_ENV" environment variable; (b) the "TMPDIR"
 * environment variable; and (c) the "TMP" environment variabley. If
 * none of those environmental variables have been defined and/or the
 * function was unable to create a suitable directory within any of
 * them, then the function tries to use a default location of "/tmp",
 * where the "/" represents the top-level directory of the local
 * system. If none of these options are successful, the function
 * returns an error code.
 *
 * \par \em [openmpi-sessions]-[user-id]@[host]:[batchid] This serves
 * as a concentrator for all Open MPI session directories for this
 * user on the local system. If it doesn't already exist, this
 * directory is created with read-write-execute permissions
 * exclusively restricted to the user. If it does exist, the access
 * permissions are checked to ensure they are correct - if not, the
 * program attempts to correct them. If they can't' be changed to the
 * correct values, an error condition is returned. The [host] and
 * [batchid] fields are included to provide uniqueness on shared file
 * systems and batch schedulers, respectively.
 *
 * \par Note: The [prefix]/openmpi-sessions-[user-id]@[host]:[batchid]
 * directory is left on the system upon termination of an application
 * and/or an Open MPI universe for future use by the user. Thus, when
 * checking a potential location for the directory, the
 * orte_session_tree_init() function first checks to see if an
 * appropriate directory already exists, and uses it if it does.
 *
 * \par \em [universe-name] A directory is created for the specified
 * universe name. This is the directory that will be used to house all
 * information relating to the specific universe. If the directory
 * already exists (indicating that the user is joining an existing
 * universe), then the function ensures that the user has exclusive
 * read-write-execute permissions on the directory.
 *
 * \par \em [job] A directory is created for the specified job
 * name. This will house all information relating to that specific
 * job, including directories for each process within that job on this
 * host.
 *
 * \par \em [process] A directory for the specific process, will house
 * all information for that process.
 *
 * \par If \c create is \c true, the directory will be created and the
 * proc_info structure will be updated.  If proc_info is false,
 */

#ifndef ORTE_SESSION_DIR_H_HAS_BEEN_INCLUDED
#define ORTE_SESSION_DIR_H_HAS_BEEN_INCLUDED

#include "orte_config.h"
#include "orte/types.h"

BEGIN_C_DECLS

/** @param create A boolean variable that indicates whether or not to
 *                create the specified directory. If set to "false",
 *                the function only checks to see if an existing
 *                directory can be found. This is typically used to
 *                locate an already existing universe for reconnection
 *                purposes. If set to "true", then the function
 *                creates the directory, if possible.
 * @param proc    Pointer to a process name for which the session
 *                dir name is desired
 *
 * @retval ORTE_SUCCESS The directory was found and/or created with
 *                the proper permissions.
 * @retval OMPI_ERROR The directory cannot be found (if create is
 *                "false") or created (if create is "true").
 */
ORTE_DECLSPEC int orte_session_dir(bool create, orte_process_name_t *proc);

/*
 * Setup session-related directory paths
 */
ORTE_DECLSPEC int orte_session_setup_base(orte_process_name_t *proc);

ORTE_DECLSPEC int orte_setup_top_session_dir(void);

/** The orte_session_dir_finalize() function performs a cleanup of the
 * session directory tree. It first removes the session directory for
 * the calling process. It then checks to see if the job-level session
 * directory is now empty - if so, it removes that level as
 * well. Finally, it checks to see if the universe-level session
 * directory is now empty - if so, it also removes that level. This
 * three-part "last-one-out" procedure ensures that the directory tree
 * is properly removed if all processes and applications within a
 * universe have completed.
 *
 * @param None
 * @retval ORTE_SUCCESS If the directory tree is properly cleaned up.
 * @retval OMPI_ERROR If something prevents the tree from being
 *                properly cleaned up.
 */
ORTE_DECLSPEC int orte_session_dir_finalize(orte_process_name_t *proc);

/** The orte_session_dir_cleanup() function performs a cleanup of the
 * session directory tree when a job is aborted. It cleans up all
 * process directories for a given job and then backs up the tree.
 *
 * @param jobid
 * @retval OMPI_SUCCESS If the directory tree is properly cleaned up.
 * @retval OMPI_ERROR If something prevents the tree from being
 *                properly cleaned up.
 */
ORTE_DECLSPEC int orte_session_dir_cleanup(orte_jobid_t jobid);

END_C_DECLS

#endif  /* ORTE_SESSION_DIR_H_HAS_BEEN_INCLUDED */
