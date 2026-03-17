/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_FEW_H
#define OPAL_FEW_H

#include "opal_config.h"

BEGIN_C_DECLS

/**
 *  Forks, execs, and waits for a subordinate program
 *
 * @param argv Null-terminated argument vector; argv[0] is the program
 * (same as arguments to execvp())
 *
 * @param status Upon success, will be filled with the return status
 * from waitpid(2).  The WIF* macros can be used to examine the value
 * (see waitpid(2)).
 *
 * @retval OPAL_SUCCESS If the child launched and exited.
 * @retval OPAL_ERR_IN_ERRNO If a failure occurred, errno should be
 * examined for the specific error.
 *
 * This function forks, execs, and waits for an executable to
 * complete.  The input argv must be a NULL-terminated array (perhaps
 * built with the opal_arr_*() interface).  Upon success, OPAL_SUCCESS
 * is returned.  This function will wait either until the child
 * process has exited or waitpid() returns an error other than EINTR.
 *
 * Note that a return of OPAL_SUCCESS does \em not imply that the child
 * process exited successfully -- it simply indicates that the child
 * process exited.  The WIF* macros (see waitpid(2)) should be used to
 * examine the status to see hold the child exited.
 *
 * \warning This function should not be called if \c orte_init()
 *          or \c MPI_Init() have been called.  This function is not
 *          safe in a multi-threaded environment in which a handler
 *          for \c SIGCHLD has been registered.
 */
OPAL_DECLSPEC int opal_few(char *argv[], int *status);

END_C_DECLS
#endif /* OPAL_FEW_H */
