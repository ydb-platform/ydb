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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file **/

#ifndef OPAL_DAEMON_INIT_H
#define OPAL_DAEMON_INIT_H

#include "opal_config.h"

BEGIN_C_DECLS

/*
 * Turn a process into a daemon.
 *
 * This function converts a process into a daemon in an orderly manner. It first forks a child process,
 * then the parent exits. The child continues on to become a session leader, reset the file mode creation
 * mask, and changes working directories to the one specified.
 *
 * @param working_dir Pointer to a character string containing the desired working directory. Providing
 * a value of NULL will cause the function to leave the program in the current working directory.
 *
 * @retval OPAL_SUCCESS Indicates that the conversion was successful
 * @retval OPAL_ERROR Indicates that the conversion was not successful - a fork could not be completed.
 */
OPAL_DECLSPEC int opal_daemon_init(char *working_dir);

END_C_DECLS

#endif /* OPAL_DAEMON_INIT_H */
