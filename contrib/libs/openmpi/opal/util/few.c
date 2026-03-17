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
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "opal_config.h"

#include <stdio.h>
#include <errno.h>
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/util/few.h"
#include "opal/util/basename.h"
#include "opal/util/argv.h"
#include "opal/constants.h"

int opal_few(char *argv[], int *status)
{
#if defined(HAVE_FORK) && defined(HAVE_EXECVE) && defined(HAVE_WAITPID)
    pid_t pid, ret;

    if ((pid = fork()) < 0) {
      return OPAL_ERR_IN_ERRNO;
    }

    /* Child execs.  If it fails to exec, exit. */

    else if (0 == pid) {
      execvp(argv[0], argv);
      exit(errno);
    }

    /* Parent loops waiting for the child to die. */

    else {
      do {
        /* If the child exited, return */

        if (pid == (ret = waitpid(pid, status, 0))) {
          break;
        }

        /* If waitpid was interrupted, loop around again */

        else if (ret < 0) {
          if (EINTR == errno) {
            continue;
          }

          /* Otherwise, some bad juju happened -- need to quit */

          return OPAL_ERR_IN_ERRNO;
        }
      } while (true);
    }

    /* Return the status to the caller */

    return OPAL_SUCCESS;
#else
    return OPAL_ERR_NOT_SUPPORTED;
#endif
}
