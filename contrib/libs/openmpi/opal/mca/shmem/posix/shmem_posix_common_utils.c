/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <errno.h>
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif  /* HAVE_FCNTL_H */
#include <string.h>
#if OPAL_HAVE_SOLARIS && !defined(_POSIX_C_SOURCE)
  #define _POSIX_C_SOURCE 200112L /* Required for shm_{open,unlink} decls */
  #include <sys/mman.h>
  #undef _POSIX_C_SOURCE
#else
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif /* HAVE_SYS_MMAN_H */
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNISTD_H */
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif /* HAVE_SYS_TYPES_H */
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif /* HAVE_NETDB_H */

#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/mca/shmem/base/base.h"
#include "opal/mca/shmem/shmem.h"

#include "shmem_posix.h"
#include "shmem_posix_common_utils.h"

/* ////////////////////////////////////////////////////////////////////////// */
int
shmem_posix_shm_open(char *posix_file_name_buff, size_t size)
{
    int attempt = 0, fd = -1;

    /* workaround for simultaneous posix shm_opens on the same node (e.g.
     * multiple Open MPI jobs sharing a node).  name collision during component
     * runtime will happen, so protect against it by trying a few times.
     */
    do {
        /* format: /open_mpi.nnnn
         * see comment in shmem_posix.h that explains why we chose to do things
         * this way.
         */
        snprintf(posix_file_name_buff, size, "%s%04d",
                 OPAL_SHMEM_POSIX_FILE_NAME_PREFIX, attempt++);
        /* the check for the existence of the object and its creation if it
         * does not exist are performed atomically.
         */
        if (-1 == (fd = shm_open(posix_file_name_buff,
                                 O_CREAT | O_EXCL | O_RDWR, 0600))) {
            int err = errno;
            /* the object already exists, so try again with a new name */
            if (EEXIST == err) {
                continue;
            }
            /* a "real" error occurred. fd is already set to -1, so get out
             * of here. we can't be selected :-(.
             */
            else {
                char hn[OPAL_MAXHOSTNAMELEN];
                gethostname(hn, sizeof(hn));
                opal_output_verbose(10, opal_shmem_base_framework.framework_output,
                     "shmem_posix_shm_open: disqualifying posix because "
                     "shm_open(2) failed with error: %s (errno %d)\n",
                     strerror(err), err);
                break;
            }
        }
        /* we found an available file name */
        else {
            break;
        }
    } while (attempt < OPAL_SHMEM_POSIX_MAX_ATTEMPTS);

    /* if we didn't find a name, let the user know that we tried and failed */
    if (attempt >= OPAL_SHMEM_POSIX_MAX_ATTEMPTS) {
        opal_output(0, "shmem: posix: file name search - max attempts exceeded."
                    "cannot continue with posix.\n");
    }
    return fd;
}

