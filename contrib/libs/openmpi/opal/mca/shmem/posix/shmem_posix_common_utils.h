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
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * shmem (shared memory backing facility) framework utilities
 */

#ifndef OPAL_SHMEM_POSIX_COMMON_UTILS_H
#define OPAL_SHMEM_POSIX_COMMON_UTILS_H

BEGIN_C_DECLS

#include "opal_config.h"

/**
 * this routine searches for an available shm_open file name.
 *
 * @return if successful, a non-negative file descriptor is returned and
 * posix_file_name_buff will contain the file name associated with the
 * successful shm_open.  otherwise, -1 is returned and the contents of
 * posix_file_name_buff are undefined.
 */
OPAL_DECLSPEC extern int shmem_posix_shm_open(char *posix_file_name_buff,
                                              size_t size);

END_C_DECLS

#endif /* OPAL_SHMEM_POSIX_COMMON_UTILS_H */
