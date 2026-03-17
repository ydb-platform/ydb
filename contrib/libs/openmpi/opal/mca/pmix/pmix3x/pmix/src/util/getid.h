/*
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_GETID_H
#define PMIX_GETID_H

#include <src/include/pmix_config.h>
#include "include/pmix_common.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

BEGIN_C_DECLS

/* lookup the effective uid and gid of a socket */
PMIX_EXPORT pmix_status_t pmix_util_getid(int sd, uid_t *uid, gid_t *gid);

END_C_DECLS

#endif /* PMIX_PRINTF_H */
