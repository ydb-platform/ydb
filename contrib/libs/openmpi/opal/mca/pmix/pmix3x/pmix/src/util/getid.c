/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/*
 * Buffer safe printf functions for portability to archaic platforms.
 */

#include <src/include/pmix_config.h>
#include "include/pmix_common.h"
#include "src/include/pmix_socket_errno.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "src/include/pmix_globals.h"
#include "src/util/error.h"
#include "src/util/output.h"

#include "src/util/getid.h"

pmix_status_t pmix_util_getid(int sd, uid_t *uid, gid_t *gid)
{
#if defined(SO_PEERCRED)
#ifdef HAVE_STRUCT_SOCKPEERCRED_UID
#define HAVE_STRUCT_UCRED_UID
    struct sockpeercred ucred;
#else
    struct ucred ucred;
#endif
    socklen_t crlen = sizeof (ucred);
#endif

#if defined(SO_PEERCRED) && (defined(HAVE_STRUCT_UCRED_UID) || defined(HAVE_STRUCT_UCRED_CR_UID))
    /* Ignore received 'cred' and validate ucred for socket instead. */
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "getid: checking getsockopt for peer credentials");
    if (getsockopt(sd, SOL_SOCKET, SO_PEERCRED, &ucred, &crlen) < 0) {
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "getid: getsockopt SO_PEERCRED failed: %s",
                            strerror (pmix_socket_errno));
        return PMIX_ERR_INVALID_CRED;
    }
#if defined(HAVE_STRUCT_UCRED_UID)
    *uid = ucred.uid;
    *gid = ucred.gid;
#else
    *uid = ucred.cr_uid;
    *gid = ucred.cr_gid;
#endif

#elif defined(HAVE_GETPEEREID)
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "getid: checking getpeereid for peer credentials");
    if (0 != getpeereid(sd, uid, gid)) {
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "getid: getsockopt getpeereid failed: %s",
                            strerror (pmix_socket_errno));
        return PMIX_ERR_INVALID_CRED;
    }
#else
    return PMIX_ERR_NOT_SUPPORTED;
#endif

    return PMIX_SUCCESS;
}
