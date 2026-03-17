/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <unistd.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include <pmix_common.h>

#include "src/include/pmix_socket_errno.h"
#include "src/include/pmix_globals.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"

#include "src/mca/psec/psec.h"
#include "psec_native.h"

static pmix_status_t native_init(void);
static void native_finalize(void);
static pmix_status_t create_cred(struct pmix_peer_t *peer,
                                 const pmix_info_t directives[], size_t ndirs,
                                 pmix_info_t **info, size_t *ninfo,
                                 pmix_byte_object_t *cred);
static pmix_status_t validate_cred(struct pmix_peer_t *peer,
                                   const pmix_info_t directives[], size_t ndirs,
                                   pmix_info_t **info, size_t *ninfo,
                                   const pmix_byte_object_t *cred);

pmix_psec_module_t pmix_native_module = {
    .name = "native",
    .init = native_init,
    .finalize = native_finalize,
    .create_cred = create_cred,
    .validate_cred = validate_cred
};

static pmix_status_t native_init(void)
{
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "psec: native init");
    return PMIX_SUCCESS;
}

static void native_finalize(void)
{
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "psec: native finalize");
}

static pmix_status_t create_cred(struct pmix_peer_t *peer,
                                 const pmix_info_t directives[], size_t ndirs,
                                 pmix_info_t **info, size_t *ninfo,
                                 pmix_byte_object_t *cred)
{
    pmix_peer_t *pr = (pmix_peer_t*)peer;
    char **types;
    size_t n, m;
    bool takeus;
    uid_t euid;
    gid_t egid;
    char *tmp, *ptr;

    /* ensure initialization */
    PMIX_BYTE_OBJECT_CONSTRUCT(cred);

    /* we may be responding to a local request for a credential, so
     * see if they specified a mechanism */
    if (NULL != directives && 0 < ndirs) {
        /* cycle across the provided info and see if they specified
         * any desired credential types */
        takeus = true;
        for (n=0; n < ndirs; n++) {
            if (0 == strncmp(directives[n].key, PMIX_CRED_TYPE, PMIX_MAX_KEYLEN)) {
                /* see if we are included */
                types = pmix_argv_split(directives[n].value.data.string, ',');
                /* start by assuming they don't want us */
                takeus = false;
                for (m=0; NULL != types[m]; m++) {
                    if (0 == strcmp(types[m], "native")) {
                        /* it's us! */
                        takeus = true;
                        break;
                    }
                }
                pmix_argv_free(types);
                break;
            }
        }
        if (!takeus) {
            PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
            return PMIX_ERR_NOT_SUPPORTED;
        }
    }

    if (PMIX_PROTOCOL_V1 == pr->protocol) {
        /* usock protocol - nothing to do */
        goto complete;
    } else if (PMIX_PROTOCOL_V2 == pr->protocol) {
        /* tcp protocol - need to provide our effective
         * uid and gid for validation on remote end */
        tmp = (char*)malloc(sizeof(uid_t) + sizeof(gid_t));
        if (NULL == tmp) {
            return PMIX_ERR_NOMEM;
        }
        euid = geteuid();
        memcpy(tmp, &euid, sizeof(uid_t));
        ptr = tmp + sizeof(uid_t);
        egid = getegid();
        memcpy(ptr, &egid, sizeof(gid_t));
        cred->bytes = tmp;
        cred->size = sizeof(uid_t) + sizeof(gid_t);
        goto complete;
    } else {
        /* unrecognized protocol */
        PMIX_ERROR_LOG(PMIX_ERR_NOT_SUPPORTED);
        return PMIX_ERR_NOT_SUPPORTED;
    }

  complete:
    if (NULL != info) {
        /* mark that this came from us */
        PMIX_INFO_CREATE(*info, 1);
        if (NULL == *info) {
            return PMIX_ERR_NOMEM;
        }
        *ninfo = 1;
        PMIX_INFO_LOAD(info[0], PMIX_CRED_TYPE, "native", PMIX_STRING);
    }
    return PMIX_SUCCESS;
}

static pmix_status_t validate_cred(struct pmix_peer_t *peer,
                                   const pmix_info_t directives[], size_t ndirs,
                                   pmix_info_t **info, size_t *ninfo,
                                   const pmix_byte_object_t *cred)
{
    pmix_peer_t *pr = (pmix_peer_t*)peer;

#if defined(SO_PEERCRED)
#ifdef HAVE_STRUCT_SOCKPEERCRED_UID
#define HAVE_STRUCT_UCRED_UID
    struct sockpeercred ucred;
#else
    struct ucred ucred;
#endif
    socklen_t crlen = sizeof (ucred);
#endif
    uid_t euid = -1;
    gid_t egid = -1;
    char *ptr;
    size_t ln;
    bool takeus;
    char **types;
    size_t n, m;
    uint32_t u32;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "psec: native validate_cred %s",
                        (NULL == cred) ? "NULL" : "NON-NULL");

    if (PMIX_PROTOCOL_V1 == pr->protocol) {
        /* usock protocol - get the remote side's uid/gid */
#if defined(SO_PEERCRED) && (defined(HAVE_STRUCT_UCRED_UID) || defined(HAVE_STRUCT_UCRED_CR_UID))
        /* Ignore received 'cred' and validate ucred for socket instead. */
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "psec:native checking getsockopt on socket %d for peer credentials", pr->sd);
        if (getsockopt(pr->sd, SOL_SOCKET, SO_PEERCRED, &ucred, &crlen) < 0) {
            pmix_output_verbose(2, pmix_globals.debug_output,
                                "psec: getsockopt SO_PEERCRED failed: %s",
                                strerror (pmix_socket_errno));
            return PMIX_ERR_INVALID_CRED;
        }
#if defined(HAVE_STRUCT_UCRED_UID)
        euid = ucred.uid;
        egid = ucred.gid;
#else
        euid = ucred.cr_uid;
        egid = ucred.cr_gid;
#endif

#elif defined(HAVE_GETPEEREID)
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "psec:native checking getpeereid on socket %d for peer credentials", pr->sd);
        if (0 != getpeereid(pr->sd, &euid, &egid)) {
            pmix_output_verbose(2, pmix_globals.debug_output,
                                "psec: getsockopt getpeereid failed: %s",
                                strerror (pmix_socket_errno));
            return PMIX_ERR_INVALID_CRED;
    }
#else
        return PMIX_ERR_NOT_SUPPORTED;
#endif
    } else if (PMIX_PROTOCOL_V2 == pr->protocol) {
        /* this is a tcp protocol, so the cred is actually the uid/gid
         * passed upwards from the client */
        if (NULL == cred) {
            /* not allowed */
            return PMIX_ERR_INVALID_CRED;
        }
        ln = cred->size;
        euid = 0;
        egid = 0;
        if (sizeof(uid_t) <= ln) {
            memcpy(&euid, cred->bytes, sizeof(uid_t));
            ln -= sizeof(uid_t);
            ptr = cred->bytes + sizeof(uid_t);
        } else {
            return PMIX_ERR_INVALID_CRED;
        }
        if (sizeof(gid_t) <= ln) {
            memcpy(&egid, ptr, sizeof(gid_t));
        } else {
            return PMIX_ERR_INVALID_CRED;
        }
    } else if (PMIX_PROTOCOL_UNDEF != pr->protocol) {
        /* don't recognize the protocol */
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* if we are responding to a local request to validate a credential,
     * then see if they specified a mechanism */
    if (NULL != directives && 0 < ndirs) {
        for (n=0; n < ndirs; n++) {
            if (0 == strncmp(directives[n].key, PMIX_CRED_TYPE, PMIX_MAX_KEYLEN)) {
                /* split the specified string */
                types = pmix_argv_split(directives[n].value.data.string, ',');
                takeus = false;
                for (m=0; NULL != types[m]; m++) {
                    if (0 == strcmp(types[m], "native")) {
                        /* it's us! */
                        takeus = true;
                        break;
                    }
                }
                pmix_argv_free(types);
                if (!takeus) {
                    return PMIX_ERR_NOT_SUPPORTED;
                }
            }
        }
    }

    /* check uid */
    if (euid != pr->info->uid) {
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "psec: socket cred contains invalid uid %u", euid);
        return PMIX_ERR_INVALID_CRED;
    }

    /* check gid */
    if (egid != pr->info->gid) {
        pmix_output_verbose(2, pmix_globals.debug_output,
                            "psec: socket cred contains invalid gid %u", egid);
        return PMIX_ERR_INVALID_CRED;
    }

    /* validated - mark that we did it */
    if (NULL != info) {
        PMIX_INFO_CREATE(*info, 3);
        if (NULL == *info) {
            return PMIX_ERR_NOMEM;
        }
        *ninfo = 3;
        /* mark that this came from us */
        PMIX_INFO_LOAD(info[0], PMIX_CRED_TYPE, "munge", PMIX_STRING);
        /* provide the uid it contained */
        u32 = euid;
        PMIX_INFO_LOAD(info[1], PMIX_USERID, &u32, PMIX_UINT32);
        /* provide the gid it contained */
        u32 = egid;
        PMIX_INFO_LOAD(info[2], PMIX_GRPID, &u32, PMIX_UINT32);
    }
    return PMIX_SUCCESS;

}
