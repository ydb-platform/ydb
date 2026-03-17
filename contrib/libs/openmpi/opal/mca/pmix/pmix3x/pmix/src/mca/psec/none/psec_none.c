/*
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <pmix_common.h>

#include "src/include/pmix_socket_errno.h"
#include "src/include/pmix_globals.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/output.h"

#include <unistd.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "src/mca/psec/psec.h"
#include "psec_none.h"

static pmix_status_t none_init(void);
static void none_finalize(void);
static pmix_status_t create_cred(struct pmix_peer_t *peer,
                                 const pmix_info_t directives[], size_t ndirs,
                                 pmix_info_t **info, size_t *ninfo,
                                 pmix_byte_object_t *cred);
static pmix_status_t validate_cred(struct pmix_peer_t *peer,
                                   const pmix_info_t directives[], size_t ndirs,
                                   pmix_info_t **info, size_t *ninfo,
                                   const pmix_byte_object_t *cred);

pmix_psec_module_t pmix_none_module = {
    .name = "none",
    .init = none_init,
    .finalize = none_finalize,
    .create_cred = create_cred,
    .validate_cred = validate_cred
};

static pmix_status_t none_init(void)
{
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "psec: none init");
    return PMIX_SUCCESS;
}

static void none_finalize(void)
{
    pmix_output_verbose(2, pmix_globals.debug_output,
                        "psec: none finalize");
}

static pmix_status_t create_cred(struct pmix_peer_t *peer,
                                 const pmix_info_t directives[], size_t ndirs,
                                 pmix_info_t **info, size_t *ninfo,
                                 pmix_byte_object_t *cred)
{
    /* ensure initialization */
    PMIX_BYTE_OBJECT_CONSTRUCT(cred);

    return PMIX_SUCCESS;
}

static pmix_status_t validate_cred(struct pmix_peer_t *peer,
                                   const pmix_info_t directives[], size_t ndirs,
                                   pmix_info_t **info, size_t *ninfo,
                                   const pmix_byte_object_t *cred)
{
    size_t n, m;
    char **types;
    bool takeus;

    pmix_output_verbose(2, pmix_globals.debug_output,
                        "psec: none always reports valid");

    /* if we are responding to a local request to validate a credential,
     * then see if they specified a mechanism */
    if (NULL != directives && 0 < ndirs) {
        for (n=0; n < ndirs; n++) {
            if (0 == strncmp(directives[n].key, PMIX_CRED_TYPE, PMIX_MAX_KEYLEN)) {
                /* split the specified string */
                types = pmix_argv_split(directives[n].value.data.string, ',');
                takeus = false;
                for (m=0; NULL != types[m]; m++) {
                    if (0 == strcmp(types[m], "none")) {
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

    /* mark that this came from us */
    if (NULL != info) {
        /* mark that this came from us */
        PMIX_INFO_CREATE(*info, 1);
        if (NULL == *info) {
            return PMIX_ERR_NOMEM;
        }
        *ninfo = 1;
        PMIX_INFO_LOAD(info[0], PMIX_CRED_TYPE, "none", PMIX_STRING);
    }
    return PMIX_SUCCESS;
}
