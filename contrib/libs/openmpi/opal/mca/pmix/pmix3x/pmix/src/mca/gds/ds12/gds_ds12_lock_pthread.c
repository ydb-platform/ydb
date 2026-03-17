/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2018 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include <pmix_common.h>

#include "src/mca/common/dstore/dstore_common.h"
#include "src/mca/gds/base/base.h"
#include "src/mca/pshmem/pshmem.h"

#include "src/util/error.h"
#include "src/util/output.h"

#include "gds_ds12_lock.h"
#include "src/mca/common/dstore/dstore_segment.h"

#define _ESH_12_PTHREAD_LOCK(rwlock, func)                  \
__pmix_attribute_extension__ ({                             \
    pmix_status_t ret = PMIX_SUCCESS;                       \
    int rc;                                                 \
    rc = pthread_rwlock_##func(rwlock);                     \
    if (0 != rc) {                                          \
        switch (errno) {                                    \
            case EINVAL:                                    \
                ret = PMIX_ERR_INIT;                        \
                break;                                      \
            case EPERM:                                     \
                ret = PMIX_ERR_NO_PERMISSIONS;              \
                break;                                      \
        }                                                   \
    }                                                       \
    if (ret) {                                              \
        pmix_output(0, "%s %d:%s lock failed: %s",          \
            __FILE__, __LINE__, __func__, strerror(errno)); \
    }                                                       \
    ret;                                                    \
})

typedef struct {
    char *lockfile;
    pmix_pshmem_seg_t *segment;
    pthread_rwlock_t *rwlock;
} ds12_lock_pthread_ctx_t;

pmix_status_t pmix_gds_ds12_lock_init(pmix_common_dstor_lock_ctx_t *ctx, const char *base_path,
                                      const char * name, uint32_t local_size, uid_t uid, bool setuid)
{
    size_t size = pmix_common_dstor_getpagesize();
    pmix_status_t rc = PMIX_SUCCESS;
    pthread_rwlockattr_t attr;
    ds12_lock_pthread_ctx_t *lock_ctx = (ds12_lock_pthread_ctx_t*)ctx;

    if (*ctx != NULL) {
        return PMIX_SUCCESS;
    }

    lock_ctx = (ds12_lock_pthread_ctx_t*)malloc(sizeof(ds12_lock_pthread_ctx_t));
    if (NULL == lock_ctx) {
        rc = PMIX_ERR_INIT;
        PMIX_ERROR_LOG(rc);
        goto error;
    }
    memset(lock_ctx, 0, sizeof(ds12_lock_pthread_ctx_t));
    *ctx = (pmix_common_dstor_lock_ctx_t*)lock_ctx;

    lock_ctx->segment = (pmix_pshmem_seg_t *)malloc(sizeof(pmix_pshmem_seg_t));
    if (NULL == lock_ctx->segment) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto error;
    }

    /* create a lock file to prevent clients from reading while server is writing
     * to the shared memory. This situation is quite often, especially in case of
     * direct modex when clients might ask for data simultaneously. */
    if(0 > asprintf(&lock_ctx->lockfile, "%s/dstore_sm.lock", base_path)) {
        rc = PMIX_ERR_OUT_OF_RESOURCE;
        PMIX_ERROR_LOG(rc);
        goto error;
    }
    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
        "%s:%d:%s _lockfile_name: %s", __FILE__, __LINE__, __func__, lock_ctx->lockfile));

    if (PMIX_PROC_IS_SERVER(pmix_globals.mypeer)) {
        if (PMIX_SUCCESS != (rc = pmix_pshmem.segment_create(lock_ctx->segment,
                                            lock_ctx->lockfile, size))) {
            PMIX_ERROR_LOG(rc);
            goto error;
        }
        memset(lock_ctx->segment->seg_base_addr, 0, size);
        if (0 != setuid) {
            if (0 > chown(lock_ctx->lockfile, (uid_t) uid, (gid_t) -1)){
                rc = PMIX_ERROR;
                PMIX_ERROR_LOG(rc);
                goto error;
            }
            /* set the mode as required */
            if (0 > chmod(lock_ctx->lockfile, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP )) {
                rc = PMIX_ERROR;
                PMIX_ERROR_LOG(rc);
                goto error;
            }
        }
        lock_ctx->rwlock = (pthread_rwlock_t *)lock_ctx->segment->seg_base_addr;

        if (0 != pthread_rwlockattr_init(&attr)) {
            rc = PMIX_ERROR;
            PMIX_ERROR_LOG(rc);
            goto error;
        }
        if (0 != pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) {
            pthread_rwlockattr_destroy(&attr);
            rc = PMIX_ERR_INIT;
            PMIX_ERROR_LOG(rc);
            goto error;
        }
#ifdef HAVE_PTHREAD_SETKIND
        if (0 != pthread_rwlockattr_setkind_np(&attr,
                                PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) {
            pthread_rwlockattr_destroy(&attr);
            PMIX_ERROR_LOG(PMIX_ERR_INIT);
            goto error;
        }
#endif
        if (0 != pthread_rwlock_init(lock_ctx->rwlock, &attr)) {
            pthread_rwlockattr_destroy(&attr);
            PMIX_ERROR_LOG(PMIX_ERR_INIT);
            goto error;
        }
        if (0 != pthread_rwlockattr_destroy(&attr)) {
            PMIX_ERROR_LOG(PMIX_ERR_INIT);
            goto error;
        }

    }
    else {
        lock_ctx->segment->seg_size = size;
        snprintf(lock_ctx->segment->seg_name, PMIX_PATH_MAX, "%s", lock_ctx->lockfile);
        if (PMIX_SUCCESS != (rc = pmix_pshmem.segment_attach(lock_ctx->segment,
                                                             PMIX_PSHMEM_RW))) {
            PMIX_ERROR_LOG(rc);
            goto error;
        }
        lock_ctx->rwlock = (pthread_rwlock_t *)lock_ctx->segment->seg_base_addr;
    }

    return PMIX_SUCCESS;

error:
    if (NULL != lock_ctx) {
        if (lock_ctx->segment) {
            /* detach & unlink from current desc */
            if (lock_ctx->segment->seg_cpid == getpid()) {
                pmix_pshmem.segment_unlink(lock_ctx->segment);
            }
            pmix_pshmem.segment_detach(lock_ctx->segment);
            lock_ctx->rwlock = NULL;
        }
        if (NULL != lock_ctx->lockfile) {
            free(lock_ctx->lockfile);
        }
        free(lock_ctx);
        *ctx = (pmix_common_dstor_lock_ctx_t*)NULL;
    }

    return rc;
}

void pmix_ds12_lock_finalize(pmix_common_dstor_lock_ctx_t *lock_ctx)
{
    ds12_lock_pthread_ctx_t *pthread_lock =
            (ds12_lock_pthread_ctx_t*)*lock_ctx;

    if (NULL == pthread_lock) {
        PMIX_ERROR_LOG(PMIX_ERR_NOT_FOUND);
        return;
    }
    if (0 != pthread_rwlock_destroy(pthread_lock->rwlock)) {
        PMIX_ERROR_LOG(PMIX_ERROR);
        return;
    }

    if (NULL == pthread_lock->segment) {
        PMIX_ERROR_LOG(PMIX_ERROR);
        return;
    }
    if (NULL == pthread_lock->lockfile) {
        PMIX_ERROR_LOG(PMIX_ERROR);
        return;
    }

    /* detach & unlink from current desc */
    if (pthread_lock->segment->seg_cpid == getpid()) {
        pmix_pshmem.segment_unlink(pthread_lock->segment);
    }
    pmix_pshmem.segment_detach(pthread_lock->segment);

    free(pthread_lock->segment);
    pthread_lock->segment = NULL;
    free(pthread_lock->lockfile);
    pthread_lock->lockfile = NULL;
    pthread_lock->rwlock = NULL;
    free(pthread_lock);
    *lock_ctx = NULL;
}

pmix_status_t pmix_ds12_lock_rd_get(pmix_common_dstor_lock_ctx_t lock_ctx)
{
    ds12_lock_pthread_ctx_t *pthread_lock = (ds12_lock_pthread_ctx_t*)lock_ctx;
    pmix_status_t rc;

    if (NULL == pthread_lock) {
        rc = PMIX_ERR_NOT_FOUND;
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    rc = _ESH_12_PTHREAD_LOCK(pthread_lock->rwlock, rdlock);

    return rc;
}

pmix_status_t pmix_ds12_lock_wr_get(pmix_common_dstor_lock_ctx_t lock_ctx)
{
    ds12_lock_pthread_ctx_t *pthread_lock = (ds12_lock_pthread_ctx_t*)lock_ctx;
    pmix_status_t rc;

    if (NULL == pthread_lock) {
        rc = PMIX_ERR_NOT_FOUND;
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    rc = _ESH_12_PTHREAD_LOCK(pthread_lock->rwlock, wrlock);

    return rc;
}

pmix_status_t pmix_ds12_lock_rw_rel(pmix_common_dstor_lock_ctx_t lock_ctx)
{
    ds12_lock_pthread_ctx_t *pthread_lock = (ds12_lock_pthread_ctx_t*)lock_ctx;
    pmix_status_t rc;

    if (NULL == pthread_lock) {
        rc = PMIX_ERR_NOT_FOUND;
        PMIX_ERROR_LOG(rc);
        return rc;
    }
    rc = _ESH_12_PTHREAD_LOCK(pthread_lock->rwlock, unlock);

    return rc;
}
