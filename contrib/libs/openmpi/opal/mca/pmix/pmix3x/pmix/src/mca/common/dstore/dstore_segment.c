/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2017 Mellanox Technologies, Inc.
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

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#ifdef HAVE_SYS_AUXV_H
#include <sys/auxv.h>
#endif

#include <pmix_common.h>

#include "src/include/pmix_globals.h"
#include "src/mca/gds/base/base.h"
#include "src/mca/pshmem/base/base.h"
#include "src/util/error.h"
#include "src/util/output.h"

#include "dstore_common.h"
#include "dstore_segment.h"

static size_t _initial_segment_size;
static size_t _meta_segment_size;
static size_t _data_segment_size;

PMIX_EXPORT int pmix_common_dstor_getpagesize(void)
{
#if defined(_SC_PAGESIZE )
    return sysconf(_SC_PAGESIZE);
#elif defined(_SC_PAGE_SIZE)
    return sysconf(_SC_PAGE_SIZE);
#else
    return 65536; /* safer to overestimate than under */
#endif
}

PMIX_EXPORT size_t pmix_common_dstor_getcacheblocksize(void)
{
    size_t cache_line = 0;

#if defined(_SC_LEVEL1_DCACHE_LINESIZE)
    cache_line = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
#endif
#if (defined(HAVE_SYS_AUXV_H)) && (defined(AT_DCACHEBSIZE))
    if (0 == cache_line) {
        cache_line = getauxval(AT_DCACHEBSIZE);
    }
#endif
    return cache_line;
}

PMIX_EXPORT void pmix_common_dstor_init_segment_info(size_t initial_segment_size,
                                    size_t meta_segment_size,
                                    size_t data_segment_size)
{
    _initial_segment_size = initial_segment_size;
    _meta_segment_size = meta_segment_size;
    _data_segment_size = data_segment_size;
}

PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_create_new_lock_seg(const char *base_path, size_t size,
                                                 const char *name, uint32_t id, uid_t uid, bool setuid)
{
    pmix_status_t rc;
    char file_name[PMIX_PATH_MAX];
    pmix_dstore_seg_desc_t *new_seg = NULL;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: segment type %d, nspace %s, id %u",
                         __FILE__, __LINE__, __func__, PMIX_DSTORE_NS_LOCK_SEGMENT,
                         name, id));

    snprintf(file_name, PMIX_PATH_MAX, "%s/smlockseg-%s", base_path, name);
    new_seg = (pmix_dstore_seg_desc_t*)malloc(sizeof(pmix_dstore_seg_desc_t));
    if (new_seg) {
            new_seg->id = id;
            new_seg->next = NULL;
            new_seg->type = PMIX_DSTORE_NS_LOCK_SEGMENT;
            rc = pmix_pshmem.segment_create(&new_seg->seg_info, file_name, size);
            if (PMIX_SUCCESS != rc) {
                PMIX_ERROR_LOG(rc);
                goto err_exit;
            }
            memset(new_seg->seg_info.seg_base_addr, 0, size);

            if (setuid > 0){
                rc = PMIX_ERR_PERM;
                if (0 > chown(file_name, (uid_t) uid, (gid_t) -1)){
                    PMIX_ERROR_LOG(rc);
                    goto err_exit;
                }
                /* set the mode as required */
                if (0 > chmod(file_name, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP )) {
                    PMIX_ERROR_LOG(rc);
                    goto err_exit;
                }
            }
        }
        return new_seg;

    err_exit:
        if( NULL != new_seg ){
            free(new_seg);
        }
        return NULL;

}

PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_attach_new_lock_seg(const char *base_path,
                                                 size_t size, const char *name, uint32_t id)
{
    pmix_status_t rc;
    pmix_dstore_seg_desc_t *new_seg = NULL;
    new_seg = (pmix_dstore_seg_desc_t*)malloc(sizeof(pmix_dstore_seg_desc_t));
    new_seg->id = id;
    new_seg->next = NULL;
    new_seg->type = PMIX_DSTORE_NS_LOCK_SEGMENT;
    new_seg->seg_info.seg_size = size;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: segment type %d, name %s, id %u",
                         __FILE__, __LINE__, __func__, new_seg->type, name, id));

    snprintf(new_seg->seg_info.seg_name, PMIX_PATH_MAX, "%s/smlockseg-%s",
             base_path, name);
    rc = pmix_pshmem.segment_attach(&new_seg->seg_info, PMIX_PSHMEM_RW);
    if (PMIX_SUCCESS != rc) {
        free(new_seg);
        new_seg = NULL;
    }
    return new_seg;
}

PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_create_new_segment(pmix_dstore_segment_type type,
                        const char *base_path, const char *name, uint32_t id,
                        uid_t uid, bool setuid)
{
    pmix_status_t rc;
    char file_name[PMIX_PATH_MAX];
    size_t size;
    pmix_dstore_seg_desc_t *new_seg = NULL;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: segment type %d, nspace %s, id %u",
                         __FILE__, __LINE__, __func__, type, name, id));

    switch (type) {
        case PMIX_DSTORE_INITIAL_SEGMENT:
            size = _initial_segment_size;
            snprintf(file_name, PMIX_PATH_MAX, "%s/initial-pmix_shared-segment-%u",
                base_path, id);
            break;
        case PMIX_DSTORE_NS_META_SEGMENT:
            size = _meta_segment_size;
            snprintf(file_name, PMIX_PATH_MAX, "%s/smseg-%s-%u", base_path, name, id);
            break;
        case PMIX_DSTORE_NS_DATA_SEGMENT:
            size = _data_segment_size;
            snprintf(file_name, PMIX_PATH_MAX, "%s/smdataseg-%s-%d", base_path, name, id);
            break;
        default:
            PMIX_ERROR_LOG(PMIX_ERROR);
            return NULL;
    }
    new_seg = (pmix_dstore_seg_desc_t*)malloc(sizeof(pmix_dstore_seg_desc_t));
    if (new_seg) {
        new_seg->id = id;
        new_seg->next = NULL;
        new_seg->type = type;
        rc = pmix_pshmem.segment_create(&new_seg->seg_info, file_name, size);
        if (PMIX_SUCCESS != rc) {
            PMIX_ERROR_LOG(rc);
            goto err_exit;
        }
        memset(new_seg->seg_info.seg_base_addr, 0, size);

        if (setuid > 0){
            rc = PMIX_ERR_PERM;
            if (0 > chown(file_name, (uid_t) uid, (gid_t) -1)){
                PMIX_ERROR_LOG(rc);
                goto err_exit;
            }
            /* set the mode as required */
            if (0 > chmod(file_name, S_IRUSR | S_IRGRP | S_IWGRP )) {
                PMIX_ERROR_LOG(rc);
                goto err_exit;
            }
        }
    }
    return new_seg;

err_exit:
    if( NULL != new_seg ){
        free(new_seg);
    }
    return NULL;
}

PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_attach_new_segment(pmix_dstore_segment_type type, const char *base_path,
                                                 const char *name, uint32_t id)
{
    pmix_status_t rc;
    pmix_dstore_seg_desc_t *new_seg = NULL;
    new_seg = (pmix_dstore_seg_desc_t*)malloc(sizeof(pmix_dstore_seg_desc_t));
    new_seg->id = id;
    new_seg->next = NULL;
    new_seg->type = type;

    PMIX_OUTPUT_VERBOSE((10, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s: segment type %d, nspace %s, id %u",
                         __FILE__, __LINE__, __func__, type, name, id));

    switch (type) {
        case PMIX_DSTORE_INITIAL_SEGMENT:
            new_seg->seg_info.seg_size = _initial_segment_size;
            snprintf(new_seg->seg_info.seg_name, PMIX_PATH_MAX, "%s/initial-pmix_shared-segment-%u",
                base_path, id);
            break;
        case PMIX_DSTORE_NS_META_SEGMENT:
            new_seg->seg_info.seg_size = _meta_segment_size;
            snprintf(new_seg->seg_info.seg_name, PMIX_PATH_MAX, "%s/smseg-%s-%u",
                base_path, name, id);
            break;
        case PMIX_DSTORE_NS_DATA_SEGMENT:
            new_seg->seg_info.seg_size = _data_segment_size;
            snprintf(new_seg->seg_info.seg_name, PMIX_PATH_MAX, "%s/smdataseg-%s-%d",
                base_path, name, id);
            break;
        default:
            free(new_seg);
            PMIX_ERROR_LOG(PMIX_ERROR);
            return NULL;
    }
    rc = pmix_pshmem.segment_attach(&new_seg->seg_info, PMIX_PSHMEM_RONLY);
    if (PMIX_SUCCESS != rc) {
        free(new_seg);
        new_seg = NULL;
        PMIX_ERROR_LOG(rc);
    }
    return new_seg;
}

PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_extend_segment(pmix_dstore_seg_desc_t *segdesc, const char *base_path,
                                             const char *name, uid_t uid, bool setuid)
{
    pmix_dstore_seg_desc_t *tmp, *seg;

    PMIX_OUTPUT_VERBOSE((2, pmix_gds_base_framework.framework_output,
                         "%s:%d:%s",
                         __FILE__, __LINE__, __func__));
    /* find last segment */
    tmp = segdesc;
    while (NULL != tmp->next) {
        tmp = tmp->next;
    }
    /* create another segment, the old one is full. */
    seg = pmix_common_dstor_create_new_segment(segdesc->type, base_path, name, tmp->id + 1, uid, setuid);
    tmp->next = seg;

    return seg;
}

PMIX_EXPORT void pmix_common_dstor_delete_sm_desc(pmix_dstore_seg_desc_t *desc)
{
    pmix_dstore_seg_desc_t *tmp;

    /* free all global segments */
    while (NULL != desc) {
        tmp = desc->next;
        /* detach & unlink from current desc */
        if (desc->seg_info.seg_cpid == getpid()) {
            pmix_pshmem.segment_unlink(&desc->seg_info);
        }
        pmix_pshmem.segment_detach(&desc->seg_info);
        free(desc);
        desc = tmp;
    }
}
