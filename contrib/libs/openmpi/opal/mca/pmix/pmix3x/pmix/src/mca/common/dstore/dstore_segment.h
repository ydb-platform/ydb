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
#include <pmix_common.h>

#include "src/include/pmix_globals.h"
#include "src/mca/gds/base/base.h"
#include "src/mca/pshmem/base/base.h"

#include "dstore_common.h"

#ifndef DSTORE_SEGMENT_H
#define DSTORE_SEGMENT_H

/* this structs are used to store information about
 * shared segments addresses locally at each process,
 * so they are common for different types of segments
 * and don't have a specific content (namespace's info,
 * rank's meta info, ranks's data). */

typedef struct pmix_dstore_seg_desc_t pmix_dstore_seg_desc_t;

typedef enum {
    PMIX_DSTORE_INITIAL_SEGMENT,
    PMIX_DSTORE_NS_META_SEGMENT,
    PMIX_DSTORE_NS_DATA_SEGMENT,
    PMIX_DSTORE_NS_LOCK_SEGMENT,
} pmix_dstore_segment_type;

struct pmix_dstore_seg_desc_t {
    pmix_dstore_segment_type type;
    pmix_pshmem_seg_t seg_info;
    uint32_t id;
    pmix_dstore_seg_desc_t *next;
};

PMIX_EXPORT int pmix_common_dstor_getpagesize(void);
PMIX_EXPORT size_t pmix_common_dstor_getcacheblocksize(void);
PMIX_EXPORT void pmix_common_dstor_init_segment_info(size_t initial_segment_size,
                        size_t meta_segment_size,
                        size_t data_segment_size);
PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_create_new_segment(pmix_dstore_segment_type type,
                        const char *base_path, const char *name, uint32_t id,
                        uid_t uid, bool setuid);
PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_attach_new_segment(pmix_dstore_segment_type type,
                        const char *base_path,
                        const char *name, uint32_t id);
PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_extend_segment(pmix_dstore_seg_desc_t *segdesc,
                        const char *base_path,
                        const char *name, uid_t uid, bool setuid);
PMIX_EXPORT void pmix_common_dstor_delete_sm_desc(pmix_dstore_seg_desc_t *desc);
PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_create_new_lock_seg(const char *base_path, size_t size,
                        const char *name, uint32_t id, uid_t uid, bool setuid);
PMIX_EXPORT pmix_dstore_seg_desc_t *pmix_common_dstor_attach_new_lock_seg(const char *base_path,
                        size_t size, const char *name, uint32_t id);

#endif // DSTORE_SEGMENT_H
