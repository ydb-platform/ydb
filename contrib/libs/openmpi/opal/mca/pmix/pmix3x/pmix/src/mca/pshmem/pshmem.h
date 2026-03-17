/*
 * Copyright (c) 2015-2016 Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PMIX_PSHMEM_H
#define PMIX_PSHMEM_H

#include <src/include/pmix_config.h>

#include <pmix_common.h>
#include "src/mca/mca.h"
#include "src/mca/base/pmix_mca_base_var.h"
#include "src/mca/base/pmix_mca_base_framework.h"


BEGIN_C_DECLS

#if !defined(MAP_FAILED)
#    define MAP_FAILED ((char*)-1)
#endif /* MAP_FAILED */

#define PMIX_SHMEM_DS_ID_INVALID -1

typedef enum {
    PMIX_PSHMEM_RONLY,
    PMIX_PSHMEM_RW
} pmix_pshmem_access_mode_t;

typedef struct pmix_pshmem_seg_t {
    /* pid of the shared memory segment creator */
    pid_t seg_cpid;
    /* ds id */
    int seg_id;
    /* size of shared memory segment */
    size_t seg_size;
    /* base address of shared memory segment */
    unsigned char *seg_base_addr;
    char seg_name[PMIX_PATH_MAX];
} pmix_pshmem_seg_t;


static inline void _segment_ds_reset(pmix_pshmem_seg_t *sm_seg)
{
    sm_seg->seg_cpid = 0;
    sm_seg->seg_id = PMIX_SHMEM_DS_ID_INVALID;
    sm_seg->seg_size = 0;
    memset(sm_seg->seg_name, '\0', PMIX_PATH_MAX);
    sm_seg->seg_base_addr = (unsigned char *)MAP_FAILED;
}

/* initialize the module */
typedef pmix_status_t (*pmix_pshmem_base_module_init_fn_t)(void);

/* finalize the module */
typedef void (*pmix_pshmem_base_module_finalize_fn_t)(void);

/**
* create a new shared memory segment and initialize members in structure
* pointed to by sm_seg.
*
* @param sm_seg   pointer to pmix_pshmem_seg_t structure
*
* @param file_name unique string identifier that must be a valid,
*                 writable path (IN).
*
* @param size     size of the shared memory segment.
*
* @return PMIX_SUCCESS on success.
*/
typedef int (*pmix_pshmem_base_module_segment_create_fn_t)(pmix_pshmem_seg_t *sm_seg,
                                                           const char *file_name, size_t size);

/**
* attach to an existing shared memory segment initialized by segment_create.
*
* @param sm_seg  pointer to initialized pmix_pshmem_seg_t typedef'd
*                structure (IN/OUT).
*
* @return        base address of shared memory segment on success. returns
*                NULL otherwise.
*/
typedef int (*pmix_pshmem_base_module_segment_attach_fn_t)(pmix_pshmem_seg_t *sm_seg,
                                                           pmix_pshmem_access_mode_t sm_mode);

/**
* detach from an existing shared memory segment.
*
* @param sm_seg  pointer to initialized pmix_pshmem_seg_t typedef'd structure
*                (IN/OUT).
*
* @return PMIX_SUCCESS on success.
*/
typedef int (*pmix_pshmem_base_module_segment_detach_fn_t)(pmix_pshmem_seg_t *sm_seg);

/**
* unlink an existing shared memory segment.
*
* @param sm_seg  pointer to initialized pmix_pshmem_seg_t typedef'd structure
*                (IN/OUT).
*
* @return PMIX_SUCCESS on success.
*/
typedef int (*pmix_pshmem_base_module_unlink_fn_t)(pmix_pshmem_seg_t *sm_seg);


/**
* structure for sm modules
*/
typedef struct {
    const char *name;
    pmix_pshmem_base_module_init_fn_t            init;
    pmix_pshmem_base_module_finalize_fn_t        finalize;
    pmix_pshmem_base_module_segment_create_fn_t  segment_create;
    pmix_pshmem_base_module_segment_attach_fn_t  segment_attach;
    pmix_pshmem_base_module_segment_detach_fn_t  segment_detach;
    pmix_pshmem_base_module_unlink_fn_t          segment_unlink;
} pmix_pshmem_base_module_t;

/* define the component structure */
struct pmix_pshmem_base_component_t {
    pmix_mca_base_component_t                       base;
    pmix_mca_base_component_data_t                  data;
    int                                             priority;
};

typedef struct pmix_pshmem_base_component_t pmix_pshmem_base_component_t;

PMIX_EXPORT extern pmix_pshmem_base_module_t pmix_pshmem;

/*
 * Macro for use in components that are of type gds
 */
#define PMIX_PSHMEM_BASE_VERSION_1_0_0 \
    PMIX_MCA_BASE_VERSION_1_0_0("pshmem", 1, 0, 0)

END_C_DECLS

#endif /* PMIX_PSHMEM_H */
