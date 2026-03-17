/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reseved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/mca/allocator/allocator.h"
#include "opal/constants.h"
#include "opal/mca/allocator/bucket/allocator_bucket_alloc.h"
#include "opal/mca/base/mca_base_var.h"

struct mca_allocator_base_module_t* mca_allocator_bucket_module_init(
    bool enable_mpi_threads,
    mca_allocator_base_component_segment_alloc_fn_t segment_alloc,
    mca_allocator_base_component_segment_free_fn_t segment_free,
    void *context);

int mca_allocator_bucket_module_open(void);

int mca_allocator_bucket_module_close(void);

void * mca_allocator_bucket_alloc_wrapper(
    struct mca_allocator_base_module_t* allocator,
    size_t size, size_t align);

static int mca_allocator_num_buckets;



int mca_allocator_bucket_finalize(struct mca_allocator_base_module_t* allocator)
{
    mca_allocator_bucket_t *bucket = (mca_allocator_bucket_t *) allocator;

    mca_allocator_bucket_cleanup(allocator);

    for (int i = 0 ; i < bucket->num_buckets ; ++i) {
        OBJ_DESTRUCT(&bucket->buckets[i].lock);
    }

    free (bucket->buckets);
    free(allocator);

    return(OPAL_SUCCESS);
}

struct mca_allocator_base_module_t* mca_allocator_bucket_module_init(
    bool enable_mpi_threads,
    mca_allocator_base_component_segment_alloc_fn_t segment_alloc,
    mca_allocator_base_component_segment_free_fn_t segment_free,
    void *context)
{
    size_t alloc_size = sizeof(mca_allocator_bucket_t);
    mca_allocator_bucket_t * retval;
    mca_allocator_bucket_t * allocator = (mca_allocator_bucket_t *) malloc(alloc_size);
    if(NULL == allocator) {
        return NULL;
    }
    retval = mca_allocator_bucket_init((mca_allocator_base_module_t *) allocator,
        mca_allocator_num_buckets,
        segment_alloc,
        segment_free);
    if(NULL == retval) {
        free(allocator);
        return NULL;
    }
    allocator->super.alc_alloc =  mca_allocator_bucket_alloc_wrapper;
    allocator->super.alc_realloc = mca_allocator_bucket_realloc;
    allocator->super.alc_free =  mca_allocator_bucket_free;
    allocator->super.alc_compact = mca_allocator_bucket_cleanup;
    allocator->super.alc_finalize = mca_allocator_bucket_finalize;
    allocator->super.alc_context = context;
    return (mca_allocator_base_module_t *) allocator;
}

static int mca_allocator_bucket_module_register(void) {
    mca_allocator_num_buckets = 30;
    (void) mca_base_component_var_register(&mca_allocator_bucket_component.allocator_version,
                                           "num_buckets", NULL, MCA_BASE_VAR_TYPE_INT, NULL, 0,
                                           MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_LOCAL, &mca_allocator_num_buckets);
    return OPAL_SUCCESS;
}

int mca_allocator_bucket_module_open(void) {
    return OPAL_SUCCESS;
}

int mca_allocator_bucket_module_close(void) {
    return OPAL_SUCCESS;
}

void * mca_allocator_bucket_alloc_wrapper(
    struct mca_allocator_base_module_t* allocator,
    size_t size,
    size_t align)
{
    if(0 == align){
        return mca_allocator_bucket_alloc(allocator, size);
    }
    return mca_allocator_bucket_alloc_align(allocator, size, align);
}


mca_allocator_base_component_t mca_allocator_bucket_component = {

  /* First, the mca_base_module_t struct containing meta information
     about the module itself */

  {
    MCA_ALLOCATOR_BASE_VERSION_2_0_0,

    "bucket", /* MCA module name */
    OPAL_MAJOR_VERSION,
    OPAL_MINOR_VERSION,
    OPAL_RELEASE_VERSION,
    mca_allocator_bucket_module_open,  /* module open */
    mca_allocator_bucket_module_close, /* module close */
    NULL,
    mca_allocator_bucket_module_register
  },
  {
      /* The component is checkpoint ready */
      MCA_BASE_METADATA_PARAM_CHECKPOINT
  },
  mca_allocator_bucket_module_init
};

