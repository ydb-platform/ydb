/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reseved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *  A generic memory bucket allocator.
 **/

#ifndef ALLOCATOR_BUCKET_ALLOC_H
#define ALLOCATOR_BUCKET_ALLOC_H

#include "opal_config.h"
#include <stdlib.h>
#include <string.h>
#include "opal/threads/mutex.h"
#include "opal/mca/allocator/allocator.h"

BEGIN_C_DECLS

/**
  * Structure for the header of each memory chunk
  */
struct mca_allocator_bucket_chunk_header_t {
    struct mca_allocator_bucket_chunk_header_t * next_in_segment;
               /**< The next chunk in the memory segment */
    /**
      * Union which holds either a pointer to the next free chunk
      * or the bucket number. Based on the current location of the chunk
      * we use one or the other of these fields. If the chunk is owned
      * by the user, then the bucket field is set, which allow us to know
      * in which specific bucket we have to put it back on free (as the
      * chunk don't have the size attached). When the allocator own the
      * chunk, the next_free fild is used, which allow us to put these
      * chunks a list of free elements.
      */
    union u {
        struct mca_allocator_bucket_chunk_header_t * next_free;
        /**< if the chunk is free this will point to the next free chunk in the bucket */
        int bucket;                  /**< the bucket number it belongs to */
    } u; /**< the union */
};
 /**
   * Typedef so we don't have to use struct
   */
typedef struct mca_allocator_bucket_chunk_header_t mca_allocator_bucket_chunk_header_t;

/**
  * Structure that heads each segment
  */
struct mca_allocator_bucket_segment_head_t {
    struct mca_allocator_bucket_chunk_header_t * first_chunk; /**< the first chunk of the header */
    struct mca_allocator_bucket_segment_head_t * next_segment; /**< the next segment in the
                                                 bucket */
};
/**
  * Typedef so we don't have to use struct
  */
typedef struct mca_allocator_bucket_segment_head_t mca_allocator_bucket_segment_head_t;

/**
  * Structure for each bucket
  */
struct mca_allocator_bucket_bucket_t {
    mca_allocator_bucket_chunk_header_t * free_chunk; /**< the first free chunk of memory */
    opal_mutex_t lock;                /**< the lock on the bucket */
    mca_allocator_bucket_segment_head_t * segment_head; /**< the list of segment headers */
};
/**
  * Typedef so we don't have to use struct
  */
typedef struct mca_allocator_bucket_bucket_t mca_allocator_bucket_bucket_t;

/**
  * Structure that holds the necessary information for each area of memory
  */
struct mca_allocator_bucket_t {
    mca_allocator_base_module_t super;          /**< makes this a child of class mca_allocator_t */
    mca_allocator_bucket_bucket_t * buckets; /**< the array of buckets */
    int num_buckets;                         /**< the number of buckets */
    mca_allocator_base_component_segment_alloc_fn_t get_mem_fn;
    /**< pointer to the function to get more memory */
    mca_allocator_base_component_segment_free_fn_t free_mem_fn;
    /**< pointer to the function to free memory */
};
/**
  * Typedef so we don't have to use struct
  */
typedef struct mca_allocator_bucket_t mca_allocator_bucket_t;

  /**
   * Initializes the mca_allocator_bucket_options_t data structure for the passed
   * parameters.
   * @param mem a pointer to the mca_allocator_t struct to be filled in
   * @param num_buckets The number of buckets the allocator will use
   * @param get_mem_funct A pointer to the function that the allocator
   * will use to get more memory
   * @param free_mem_funct A pointer to the function that the allocator
   * will use to free memory
   *
   * @retval Pointer to the initialized mca_allocator_bucket_options_t structure
   * @retval NULL if there was an error
   */
    mca_allocator_bucket_t *mca_allocator_bucket_init(mca_allocator_base_module_t * mem,
                                       int num_buckets,
                                       mca_allocator_base_component_segment_alloc_fn_t get_mem_funct,
                                       mca_allocator_base_component_segment_free_fn_t free_mem_funct);
/**
   * Accepts a request for memory in a specific region defined by the
   * mca_allocator_bucket_options_t struct and returns a pointer to memory in that
   * region or NULL if there was an error
   *
   * @param mem A pointer to the appropriate struct for the area of memory.
   * @param size The size of the requested area of memory
   *
   * @retval Pointer to the area of memory if the allocation was successful
   * @retval NULL if the allocation was unsuccessful
   */
    void * mca_allocator_bucket_alloc(
        mca_allocator_base_module_t * mem,
        size_t size);

/**
   * Accepts a request for memory in a specific region defined by the
   * mca_allocator_bucket_options_t struct and aligned by the specified amount and
   * returns a pointer to memory in that region or NULL if there was an error
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   * @param size The size of the requested area of memory
   * @param alignment The requested alignment of the new area of memory. This
   * MUST be a power of 2.
   *
   * @retval Pointer to the area of memory if the allocation was successful
   * @retval NULL if the allocation was unsuccessful
   *
   */
    void * mca_allocator_bucket_alloc_align(
        mca_allocator_base_module_t * mem,
        size_t size,
        size_t alignment);

/**
   * Attempts to resize the passed region of memory into a larger or a smaller
   * region. If it is unsuccessful, it will return NULL and the passed area of
   * memory will be untouched.
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   * @param size The size of the requested area of memory
   * @param ptr A pointer to the region of memory to be resized
   *
   * @retval Pointer to the area of memory if the reallocation was successful
   * @retval NULL if the allocation was unsuccessful
   *
   */
    void * mca_allocator_bucket_realloc(
        mca_allocator_base_module_t * mem,
        void * ptr,
        size_t size);

/**
   * Frees the passed region of memory
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   * @param ptr A pointer to the region of memory to be freed
   *
   * @retval None
   *
   */
    void mca_allocator_bucket_free(mca_allocator_base_module_t * mem,
                                   void * ptr);

/**
   * Frees all the memory from all the buckets back to the system. Note that
   * this function only frees memory that was previously freed with
   * mca_allocator_bucket_free().
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   *
   * @retval None
   *
   */
    int mca_allocator_bucket_cleanup(mca_allocator_base_module_t * mem);

/**
   * Cleanup all resources held by this allocator.
   *
   * @param mem A pointer to the appropriate struct for the area of
   * memory.
   *
   * @retval None
   *
   */
    int mca_allocator_bucket_finalize(mca_allocator_base_module_t * mem);

OPAL_DECLSPEC extern mca_allocator_base_component_t mca_allocator_bucket_component;

END_C_DECLS

#endif /* ALLOCATOR_BUCKET_ALLOC_H */
