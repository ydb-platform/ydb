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
 * Copyright (c) 2007      IBM Corp.,  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/mca/allocator/bucket/allocator_bucket_alloc.h"
/**
  * The define controls the size in bytes of the 1st bucket and hence every one
  * afterwards.
  */
#define MCA_ALLOCATOR_BUCKET_1_SIZE 8
/**
  * This is the number of left bit shifts from 1 needed to get to the number of
  * bytes in the initial memory buckets
  */
#define MCA_ALLOCATOR_BUCKET_1_BITSHIFTS 3

 /*
   * Initializes the mca_allocator_bucket_options_t data structure for the passed
   * parameters.
   */
mca_allocator_bucket_t * mca_allocator_bucket_init(
    mca_allocator_base_module_t * mem,
    int num_buckets,
    mca_allocator_base_component_segment_alloc_fn_t get_mem_funct,
    mca_allocator_base_component_segment_free_fn_t free_mem_funct)
{
    mca_allocator_bucket_t * mem_options = (mca_allocator_bucket_t *) mem;
    int i;
    size_t size;
    /* if a bad value is used for the number of buckets, default to 30 */
    if(num_buckets <= 0) {
        num_buckets = 30;
    }
    /* initialize the array of buckets */
    size = sizeof(mca_allocator_bucket_bucket_t) * num_buckets;
    mem_options->buckets = (mca_allocator_bucket_bucket_t*) malloc(size);
    if(NULL == mem_options->buckets) {
        return(NULL);
    }
    for(i = 0; i < num_buckets; i++) {
        mem_options->buckets[i].free_chunk = NULL;
        mem_options->buckets[i].segment_head = NULL;
        OBJ_CONSTRUCT(&(mem_options->buckets[i].lock), opal_mutex_t);
    }
    mem_options->num_buckets = num_buckets;
    mem_options->get_mem_fn = get_mem_funct;
    mem_options->free_mem_fn = free_mem_funct;
    return(mem_options);
}

/*
   * Accepts a request for memory in a specific region defined by the
   * mca_allocator_bucket_options_t struct and returns a pointer to memory in that
   * region or NULL if there was an error
   *
   */
void * mca_allocator_bucket_alloc(mca_allocator_base_module_t * mem,
                                  size_t size)
{
    mca_allocator_bucket_t * mem_options = (mca_allocator_bucket_t *) mem;
    /* initialize for the later bit shifts */
    int bucket_num = 0;
    size_t bucket_size = MCA_ALLOCATOR_BUCKET_1_SIZE;
    size_t allocated_size;
    mca_allocator_bucket_chunk_header_t * chunk;
    mca_allocator_bucket_chunk_header_t * first_chunk;
    mca_allocator_bucket_segment_head_t * segment_header;
    /* add the size of the header into the amount we need to request */
    size += sizeof(mca_allocator_bucket_chunk_header_t);

    /* figure out which bucket it will come from. */
    while(size > bucket_size) {
        bucket_num++;
        bucket_size <<= 1;
    }

    /* now that we know what bucket it will come from, we must get the lock */
    OPAL_THREAD_LOCK(&(mem_options->buckets[bucket_num].lock));
    /* see if there is already a free chunk */
    if(NULL != mem_options->buckets[bucket_num].free_chunk) {
        chunk = mem_options->buckets[bucket_num].free_chunk;
        mem_options->buckets[bucket_num].free_chunk = chunk->u.next_free;
        chunk->u.bucket = bucket_num;
        /* go past the header */
        chunk += 1;
        /*release the lock */
        OPAL_THREAD_UNLOCK(&(mem_options->buckets[bucket_num].lock));
        return((void *) chunk);
    }
    /* figure out the size of bucket we need */
    allocated_size = bucket_size;
    /* we have to add in the size of the segment header into the
     * amount we need to request */
    allocated_size += sizeof(mca_allocator_bucket_segment_head_t);
    /* attempt to get the memory */
    segment_header = (mca_allocator_bucket_segment_head_t *)
                   mem_options->get_mem_fn(mem_options->super.alc_context, &allocated_size);
    if(NULL == segment_header) {
        /* release the lock */
        OPAL_THREAD_UNLOCK(&(mem_options->buckets[bucket_num].lock));
        return(NULL);
    }
    /* if were allocated more memory then we actually need, then we will try to
     * break it up into multiple chunks in the current bucket */
    allocated_size -= (sizeof(mca_allocator_bucket_segment_head_t) + bucket_size);
    chunk = first_chunk = segment_header->first_chunk =
                  (mca_allocator_bucket_chunk_header_t *) (segment_header + 1);
    /* add the segment into the segment list */
    segment_header->next_segment = mem_options->buckets[bucket_num].segment_head;
    mem_options->buckets[bucket_num].segment_head = segment_header;
    if(allocated_size >= bucket_size) {
        mem_options->buckets[bucket_num].free_chunk =
                        (mca_allocator_bucket_chunk_header_t *) ((char *) chunk + bucket_size);
        chunk->next_in_segment = (mca_allocator_bucket_chunk_header_t *)
                                   ((char *)chunk + bucket_size);
        while(allocated_size >= bucket_size) {
            chunk = (mca_allocator_bucket_chunk_header_t *) ((char *) chunk + bucket_size);
            chunk->u.next_free = (mca_allocator_bucket_chunk_header_t *)
                                 ((char *) chunk + bucket_size);
            chunk->next_in_segment = chunk->u.next_free;
            allocated_size -= bucket_size;
        }
        chunk->next_in_segment = first_chunk;
        chunk->u.next_free = NULL;
    } else {
        first_chunk->next_in_segment = first_chunk;
    }
    first_chunk->u.bucket = bucket_num;
    OPAL_THREAD_UNLOCK(&(mem_options->buckets[bucket_num].lock));
    /* return the memory moved past the header */
    return((void *) (first_chunk + 1));
}

/*
  * allocates an aligned region of memory
  */
void * mca_allocator_bucket_alloc_align(mca_allocator_base_module_t * mem,
                                        size_t size, size_t alignment)
{
    mca_allocator_bucket_t * mem_options = (mca_allocator_bucket_t *) mem;
    int bucket_num = 1;
    void * ptr;
    size_t aligned_max_size, bucket_size;
    size_t alignment_off, allocated_size;
    mca_allocator_bucket_chunk_header_t * chunk;
    mca_allocator_bucket_chunk_header_t * first_chunk;
    mca_allocator_bucket_segment_head_t * segment_header;
    char * aligned_memory;

    /* since we do not have a way to get pre aligned memory, we need to request
     * a chunk then return an aligned spot in it. In the worst case we need
     * the requested size plus the alignment and the header size */
    aligned_max_size = size + alignment + sizeof(mca_allocator_bucket_chunk_header_t)
                       + sizeof(mca_allocator_bucket_segment_head_t);
    bucket_size = size + sizeof(mca_allocator_bucket_chunk_header_t);
    allocated_size = aligned_max_size;
    /* get some memory */
    ptr = mem_options->get_mem_fn(mem_options->super.alc_context, &allocated_size);
    if(NULL == ptr) {
        return(NULL);
    }
    /* the first part of the memory is the segment header */
    segment_header = (mca_allocator_bucket_segment_head_t *) ptr;
    /* we temporarily define the first chunk to be right after the segment_header */
    first_chunk = (mca_allocator_bucket_chunk_header_t *) (segment_header + 1);

    /* we want to align the memory right after the header, so we go past the header */
    aligned_memory = (char *) (first_chunk + 1);
    /* figure out how much the alignment is off by */
    alignment_off = ((size_t)  aligned_memory) % alignment;
    aligned_memory += (alignment - alignment_off);
    /* we now have an aligned piece of memory. Now we have to put the chunk
     * header right before the aligned memory                           */
    first_chunk = (mca_allocator_bucket_chunk_header_t *) aligned_memory - 1;
    while(bucket_size > MCA_ALLOCATOR_BUCKET_1_SIZE) {
        bucket_size >>= 1;
        bucket_num++;
    }
    bucket_size = 1;
    bucket_size <<= MCA_ALLOCATOR_BUCKET_1_BITSHIFTS + bucket_num;

    /* if were allocated more memory then we actually need, then we will try to
     * break it up into multiple chunks in the current bucket */
    allocated_size -= aligned_max_size;
    chunk = segment_header->first_chunk = first_chunk;
    /* we now need to get a lock on the bucket */
    OPAL_THREAD_LOCK(&(mem_options->buckets[bucket_num].lock));
    /* add the segment into the segment list */
    segment_header->next_segment = mem_options->buckets[bucket_num].segment_head;
    mem_options->buckets[bucket_num].segment_head = segment_header;
    if(allocated_size >= bucket_size) {
        mem_options->buckets[bucket_num].free_chunk =
                        (mca_allocator_bucket_chunk_header_t *) ((char *) chunk + bucket_size);
        chunk->next_in_segment = (mca_allocator_bucket_chunk_header_t *)
                                   ((char *)chunk + bucket_size);
        while(allocated_size >= bucket_size) {
            chunk = (mca_allocator_bucket_chunk_header_t *) ((char *) chunk + bucket_size);
            chunk->u.next_free = (mca_allocator_bucket_chunk_header_t *)
                                 ((char *) chunk + bucket_size);
            chunk->next_in_segment = chunk->u.next_free;
            allocated_size -= bucket_size;
        }
        chunk->next_in_segment = first_chunk;
        chunk->u.next_free = NULL;
    } else {
        first_chunk->next_in_segment = first_chunk;
    }
    first_chunk->u.bucket = bucket_num;
    OPAL_THREAD_UNLOCK(&(mem_options->buckets[bucket_num].lock));
    /* return the aligned memory */
    return((void *) (aligned_memory));
}

/*
  * function to reallocate the segment of memory
  */
void * mca_allocator_bucket_realloc(mca_allocator_base_module_t * mem,
                                    void * ptr, size_t size)
{
    mca_allocator_bucket_t * mem_options = (mca_allocator_bucket_t *) mem;
    /* initialize for later bit shifts */
    size_t bucket_size = 1;
    int bucket_num;
    void * ret_ptr;
    /* get the header of the chunk */
    mca_allocator_bucket_chunk_header_t * chunk = (mca_allocator_bucket_chunk_header_t *) ptr - 1;
    bucket_num = chunk->u.bucket;

    bucket_size <<= (bucket_num + MCA_ALLOCATOR_BUCKET_1_BITSHIFTS);
    /* since the header area is not available to the user, we need to
     * subtract off the header size                 */
    bucket_size -= sizeof(mca_allocator_bucket_chunk_header_t);
    /* if the requested size is less than or equal to what they ask for,
     * just give them back what they passed in      */
    if(size <= bucket_size) {
        return(ptr);
    }
    /* we need a new space in memory, so let's get it */
    ret_ptr = mca_allocator_bucket_alloc((mca_allocator_base_module_t *) mem_options, size);
    if(NULL == ret_ptr) {
        /* we were unable to get a larger area of memory */
        return(NULL);
    }
    /* copy what they have in memory to the new spot */
    memcpy(ret_ptr, ptr, bucket_size);
    /* free the old area in memory */
    mca_allocator_bucket_free((mca_allocator_base_module_t *) mem_options, ptr);
    return(ret_ptr);
}


/*
   * Frees the passed region of memory
   *
   */
void mca_allocator_bucket_free(mca_allocator_base_module_t * mem, void * ptr)
{
    mca_allocator_bucket_t * mem_options = (mca_allocator_bucket_t *) mem;
    mca_allocator_bucket_chunk_header_t * chunk  = (mca_allocator_bucket_chunk_header_t *) ptr - 1;
    int bucket_num = chunk->u.bucket;
    OPAL_THREAD_LOCK(&(mem_options->buckets[bucket_num].lock));
    chunk->u.next_free = mem_options->buckets[bucket_num].free_chunk;
    mem_options->buckets[bucket_num].free_chunk = chunk;
    OPAL_THREAD_UNLOCK(&(mem_options->buckets[bucket_num].lock));
}

/*
   * Frees all the memory from all the buckets back to the system. Note that
   * this function only frees memory that was previously freed with
   * mca_allocator_bucket_free().
   *
   */
int mca_allocator_bucket_cleanup(mca_allocator_base_module_t * mem)
{
    mca_allocator_bucket_t * mem_options = (mca_allocator_bucket_t *) mem;
    int i;
    mca_allocator_bucket_chunk_header_t * next_chunk;
    mca_allocator_bucket_chunk_header_t * chunk;
    mca_allocator_bucket_chunk_header_t * first_chunk;
    mca_allocator_bucket_segment_head_t ** segment_header;
    mca_allocator_bucket_segment_head_t * segment;
    bool empty = true;

    for(i = 0; i < mem_options->num_buckets; i++) {
        OPAL_THREAD_LOCK(&(mem_options->buckets[i].lock));
        segment_header = &(mem_options->buckets[i].segment_head);
        if( NULL == (*segment_header) ) {
            OPAL_THREAD_UNLOCK(&(mem_options->buckets[i].lock));
            continue;
        }
        /* first we suppose the execution is correct and all chunks
         * have been correctly released. Therefore, if we make sure
         * all segments only contain free items then we can release
         * everything in one go.
         */
        empty = true;
        segment = mem_options->buckets[i].segment_head;
        while( (true == empty) && (NULL != segment) ) {
            first_chunk = segment->first_chunk;
            chunk = first_chunk;
            /* determine if the segment is free */
            do {
                if(chunk->u.bucket == i) {
                    empty = false;
                    break;
                }
                chunk = chunk->next_in_segment;
            } while(chunk != first_chunk);
            /* go to next segment */
            segment = segment->next_segment;
        }
        if( true == empty ) {  /* all segments ready for release */
            mca_allocator_bucket_segment_head_t* next_segment;
            segment = mem_options->buckets[i].segment_head;
            while( NULL != segment ) {
                next_segment = segment->next_segment;
                /* free the memory */
                if(mem_options->free_mem_fn)
                    mem_options->free_mem_fn(mem->alc_context, segment);
                segment = next_segment;
            }
            mem_options->buckets[i].free_chunk = NULL;
            mem_options->buckets[i].segment_head = NULL;
        } else {
            /* traverse the list of segment headers until we hit NULL */
            while(NULL != *segment_header) {
                first_chunk = (*segment_header)->first_chunk;
                chunk = first_chunk;
                empty = true;
                /* determine if the segment is free */
                do {
                    if(chunk->u.bucket == i) {
                        empty = false;
                    }
                    chunk = chunk->next_in_segment;
                } while(empty && (chunk != first_chunk));
                if(empty) {
                    chunk = first_chunk;
                    /* remove the chunks from the free list */
                    do {
                        if(mem_options->buckets[i].free_chunk == chunk) {
                            mem_options->buckets[i].free_chunk = chunk->u.next_free;
                        } else {
                            next_chunk = mem_options->buckets[i].free_chunk;
                            while(next_chunk->u.next_free != chunk) {
                                next_chunk = next_chunk->u.next_free;
                            }
                            next_chunk->u.next_free = chunk->u.next_free;
                        }
                    } while((chunk = chunk->next_in_segment) != first_chunk);
                    /* set the segment list to point to the next segment */
                    segment = *segment_header;
                    *segment_header = segment->next_segment;
                    /* free the memory */
                    if(mem_options->free_mem_fn)
                        mem_options->free_mem_fn(mem->alc_context, segment);
                } else {
                    /* go to next segment */
                    segment_header = &((*segment_header)->next_segment);
                }
            }
        }
        /* relese the lock on the bucket */
        OPAL_THREAD_UNLOCK(&(mem_options->buckets[i].lock));
    }
    return(OPAL_SUCCESS);
}
