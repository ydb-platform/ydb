/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2016, Hewlett Packard Enterprise Development LP
   Copyright (C) 2018 - 2019, Advanced Micro Devices, Inc.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name(s) of the copyright holder(s) nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

#ifndef BLIS_MEMBRK_H
#define BLIS_MEMBRK_H

// membrk init

BLIS_INLINE void bli_membrk_init_mutex( membrk_t* membrk )
{
	bli_pthread_mutex_init( &(membrk->mutex), NULL );
}

BLIS_INLINE void bli_membrk_finalize_mutex( membrk_t* membrk )
{
	bli_pthread_mutex_destroy( &(membrk->mutex) );
}

// membrk query

BLIS_INLINE pool_t* bli_membrk_pool( dim_t pool_index, membrk_t* membrk )
{
	return &(membrk->pools[ pool_index ]);
}

BLIS_INLINE siz_t bli_membrk_align_size( membrk_t* membrk )
{
	return membrk->align_size;
}

BLIS_INLINE malloc_ft bli_membrk_malloc_fp( membrk_t* membrk )
{
	return membrk->malloc_fp;
}

BLIS_INLINE free_ft bli_membrk_free_fp( membrk_t* membrk )
{
	return membrk->free_fp;
}

// membrk modification

BLIS_INLINE void bli_membrk_set_align_size( siz_t align_size, membrk_t* membrk )
{
	membrk->align_size = align_size;
}

BLIS_INLINE void bli_membrk_set_malloc_fp( malloc_ft malloc_fp, membrk_t* membrk )
{
	membrk->malloc_fp = malloc_fp;
}

BLIS_INLINE void bli_membrk_set_free_fp( free_ft free_fp, membrk_t* membrk )
{
	membrk->free_fp = free_fp;
}

// membrk action

BLIS_INLINE void bli_membrk_lock( membrk_t* membrk )
{
	bli_pthread_mutex_lock( &(membrk->mutex) );
}

BLIS_INLINE void bli_membrk_unlock( membrk_t* membrk )
{
	bli_pthread_mutex_unlock( &(membrk->mutex) );
}

// -----------------------------------------------------------------------------

membrk_t* bli_membrk_query( void );

void bli_membrk_init
     (
       cntx_t*   cntx
     );
void bli_membrk_finalize
     (
       void
     );

void bli_membrk_acquire_m
     (
       rntm_t*   rntm,
       siz_t     req_size,
       packbuf_t buf_type,
       mem_t*    mem
     );

void bli_membrk_release
     (
       rntm_t* rntm,
       mem_t*  mem
     );

void bli_membrk_rntm_set_membrk
     (
       rntm_t* rntm
     );

siz_t bli_membrk_pool_size
     (
       membrk_t* membrk,
       packbuf_t buf_type
     );

// ----------------------------------------------------------------------------

void bli_membrk_init_pools
     (
       cntx_t*   cntx,
       membrk_t* membrk
     );
void bli_membrk_finalize_pools
     (
       membrk_t* membrk
     );

void bli_membrk_compute_pool_block_sizes
     (
       siz_t*  bs_a,
       siz_t*  bs_b,
       siz_t*  bs_c,
       cntx_t* cntx
     );
void bli_membrk_compute_pool_block_sizes_dt
     (
       num_t   dt,
       siz_t*  bs_a,
       siz_t*  bs_b,
       siz_t*  bs_c,
       cntx_t* cntx
     );

#endif

