/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

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

#include "blis.h"

// The small block allocator: an apool_t of array_t of pool_t.
static apool_t sba;

apool_t* bli_sba_query( void )
{
	return &sba;
}

// -----------------------------------------------------------------------------

void bli_sba_init( void )
{
	bli_apool_init( &sba );
}

void bli_sba_finalize( void )
{
	bli_apool_finalize( &sba );
}

void* bli_sba_acquire
     (
       rntm_t* restrict rntm,
       siz_t            req_size
     )
{
	void* block;

#ifdef BLIS_ENABLE_SBA_POOLS
	if ( rntm == NULL )
	{
		block = bli_malloc_intl( req_size );
	}
	else
	{
		pblk_t pblk;

		// Query the small block pool from the rntm.
		pool_t* restrict pool = bli_rntm_sba_pool( rntm );

		// Query the block_size of the pool_t so that we can request the exact
		// size present.
		const siz_t block_size = bli_pool_block_size( pool );

		// Sanity check: Make sure the requested size is no larger than the
		// block_size field of the pool.
		if ( block_size < req_size )
		{
			printf( "bli_sba_acquire(): ** pool block_size is %d but req_size is %d.\n",
			        ( int )block_size, ( int )req_size );
			bli_abort();
		}

		// Check out a block using the block_size queried above.
		bli_pool_checkout_block( block_size, &pblk, pool );

		// The block address is stored within the pblk_t.
		block = bli_pblk_buf( &pblk );
	}
#else

	block = bli_malloc_intl( req_size );

#endif

	// Return the address obtained from the pblk_t.
	return block;
}

void bli_sba_release
     (
       rntm_t* restrict rntm,
       void*   restrict block
     )
{
#ifdef BLIS_ENABLE_SBA_POOLS
	if ( rntm == NULL )
	{
		bli_free_intl( block );
	}
	else
	{
		pblk_t pblk;

		// Query the small block pool from the rntm.
		pool_t* restrict pool = bli_rntm_sba_pool( rntm );

		// Query the block_size field from the pool. This is not super-important
		// for this particular application of the pool_t (that is, the "leaf"
		// component of the sba), but it seems like good housekeeping to maintain
		// the block_size field of the pblk_t in case its ever needed/read.
		const siz_t block_size = bli_pool_block_size( pool );

		// Embed the block's memory address into a pblk_t, along with the
		// block_size queried from the pool.
		bli_pblk_set_buf( block, &pblk );
		bli_pblk_set_block_size( block_size, &pblk );

		// Check the pblk_t back into the pool_t. (It's okay that the pblk_t is
		// a local variable since its contents are copied into the pool's internal
		// data structure--an array of pblk_t.)
		bli_pool_checkin_block( &pblk, pool );
	}
#else

	bli_free_intl( block );

#endif
}

array_t* bli_sba_checkout_array
     (
       const siz_t n_threads
     )
{
	#ifndef BLIS_ENABLE_SBA_POOLS
	return NULL;
	#endif

	return bli_apool_checkout_array( n_threads, &sba );
}

void bli_sba_checkin_array
     (
       array_t* restrict array
     )
{
	#ifndef BLIS_ENABLE_SBA_POOLS
	return;
	#endif

	bli_apool_checkin_array( array, &sba );
}

void bli_sba_rntm_set_pool
     (
       siz_t             index,
       array_t* restrict array,
       rntm_t*  restrict rntm
     )
{
	#ifndef BLIS_ENABLE_SBA_POOLS
	bli_rntm_set_sba_pool( NULL, rntm );
	return;
	#endif

	// Query the pool_t* in the array_t corresponding to index.
	pool_t* restrict pool = bli_apool_array_elem( index, array );

	// Embed the pool_t* into the rntm_t.
	bli_rntm_set_sba_pool( pool, rntm );
}


