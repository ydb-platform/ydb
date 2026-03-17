/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
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

void bli_cntx_clear( cntx_t* cntx )
{
	// Fill the entire cntx_t structure with zeros.
	memset( ( void* )cntx, 0, sizeof( cntx_t ) );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_blkszs( ind_t method, dim_t n_bs, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default blocksizes. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default
	// blocksizes across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_blkszs
	   (
	     ind_t   method = BLIS_NAT,
	     dim_t   n_bs,
	     bszid_t bs0_id, blksz_t* blksz0, bszid_t bm0_id,
	     bszid_t bs1_id, blksz_t* blksz1, bszid_t bm1_id,
	     bszid_t bs2_id, blksz_t* blksz2, bszid_t bm2_id,
	     ...
	     cntx_t* cntx
	   );

	   void bli_cntx_set_blkszs
	   (
	     ind_t   method != BLIS_NAT,
	     dim_t   n_bs,
	     bszid_t bs0_id, blksz_t* blksz0, bszid_t bm0_id, dim_t def_scalr0, dim_t max_scalr0,
	     bszid_t bs1_id, blksz_t* blksz1, bszid_t bm1_id, dim_t def_scalr1, dim_t max_scalr1,
	     bszid_t bs2_id, blksz_t* blksz2, bszid_t bm2_id, dim_t def_scalr2, dim_t max_scalr2,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bszid_t*  bszids = bli_malloc_intl( n_bs * sizeof( bszid_t  ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	blksz_t** blkszs = bli_malloc_intl( n_bs * sizeof( blksz_t* ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bszid_t*  bmults = bli_malloc_intl( n_bs * sizeof( bszid_t  ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	double*   dsclrs = bli_malloc_intl( n_bs * sizeof( double   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	double*   msclrs = bli_malloc_intl( n_bs * sizeof( double   ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_bs );

	// Handle native and induced method cases separately.
	if ( method == BLIS_NAT )
	{
		// Process n_bs tuples.
		for ( i = 0; i < n_bs; ++i )
		{
			// Here, we query the variable argument list for:
			// - the bszid_t of the blocksize we're about to process,
			// - the address of the blksz_t object,
			// - the bszid_t of the multiple we need to associate with
			//   the blksz_t object.
			bszid_t  bs_id = ( bszid_t  )va_arg( args, bszid_t  );
			blksz_t* blksz = ( blksz_t* )va_arg( args, blksz_t* );
			bszid_t  bm_id = ( bszid_t  )va_arg( args, bszid_t  );

			// Store the values in our temporary arrays.
			bszids[ i ] = bs_id;
			blkszs[ i ] = blksz;
			bmults[ i ] = bm_id;
		}
	}
	else // if induced method execution was indicated
	{
		// Process n_bs tuples.
		for ( i = 0; i < n_bs; ++i )
		{
			// Here, we query the variable argument list for:
			// - the bszid_t of the blocksize we're about to process,
			// - the address of the blksz_t object,
			// - the bszid_t of the multiple we  need to associate with
			//   the blksz_t object,
			// - the scalars we wish to apply to the real blocksizes to
			//   come up with the induced complex blocksizes (for default
			//   and maximum blocksizes).
			bszid_t  bs_id = ( bszid_t  )va_arg( args, bszid_t  );
			blksz_t* blksz = ( blksz_t* )va_arg( args, blksz_t* );
			bszid_t  bm_id = ( bszid_t  )va_arg( args, bszid_t  );
			double   dsclr = ( double   )va_arg( args, double   );
			double   msclr = ( double   )va_arg( args, double   );

			// Store the values in our temporary arrays.
			bszids[ i ] = bs_id;
			blkszs[ i ] = blksz;
			bmults[ i ] = bm_id;
			dsclrs[ i ] = dsclr;
			msclrs[ i ] = msclr;
		}
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Save the execution type into the context.
	bli_cntx_set_method( method, cntx );

	// Query the context for the addresses of:
	// - the blocksize object array
	// - the blocksize multiple array

	blksz_t* cntx_blkszs = bli_cntx_blkszs_buf( cntx );
	bszid_t* cntx_bmults = bli_cntx_bmults_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context. Notice that the blksz_t* pointers were saved, rather than
	// the objects themselves, but we copy the contents of the objects
	// when copying into the context.

	// Handle native and induced method cases separately.
	if ( method == BLIS_NAT )
	{
		// Process each blocksize id tuple provided.
		for ( i = 0; i < n_bs; ++i )
		{
			// Read the current blocksize id, blksz_t* pointer, blocksize
			// multiple id, and blocksize scalar.
			bszid_t  bs_id = bszids[ i ];
			bszid_t  bm_id = bmults[ i ];

			blksz_t* blksz = blkszs[ i ];

			blksz_t* cntx_blksz = &cntx_blkszs[ bs_id ];

			// Copy the blksz_t object contents into the appropriate
			// location within the context's blksz_t array. Do the same
			// for the blocksize multiple id.
			//cntx_blkszs[ bs_id ] = *blksz;
			//bli_blksz_copy( blksz, cntx_blksz );
			bli_blksz_copy_if_pos( blksz, cntx_blksz );

			// Copy the blocksize multiple id into the context.
			cntx_bmults[ bs_id ] = bm_id;
		}
	}
	else
	{
		// Process each blocksize id tuple provided.
		for ( i = 0; i < n_bs; ++i )
		{
			// Read the current blocksize id, blksz_t pointer, blocksize
			// multiple id, and blocksize scalar.
			bszid_t  bs_id = bszids[ i ];
			bszid_t  bm_id = bmults[ i ];
			double   dsclr = dsclrs[ i ];
			double   msclr = msclrs[ i ];

			blksz_t* blksz = blkszs[ i ];
			// NOTE: This is a bug! We need to grab the actual blocksize
			// multiple, which is not at blkszs[i], but rather somewhere else
			// in the array. In order to fix this, you probably need to store
			// the contents of blkszs (and all the other arrays) by bs_id
			// rather than i in the first loop.
			blksz_t* bmult = blkszs[ i ];

			blksz_t* cntx_blksz = &cntx_blkszs[ bs_id ];

			// Copy the real domain values of the source blksz_t object into
			// the context, duplicating into the complex domain fields.
			bli_blksz_copy_dt( BLIS_FLOAT,  blksz, BLIS_FLOAT,    cntx_blksz );
			bli_blksz_copy_dt( BLIS_DOUBLE, blksz, BLIS_DOUBLE,   cntx_blksz );
			bli_blksz_copy_dt( BLIS_FLOAT,  blksz, BLIS_SCOMPLEX, cntx_blksz );
			bli_blksz_copy_dt( BLIS_DOUBLE, blksz, BLIS_DCOMPLEX, cntx_blksz );

			// If the default blocksize scalar is non-unit, we need to scale
			// the complex domain default blocksizes.
			if ( dsclr != 1.0 )
			{
				// Scale the complex domain default blocksize values in the
				// blocksize object.
				bli_blksz_scale_def( 1, ( dim_t )dsclr, BLIS_SCOMPLEX, cntx_blksz );
				bli_blksz_scale_def( 1, ( dim_t )dsclr, BLIS_DCOMPLEX, cntx_blksz );

				// Perform rounding to ensure the newly scaled values are still
				// multiples of their register blocksize multiples. But only
				// perform this rounding when the blocksize id is not equal to
				// the blocksize multiple id (ie: we don't round down scaled
				// register blocksizes since they are their own multiples).
				// Also, we skip the rounding for 1m since it should never need
				// such rounding.
				if ( bs_id != bm_id && method != BLIS_1M )
				{
					// Round the newly-scaled blocksizes down to their multiple.
					bli_blksz_reduce_def_to( BLIS_FLOAT,  bmult, BLIS_SCOMPLEX, cntx_blksz );
					bli_blksz_reduce_def_to( BLIS_DOUBLE, bmult, BLIS_DCOMPLEX, cntx_blksz );
				}
			}

			// Similarly, if the maximum blocksize scalar is non-unit, we need
			// to scale the complex domain maximum blocksizes.
			if ( msclr != 1.0 )
			{
				// Scale the complex domain maximum blocksize values in the
				// blocksize object.
				bli_blksz_scale_max( 1, ( dim_t )msclr, BLIS_SCOMPLEX, cntx_blksz );
				bli_blksz_scale_max( 1, ( dim_t )msclr, BLIS_DCOMPLEX, cntx_blksz );

				// Perform rounding to ensure the newly scaled values are still
				// multiples of their register blocksize multiples. But only
				// perform this rounding when the blocksize id is not equal to
				// the blocksize multiple id (ie: we don't round down scaled
				// register blocksizes since they are their own multiples).
				// Also, we skip the rounding for 1m since it should never need
				// such rounding.
				if ( bs_id != bm_id && method != BLIS_1M )
				{
					// Round the newly-scaled blocksizes down to their multiple.
					bli_blksz_reduce_max_to( BLIS_FLOAT,  bmult, BLIS_SCOMPLEX, cntx_blksz );
					bli_blksz_reduce_max_to( BLIS_DOUBLE, bmult, BLIS_DCOMPLEX, cntx_blksz );
				}
			}

			// Copy the blocksize multiple id into the context.
			cntx_bmults[ bs_id ] = bm_id;
		}
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( blkszs );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( bszids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( bmults );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( dsclrs );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( msclrs );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_ind_blkszs( ind_t method, dim_t n_bs, ... )
{
	/* Example prototypes:

	   void bli_gks_cntx_set_ind_blkszs
	   (
	     ind_t   method != BLIS_NAT,
	     dim_t   n_bs,
	     bszid_t bs0_id, dim_t def_scalr0, dim_t max_scalr0,
	     bszid_t bs1_id, dim_t def_scalr1, dim_t max_scalr1,
	     bszid_t bs2_id, dim_t def_scalr2, dim_t max_scalr2,
	     ...
	     cntx_t* cntx
	   );
	
		NOTE: This function modifies an existing context that is presumed
		to have been initialized for native execution.
	*/

	va_list   args;
	dim_t     i;

	// Return early if called with BLIS_NAT.
	if ( method == BLIS_NAT ) return;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_ind_blkszs(): " );
	#endif
	bszid_t* bszids = bli_malloc_intl( n_bs * sizeof( bszid_t  ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_ind_blkszs(): " );
	#endif
	double*  dsclrs = bli_malloc_intl( n_bs * sizeof( double   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_ind_blkszs(): " );
	#endif
	double*  msclrs = bli_malloc_intl( n_bs * sizeof( double   ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_bs );

	{
		// Process n_bs tuples.
		for ( i = 0; i < n_bs; ++i )
		{
			// Here, we query the variable argument list for:
			// - the bszid_t of the blocksize we're about to process,
			// - the scalars we wish to apply to the real blocksizes to
			//   come up with the induced complex blocksizes (for default
			//   and maximum blocksizes).
			bszid_t  bs_id = ( bszid_t )va_arg( args, bszid_t  );
			double   dsclr = ( double  )va_arg( args, double   );
			double   msclr = ( double  )va_arg( args, double   );

			// Store the values in our temporary arrays.
			bszids[ i ] = bs_id;
			dsclrs[ i ] = dsclr;
			msclrs[ i ] = msclr;
		}
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Save the execution type into the context.
	bli_cntx_set_method( method, cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	{
		// Process each blocksize id tuple provided.
		for ( i = 0; i < n_bs; ++i )
		{
			// Read the current blocksize id, blocksize multiple id,
			// and blocksize scalar.
			bszid_t  bs_id = bszids[ i ];
			double   dsclr = dsclrs[ i ];
			double   msclr = msclrs[ i ];

			//blksz_t* cntx_blksz = &cntx_blkszs[ bs_id ];

			// Query the blocksize multiple's blocksize id.
			bszid_t  bm_id = bli_cntx_get_bmult_id( bs_id, cntx );

			// Query the context for the blksz_t object assoicated with the
			// current blocksize id, and also query the object corresponding
			// to the blocksize multiple.
			blksz_t* cntx_blksz = bli_cntx_get_blksz( bs_id, cntx );
			blksz_t* cntx_bmult = bli_cntx_get_bmult( bs_id, cntx );

			// Copy the real domain values of the blksz_t object into the
			// the complex domain slots of the same object.
			bli_blksz_copy_dt( BLIS_FLOAT,  cntx_blksz, BLIS_SCOMPLEX, cntx_blksz );
			bli_blksz_copy_dt( BLIS_DOUBLE, cntx_blksz, BLIS_DCOMPLEX, cntx_blksz );

			// If the default blocksize scalar is non-unit, we need to scale
			// the complex domain default blocksizes.
			if ( dsclr != 1.0 )
			{
				// Scale the complex domain default blocksize values in the
				// blocksize object.
				bli_blksz_scale_def( 1, ( dim_t )dsclr, BLIS_SCOMPLEX, cntx_blksz );
				bli_blksz_scale_def( 1, ( dim_t )dsclr, BLIS_DCOMPLEX, cntx_blksz );

				// Perform rounding to ensure the newly scaled values are still
				// multiples of their register blocksize multiples. But only
				// perform this rounding when the blocksize id is not equal to
				// the blocksize multiple id (ie: we don't round down scaled
				// register blocksizes since they are their own multiples).
				// Also, we skip the rounding for 1m since it should never need
				// such rounding.
				if ( bs_id != bm_id && method != BLIS_1M )
				{
					// Round the newly-scaled blocksizes down to their multiple.
					bli_blksz_reduce_def_to( BLIS_FLOAT,  cntx_bmult, BLIS_SCOMPLEX, cntx_blksz );
					bli_blksz_reduce_def_to( BLIS_DOUBLE, cntx_bmult, BLIS_DCOMPLEX, cntx_blksz );
				}
			}

			// Similarly, if the maximum blocksize scalar is non-unit, we need
			// to scale the complex domain maximum blocksizes.
			if ( msclr != 1.0 )
			{
				// Scale the complex domain maximum blocksize values in the
				// blocksize object.
				bli_blksz_scale_max( 1, ( dim_t )msclr, BLIS_SCOMPLEX, cntx_blksz );
				bli_blksz_scale_max( 1, ( dim_t )msclr, BLIS_DCOMPLEX, cntx_blksz );

				// Perform rounding to ensure the newly scaled values are still
				// multiples of their register blocksize multiples. But only
				// perform this rounding when the blocksize id is not equal to
				// the blocksize multiple id (ie: we don't round down scaled
				// register blocksizes since they are their own multiples).
				// Also, we skip the rounding for 1m since it should never need
				// such rounding.
				if ( bs_id != bm_id && method != BLIS_1M )
				{
					// Round the newly-scaled blocksizes down to their multiple.
					bli_blksz_reduce_max_to( BLIS_FLOAT,  cntx_bmult, BLIS_SCOMPLEX, cntx_blksz );
					bli_blksz_reduce_max_to( BLIS_DOUBLE, cntx_bmult, BLIS_DCOMPLEX, cntx_blksz );
				}
			}
		}
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_ind_blkszs(): " );
	#endif
	bli_free_intl( bszids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_ind_blkszs(): " );
	#endif
	bli_free_intl( dsclrs );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_ind_blkszs(): " );
	#endif
	bli_free_intl( msclrs );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l3_nat_ukrs( dim_t n_ukrs, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default level-3 microkernels. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default
	// microkernels across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_l3_nat_ukrs
	   (
	     dim_t   n_ukrs,
	     l3ukr_t ukr0_id, num_t dt0, void_fp ukr0_fp, bool pref0,
	     l3ukr_t ukr1_id, num_t dt1, void_fp ukr1_fp, bool pref1,
	     l3ukr_t ukr2_id, num_t dt2, void_fp ukr2_fp, bool pref2,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	l3ukr_t* ukr_ids   = bli_malloc_intl( n_ukrs * sizeof( l3ukr_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	num_t*   ukr_dts   = bli_malloc_intl( n_ukrs * sizeof( num_t   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	void_fp* ukr_fps   = bli_malloc_intl( n_ukrs * sizeof( void_fp ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	bool*    ukr_prefs = bli_malloc_intl( n_ukrs * sizeof( bool    ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_ukrs );

	// Process n_ukrs tuples.
	for ( i = 0; i < n_ukrs; ++i )
	{
		// Here, we query the variable argument list for:
		// - the l3ukr_t of the kernel we're about to process,
		// - the datatype of the kernel,
		// - the kernel function pointer, and
		// - the kernel function storage preference
		// that we need to store to the context.

		// NOTE: Though bool_t is no longer used, the following comment is
		// being kept for historical reasons.
		// The type that we pass into the va_arg() macro for the ukr
		// preference matters. Using 'bool_t' may cause breakage on 64-bit
		// systems that define int as 32 bits and long int and pointers as
		// 64 bits. The problem is that TRUE or FALSE are defined as 1 and
		// 0, respectively, and when "passed" into the variadic function
		// they come with no contextual typecast. Thus, default rules of
		// argument promotion kick in to treat these integer literals as
		// being of type int. Thus, we need to let va_arg() treat the TRUE
		// or FALSE value as an int, even if we cast it to and store it
		// within a bool_t afterwards.
		const l3ukr_t  ukr_id   = ( l3ukr_t )va_arg( args, l3ukr_t );
		const num_t    ukr_dt   = ( num_t   )va_arg( args, num_t   );
		      void_fp  ukr_fp   = ( void_fp )va_arg( args, void_fp );
		const bool     ukr_pref = ( bool    )va_arg( args, int     );

		// Store the values in our temporary arrays.
		ukr_ids[ i ]   = ukr_id;
		ukr_dts[ i ]   = ukr_dt;
		ukr_fps[ i ]   = ukr_fp;
		ukr_prefs[ i ] = ukr_pref;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the addresses of:
	// - the l3 virtual ukernel func_t array
	// - the l3 native ukernel func_t array
	// - the l3 native ukernel preferences array
	func_t*  cntx_l3_vir_ukrs       = bli_cntx_l3_vir_ukrs_buf( cntx );
	func_t*  cntx_l3_nat_ukrs       = bli_cntx_l3_nat_ukrs_buf( cntx );
	mbool_t* cntx_l3_nat_ukrs_prefs = bli_cntx_l3_nat_ukrs_prefs_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_ukrs; ++i )
	{
		// Read the current ukernel id, ukernel datatype, ukernel function
		// pointer, and ukernel preference.
		const l3ukr_t ukr_id   = ukr_ids[ i ];
		const num_t   ukr_dt   = ukr_dts[ i ];
		      void_fp ukr_fp   = ukr_fps[ i ];
		const bool    ukr_pref = ukr_prefs[ i ];

		// Index into the func_t and mbool_t for the current kernel id
		// being processed.
		func_t*       vukrs  = &cntx_l3_vir_ukrs[ ukr_id ];
		func_t*       ukrs   = &cntx_l3_nat_ukrs[ ukr_id ];
		mbool_t*      prefs  = &cntx_l3_nat_ukrs_prefs[ ukr_id ];

		// Store the ukernel function pointer and preference values into
		// the context. Notice that we redundantly store the native
		// ukernel address in both the native and virtual ukernel slots
		// in the context. This is standard practice when creating a
		// native context. (Induced method contexts will overwrite the
		// virtual function pointer with the address of the appropriate
		// virtual ukernel.)
		bli_func_set_dt( ukr_fp, ukr_dt, vukrs );
		bli_func_set_dt( ukr_fp, ukr_dt, ukrs );
		bli_mbool_set_dt( ukr_pref, ukr_dt, prefs );
	}

	// Free the temporary local arrays.
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	bli_free_intl( ukr_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	bli_free_intl( ukr_dts );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	bli_free_intl( ukr_fps );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_nat_ukrs(): " );
	#endif
	bli_free_intl( ukr_prefs );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l3_vir_ukrs( dim_t n_ukrs, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default level-3 virtual microkernels. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default
	// microkernels across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_l3_vir_ukrs
	   (
	     dim_t   n_ukrs,
	     l3ukr_t ukr0_id, num_t dt0, void_fp ukr0_fp,
	     l3ukr_t ukr1_id, num_t dt1, void_fp ukr1_fp,
	     l3ukr_t ukr2_id, num_t dt2, void_fp ukr2_fp,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_vir_ukrs(): " );
	#endif
	l3ukr_t* ukr_ids   = bli_malloc_intl( n_ukrs * sizeof( l3ukr_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_vir_ukrs(): " );
	#endif
	num_t*   ukr_dts   = bli_malloc_intl( n_ukrs * sizeof( num_t   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_vir_ukrs(): " );
	#endif
	void_fp* ukr_fps   = bli_malloc_intl( n_ukrs * sizeof( void_fp ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_ukrs );

	// Process n_ukrs tuples.
	for ( i = 0; i < n_ukrs; ++i )
	{
		// Here, we query the variable argument list for:
		// - the l3ukr_t of the kernel we're about to process,
		// - the datatype of the kernel, and
		// - the kernel function pointer.
		// that we need to store to the context.
		const l3ukr_t  ukr_id   = ( l3ukr_t )va_arg( args, l3ukr_t );
		const num_t    ukr_dt   = ( num_t   )va_arg( args, num_t   );
		      void_fp  ukr_fp   = ( void_fp )va_arg( args, void_fp );

		// Store the values in our temporary arrays.
		ukr_ids[ i ]   = ukr_id;
		ukr_dts[ i ]   = ukr_dt;
		ukr_fps[ i ]   = ukr_fp;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the addresses of:
	// - the l3 virtual ukernel func_t array
	func_t*  cntx_l3_vir_ukrs       = bli_cntx_l3_vir_ukrs_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_ukrs; ++i )
	{
		// Read the current ukernel id, ukernel datatype, ukernel function
		// pointer, and ukernel preference.
		const l3ukr_t ukr_id   = ukr_ids[ i ];
		const num_t   ukr_dt   = ukr_dts[ i ];
		      void_fp ukr_fp   = ukr_fps[ i ];

		// Index into the func_t and mbool_t for the current kernel id
		// being processed.
		func_t*       vukrs  = &cntx_l3_vir_ukrs[ ukr_id ];

		// Store the ukernel function pointer and preference values into
		// the context. Notice that we redundantly store the native
		// ukernel address in both the native and virtual ukernel slots
		// in the context. This is standard practice when creating a
		// native context. (Induced method contexts will overwrite the
		// virtual function pointer with the address of the appropriate
		// virtual ukernel.)
		bli_func_set_dt( ukr_fp, ukr_dt, vukrs );
	}

	// Free the temporary local arrays.
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_vir_ukrs(): " );
	#endif
	bli_free_intl( ukr_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_vir_ukrs(): " );
	#endif
	bli_free_intl( ukr_dts );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_vir_ukrs(): " );
	#endif
	bli_free_intl( ukr_fps );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l3_sup_thresh( dim_t n_thresh, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default thresholds for small/unpacked matrix handling. It should
	// be called after bli_cntx_init_defaults() so that the context begins
	// with default thresholds.

	/* Example prototypes:

	   void bli_cntx_set_l3_sup_thresh
	   (
	     dim_t      n_thresh,
	     threshid_t th0_id, blksz_t* blksz0,
	     threshid_t th1_id, blksz_t* blksz1,
	     ...
	     cntx_t* cntx
	   );

	*/

	va_list     args;
	dim_t       i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_thresh(): " );
	#endif
	threshid_t* threshids = bli_malloc_intl( n_thresh * sizeof( threshid_t  ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_thresh(): " );
	#endif
	blksz_t**   threshs = bli_malloc_intl( n_thresh * sizeof( blksz_t* ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_thresh );

	// Process n_thresh tuples.
	for ( i = 0; i < n_thresh; ++i )
	{
		// Here, we query the variable argument list for:
		// - the threshid_t of the threshold we're about to process,
		// - the address of the blksz_t object,
		threshid_t th_id  = ( threshid_t )va_arg( args, threshid_t );
		blksz_t*   thresh = ( blksz_t*   )va_arg( args, blksz_t*   );

		// Store the values in our temporary arrays.
		threshids[ i ] = th_id;
		threshs[ i ]   = thresh;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the addresses of:
	// - the threshold array
	blksz_t* cntx_threshs = bli_cntx_l3_sup_thresh_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context. Notice that the blksz_t* pointers were saved, rather than
	// the objects themselves, but we copy the contents of the objects
	// when copying into the context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_thresh; ++i )
	{
		// Read the current blocksize id, blksz_t* pointer, blocksize
		// multiple id, and blocksize scalar.
		threshid_t th_id  = threshids[ i ];
		blksz_t*   thresh = threshs[ i ];

		blksz_t* cntx_thresh = &cntx_threshs[ th_id ];

		// Copy the blksz_t object contents into the appropriate
		// location within the context's blksz_t array.
		//cntx_threshs[ th_id ] = *thresh;
		//bli_blksz_copy( thresh, cntx_thresh );
		bli_blksz_copy_if_pos( thresh, cntx_thresh );
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_thresh(): " );
	#endif
	bli_free_intl( threshs );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_thresh(): " );
	#endif
	bli_free_intl( threshids );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l3_sup_handlers( dim_t n_ops, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default level-3 operation handler for small/unpacked matrices. It
	// should be called after bli_cntx_init_defaults() so that the context
	// begins with default sup handlers across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_l3_sup_handlers
	   (
	     dim_t   n_ops,
	     opid_t  op0_id, void* handler0_fp,
	     opid_t  op1_id, void* handler1_fp,
	     opid_t  op2_id, void* handler2_fp,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_handlers(): " );
	#endif
	opid_t* op_ids = bli_malloc_intl( n_ops * sizeof( opid_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_handlers(): " );
	#endif
	void**  op_fps = bli_malloc_intl( n_ops * sizeof( void*  ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_ops );

	// Process n_ukrs tuples.
	for ( i = 0; i < n_ops; ++i )
	{
		// Here, we query the variable argument list for:
		// - the opid_t of the operation we're about to process,
		// - the sup handler function pointer
		// that we need to store to the context.
		const opid_t op_id = ( opid_t )va_arg( args, opid_t );
		      void*  op_fp = ( void*  )va_arg( args, void*  );

		// Store the values in our temporary arrays.
		op_ids[ i ] = op_id;
		op_fps[ i ] = op_fp;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the addresses of:
	// - the l3 small/unpacked handlers array
	void** cntx_l3_sup_handlers = bli_cntx_l3_sup_handlers_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	// Process each operation id tuple provided.
	for ( i = 0; i < n_ops; ++i )
	{
		// Read the current operation id and handler function pointer.
		const opid_t op_id = op_ids[ i ];
		      void*  op_fp = op_fps[ i ];

		// Store the sup handler function pointer into the slot for the
		// specified operation id.
		cntx_l3_sup_handlers[ op_id ] = op_fp;
	}

	// Free the temporary local arrays.
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_handlers(): " );
	#endif
	bli_free_intl( op_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_handlers(): " );
	#endif
	bli_free_intl( op_fps );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l3_sup_blkszs( dim_t n_bs, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default l3 sup blocksizes. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default
	// blocksizes across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_blkszs
	   (
	     dim_t   n_bs,
	     bszid_t bs0_id, blksz_t* blksz0,
	     bszid_t bs1_id, blksz_t* blksz1,
	     bszid_t bs2_id, blksz_t* blksz2,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bszid_t* bszids = bli_malloc_intl( n_bs * sizeof( bszid_t  ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	blksz_t** blkszs = bli_malloc_intl( n_bs * sizeof( blksz_t* ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_bs );

	// Process n_bs tuples.
	for ( i = 0; i < n_bs; ++i )
	{
		// Here, we query the variable argument list for:
		// - the bszid_t of the blocksize we're about to process,
		// - the address of the blksz_t object.
		bszid_t  bs_id = ( bszid_t  )va_arg( args, bszid_t  );
		blksz_t* blksz = ( blksz_t* )va_arg( args, blksz_t* );

		// Store the values in our temporary arrays.
		bszids[ i ] = bs_id;
		blkszs[ i ] = blksz;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the addresses of:
	// - the blocksize object array
	blksz_t* cntx_l3_sup_blkszs = bli_cntx_l3_sup_blkszs_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context. Notice that the blksz_t* pointers were saved, rather than
	// the objects themselves, but we copy the contents of the objects
	// when copying into the context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_bs; ++i )
	{
		// Read the current blocksize id, blksz_t* pointer, blocksize
		// multiple id, and blocksize scalar.
		bszid_t  bs_id = bszids[ i ];
		blksz_t* blksz = blkszs[ i ];

		blksz_t* cntx_l3_sup_blksz = &cntx_l3_sup_blkszs[ bs_id ];

		// Copy the blksz_t object contents into the appropriate
		// location within the context's blksz_t array.
		//cntx_l3_sup_blkszs[ bs_id ] = *blksz;
		//bli_blksz_copy( blksz, cntx_l3_sup_blksz );
		bli_blksz_copy_if_pos( blksz, cntx_l3_sup_blksz );
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( blkszs );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_blkszs(): " );
	#endif
	bli_free_intl( bszids );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l3_sup_kers( dim_t n_ukrs, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default level-3 microkernels for small/unpacked matrices. It
	// should be called after bli_cntx_init_defaults() so that the context
	// begins with default sup micro/millikernels across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_l3_sup_kers
	   (
	     dim_t   n_ukrs,
	     stor3_t stor_id0, num_t dt0, void* ukr0_fp, bool pref0,
	     stor3_t stor_id1, num_t dt1, void* ukr1_fp, bool pref1,
	     stor3_t stor_id2, num_t dt2, void* ukr2_fp, bool pref2,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	stor3_t* st3_ids   = bli_malloc_intl( n_ukrs * sizeof( stor3_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	num_t*   ukr_dts   = bli_malloc_intl( n_ukrs * sizeof( num_t   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	void**   ukr_fps   = bli_malloc_intl( n_ukrs * sizeof( void*   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	bool*    ukr_prefs = bli_malloc_intl( n_ukrs * sizeof( bool    ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_ukrs );

	// Process n_ukrs tuples.
	for ( i = 0; i < n_ukrs; ++i )
	{
		// Here, we query the variable argument list for:
		// - the stor3_t storage case being assigned to the kernel we're
		//   about to process,
		// - the datatype of the kernel,
		// - the kernel function pointer, and
		// - the kernel function storage preference
		// that we need to store to the context.
		const stor3_t  st3_id   = ( stor3_t )va_arg( args, stor3_t );
		const num_t    ukr_dt   = ( num_t   )va_arg( args, num_t   );
		      void*    ukr_fp   = ( void*   )va_arg( args, void*   );
		const bool     ukr_pref = ( bool    )va_arg( args, int     );

		// Store the values in our temporary arrays.
		st3_ids[ i ]   = st3_id;
		ukr_dts[ i ]   = ukr_dt;
		ukr_fps[ i ]   = ukr_fp;
		ukr_prefs[ i ] = ukr_pref;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the addresses of:
	// - the l3 small/unpacked ukernel func_t array
	// - the l3 small/unpacked ukernel preferences array
	func_t*  cntx_l3_sup_kers       = bli_cntx_l3_sup_kers_buf( cntx );
	mbool_t* cntx_l3_sup_kers_prefs = bli_cntx_l3_sup_kers_prefs_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

#if 0
	dim_t sup_map[ BLIS_NUM_LEVEL3_SUP_UKRS ][2];

	// Create the small/unpacked ukernel mappings:
	// - rv -> rrr 0, rcr 2
	// - rg -> rrc 1, rcc 3
	// - cv -> ccr 6, ccc 7
	// - cg -> crr 4, crc 5
	// - rd -> rrc 1
	// - cd -> crc 5
	// - rc -> rcc 3
	// - cr -> crr 4
	// - gx -> xxx 8
	// NOTE: We only need to set one slot in the context l3_sup_kers array
	// for the general-stride/generic ukernel type, but since the loop below
	// needs to be set up to set two slots to accommodate the RV, RG, CV, and
	// CG, ukernel types, we will just be okay with the GX ukernel being set
	// redundantly. (The RD, CD, CR, and RC ukernel types are set redundantly
	// for the same reason.)
	sup_map[ BLIS_GEMMSUP_RV_UKR ][0] = BLIS_RRR;
	sup_map[ BLIS_GEMMSUP_RV_UKR ][1] = BLIS_RCR;
	sup_map[ BLIS_GEMMSUP_RG_UKR ][0] = BLIS_RRC;
	sup_map[ BLIS_GEMMSUP_RG_UKR ][1] = BLIS_RCC;
	sup_map[ BLIS_GEMMSUP_CV_UKR ][0] = BLIS_CCR;
	sup_map[ BLIS_GEMMSUP_CV_UKR ][1] = BLIS_CCC;
	sup_map[ BLIS_GEMMSUP_CG_UKR ][0] = BLIS_CRR;
	sup_map[ BLIS_GEMMSUP_CG_UKR ][1] = BLIS_CRC;

	sup_map[ BLIS_GEMMSUP_RD_UKR ][0] = BLIS_RRC;
	sup_map[ BLIS_GEMMSUP_RD_UKR ][1] = BLIS_RRC;
	sup_map[ BLIS_GEMMSUP_CD_UKR ][0] = BLIS_CRC;
	sup_map[ BLIS_GEMMSUP_CD_UKR ][1] = BLIS_CRC;

	sup_map[ BLIS_GEMMSUP_RC_UKR ][0] = BLIS_RCC;
	sup_map[ BLIS_GEMMSUP_RC_UKR ][1] = BLIS_RCC;
	sup_map[ BLIS_GEMMSUP_CR_UKR ][0] = BLIS_CRR;
	sup_map[ BLIS_GEMMSUP_CR_UKR ][1] = BLIS_CRR;

	sup_map[ BLIS_GEMMSUP_GX_UKR ][0] = BLIS_XXX;
	sup_map[ BLIS_GEMMSUP_GX_UKR ][1] = BLIS_XXX;
#endif

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_ukrs; ++i )
	{
		// Read the current stor3_t id, ukernel datatype, ukernel function
		// pointer, and ukernel preference.
		const stor3_t st3_id   = st3_ids[ i ];
		const num_t   ukr_dt   = ukr_dts[ i ];
		      void*   ukr_fp   = ukr_fps[ i ];
		const bool    ukr_pref = ukr_prefs[ i ];

		// Index to the func_t and mbool_t for the current stor3_t id
		// being processed.
		func_t*  ukrs   = &cntx_l3_sup_kers[ st3_id ];
		mbool_t* prefs  = &cntx_l3_sup_kers_prefs[ st3_id ];

		// Store the ukernel function pointer and preference values into
		// the stor3_t location in the context.
		bli_func_set_dt( ukr_fp, ukr_dt, ukrs );
		bli_mbool_set_dt( ukr_pref, ukr_dt, prefs );
	}

	// Free the temporary local arrays.
	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	bli_free_intl( st3_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	bli_free_intl( ukr_dts );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	bli_free_intl( ukr_fps );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l3_sup_kers(): " );
	#endif
	bli_free_intl( ukr_prefs );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l1f_kers( dim_t n_kers, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default level-1f kernels. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default l1f
	// kernels across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_l1f_kers
	   (
	     dim_t   n_ukrs,
	     l1fkr_t ker0_id, num_t ker0_dt, void_fp ker0_fp,
	     l1fkr_t ker1_id, num_t ker1_dt, void_fp ker1_fp,
	     l1fkr_t ker2_id, num_t ker2_dt, void_fp ker2_fp,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1f_kers(): " );
	#endif
	l1fkr_t* ker_ids   = bli_malloc_intl( n_kers * sizeof( l1fkr_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1f_kers(): " );
	#endif
	num_t*   ker_dts   = bli_malloc_intl( n_kers * sizeof( num_t   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1f_kers(): " );
	#endif
	void_fp* ker_fps   = bli_malloc_intl( n_kers * sizeof( void_fp ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_kers );

	// Process n_kers tuples.
	for ( i = 0; i < n_kers; ++i )
	{
		// Here, we query the variable argument list for:
		// - the l1fkr_t of the kernel we're about to process,
		// - the datatype of the kernel, and
		// - the kernel function pointer
		// that we need to store to the context.
		const l1fkr_t  ker_id   = ( l1fkr_t )va_arg( args, l1fkr_t );
		const num_t    ker_dt   = ( num_t   )va_arg( args, num_t   );
		      void_fp  ker_fp   = ( void_fp )va_arg( args, void_fp );

		// Store the values in our temporary arrays.
		ker_ids[ i ]   = ker_id;
		ker_dts[ i ]   = ker_dt;
		ker_fps[ i ]   = ker_fp;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the address of:
	// - the level-1f kernels func_t array
	func_t* cntx_l1f_kers = bli_cntx_l1f_kers_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_kers; ++i )
	{
		// Read the current kernel id, kernel datatype, and kernel function
		// pointer.
		const l1fkr_t ker_id   = ker_ids[ i ];
		const num_t   ker_dt   = ker_dts[ i ];
		      void_fp ker_fp   = ker_fps[ i ];

		// Index into the func_t and mbool_t for the current kernel id
		// being processed.
		func_t*       kers     = &cntx_l1f_kers[ ker_id ];

		// Store the ukernel function pointer and preference values into
		// the context.
		bli_func_set_dt( ker_fp, ker_dt, kers );
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1f_kers(): " );
	#endif
	bli_free_intl( ker_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1f_kers(): " );
	#endif
	bli_free_intl( ker_dts );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1f_kers(): " );
	#endif
	bli_free_intl( ker_fps );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_l1v_kers( dim_t n_kers, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default level-1v kernels. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default l1v
	// kernels across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_l1v_kers
	   (
	     dim_t   n_ukrs,
	     l1vkr_t ker0_id, num_t ker0_dt, void_fp ker0_fp,
	     l1vkr_t ker1_id, num_t ker1_dt, void_fp ker1_fp,
	     l1vkr_t ker2_id, num_t ker2_dt, void_fp ker2_fp,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1v_kers(): " );
	#endif
	l1vkr_t* ker_ids   = bli_malloc_intl( n_kers * sizeof( l1vkr_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1v_kers(): " );
	#endif
	num_t*   ker_dts   = bli_malloc_intl( n_kers * sizeof( num_t   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1v_kers(): " );
	#endif
	void_fp* ker_fps   = bli_malloc_intl( n_kers * sizeof( void_fp ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_kers );

	// Process n_kers tuples.
	for ( i = 0; i < n_kers; ++i )
	{
		// Here, we query the variable argument list for:
		// - the l1vkr_t of the kernel we're about to process,
		// - the datatype of the kernel, and
		// - the kernel function pointer
		// that we need to store to the context.
		const l1vkr_t  ker_id   = ( l1vkr_t )va_arg( args, l1vkr_t );
		const num_t    ker_dt   = ( num_t   )va_arg( args, num_t   );
		      void_fp  ker_fp   = ( void_fp )va_arg( args, void_fp );

		// Store the values in our temporary arrays.
		ker_ids[ i ]   = ker_id;
		ker_dts[ i ]   = ker_dt;
		ker_fps[ i ]   = ker_fp;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the address of:
	// - the level-1v kernels func_t array
	func_t* cntx_l1v_kers = bli_cntx_l1v_kers_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_kers; ++i )
	{
		// Read the current kernel id, kernel datatype, and kernel function
		// pointer.
		const l1vkr_t ker_id   = ker_ids[ i ];
		const num_t   ker_dt   = ker_dts[ i ];
		      void_fp ker_fp   = ker_fps[ i ];

		// Index into the func_t and mbool_t for the current kernel id
		// being processed.
		func_t*       kers     = &cntx_l1v_kers[ ker_id ];

		// Store the ukernel function pointer and preference values into
		// the context.
		bli_func_set_dt( ker_fp, ker_dt, kers );
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1v_kers(): " );
	#endif
	bli_free_intl( ker_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1v_kers(): " );
	#endif
	bli_free_intl( ker_dts );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_l1v_kers(): " );
	#endif
	bli_free_intl( ker_fps );
}

// -----------------------------------------------------------------------------

void bli_cntx_set_packm_kers( dim_t n_kers, ... )
{
	// This function can be called from the bli_cntx_init_*() function for
	// a particular architecture if the kernel developer wishes to use
	// non-default packing kernels. It should be called after
	// bli_cntx_init_defaults() so that the context begins with default packm
	// kernels across all datatypes.

	/* Example prototypes:

	   void bli_cntx_set_packm_kers
	   (
	     dim_t   n_ukrs,
	     l1mkr_t ker0_id, num_t ker0_dt, void_fp ker0_fp,
	     l1mkr_t ker1_id, num_t ker1_dt, void_fp ker1_fp,
	     l1mkr_t ker2_id, num_t ker2_dt, void_fp ker2_fp,
	     ...
	     cntx_t* cntx
	   );
	*/

	va_list   args;
	dim_t     i;

	// Allocate some temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_packm_kers(): " );
	#endif
	l1mkr_t* ker_ids   = bli_malloc_intl( n_kers * sizeof( l1mkr_t ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_packm_kers(): " );
	#endif
	num_t*   ker_dts   = bli_malloc_intl( n_kers * sizeof( num_t   ) );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_packm_kers(): " );
	#endif
	void_fp* ker_fps   = bli_malloc_intl( n_kers * sizeof( void_fp ) );

	// -- Begin variable argument section --

	// Initialize variable argument environment.
	va_start( args, n_kers );

	// Process n_kers tuples.
	for ( i = 0; i < n_kers; ++i )
	{
		// Here, we query the variable argument list for:
		// - the l1mkr_t of the kernel we're about to process,
		// - the datatype of the kernel, and
		// - the kernel function pointer
		// that we need to store to the context.
		const l1mkr_t  ker_id   = ( l1mkr_t )va_arg( args, l1mkr_t );
		const num_t    ker_dt   = ( num_t   )va_arg( args, num_t   );
		      void_fp  ker_fp   = ( void_fp )va_arg( args, void_fp );

		// Store the values in our temporary arrays.
		ker_ids[ i ]   = ker_id;
		ker_dts[ i ]   = ker_dt;
		ker_fps[ i ]   = ker_fp;
	}

	// The last argument should be the context pointer.
	cntx_t* cntx = ( cntx_t* )va_arg( args, cntx_t* );

	// Shutdown variable argument environment and clean up stack.
	va_end( args );

	// -- End variable argument section --

	// Query the context for the address of:
	// - the packm kernels func_t array
	func_t* cntx_packm_kers = bli_cntx_packm_kers_buf( cntx );

	// Now that we have the context address, we want to copy the values
	// from the temporary buffers into the corresponding buffers in the
	// context.

	// Process each blocksize id tuple provided.
	for ( i = 0; i < n_kers; ++i )
	{
		// Read the current kernel id, kernel datatype, and kernel function
		// pointer.
		const l1mkr_t ker_id   = ker_ids[ i ];
		const num_t   ker_dt   = ker_dts[ i ];
		      void_fp ker_fp   = ker_fps[ i ];

		// Index into the func_t and mbool_t for the current kernel id
		// being processed.
		func_t*       kers     = &cntx_packm_kers[ ker_id ];

		// Store the ukernel function pointer and preference values into
		// the context.
		bli_func_set_dt( ker_fp, ker_dt, kers );
	}

	// Free the temporary local arrays.

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_packm_kers(): " );
	#endif
	bli_free_intl( ker_ids );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_packm_kers(): " );
	#endif
	bli_free_intl( ker_dts );

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_cntx_set_packm_kers(): " );
	#endif
	bli_free_intl( ker_fps );
}

// -----------------------------------------------------------------------------

void bli_cntx_print( cntx_t* cntx )
{
	dim_t i;

	// Print the values stored in the blksz_t objects.
	printf( "                               s                d                c                z\n" );

	for ( i = 0; i < BLIS_NUM_BLKSZS; ++i )
	{
		printf( "blksz/mult %2lu:  %13lu/%2lu %13lu/%2lu %13lu/%2lu %13lu/%2lu\n",
		         ( unsigned long )i,
		         ( unsigned long )bli_cntx_get_blksz_def_dt( BLIS_FLOAT,    i, cntx ),
		         ( unsigned long )bli_cntx_get_bmult_dt    ( BLIS_FLOAT,    i, cntx ),
		         ( unsigned long )bli_cntx_get_blksz_def_dt( BLIS_DOUBLE,   i, cntx ),
		         ( unsigned long )bli_cntx_get_bmult_dt    ( BLIS_DOUBLE,   i, cntx ),
		         ( unsigned long )bli_cntx_get_blksz_def_dt( BLIS_SCOMPLEX, i, cntx ),
		         ( unsigned long )bli_cntx_get_bmult_dt    ( BLIS_SCOMPLEX, i, cntx ),
		         ( unsigned long )bli_cntx_get_blksz_def_dt( BLIS_DCOMPLEX, i, cntx ),
		         ( unsigned long )bli_cntx_get_bmult_dt    ( BLIS_DCOMPLEX, i, cntx )
		      );
	}

	for ( i = 0; i < BLIS_NUM_LEVEL3_UKRS; ++i )
	{
		func_t* ukr = bli_cntx_get_l3_vir_ukrs( i, cntx );

		printf( "l3 vir ukr %2lu:  %16p %16p %16p %16p\n",
		        ( unsigned long )i,
		        bli_func_get_dt( BLIS_FLOAT,    ukr ),
		        bli_func_get_dt( BLIS_DOUBLE,   ukr ),
		        bli_func_get_dt( BLIS_SCOMPLEX, ukr ),
		        bli_func_get_dt( BLIS_DCOMPLEX, ukr )
		      );
	}

	for ( i = 0; i < BLIS_NUM_3OP_RC_COMBOS; ++i )
	{
		func_t* ukr = bli_cntx_get_l3_sup_kers( i, cntx );

		printf( "l3 sup ukr %2lu:  %16p %16p %16p %16p\n",
		        ( unsigned long )i,
		        bli_func_get_dt( BLIS_FLOAT,    ukr ),
		        bli_func_get_dt( BLIS_DOUBLE,   ukr ),
		        bli_func_get_dt( BLIS_SCOMPLEX, ukr ),
		        bli_func_get_dt( BLIS_DCOMPLEX, ukr )
		      );
	}

	for ( i = 0; i < BLIS_NUM_LEVEL1F_KERS; ++i )
	{
		func_t* ker = bli_cntx_get_l1f_kers( i, cntx );

		printf( "l1f ker    %2lu:  %16p %16p %16p %16p\n",
		        ( unsigned long )i,
		        bli_func_get_dt( BLIS_FLOAT,    ker ),
		        bli_func_get_dt( BLIS_DOUBLE,   ker ),
		        bli_func_get_dt( BLIS_SCOMPLEX, ker ),
		        bli_func_get_dt( BLIS_DCOMPLEX, ker )
		      );
	}

	for ( i = 0; i < BLIS_NUM_LEVEL1V_KERS; ++i )
	{
		func_t* ker = bli_cntx_get_l1v_kers( i, cntx );

		printf( "l1v ker    %2lu:  %16p %16p %16p %16p\n",
		        ( unsigned long )i,
		        bli_func_get_dt( BLIS_FLOAT,    ker ),
		        bli_func_get_dt( BLIS_DOUBLE,   ker ),
		        bli_func_get_dt( BLIS_SCOMPLEX, ker ),
		        bli_func_get_dt( BLIS_DCOMPLEX, ker )
		      );
	}

	{
		ind_t method = bli_cntx_method( cntx );

		printf( "ind method   : %lu\n", ( unsigned long )method );
	}
}

