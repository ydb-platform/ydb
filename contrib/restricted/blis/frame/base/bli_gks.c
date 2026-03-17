/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018-2019, Advanced Micro Devices, Inc.

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

// The array of cntx_t* pointers to cache modified contexts used by
// induced methods.
static cntx_t** gks[ BLIS_NUM_ARCHS ];

// The array of function pointers holding the registered context initialization
// functions for induced methods.
static void_fp  cntx_ind_init[ BLIS_NUM_ARCHS ];

// The array of function pointers holding the registered context initialization
// functions for reference kernels.
static void_fp  cntx_ref_init[ BLIS_NUM_ARCHS ];

// Define a function pointer type for context initialization functions.
typedef void (*nat_cntx_init_ft)( cntx_t* cntx );
typedef void (*ref_cntx_init_ft)( cntx_t* cntx );
typedef void (*ind_cntx_init_ft)( ind_t method, num_t dt, cntx_t* cntx );

// -----------------------------------------------------------------------------

void bli_gks_init( void )
{
	{
		// Initialize the internal data structure we use to track registered
		// contexts.
		bli_gks_init_index();

		// Register a context for each architecture that was #define'd in
		// bli_config.h.

		// Intel architectures
#ifdef BLIS_CONFIG_SKX
		bli_gks_register_cntx( BLIS_ARCH_SKX,         bli_cntx_init_skx,
		                                              bli_cntx_init_skx_ref,
		                                              bli_cntx_init_skx_ind );
#endif
#ifdef BLIS_CONFIG_KNL
		bli_gks_register_cntx( BLIS_ARCH_KNL,         bli_cntx_init_knl,
		                                              bli_cntx_init_knl_ref,
		                                              bli_cntx_init_knl_ind );
#endif
#ifdef BLIS_CONFIG_KNC
		bli_gks_register_cntx( BLIS_ARCH_KNC,         bli_cntx_init_knc,
		                                              bli_cntx_init_knc_ref,
		                                              bli_cntx_init_knc_ind );
#endif
#ifdef BLIS_CONFIG_HASWELL
		bli_gks_register_cntx( BLIS_ARCH_HASWELL,     bli_cntx_init_haswell,
		                                              bli_cntx_init_haswell_ref,
		                                              bli_cntx_init_haswell_ind );
#endif
#ifdef BLIS_CONFIG_SANDYBRIDGE
		bli_gks_register_cntx( BLIS_ARCH_SANDYBRIDGE, bli_cntx_init_sandybridge,
		                                              bli_cntx_init_sandybridge_ref,
		                                              bli_cntx_init_sandybridge_ind );
#endif
#ifdef BLIS_CONFIG_PENRYN
		bli_gks_register_cntx( BLIS_ARCH_PENRYN,      bli_cntx_init_penryn,
		                                              bli_cntx_init_penryn_ref,
		                                              bli_cntx_init_penryn_ind );
#endif

		// AMD architectures
#ifdef BLIS_CONFIG_ZEN2
		bli_gks_register_cntx( BLIS_ARCH_ZEN2,        bli_cntx_init_zen2,
		                                              bli_cntx_init_zen2_ref,
		                                              bli_cntx_init_zen2_ind );
#endif
#ifdef BLIS_CONFIG_ZEN
		bli_gks_register_cntx( BLIS_ARCH_ZEN,         bli_cntx_init_zen,
		                                              bli_cntx_init_zen_ref,
		                                              bli_cntx_init_zen_ind );
#endif
#ifdef BLIS_CONFIG_EXCAVATOR
		bli_gks_register_cntx( BLIS_ARCH_EXCAVATOR,   bli_cntx_init_excavator,
		                                              bli_cntx_init_excavator_ref,
		                                              bli_cntx_init_excavator_ind );
#endif
#ifdef BLIS_CONFIG_STEAMROLLER
		bli_gks_register_cntx( BLIS_ARCH_STEAMROLLER, bli_cntx_init_steamroller,
		                                              bli_cntx_init_steamroller_ref,
		                                              bli_cntx_init_steamroller_ind );
#endif
#ifdef BLIS_CONFIG_PILEDRIVER
		bli_gks_register_cntx( BLIS_ARCH_PILEDRIVER,  bli_cntx_init_piledriver,
		                                              bli_cntx_init_piledriver_ref,
		                                              bli_cntx_init_piledriver_ind );
#endif
#ifdef BLIS_CONFIG_BULLDOZER
		bli_gks_register_cntx( BLIS_ARCH_BULLDOZER,   bli_cntx_init_bulldozer,
		                                              bli_cntx_init_bulldozer_ref,
		                                              bli_cntx_init_bulldozer_ind );
#endif

		// ARM architectures
#ifdef BLIS_CONFIG_THUNDERX2
		bli_gks_register_cntx( BLIS_ARCH_THUNDERX2,   bli_cntx_init_thunderx2,
		                                              bli_cntx_init_thunderx2_ref,
		                                              bli_cntx_init_thunderx2_ind );
#endif
#ifdef BLIS_CONFIG_CORTEXA57
		bli_gks_register_cntx( BLIS_ARCH_CORTEXA57,   bli_cntx_init_cortexa57,
		                                              bli_cntx_init_cortexa57_ref,
		                                              bli_cntx_init_cortexa57_ind );
#endif
#ifdef BLIS_CONFIG_CORTEXA53
		bli_gks_register_cntx( BLIS_ARCH_CORTEXA53,   bli_cntx_init_cortexa53,
		                                              bli_cntx_init_cortexa53_ref,
		                                              bli_cntx_init_cortexa53_ind );
#endif
#ifdef BLIS_CONFIG_CORTEXA15
		bli_gks_register_cntx( BLIS_ARCH_CORTEXA15,   bli_cntx_init_cortexa15,
		                                              bli_cntx_init_cortexa15_ref,
		                                              bli_cntx_init_cortexa15_ind );
#endif
#ifdef BLIS_CONFIG_CORTEXA9
		bli_gks_register_cntx( BLIS_ARCH_CORTEXA9,    bli_cntx_init_cortexa9,
		                                              bli_cntx_init_cortexa9_ref,
		                                              bli_cntx_init_cortexa9_ind );
#endif

		// IBM architectures
#ifdef BLIS_CONFIG_POWER10
		bli_gks_register_cntx( BLIS_ARCH_POWER10,     bli_cntx_init_power10,
		                                              bli_cntx_init_power10_ref,
		                                              bli_cntx_init_power10_ind );
#endif													  
#ifdef BLIS_CONFIG_POWER9
		bli_gks_register_cntx( BLIS_ARCH_POWER9,      bli_cntx_init_power9,
		                                              bli_cntx_init_power9_ref,
		                                              bli_cntx_init_power9_ind );
#endif
#ifdef BLIS_CONFIG_POWER7
		bli_gks_register_cntx( BLIS_ARCH_POWER7,      bli_cntx_init_power7,
		                                              bli_cntx_init_power7_ref,
		                                              bli_cntx_init_power7_ind );
#endif
#ifdef BLIS_CONFIG_BGQ
		bli_gks_register_cntx( BLIS_ARCH_BGQ,         bli_cntx_init_bgq,
		                                              bli_cntx_init_bgq_ref,
		                                              bli_cntx_init_bgq_ind );
#endif

		// Generic architectures
#ifdef BLIS_CONFIG_GENERIC
		bli_gks_register_cntx( BLIS_ARCH_GENERIC,     bli_cntx_init_generic,
		                                              bli_cntx_init_generic_ref,
		                                              bli_cntx_init_generic_ind );
#endif
	}
}

// -----------------------------------------------------------------------------

void bli_gks_finalize( void )
{
	arch_t id;
	ind_t  ind;

	// BEGIN CRITICAL SECTION
	// NOTE: This critical section is implicit. We assume this function is only
	// called from within the critical section within bli_finalize().
	{

		// Iterate over the architectures in the gks array.
		for ( id = 0; id < BLIS_NUM_ARCHS; ++id )
		{
			cntx_t** restrict gks_id = gks[ id ];

			// Only consider context arrays for architectures that were allocated
			// in the first place.
			if ( gks_id != NULL )
			{
				// Iterate over the induced methods in the current sub-array
				// referenced by cntx_pp.
				for ( ind = 0; ind < BLIS_NUM_IND_METHODS; ++ind )
				{
					cntx_t* restrict gks_id_ind = gks_id[ ind ];

					// If the current context was allocated, free it.
					if ( gks_id_ind != NULL )
					{
						#ifdef BLIS_ENABLE_MEM_TRACING
						printf( "bli_gks_finalize(): cntx for ind_t %d: ", ( int )ind );
						#endif

						bli_free_intl( gks_id_ind );
					}
				}

				#ifdef BLIS_ENABLE_MEM_TRACING
				printf( "bli_gks_finalize(): gks for arch_t %d: ", ( int )id );
				#endif

				// Free the array of BLIS_NUM_IND_METHODS cntx* elements.
				bli_free_intl( gks_id );
			}
		}

	}
	// END CRITICAL SECTION
}

// -----------------------------------------------------------------------------

void bli_gks_init_index( void )
{
	// This function is called by bli_gks_init(). It simply initializes all
	// architecture id elements of the internal arrays to NULL. 

	const size_t gks_size = sizeof( cntx_t* ) * BLIS_NUM_ARCHS;
	const size_t fpa_size = sizeof( void_fp ) * BLIS_NUM_ARCHS;

	// Set every entry in gks and context init function pointer arrays to
	// zero/NULL. This is done so that later on we know which ones were
	// allocated.
	memset( gks,           0, gks_size );
	memset( cntx_ref_init, 0, fpa_size );
	memset( cntx_ind_init, 0, fpa_size );
}

// -----------------------------------------------------------------------------

cntx_t* bli_gks_lookup_nat_cntx
     (
       arch_t id
     )
{
	// Return the address of the (native) context for a given architecture id.
	// This function assumes the architecture has already been registered.

	return bli_gks_lookup_ind_cntx( id, BLIS_NAT );
}

// -----------------------------------------------------------------------------

cntx_t* bli_gks_lookup_ind_cntx
     (
       arch_t id,
       ind_t  ind
     )
{
	// Return the address of the context for a given architecture id and
	// induced method. This function assumes the architecture has already
	// been registered. Note that this function returns NULL if the induced
	// method hasn't yet been called (and thus its context pointer is still
	// NULL).

	// Sanity check: verify that the arch_t id is valid.
	if ( bli_error_checking_is_enabled() )
	{
		err_t e_val = bli_check_valid_arch_id( id );
		bli_check_error_code( e_val );
	}

	// Index into the array of context pointers for the given architecture id,
	// and then index into the subarray for the given induced method.
	cntx_t** restrict gks_id     = gks[ id ];
	cntx_t*  restrict gks_id_ind = gks_id[ ind ];

	// Return the context pointer at gks_id_ind.
	return gks_id_ind;
}

// -----------------------------------------------------------------------------

cntx_t** bli_gks_lookup_id
     (
       arch_t id
     )
{
	// Return the address of the array of context pointers for a given
	// architecture id. This function is only used for sanity check purposes
	// to ensure that the underlying data structures for a particular id are
	// initialized.

	// Index into the array of context pointers for the given architecture id.
	cntx_t** restrict gks_id = gks[ id ];

	// Return the context pointer at gks_id_ind.
	return gks_id;
}

// -----------------------------------------------------------------------------

void bli_gks_register_cntx
     (
       arch_t  id,
       void_fp nat_fp,
       void_fp ref_fp,
       void_fp ind_fp
     )
{
	// This function is called by bli_gks_init() for each architecture that
	// will be supported by BLIS. It takes an architecture id and three
	// function pointers, one to a function that initializes a native context
	// (supplied by the kernel developer), one to a function that initializes
	// a reference context (with function pointers specific to the architecture
	// associated with id), and one to a function that initializes a
	// context for use with induced methods (again, with function pointers
	// to the architecture). The latter two functions are automatically
	// generated by the framework. Unlike with native contexts, we don't
	// actually store the induced contexts until that induced method is
	// called, and we don't ever store reference contexts. For this reason, we
	// can get away with only storing the pointers to the initialization
	// functions for those latter two types of contexts, which we can then
	// call at a later time when those contexts are needed.

	// Sanity check: verify that the arch_t id is valid.
	if ( bli_error_checking_is_enabled() )
	{
		err_t e_val = bli_check_valid_arch_id( id );
		bli_check_error_code( e_val );
	}

	nat_cntx_init_ft f = nat_fp;

	// First, store the function pointers to the context initialization
	// functions for reference kernels and induced method execution. The
	// former will be used whenever we need to obtain reference kernels and
	// latter will be used later on if the user calls a level-3 function
	// with induced execution enabled. 
	cntx_ref_init[ id ] = ref_fp;
	cntx_ind_init[ id ] = ind_fp;

	// If the the context array pointer isn't NULL, then it means the given
	// architecture id has already registered (and the underlying memory
	// allocations and context initializations have already been performed).
	// This is really just a safety feature to prevent memory leaks; this
	// early return should never occur, because the caller should never try
	// to register with an architecture id that has already been registered.
	if ( gks[ id ] != NULL ) return;

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_gks_register_cntx(): " );
	#endif

	// At this point, we know the pointer to the array of cntx_t* is NULL and
	// needs to be allocated. Allocate the memory and initialize it to
	// zeros/NULL, storing the address of the alloacted memory at the element
	// for the current architecture id.
	gks[ id ] = bli_calloc_intl( sizeof( cntx_t* ) * BLIS_NUM_IND_METHODS );

	// Alias the allocated array for readability.
	cntx_t** restrict gks_id = gks[ id ];

	#ifdef BLIS_ENABLE_MEM_TRACING
	printf( "bli_gks_register_cntx(): " );
	#endif

	// Allocate memory for a single context and store the address at
	// the element in the gks[ id ] array that is reserved for native
	// execution.
	gks_id[ BLIS_NAT ] = bli_calloc_intl( sizeof( cntx_t ) );

	// Alias the allocated context address for readability.
	cntx_t* restrict gks_id_nat = gks_id[ BLIS_NAT ];

	// Call the context initialization function on the element of the newly
	// allocated array corresponding to native execution.
	f( gks_id_nat );

	// Verify that cache blocksizes are whole multiples of register blocksizes.
	// Specifically, verify that:
	//   - MC is a whole multiple of MR.
	//   - NC is a whole multiple of NR.
	//   - KC is a whole multiple of KR.
	// These constraints are enforced because it makes it easier to handle diagonals
	// in the macro-kernel implementations. Additionally, we optionally verify that:
	//   - MC is a whole multiple of NR.
	//   - NC is a whole multiple of MR.
	// These latter constraints, guarded by #ifndef BLIS_RELAX_MCNR_NCMR_CONSTRAINTS
	// below, are only enforced when we wish to be able to handle the trsm right-
	// side case handling that swaps A and B, so that B is the triangular matrix,
	// with NR blocking used to pack A and MR blocking used to pack B, with the
	// arguments to the gemmtrsm microkernel swapped at the last minute, as the
	// kernel is called.
	err_t e_val;

	blksz_t* restrict mc = bli_cntx_get_blksz( BLIS_MC, gks_id_nat );
	blksz_t* restrict nc = bli_cntx_get_blksz( BLIS_NC, gks_id_nat );
	blksz_t* restrict kc = bli_cntx_get_blksz( BLIS_KC, gks_id_nat );
	blksz_t* restrict mr = bli_cntx_get_blksz( BLIS_MR, gks_id_nat );
	blksz_t* restrict nr = bli_cntx_get_blksz( BLIS_NR, gks_id_nat );
	blksz_t* restrict kr = bli_cntx_get_blksz( BLIS_KR, gks_id_nat );

	e_val = bli_check_valid_mc_mod_mult( mc, mr ); bli_check_error_code( e_val );
	e_val = bli_check_valid_nc_mod_mult( nc, nr ); bli_check_error_code( e_val );
	e_val = bli_check_valid_kc_mod_mult( kc, kr ); bli_check_error_code( e_val );
#ifndef BLIS_RELAX_MCNR_NCMR_CONSTRAINTS
	e_val = bli_check_valid_mc_mod_mult( mc, nr ); bli_check_error_code( e_val );
	e_val = bli_check_valid_nc_mod_mult( nc, mr ); bli_check_error_code( e_val );
#endif
}

// -----------------------------------------------------------------------------

cntx_t* bli_gks_query_cntx( void )
{
	return bli_gks_query_nat_cntx();
}

cntx_t* bli_gks_query_nat_cntx( void )
{
	bli_init_once();

	// Return the address of the native context for the architecture id
	// corresponding to the current hardware, as determined by
	// bli_arch_query_id().

	// Query the architecture id.
	arch_t id = bli_arch_query_id();

	// Use the architecture id to look up a pointer to its context.
	cntx_t* cntx = bli_gks_lookup_nat_cntx( id );

	return cntx;
}

// -----------------------------------------------------------------------------

cntx_t* bli_gks_query_cntx_noinit( void )
{
	// This function is identical to bli_gks_query_cntx(), except that it
	// does not call bli_init_once().

	// Query the architecture id.
	arch_t id = bli_arch_query_id();

	// Use the architecture id to look up a pointer to its context.
	cntx_t* cntx = bli_gks_lookup_nat_cntx( id );

	return cntx;
}

// -----------------------------------------------------------------------------

// A mutex to allow synchronous access to the gks when it needs to be updated
// with a new entry corresponding to a context for an ind_t value.
static bli_pthread_mutex_t gks_mutex = BLIS_PTHREAD_MUTEX_INITIALIZER;

cntx_t* bli_gks_query_ind_cntx
     (
       ind_t ind,
       num_t dt
     )
{
	bli_init_once();

	cntx_t* gks_id_ind;

	// Return the address of a context that will be suited for executing a
	// level-3 operation via the requested induced method (and datatype) for
	// the architecture id corresponding to the current hardware, as
	// determined by bli_arch_query_id().

	// This function is called when a level-3 operation via induced method is
	// called, e.g. bli_gemm1m(). If this is the first time that induced method
	// is being executed since bli_gks_init(), the necessary context structure
	// is allocated and initialized. If this is not the first time, then the
	// address of a previously-allocated and initialized (cached) context is
	// returned. Note that much of this must be done with mutual exclusion to
	// ensure thread safety and deterministic behavior.

	// Query the architecture id.
	arch_t id = bli_arch_query_id();

	// Sanity check: verify that the arch_t id is valid.
	if ( bli_error_checking_is_enabled() )
	{
		err_t e_val = bli_check_valid_arch_id( id );
		bli_check_error_code( e_val );
	}

	// NOTE: These initial statements can reside outside of the critical section
	// because gks[ id ] should have already been allocated, and the native
	// context in that array should have already been allocated/initialized.

	// Query the gks for the array of context pointers corresponding to the
	// given architecture id.
	cntx_t** restrict gks_id     = gks[ id ];
	cntx_t*  restrict gks_id_nat = gks_id[ BLIS_NAT ];

	// If for some reason the native context was requested, we can return
	// its address early.
	if ( ind == BLIS_NAT ) return gks_id_nat;

	// This function assumes that the architecture idenified by id has
	// already been registered with the gks (which guarantees that
	// gks[ id ] is non-NULL and gks[ id ][ BLIS_NAT ] is also non-NULL
	// and refers to a context initialized with valid data).

	// Acquire the mutex protecting the gks.
	bli_pthread_mutex_lock( &gks_mutex );

	// BEGIN CRITICAL SECTION
	{
		// Alias for readability the element of gks_id associated with the
		// requested induced method.
		gks_id_ind = gks_id[ ind ];

		// If the context pointer is NULL, then we know we must allocate and
		// then initialize the context before returning its address.
		if ( gks_id_ind == NULL )
		{
			// If gks_id_ind is NULL, then we know we must allocate and then
			// initialize the context, storing its address back to
			// gks_id[ ind ].
			gks_id_ind    = bli_calloc_intl( sizeof( cntx_t ) );
			gks_id[ ind ] = gks_id_ind;

			// Before we can call the induced method context initialization
			// function on the newly allocated structure, we must first copy
			// over the contents of the native context.
			*gks_id_ind = *gks_id_nat;
			
			// Use the architecture id to look up the function pointer to the
			// context initialization function for induced methods.
			ind_cntx_init_ft f = cntx_ind_init[ id ];

			// Now we modify the context (so that it contains the proper values
			// for its induced method) by calling the context initialization
			// function for the current induced method. (That function assumes
			// that the context is pre- initialized with values for native
			// execution.)
			f( ind, dt, gks_id_ind );
		}
	}
	// END CRITICAL SECTION

	// Release the mutex protecting the gks.
	bli_pthread_mutex_unlock( &gks_mutex );

	// Return the address of the newly-allocated/initialized context.
	return gks_id_ind;

}

// -----------------------------------------------------------------------------

void bli_gks_init_ref_cntx
    (
      cntx_t* cntx
    )
{
	// Query the architecture id.
	arch_t id = bli_arch_query_id();

	// Sanity check: verify that the arch_t id is valid.
	if ( bli_error_checking_is_enabled() )
	{
		err_t e_val = bli_check_valid_arch_id( id );
		bli_check_error_code( e_val );
	}

	// Obtain the function pointer to the context initialization function for
	// reference kernels.
	ref_cntx_init_ft f = cntx_ref_init[ id ];

	// Initialize the caller's context with reference kernels and related values.
	f( cntx );
}

// -----------------------------------------------------------------------------

bool bli_gks_cntx_l3_nat_ukr_is_ref
     (
       num_t   dt,
       l3ukr_t ukr_id,
       cntx_t* cntx
     )
{
	cntx_t ref_cntx;

	// Initialize a context with reference kernels for the arch_t id queried
	// via bli_arch_query_id().
	bli_gks_init_ref_cntx( &ref_cntx );

	// Query each context for the micro-kernel function pointer for the
	// specified datatype.
	void_fp ref_fp = bli_cntx_get_l3_nat_ukr_dt( dt, ukr_id, &ref_cntx );
	void_fp fp     = bli_cntx_get_l3_nat_ukr_dt( dt, ukr_id, cntx );

	// Return the result.
	return fp == ref_fp;
}

//
// -- level-3 micro-kernel implementation strings ------------------------------
//

static char* bli_gks_l3_ukr_impl_str[BLIS_NUM_UKR_IMPL_TYPES] =
{
	"refrnce",
	"virtual",
	"optimzd",
	"notappl",
};

// -----------------------------------------------------------------------------

char* bli_gks_l3_ukr_impl_string( l3ukr_t ukr, ind_t method, num_t dt )
{
	kimpl_t ki;

	// Query the context for the current induced method and datatype, and
	// then query the ukernel function pointer for the given datatype from
	// that context.
	cntx_t* cntx  = bli_gks_query_ind_cntx( method, dt );
	void_fp fp    = bli_cntx_get_l3_vir_ukr_dt( dt, ukr, cntx );

	// Check whether the ukernel function pointer is NULL for the given
	// datatype. If it is NULL, return the string for not applicable.
	// Otherwise, query the ukernel implementation type using the method
	// provided and return the associated string.
	if ( fp == NULL )
		ki = BLIS_NOTAPPLIC_UKERNEL;
	else
		ki = bli_gks_l3_ukr_impl_type( ukr, method, dt );

	return bli_gks_l3_ukr_impl_str[ ki ];
}

#if 0
char* bli_gks_l3_ukr_avail_impl_string( l3ukr_t ukr, num_t dt )
{
	opid_t  oper;
	ind_t   method;
	kimpl_t ki;

	// We need to decide which operation we will use to query the
	// current available induced method. If the ukr type given is
	// BLIS_GEMM_UKR, we use gemm. Otherwise, we use trsm (since
	// the four other defined ukr types are trsm-related).
	if ( ukr == BLIS_GEMM_UKR ) oper = BLIS_GEMM;
	else                        oper = BLIS_TRSM;

	// Query the current available induced method using the
	// chosen operation id type.
	method = bli_l3_ind_oper_find_avail( oper, dt );

	// Query the ukernel implementation type using the current
	// available method.
	ki = bli_gks_l3_ukr_impl_type( ukr, method, dt );

	return bli_ukr_impl_str[ ki ];
}
#endif

kimpl_t bli_gks_l3_ukr_impl_type( l3ukr_t ukr, ind_t method, num_t dt )
{
	// If the current available induced method is not native, it
	// must be virtual.
	if ( method != BLIS_NAT ) return BLIS_VIRTUAL_UKERNEL;
	else
	{
		// If the current available induced method for the gemm
		// operation is native, then it might be reference or
		// optimized. To determine which, we compare the
		// datatype-specific function pointer within the ukrs
		// object corresponding to the current available induced
		// method to the typed function pointer within the known
		// reference ukrs object.

		cntx_t ref_cntx_l;

		// Query the architecture id.
		arch_t id = bli_arch_query_id();

		// Sanity check: verify that the arch_t id is valid.
		if ( bli_error_checking_is_enabled() )
		{
			err_t e_val = bli_check_valid_arch_id( id );
			bli_check_error_code( e_val );
		}

		// Obtain the function pointer to the context initialization function
		// for reference kernels.
		ref_cntx_init_ft f = cntx_ref_init[ id ];

		// Initialize a local context with reference kernels and related values.
		f( &ref_cntx_l );

		// Query the native context from the gks.
		cntx_t* nat_cntx = bli_gks_lookup_nat_cntx( id );

		// Query the native ukernel func_t from both the native and reference
		// contexts.
		void_fp nat_fp = bli_cntx_get_l3_nat_ukr_dt( dt, ukr, nat_cntx );
		void_fp ref_fp = bli_cntx_get_l3_nat_ukr_dt( dt, ukr, &ref_cntx_l );

		if ( nat_fp == ref_fp ) return BLIS_REFERENCE_UKERNEL;
		else                    return BLIS_OPTIMIZED_UKERNEL;
	}
}

