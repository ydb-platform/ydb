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

#ifndef BLIS_RNTM_H
#define BLIS_RNTM_H


// Runtime object type (defined in bli_type_defs.h)

/*
typedef struct rntm_s
{
	bool      auto_factor;

	dim_t     num_threads;
	dim_t*    thrloop;
	bool      pack_a;
	bool      pack_b;
	bool      l3_sup;

	pool_t*   sba_pool;
	membrk_t* membrk;

} rntm_t;
*/

//
// -- rntm_t query (public API) ------------------------------------------------
//

BLIS_INLINE bool bli_rntm_auto_factor( rntm_t* rntm )
{
	return rntm->auto_factor;
}

BLIS_INLINE dim_t bli_rntm_num_threads( rntm_t* rntm )
{
	return rntm->num_threads;
}

BLIS_INLINE dim_t bli_rntm_ways_for( bszid_t bszid, rntm_t* rntm )
{
	return rntm->thrloop[ bszid ];
}

BLIS_INLINE dim_t bli_rntm_jc_ways( rntm_t* rntm )
{
	return bli_rntm_ways_for( BLIS_NC, rntm );
}
BLIS_INLINE dim_t bli_rntm_pc_ways( rntm_t* rntm )
{
	return bli_rntm_ways_for( BLIS_KC, rntm );
}
BLIS_INLINE dim_t bli_rntm_ic_ways( rntm_t* rntm )
{
	return bli_rntm_ways_for( BLIS_MC, rntm );
}
BLIS_INLINE dim_t bli_rntm_jr_ways( rntm_t* rntm )
{
	return bli_rntm_ways_for( BLIS_NR, rntm );
}
BLIS_INLINE dim_t bli_rntm_ir_ways( rntm_t* rntm )
{
	return bli_rntm_ways_for( BLIS_MR, rntm );
}
BLIS_INLINE dim_t bli_rntm_pr_ways( rntm_t* rntm )
{
	return bli_rntm_ways_for( BLIS_KR, rntm );
}

BLIS_INLINE bool bli_rntm_pack_a( rntm_t* rntm )
{
	return ( bool )( rntm->pack_a );
}
BLIS_INLINE bool bli_rntm_pack_b( rntm_t* rntm )
{
	return ( bool )( rntm->pack_b );
}

BLIS_INLINE bool bli_rntm_l3_sup( rntm_t* rntm )
{
	return rntm->l3_sup;
}

//
// -- rntm_t query (internal use only) -----------------------------------------
//

BLIS_INLINE pool_t* bli_rntm_sba_pool( rntm_t* rntm )
{
	return rntm->sba_pool;
}

BLIS_INLINE membrk_t* bli_rntm_membrk( rntm_t* rntm )
{
	return rntm->membrk;
}

#if 0
BLIS_INLINE dim_t bli_rntm_equals( rntm_t* rntm1, rntm_t* rntm2 )
{
	const bool nt = bli_rntm_num_threads( rntm1 ) == bli_rntm_num_threads( rntm2 );
	const bool jc = bli_rntm_jc_ways( rntm1 ) == bli_rntm_jc_ways( rntm2 );
	const bool pc = bli_rntm_pc_ways( rntm1 ) == bli_rntm_pc_ways( rntm2 );
	const bool ic = bli_rntm_ic_ways( rntm1 ) == bli_rntm_ic_ways( rntm2 );
	const bool jr = bli_rntm_jr_ways( rntm1 ) == bli_rntm_jr_ways( rntm2 );
	const bool ir = bli_rntm_ir_ways( rntm1 ) == bli_rntm_ir_ways( rntm2 );
	const bool pr = bli_rntm_pr_ways( rntm1 ) == bli_rntm_pr_ways( rntm2 );

	if ( nt && jc && pc && ic && jr && ir && pr ) return TRUE;
	else                                          return FALSE;
}
#endif

//
// -- rntm_t modification (internal use only) ----------------------------------
//

BLIS_INLINE void bli_rntm_set_auto_factor_only( bool auto_factor, rntm_t* rntm )
{
	rntm->auto_factor = auto_factor;
}

BLIS_INLINE void bli_rntm_set_num_threads_only( dim_t nt, rntm_t* rntm )
{
	rntm->num_threads = nt;
}

BLIS_INLINE void bli_rntm_set_ways_for_only( bszid_t loop, dim_t n_ways, rntm_t* rntm )
{
	rntm->thrloop[ loop ] = n_ways;
}

BLIS_INLINE void bli_rntm_set_jc_ways_only( dim_t ways, rntm_t* rntm )
{
	bli_rntm_set_ways_for_only( BLIS_NC, ways, rntm );
}
BLIS_INLINE void bli_rntm_set_pc_ways_only( dim_t ways, rntm_t* rntm )
{
	bli_rntm_set_ways_for_only( BLIS_KC, ways, rntm );
}
BLIS_INLINE void bli_rntm_set_ic_ways_only( dim_t ways, rntm_t* rntm )
{
	bli_rntm_set_ways_for_only( BLIS_MC, ways, rntm );
}
BLIS_INLINE void bli_rntm_set_jr_ways_only( dim_t ways, rntm_t* rntm )
{
	bli_rntm_set_ways_for_only( BLIS_NR, ways, rntm );
}
BLIS_INLINE void bli_rntm_set_ir_ways_only( dim_t ways, rntm_t* rntm )
{
	bli_rntm_set_ways_for_only( BLIS_MR, ways, rntm );
}
BLIS_INLINE void bli_rntm_set_pr_ways_only( dim_t ways, rntm_t* rntm )
{
	bli_rntm_set_ways_for_only( BLIS_KR, ways, rntm );
}

BLIS_INLINE void bli_rntm_set_ways_only( dim_t jc, dim_t pc, dim_t ic, dim_t jr, dim_t ir, rntm_t* rntm )
{
	// Record the number of ways of parallelism per loop.
	bli_rntm_set_jc_ways_only( jc, rntm );
	bli_rntm_set_pc_ways_only( pc, rntm );
	bli_rntm_set_ic_ways_only( ic, rntm );
	bli_rntm_set_jr_ways_only( jr, rntm );
	bli_rntm_set_ir_ways_only( ir, rntm );
	bli_rntm_set_pr_ways_only(  1, rntm );
}

BLIS_INLINE void bli_rntm_set_sba_pool( pool_t* sba_pool, rntm_t* rntm )
{
	rntm->sba_pool = sba_pool;
}

BLIS_INLINE void bli_rntm_set_membrk( membrk_t* membrk, rntm_t* rntm )
{
	rntm->membrk = membrk;
}

BLIS_INLINE void bli_rntm_clear_num_threads_only( rntm_t* rntm )
{
	bli_rntm_set_num_threads_only( -1, rntm );
}
BLIS_INLINE void bli_rntm_clear_ways_only( rntm_t* rntm )
{
	bli_rntm_set_ways_only( -1, -1, -1, -1, -1, rntm );
}
BLIS_INLINE void bli_rntm_clear_sba_pool( rntm_t* rntm )
{
	bli_rntm_set_sba_pool( NULL, rntm );
}
BLIS_INLINE void bli_rntm_clear_membrk( rntm_t* rntm )
{
	bli_rntm_set_membrk( NULL, rntm );
}

//
// -- rntm_t modification (public API) -----------------------------------------
//

BLIS_INLINE void bli_rntm_set_num_threads( dim_t nt, rntm_t* rntm )
{
	// Record the total number of threads to use.
	bli_rntm_set_num_threads_only( nt, rntm );

	// Set the individual ways of parallelism to default states.
	bli_rntm_clear_ways_only( rntm );
}

BLIS_INLINE void bli_rntm_set_ways( dim_t jc, dim_t pc, dim_t ic, dim_t jr, dim_t ir, rntm_t* rntm )
{
	// Record the number of ways of parallelism per loop.
	bli_rntm_set_jc_ways_only( jc, rntm );
	bli_rntm_set_pc_ways_only( pc, rntm );
	bli_rntm_set_ic_ways_only( ic, rntm );
	bli_rntm_set_jr_ways_only( jr, rntm );
	bli_rntm_set_ir_ways_only( ir, rntm );
	bli_rntm_set_pr_ways_only(  1, rntm );

	// Set the num_threads field to a default state.
	bli_rntm_clear_num_threads_only( rntm );
}

BLIS_INLINE void bli_rntm_set_pack_a( bool pack_a, rntm_t* rntm )
{
	// Set the bool indicating whether matrix A should be packed.
	rntm->pack_a = pack_a;
}
BLIS_INLINE void bli_rntm_set_pack_b( bool pack_b, rntm_t* rntm )
{
	// Set the bool indicating whether matrix B should be packed.
	rntm->pack_b = pack_b;
}

BLIS_INLINE void bli_rntm_set_l3_sup( bool l3_sup, rntm_t* rntm )
{
	// Set the bool indicating whether level-3 sup handling is enabled.
	rntm->l3_sup = l3_sup;
}
BLIS_INLINE void bli_rntm_enable_l3_sup( rntm_t* rntm )
{
	bli_rntm_set_l3_sup( TRUE, rntm );
}
BLIS_INLINE void bli_rntm_disable_l3_sup( rntm_t* rntm )
{
	bli_rntm_set_l3_sup( FALSE, rntm );
}

//
// -- rntm_t modification (internal use only) ----------------------------------
//

BLIS_INLINE void bli_rntm_clear_pack_a( rntm_t* rntm )
{
	bli_rntm_set_pack_a( FALSE, rntm );
}
BLIS_INLINE void bli_rntm_clear_pack_b( rntm_t* rntm )
{
	bli_rntm_set_pack_b( FALSE, rntm );
}
BLIS_INLINE void bli_rntm_clear_l3_sup( rntm_t* rntm )
{
	bli_rntm_set_l3_sup( TRUE, rntm );
}

//
// -- rntm_t initialization ----------------------------------------------------
//

// NOTE: Initialization is not necessary as long the user calls at least ONE
// of the public "set" accessors, each of which guarantees that the rntm_t
// will be in a good state upon return.

#define BLIS_RNTM_INITIALIZER \
        { \
          .auto_factor = TRUE, \
          .num_threads = -1, \
          .thrloop     = { -1, -1, -1, -1, -1, -1 }, \
          .pack_a      = FALSE, \
          .pack_b      = FALSE, \
          .l3_sup      = TRUE, \
          .sba_pool    = NULL, \
          .membrk      = NULL, \
        }  \

BLIS_INLINE void bli_rntm_init( rntm_t* rntm )
{
	bli_rntm_set_auto_factor_only( TRUE, rntm );

	bli_rntm_clear_num_threads_only( rntm );
	bli_rntm_clear_ways_only( rntm );
	bli_rntm_clear_pack_a( rntm );
	bli_rntm_clear_pack_b( rntm );
	bli_rntm_clear_l3_sup( rntm );

	bli_rntm_clear_sba_pool( rntm );
	bli_rntm_clear_membrk( rntm );
}

// -- rntm_t total thread calculation ------------------------------------------

BLIS_INLINE dim_t bli_rntm_calc_num_threads
     (
       rntm_t*  restrict rntm
     )
{
	dim_t n_threads;

	n_threads  = bli_rntm_ways_for( BLIS_NC, rntm );
	n_threads *= bli_rntm_ways_for( BLIS_KC, rntm );
	n_threads *= bli_rntm_ways_for( BLIS_MC, rntm );
	n_threads *= bli_rntm_ways_for( BLIS_NR, rntm );
	n_threads *= bli_rntm_ways_for( BLIS_MR, rntm );

	return n_threads;
}

// -----------------------------------------------------------------------------

// Function prototypes

BLIS_EXPORT_BLIS void bli_rntm_init_from_global( rntm_t* rntm );

BLIS_EXPORT_BLIS void bli_rntm_set_ways_for_op
     (
       opid_t  l3_op,
       side_t  side,
       dim_t   m,
       dim_t   n,
       dim_t   k,
       rntm_t* rntm
     );

void bli_rntm_set_ways_from_rntm
     (
       dim_t   m,
       dim_t   n,
       dim_t   k,
       rntm_t* rntm
     );

void bli_rntm_set_ways_from_rntm_sup
     (
       dim_t   m,
       dim_t   n,
       dim_t   k,
       rntm_t* rntm
     );

void bli_rntm_print
     (
       rntm_t* rntm
     );

dim_t bli_rntm_calc_num_threads_in
     (
       bszid_t* restrict bszid_cur,
       rntm_t*  restrict rntm
     );

#endif

