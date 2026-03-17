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

#ifndef BLIS_THREAD_H
#define BLIS_THREAD_H

// Include thread communicator (thrcomm_t) object definitions and prototypes.
#include "bli_thrcomm.h"

// Include thread info (thrinfo_t) object definitions and prototypes.
#include "bli_thrinfo.h"
#include "bli_thrinfo_sup.h"

// Include some operation-specific thrinfo_t prototypes.
// Note that the bli_packm_thrinfo.h must be included before the others!
#include "bli_packm_thrinfo.h"
#include "bli_l3_thrinfo.h"

// Include the level-3 thread decorator and related definitions and prototypes
// for the conventional code path.
#include "bli_l3_decor.h"

// Include the level-3 thread decorator and related definitions and prototypes
// for the sup code path.
#include "bli_l3_sup_decor.h"

// Initialization-related prototypes.
void bli_thread_init( void );
void bli_thread_finalize( void );

// Thread range-related prototypes.

BLIS_EXPORT_BLIS
void bli_thread_range_sub
     (
       thrinfo_t* thread,
       dim_t      n,
       dim_t      bf,
       bool       handle_edge_low,
       dim_t*     start,
       dim_t*     end
     );

#undef  GENPROT
#define GENPROT( opname ) \
\
siz_t PASTEMAC0( opname ) \
     ( \
       dir_t      direct, \
       thrinfo_t* thr, \
       obj_t*     a, \
       obj_t*     b, \
       obj_t*     c, \
       cntl_t*    cntl, \
       cntx_t*    cntx, \
       dim_t*     start, \
       dim_t*     end  \
     );

GENPROT( thread_range_mdim )
GENPROT( thread_range_ndim )

#undef  GENPROT
#define GENPROT( opname ) \
\
siz_t PASTEMAC0( opname ) \
     ( \
       thrinfo_t* thr, \
       obj_t*     a, \
       blksz_t*   bmult, \
       dim_t*     start, \
       dim_t*     end  \
     );

GENPROT( thread_range_l2r )
GENPROT( thread_range_r2l )
GENPROT( thread_range_t2b )
GENPROT( thread_range_b2t )

GENPROT( thread_range_weighted_l2r )
GENPROT( thread_range_weighted_r2l )
GENPROT( thread_range_weighted_t2b )
GENPROT( thread_range_weighted_b2t )


dim_t bli_thread_range_width_l
     (
       doff_t diagoff_j,
       dim_t  m,
       dim_t  n_j,
       dim_t  j,
       dim_t  n_way,
       dim_t  bf,
       dim_t  bf_left,
       double area_per_thr,
       bool   handle_edge_low
     );
siz_t bli_find_area_trap_l
     (
       dim_t  m,
       dim_t  n,
       doff_t diagoff
     );
siz_t bli_thread_range_weighted_sub
     (
       thrinfo_t* restrict thread,
       doff_t              diagoff,
       uplo_t              uplo,
       dim_t               m,
       dim_t               n,
       dim_t               bf,
       bool                handle_edge_low,
       dim_t*     restrict j_start_thr,
       dim_t*     restrict j_end_thr
     );

// -----------------------------------------------------------------------------

// Factorization and partitioning prototypes
typedef struct
{
    dim_t n;
    dim_t sqrt_n;
    dim_t f;
} bli_prime_factors_t;

void bli_prime_factorization(dim_t n, bli_prime_factors_t* factors);

dim_t bli_next_prime_factor(bli_prime_factors_t* factors);
bool  bli_is_prime( dim_t n );

void bli_thread_partition_2x2
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     );
void bli_thread_partition_2x2_slow
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     );
void bli_thread_partition_2x2_fast
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     );

// -----------------------------------------------------------------------------

dim_t bli_gcd( dim_t x, dim_t y );
dim_t bli_lcm( dim_t x, dim_t y );
dim_t bli_ipow( dim_t base, dim_t power );

// -----------------------------------------------------------------------------

BLIS_EXPORT_BLIS dim_t bli_thread_get_jc_nt( void );
BLIS_EXPORT_BLIS dim_t bli_thread_get_pc_nt( void );
BLIS_EXPORT_BLIS dim_t bli_thread_get_ic_nt( void );
BLIS_EXPORT_BLIS dim_t bli_thread_get_jr_nt( void );
BLIS_EXPORT_BLIS dim_t bli_thread_get_ir_nt( void );
BLIS_EXPORT_BLIS dim_t bli_thread_get_num_threads( void );

BLIS_EXPORT_BLIS void  bli_thread_set_ways( dim_t jc, dim_t pc, dim_t ic, dim_t jr, dim_t ir );
BLIS_EXPORT_BLIS void  bli_thread_set_num_threads( dim_t value );

void  bli_thread_init_rntm_from_env( rntm_t* rntm );

// -----------------------------------------------------------------------------

BLIS_INLINE void bli_thread_range_jrir_rr
     (
       thrinfo_t* thread,
       dim_t      n,
       dim_t      bf,
       bool       handle_edge_low,
       dim_t*     start,
       dim_t*     end,
       dim_t*     inc
     )
{
	// Use interleaved partitioning of jr/ir loops.
	*start = bli_thread_work_id( thread );
	*inc   = bli_thread_n_way( thread );
	*end   = n;
}

BLIS_INLINE void bli_thread_range_jrir_sl
     (
       thrinfo_t* thread,
       dim_t      n,
       dim_t      bf,
       bool       handle_edge_low,
       dim_t*     start,
       dim_t*     end,
       dim_t*     inc
     )
{
	// Use contiguous slab partitioning of jr/ir loops.
	bli_thread_range_sub( thread, n, bf, handle_edge_low, start, end );
	*inc = 1;
}

BLIS_INLINE void bli_thread_range_jrir
     (
       thrinfo_t* thread,
       dim_t      n,
       dim_t      bf,
       bool       handle_edge_low,
       dim_t*     start,
       dim_t*     end,
       dim_t*     inc
     )
{
	// Define a general-purpose version of bli_thread_range_jrir() whose
	// definition depends on whether slab or round-robin partitioning was
	// requested at configure-time.
#ifdef BLIS_ENABLE_JRIR_SLAB
	bli_thread_range_jrir_sl( thread, n, bf, handle_edge_low, start, end, inc );
#else
	bli_thread_range_jrir_rr( thread, n, bf, handle_edge_low, start, end, inc );
#endif
}

#if 0
BLIS_INLINE void bli_thread_range_weighted_jrir
     (
       thrinfo_t* thread,
       doff_t     diagoff,
       uplo_t     uplo,
       dim_t      m,
       dim_t      n,
       dim_t      bf,
       bool       handle_edge_low,
       dim_t*     start,
       dim_t*     end,
       dim_t*     inc
     )
{
#ifdef BLIS_ENABLE_JRIR_SLAB

	// Use contiguous slab partitioning for jr/ir loops.
	bli_thread_range_weighted_sub( thread, diagoff, uplo, m, n, bf,
	                               handle_edge_low, start, end );

	*start = *start / bf; *inc = 1;

	if ( *end % bf ) *end = *end / bf + 1;
	else             *end = *end / bf;

#else

	// Use interleaved partitioning of jr/ir loops.
	*start = bli_thread_work_id( thread );
	*inc   = bli_thread_n_way( thread );
	*end   = n;

#endif
}
#endif

#endif

