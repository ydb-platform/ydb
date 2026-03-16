/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin

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

/*
void bli_l3_prune_unref_mparts_m
     (
       obj_t*  a,
       obj_t*  b,
       obj_t*  c,
       cntl_t* cntl
     )
{
	// Query the operation family.
	opid_t family = bli_cntl_family( cntl );

	if      ( family == BLIS_GEMM ) return; // No pruning is necessary for gemm.
	else if ( family == BLIS_HERK ) bli_herk_prune_unref_mparts_m( a, b, c );
	else if ( family == BLIS_TRMM ) bli_trmm_prune_unref_mparts_m( a, b, c );
	else if ( family == BLIS_TRSM ) bli_trsm_prune_unref_mparts_m( a, b, c );
}
*/

#undef  GENFRONT
#define GENFRONT( dim ) \
\
void PASTEMAC(l3_prune_unref_mparts_,dim) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c, \
       cntl_t* cntl  \
     ) \
{ \
	/* Query the operation family. */ \
	opid_t family = bli_cntl_family( cntl ); \
\
	if      ( family == BLIS_GEMM ) return; /* No pruning is necessary for gemm. */ \
	else if ( family == BLIS_HERK ) PASTEMAC(herk_prune_unref_mparts_,dim)( a, b, c ); \
	else if ( family == BLIS_TRMM ) PASTEMAC(trmm_prune_unref_mparts_,dim)( a, b, c ); \
	else if ( family == BLIS_TRSM ) PASTEMAC(trsm_prune_unref_mparts_,dim)( a, b, c ); \
}

GENFRONT( m )
GENFRONT( n )
GENFRONT( k )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_prune_unref_mparts_m) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c  \
     ) \
{ \
	/* No pruning is necessary for gemm. */ \
} \
void PASTEMAC(opname,_prune_unref_mparts_n) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c  \
     ) \
{ \
	/* No pruning is necessary for gemm. */ \
} \
void PASTEMAC(opname,_prune_unref_mparts_k) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c  \
     ) \
{ \
	/* No pruning is necessary for gemm. */ \
}

GENFRONT( gemm )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_prune_unref_mparts_m) \
     ( \
       obj_t*  a, \
       obj_t*  ah, \
       obj_t*  c  \
     ) \
{ \
	/* Prune any unreferenced part from the subpartition of C (that would
	   be encountered from partitioning in the m dimension) and adjust the
	   subpartition of A accordingly. */ \
	bli_prune_unref_mparts( c, BLIS_M, a, BLIS_M ); \
} \
void PASTEMAC(opname,_prune_unref_mparts_n) \
     ( \
       obj_t*  a, \
       obj_t*  ah, \
       obj_t*  c  \
     ) \
{ \
	/* Prune any unreferenced part from the subpartition of C (that would
	   be encountered from partitioning in the n dimension) and adjust the
	   subpartition of Ah accordingly. */ \
	bli_prune_unref_mparts( c, BLIS_N, ah, BLIS_N ); \
} \
void PASTEMAC(opname,_prune_unref_mparts_k) \
     ( \
       obj_t*  a, \
       obj_t*  ah, \
       obj_t*  c  \
     ) \
{ \
	/* As long as A and Ah are general in structure, no pruning should be
	   for the k dimension. */ \
}

GENFRONT( herk )

// -----------------------------------------------------------------------------

#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_prune_unref_mparts_m) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c  \
     ) \
{ \
	/* Prune any unreferenced part from the subpartition of A (that would
	   be encountered from partitioning in the m dimension) and adjust the
	   subpartition of C accordingly. */ \
	bli_prune_unref_mparts( a, BLIS_M, c, BLIS_M ); \
} \
void PASTEMAC(opname,_prune_unref_mparts_n) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c  \
     ) \
{ \
	/* Prune any unreferenced part from the subpartition of B (that would
	   be encountered from partitioning in the n dimension) and adjust the
	   subpartition of C accordingly. */ \
	bli_prune_unref_mparts( b, BLIS_N, c, BLIS_N ); \
} \
void PASTEMAC(opname,_prune_unref_mparts_k) \
     ( \
       obj_t*  a, \
       obj_t*  b, \
       obj_t*  c  \
     ) \
{ \
	/* Prune any unreferenced part from the subpartition of A (that would
	   be encountered from partitioning in the k dimension) and adjust the
	   subpartition of B accordingly. */ \
	bli_prune_unref_mparts( a, BLIS_N, b, BLIS_M ); \
\
	/* Prune any unreferenced part from the subpartition of B (that would
	   be encountered from partitioning in the k dimension) and adjust the
	   subpartition of A accordingly. */ \
	bli_prune_unref_mparts( b, BLIS_M, a, BLIS_N ); \
}

GENFRONT( trmm )
GENFRONT( trsm )


