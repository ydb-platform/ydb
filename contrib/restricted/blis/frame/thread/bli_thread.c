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

thrinfo_t BLIS_PACKM_SINGLE_THREADED = {};
thrinfo_t BLIS_GEMM_SINGLE_THREADED  = {};
thrcomm_t BLIS_SINGLE_COMM           = {};

// The global rntm_t structure. (The definition resides in bli_rntm.c.)
extern rntm_t global_rntm;

// A mutex to allow synchronous access to global_rntm. (The definition
// resides in bli_rntm.c.)
extern bli_pthread_mutex_t global_rntm_mutex;

// -----------------------------------------------------------------------------

void bli_thread_init( void )
{
	bli_thrcomm_init( 1, &BLIS_SINGLE_COMM );
	bli_packm_thrinfo_init_single( &BLIS_PACKM_SINGLE_THREADED );
	bli_l3_thrinfo_init_single( &BLIS_GEMM_SINGLE_THREADED );

	// Read the environment variables and use them to initialize the
	// global runtime object.
	bli_thread_init_rntm_from_env( &global_rntm );
}

void bli_thread_finalize( void )
{
}

// -----------------------------------------------------------------------------

void bli_thread_range_sub
     (
       thrinfo_t* thread,
       dim_t      n,
       dim_t      bf,
       bool       handle_edge_low,
       dim_t*     start,
       dim_t*     end
     )
{
	dim_t      n_way      = bli_thread_n_way( thread );

	if ( n_way == 1 ) { *start = 0; *end = n; return; }

	dim_t      work_id    = bli_thread_work_id( thread );

	dim_t      all_start  = 0;
	dim_t      all_end    = n;

	dim_t      size       = all_end - all_start;

	dim_t      n_bf_whole = size / bf;
	dim_t      n_bf_left  = size % bf;

	dim_t      n_bf_lo    = n_bf_whole / n_way;
	dim_t      n_bf_hi    = n_bf_whole / n_way;

	// In this function, we partition the space between all_start and
	// all_end into n_way partitions, each a multiple of block_factor
	// with the exception of the one partition that recieves the
	// "edge" case (if applicable).
	//
	// Here are examples of various thread partitionings, in units of
	// the block_factor, when n_way = 4. (A '+' indicates the thread
	// that receives the leftover edge case (ie: n_bf_left extra
	// rows/columns in its sub-range).
	//                                        (all_start ... all_end)
	// n_bf_whole  _left  hel  n_th_lo  _hi   thr0  thr1  thr2  thr3
	//         12     =0    f        0    4      3     3     3     3
	//         12     >0    f        0    4      3     3     3     3+
	//         13     >0    f        1    3      4     3     3     3+
	//         14     >0    f        2    2      4     4     3     3+
	//         15     >0    f        3    1      4     4     4     3+
	//         15     =0    f        3    1      4     4     4     3
	//
	//         12     =0    t        4    0      3     3     3     3
	//         12     >0    t        4    0      3+    3     3     3
	//         13     >0    t        3    1      3+    3     3     4
	//         14     >0    t        2    2      3+    3     4     4
	//         15     >0    t        1    3      3+    4     4     4
	//         15     =0    t        1    3      3     4     4     4

	// As indicated by the table above, load is balanced as equally
	// as possible, even in the presence of an edge case.

	// First, we must differentiate between cases where the leftover
	// "edge" case (n_bf_left) should be allocated to a thread partition
	// at the low end of the index range or the high end.

	if ( handle_edge_low == FALSE )
	{
		// Notice that if all threads receive the same number of
		// block_factors, those threads are considered "high" and
		// the "low" thread group is empty.
		dim_t n_th_lo = n_bf_whole % n_way;
		//dim_t n_th_hi = n_way - n_th_lo;

		// If some partitions must have more block_factors than others
		// assign the slightly larger partitions to lower index threads.
		if ( n_th_lo != 0 ) n_bf_lo += 1;

		// Compute the actual widths (in units of rows/columns) of
		// individual threads in the low and high groups.
		dim_t size_lo = n_bf_lo * bf;
		dim_t size_hi = n_bf_hi * bf;

		// Precompute the starting indices of the low and high groups.
		dim_t lo_start = all_start;
		dim_t hi_start = all_start + n_th_lo * size_lo;

		// Compute the start and end of individual threads' ranges
		// as a function of their work_ids and also the group to which
		// they belong (low or high).
		if ( work_id < n_th_lo )
		{
			*start = lo_start + (work_id  ) * size_lo;
			*end   = lo_start + (work_id+1) * size_lo;
		}
		else // if ( n_th_lo <= work_id )
		{
			*start = hi_start + (work_id-n_th_lo  ) * size_hi;
			*end   = hi_start + (work_id-n_th_lo+1) * size_hi;

			// Since the edge case is being allocated to the high
			// end of the index range, we have to advance the last
			// thread's end.
			if ( work_id == n_way - 1 ) *end += n_bf_left;
		}
	}
	else // if ( handle_edge_low == TRUE )
	{
		// Notice that if all threads receive the same number of
		// block_factors, those threads are considered "low" and
		// the "high" thread group is empty.
		dim_t n_th_hi = n_bf_whole % n_way;
		dim_t n_th_lo = n_way - n_th_hi;

		// If some partitions must have more block_factors than others
		// assign the slightly larger partitions to higher index threads.
		if ( n_th_hi != 0 ) n_bf_hi += 1;

		// Compute the actual widths (in units of rows/columns) of
		// individual threads in the low and high groups.
		dim_t size_lo = n_bf_lo * bf;
		dim_t size_hi = n_bf_hi * bf;

		// Precompute the starting indices of the low and high groups.
		dim_t lo_start = all_start;
		dim_t hi_start = all_start + n_th_lo * size_lo
		                           + n_bf_left;

		// Compute the start and end of individual threads' ranges
		// as a function of their work_ids and also the group to which
		// they belong (low or high).
		if ( work_id < n_th_lo )
		{
			*start = lo_start + (work_id  ) * size_lo;
			*end   = lo_start + (work_id+1) * size_lo;

			// Since the edge case is being allocated to the low
			// end of the index range, we have to advance the
			// starts/ends accordingly.
			if ( work_id == 0 )   *end   += n_bf_left;
			else                { *start += n_bf_left;
			                      *end   += n_bf_left; }
		}
		else // if ( n_th_lo <= work_id )
		{
			*start = hi_start + (work_id-n_th_lo  ) * size_hi;
			*end   = hi_start + (work_id-n_th_lo+1) * size_hi;
		}
	}
}

siz_t bli_thread_range_l2r
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	num_t dt = bli_obj_dt( a );
	dim_t m  = bli_obj_length_after_trans( a );
	dim_t n  = bli_obj_width_after_trans( a );
	dim_t bf = bli_blksz_get_def( dt, bmult );

	bli_thread_range_sub( thr, n, bf,
	                      FALSE, start, end );

	return m * ( *end - *start );
}

siz_t bli_thread_range_r2l
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	num_t dt = bli_obj_dt( a );
	dim_t m  = bli_obj_length_after_trans( a );
	dim_t n  = bli_obj_width_after_trans( a );
	dim_t bf = bli_blksz_get_def( dt, bmult );

	bli_thread_range_sub( thr, n, bf,
	                      TRUE, start, end );

	return m * ( *end - *start );
}

siz_t bli_thread_range_t2b
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	num_t dt = bli_obj_dt( a );
	dim_t m  = bli_obj_length_after_trans( a );
	dim_t n  = bli_obj_width_after_trans( a );
	dim_t bf = bli_blksz_get_def( dt, bmult );

	bli_thread_range_sub( thr, m, bf,
	                      FALSE, start, end );

	return n * ( *end - *start );
}

siz_t bli_thread_range_b2t
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	num_t dt = bli_obj_dt( a );
	dim_t m  = bli_obj_length_after_trans( a );
	dim_t n  = bli_obj_width_after_trans( a );
	dim_t bf = bli_blksz_get_def( dt, bmult );

	bli_thread_range_sub( thr, m, bf,
	                      TRUE, start, end );

	return n * ( *end - *start );
}

// -----------------------------------------------------------------------------

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
     )
{
	dim_t width;

	// In this function, we assume that we are somewhere in the process of
	// partitioning an m x n lower-stored region (with arbitrary diagonal
	// offset) n_ways along the n dimension (into column panels). The value
	// j identifies the left-to-right subpartition index (from 0 to n_way-1)
	// of the subpartition whose width we are about to compute using the
	// area per thread determined by the caller. n_j is the number of
	// columns in the remaining region of the matrix being partitioned,
	// and diagoff_j is that region's diagonal offset.

	// If this is the last subpartition, the width is simply equal to n_j.
	// Note that this statement handles cases where the "edge case" (if
	// one exists) is assigned to the high end of the index range (ie:
	// handle_edge_low == FALSE).
	if ( j == n_way - 1 ) return n_j;

	// At this point, we know there are at least two subpartitions left.
	// We also know that IF the submatrix contains a completely dense
	// rectangular submatrix, it will occur BEFORE the triangular (or
	// trapezoidal) part.

	// Here, we implement a somewhat minor load balancing optimization
	// that ends up getting employed only for relatively small matrices.
	// First, recall that all subpartition widths will be some multiple
	// of the blocking factor bf, except perhaps either the first or last
	// subpartition, which will receive the edge case, if it exists.
	// Also recall that j represents the current thread (or thread group,
	// or "caucus") for which we are computing a subpartition width.
	// If n_j is sufficiently small that we can only allocate bf columns
	// to each of the remaining threads, then we set the width to bf. We
	// do not allow the subpartition width to be less than bf, so, under
	// some conditions, if n_j is small enough, some of the reamining
	// threads may not get any work. For the purposes of this lower bound
	// on work (ie: width >= bf), we allow the edge case to count as a
	// "full" set of bf columns.
	{
		dim_t n_j_bf = n_j / bf + ( bf_left > 0 ? 1 : 0 );

		if ( n_j_bf <= n_way - j )
		{
			if ( j == 0 && handle_edge_low )
				width = ( bf_left > 0 ? bf_left : bf );
			else
				width = bf;

			// Make sure that the width does not exceed n_j. This would
			// occur if and when n_j_bf < n_way - j; that is, when the
			// matrix being partitioned is sufficiently small relative to
			// n_way such that there is not even enough work for every
			// (remaining) thread to get bf (or bf_left) columns. The
			// net effect of this safeguard is that some threads may get
			// assigned empty ranges (ie: no work), which of course must
			// happen in some situations.
			if ( width > n_j ) width = n_j;

			return width;
		}
	}

	// This block computes the width assuming that we are entirely within
	// a dense rectangle that precedes the triangular (or trapezoidal)
	// part.
	{
		// First compute the width of the current panel under the
		// assumption that the diagonal offset would not intersect.
		width = ( dim_t )bli_round( ( double )area_per_thr / ( double )m );

		// Adjust the width, if necessary. Specifically, we may need
		// to allocate the edge case to the first subpartition, if
		// requested; otherwise, we just need to ensure that the
		// subpartition is a multiple of the blocking factor.
		if ( j == 0 && handle_edge_low )
		{
			if ( width % bf != bf_left ) width += bf_left - ( width % bf );
		}
		else // if interior case
		{
			// Round up to the next multiple of the blocking factor.
			//if ( width % bf != 0       ) width += bf      - ( width % bf );
			// Round to the nearest multiple of the blocking factor.
			if ( width % bf != 0       ) width = bli_round_to_mult( width, bf );
		}
	}

	// We need to recompute width if the panel, according to the width
	// as currently computed, would intersect the diagonal.
	if ( diagoff_j < width )
	{
		dim_t offm_inc, offn_inc;

		// Prune away the unstored region above the diagonal, if it exists.
		// Note that the entire region was pruned initially, so we know that
		// we don't need to try to prune the right side. (Also, we discard
		// the offset deltas since we don't need to actually index into the
		// subpartition.)
		bli_prune_unstored_region_top_l( &diagoff_j, &m, &n_j, &offm_inc );
		//bli_prune_unstored_region_right_l( &diagoff_j, &m, &n_j, &offn_inc );

		// We don't need offm_inc, offn_inc here. These statements should
		// prevent compiler warnings.
		( void )offm_inc;
		( void )offn_inc;

		// Prepare to solve a quadratic equation to find the width of the
		// current (jth) subpartition given the m dimension, diagonal offset,
		// and area.
		// NOTE: We know that the +/- in the quadratic formula must be a +
		// here because we know that the desired solution (the subpartition
		// width) will be smaller than (m + diagoff), not larger. If you
		// don't believe me, draw a picture!
		const double a = -0.5;
		const double b = ( double )m + ( double )diagoff_j + 0.5;
		const double c = -0.5 * (   ( double )diagoff_j *
		                          ( ( double )diagoff_j + 1.0 )
		                        ) - area_per_thr;
		const double r = b * b - 4.0 * a * c;

		// If the quadratic solution is not imaginary, round it and use that
		// as our width, but make sure it didn't round to zero. Otherwise,
		// discard the quadratic solution and leave width, as previously
		// computed, unchanged.
		if ( r >= 0.0 )
		{
			const double x = ( -b + sqrt( r ) ) / ( 2.0 * a );

			width = ( dim_t )bli_round( x );
			if ( width == 0 ) width = 1;
		}

		// Adjust the width, if necessary.
		if ( j == 0 && handle_edge_low )
		{
			if ( width % bf != bf_left ) width += bf_left - ( width % bf );
		}
		else // if interior case
		{
			// Round up to the next multiple of the blocking factor.
			//if ( width % bf != 0       ) width += bf      - ( width % bf );
			// Round to the nearest multiple of the blocking factor.
			if ( width % bf != 0       ) width = bli_round_to_mult( width, bf );
		}
	}

	// Make sure that the width, after being adjusted, does not cause the
	// subpartition to exceed n_j.
	if ( width > n_j ) width = n_j;

	return width;
}

siz_t bli_find_area_trap_l
     (
       dim_t  m,
       dim_t  n,
       doff_t diagoff
     )
{
	dim_t  offm_inc = 0;
	dim_t  offn_inc = 0;
	double tri_area;
	double area;

	// Prune away any rectangular region above where the diagonal
	// intersects the left edge of the subpartition, if it exists.
	bli_prune_unstored_region_top_l( &diagoff, &m, &n, &offm_inc );

	// Prune away any rectangular region to the right of where the
	// diagonal intersects the bottom edge of the subpartition, if
	// it exists. (This shouldn't ever be needed, since the caller
	// would presumably have already performed rightward pruning,
	// but it's here just in case.)
	bli_prune_unstored_region_right_l( &diagoff, &m, &n, &offn_inc );

	( void )offm_inc;
	( void )offn_inc;

	// Compute the area of the empty triangle so we can subtract it
	// from the area of the rectangle that bounds the subpartition.
	if ( bli_intersects_diag_n( diagoff, m, n ) )
	{
		double tri_dim = ( double )( n - diagoff - 1 );
		tri_area = tri_dim * ( tri_dim + 1.0 ) / 2.0;
	}
	else
	{
		// If the diagonal does not intersect the trapezoid, then
		// we can compute the area as a simple rectangle.
		tri_area = 0.0;
	}

	area = ( double )m * ( double )n - tri_area;

	return ( siz_t )area;
}

// -----------------------------------------------------------------------------

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
     )
{
	dim_t      n_way   = bli_thread_n_way( thread );
	dim_t      my_id   = bli_thread_work_id( thread );

	dim_t      bf_left = n % bf;

	dim_t      j;

	dim_t      off_j;
	doff_t     diagoff_j;
	dim_t      n_left;

	dim_t      width_j;

	dim_t      offm_inc, offn_inc;

	double     tri_dim, tri_area;
	double     area_total, area_per_thr;

	siz_t      area = 0;

	// In this function, we assume that the caller has already determined
	// that (a) the diagonal intersects the submatrix, and (b) the submatrix
	// is either lower- or upper-stored.

	if ( bli_is_lower( uplo ) )
	{
		// Prune away the unstored region above the diagonal, if it exists,
		// and then to the right of where the diagonal intersects the bottom,
		// if it exists. (Also, we discard the offset deltas since we don't
		// need to actually index into the subpartition.)
		bli_prune_unstored_region_top_l( &diagoff, &m, &n, &offm_inc );
		bli_prune_unstored_region_right_l( &diagoff, &m, &n, &offn_inc );

		// We don't need offm_inc, offn_inc here. These statements should
		// prevent compiler warnings.
		( void )offm_inc;
		( void )offn_inc;

		// Now that pruning has taken place, we know that diagoff >= 0.

		// Compute the total area of the submatrix, accounting for the
		// location of the diagonal, and divide it by the number of ways
		// of parallelism.
		tri_dim      = ( double )( n - diagoff - 1 );
		tri_area     = tri_dim * ( tri_dim + 1.0 ) / 2.0;
		area_total   = ( double )m * ( double )n - tri_area;
		area_per_thr = area_total / ( double )n_way;

		// Initialize some variables prior to the loop: the offset to the
		// current subpartition, the remainder of the n dimension, and
		// the diagonal offset of the current subpartition.
		off_j     = 0;
		diagoff_j = diagoff;
		n_left    = n;

		// Iterate over the subpartition indices corresponding to each
		// thread/caucus participating in the n_way parallelism.
		for ( j = 0; j < n_way; ++j )
		{
			// Compute the width of the jth subpartition, taking the
			// current diagonal offset into account, if needed.
			width_j =
			bli_thread_range_width_l
			(
			  diagoff_j, m, n_left,
			  j, n_way,
			  bf, bf_left,
			  area_per_thr,
			  handle_edge_low
			);

			// If the current thread belongs to caucus j, this is his
			// subpartition. So we compute the implied index range and
			// end our search.
			if ( j == my_id )
			{
				*j_start_thr = off_j;
				*j_end_thr   = off_j + width_j;

				area = bli_find_area_trap_l( m, width_j, diagoff_j );

				break;
			}

			// Shift the current subpartition's starting and diagonal offsets,
			// as well as the remainder of the n dimension, according to the
			// computed width, and then iterate to the next subpartition.
			off_j     += width_j;
			diagoff_j -= width_j;
			n_left    -= width_j;
		}
	}
	else // if ( bli_is_upper( uplo ) )
	{
		// Express the upper-stored case in terms of the lower-stored case.

		// First, we convert the upper-stored trapezoid to an equivalent
		// lower-stored trapezoid by rotating it 180 degrees.
		bli_rotate180_trapezoid( &diagoff, &uplo, &m, &n );

		// Now that the trapezoid is "flipped" in the n dimension, negate
		// the bool that encodes whether to handle the edge case at the
		// low (or high) end of the index range.
		bli_toggle_bool( &handle_edge_low );

		// Compute the appropriate range for the rotated trapezoid.
		area = bli_thread_range_weighted_sub
		(
		  thread, diagoff, uplo, m, n, bf,
		  handle_edge_low,
		  j_start_thr, j_end_thr
		);

		// Reverse the indexing basis for the subpartition ranges so that
		// the indices, relative to left-to-right iteration through the
		// unrotated upper-stored trapezoid, map to the correct columns
		// (relative to the diagonal). This amounts to subtracting the
		// range from n.
		bli_reverse_index_direction( n, j_start_thr, j_end_thr );
	}

	return area;
}

siz_t bli_thread_range_mdim
     (
       dir_t      direct,
       thrinfo_t* thr,
       obj_t*     a,
       obj_t*     b,
       obj_t*     c,
       cntl_t*    cntl,
       cntx_t*    cntx,
       dim_t*     start,
       dim_t*     end
     )
{
	bszid_t  bszid  = bli_cntl_bszid( cntl );
	opid_t   family = bli_cntl_family( cntl );

	// This is part of trsm's current implementation, whereby right side
	// cases are implemented in left-side micro-kernels, which requires
	// we swap the usage of the register blocksizes for the purposes of
	// packing A and B.
	if ( family == BLIS_TRSM )
	{
		if ( bli_obj_root_is_triangular( a ) ) bszid = BLIS_MR;
		else                                   bszid = BLIS_NR;
	}

	blksz_t* bmult  = bli_cntx_get_bmult( bszid, cntx );
	obj_t*   x;
	bool     use_weighted;

	// Use the operation family to choose the one of the two matrices
	// being partitioned that potentially has structure, and also to
	// decide whether or not we need to use weighted range partitioning.
	// NOTE: It's important that we use non-weighted range partitioning
	// for hemm and symm (ie: the gemm family) because the weighted
	// function will mistakenly skip over unstored regions of the
	// structured matrix, even though they represent part of that matrix
	// that will be dense and full (after packing).
	if      ( family == BLIS_GEMM ) { x = a; use_weighted = FALSE; }
	else if ( family == BLIS_HERK ) { x = c; use_weighted = TRUE;  }
	else if ( family == BLIS_TRMM ) { x = a; use_weighted = TRUE;  }
	else    /*family == BLIS_TRSM*/ { x = a; use_weighted = FALSE; }

	if ( use_weighted )
	{
		if ( direct == BLIS_FWD )
			return bli_thread_range_weighted_t2b( thr, x, bmult, start, end );
		else
			return bli_thread_range_weighted_b2t( thr, x, bmult, start, end );
	}
	else
	{
		if ( direct == BLIS_FWD )
			return bli_thread_range_t2b( thr, x, bmult, start, end );
		else
			return bli_thread_range_b2t( thr, x, bmult, start, end );
	}
}

siz_t bli_thread_range_ndim
     (
       dir_t      direct,
       thrinfo_t* thr,
       obj_t*     a,
       obj_t*     b,
       obj_t*     c,
       cntl_t*    cntl,
       cntx_t*    cntx,
       dim_t*     start,
       dim_t*     end
     )
{
	bszid_t  bszid  = bli_cntl_bszid( cntl );
	opid_t   family = bli_cntl_family( cntl );

	// This is part of trsm's current implementation, whereby right side
	// cases are implemented in left-side micro-kernels, which requires
	// we swap the usage of the register blocksizes for the purposes of
	// packing A and B.
	if ( family == BLIS_TRSM )
	{
		if ( bli_obj_root_is_triangular( b ) ) bszid = BLIS_MR;
		else                                   bszid = BLIS_NR;
	}

	blksz_t* bmult  = bli_cntx_get_bmult( bszid, cntx );
	obj_t*   x;
	bool     use_weighted;

	// Use the operation family to choose the one of the two matrices
	// being partitioned that potentially has structure, and also to
	// decide whether or not we need to use weighted range partitioning.
	// NOTE: It's important that we use non-weighted range partitioning
	// for hemm and symm (ie: the gemm family) because the weighted
	// function will mistakenly skip over unstored regions of the
	// structured matrix, even though they represent part of that matrix
	// that will be dense and full (after packing).
	if      ( family == BLIS_GEMM ) { x = b; use_weighted = FALSE; }
	else if ( family == BLIS_HERK ) { x = c; use_weighted = TRUE;  }
	else if ( family == BLIS_TRMM ) { x = b; use_weighted = TRUE;  }
	else    /*family == BLIS_TRSM*/ { x = b; use_weighted = FALSE; }

	if ( use_weighted )
	{
		if ( direct == BLIS_FWD )
			return bli_thread_range_weighted_l2r( thr, x, bmult, start, end );
		else
			return bli_thread_range_weighted_r2l( thr, x, bmult, start, end );
	}
	else
	{
		if ( direct == BLIS_FWD )
			return bli_thread_range_l2r( thr, x, bmult, start, end );
		else
			return bli_thread_range_r2l( thr, x, bmult, start, end );
	}
}

siz_t bli_thread_range_weighted_l2r
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	siz_t area;

	// This function assigns area-weighted ranges in the n dimension
	// where the total range spans 0 to n-1 with 0 at the left end and
	// n-1 at the right end.

	if ( bli_obj_intersects_diag( a ) &&
	     bli_obj_is_upper_or_lower( a ) )
	{
		num_t  dt      = bli_obj_dt( a );
		doff_t diagoff = bli_obj_diag_offset( a );
		uplo_t uplo    = bli_obj_uplo( a );
		dim_t  m       = bli_obj_length( a );
		dim_t  n       = bli_obj_width( a );
		dim_t  bf      = bli_blksz_get_def( dt, bmult );

		// Support implicit transposition.
		if ( bli_obj_has_trans( a ) )
		{
			bli_reflect_about_diag( &diagoff, &uplo, &m, &n );
		}

		area =
		bli_thread_range_weighted_sub
		(
		  thr, diagoff, uplo, m, n, bf,
		  FALSE, start, end
		);
	}
	else // if dense or zeros
	{
		area = bli_thread_range_l2r
		(
		  thr, a, bmult,
		  start, end
		);
	}

	return area;
}

siz_t bli_thread_range_weighted_r2l
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	siz_t area;

	// This function assigns area-weighted ranges in the n dimension
	// where the total range spans 0 to n-1 with 0 at the right end and
	// n-1 at the left end.

	if ( bli_obj_intersects_diag( a ) &&
	     bli_obj_is_upper_or_lower( a ) )
	{
		num_t  dt      = bli_obj_dt( a );
		doff_t diagoff = bli_obj_diag_offset( a );
		uplo_t uplo    = bli_obj_uplo( a );
		dim_t  m       = bli_obj_length( a );
		dim_t  n       = bli_obj_width( a );
		dim_t  bf      = bli_blksz_get_def( dt, bmult );

		// Support implicit transposition.
		if ( bli_obj_has_trans( a ) )
		{
			bli_reflect_about_diag( &diagoff, &uplo, &m, &n );
		}

		bli_rotate180_trapezoid( &diagoff, &uplo, &m, &n );

		area =
		bli_thread_range_weighted_sub
		(
		  thr, diagoff, uplo, m, n, bf,
		  TRUE, start, end
		);
	}
	else // if dense or zeros
	{
		area = bli_thread_range_r2l
		(
		  thr, a, bmult,
		  start, end
		);
	}

	return area;
}

siz_t bli_thread_range_weighted_t2b
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	siz_t area;

	// This function assigns area-weighted ranges in the m dimension
	// where the total range spans 0 to m-1 with 0 at the top end and
	// m-1 at the bottom end.

	if ( bli_obj_intersects_diag( a ) &&
	     bli_obj_is_upper_or_lower( a ) )
	{
		num_t  dt      = bli_obj_dt( a );
		doff_t diagoff = bli_obj_diag_offset( a );
		uplo_t uplo    = bli_obj_uplo( a );
		dim_t  m       = bli_obj_length( a );
		dim_t  n       = bli_obj_width( a );
		dim_t  bf      = bli_blksz_get_def( dt, bmult );

		// Support implicit transposition.
		if ( bli_obj_has_trans( a ) )
		{
			bli_reflect_about_diag( &diagoff, &uplo, &m, &n );
		}

		bli_reflect_about_diag( &diagoff, &uplo, &m, &n );

		area =
		bli_thread_range_weighted_sub
		(
		  thr, diagoff, uplo, m, n, bf,
		  FALSE, start, end
		);
	}
	else // if dense or zeros
	{
		area = bli_thread_range_t2b
		(
		  thr, a, bmult,
		  start, end
		);
	}

	return area;
}

siz_t bli_thread_range_weighted_b2t
     (
       thrinfo_t* thr,
       obj_t*     a,
       blksz_t*   bmult,
       dim_t*     start,
       dim_t*     end
     )
{
	siz_t area;

	// This function assigns area-weighted ranges in the m dimension
	// where the total range spans 0 to m-1 with 0 at the bottom end and
	// m-1 at the top end.

	if ( bli_obj_intersects_diag( a ) &&
	     bli_obj_is_upper_or_lower( a ) )
	{
		num_t  dt      = bli_obj_dt( a );
		doff_t diagoff = bli_obj_diag_offset( a );
		uplo_t uplo    = bli_obj_uplo( a );
		dim_t  m       = bli_obj_length( a );
		dim_t  n       = bli_obj_width( a );
		dim_t  bf      = bli_blksz_get_def( dt, bmult );

		// Support implicit transposition.
		if ( bli_obj_has_trans( a ) )
		{
			bli_reflect_about_diag( &diagoff, &uplo, &m, &n );
		}

		bli_reflect_about_diag( &diagoff, &uplo, &m, &n );

		bli_rotate180_trapezoid( &diagoff, &uplo, &m, &n );

		area = bli_thread_range_weighted_sub
		(
		  thr, diagoff, uplo, m, n, bf,
		  TRUE, start, end
		);
	}
	else // if dense or zeros
	{
		area = bli_thread_range_b2t
		(
		  thr, a, bmult,
		  start, end
		);
	}

	return area;
}

// -----------------------------------------------------------------------------

void bli_prime_factorization( dim_t n, bli_prime_factors_t* factors )
{
	factors->n = n;
	factors->sqrt_n = ( dim_t )sqrt( ( double )n );
	factors->f = 2;
}

dim_t bli_next_prime_factor( bli_prime_factors_t* factors )
{
	// Return the prime factorization of the original number n one-by-one.
	// Return 1 after all factors have been exhausted.

	// Looping over possible factors in increasing order assures we will
	// only return prime factors (a la the Sieve of Eratosthenes).
	while ( factors->f <= factors->sqrt_n )
	{
		// Special cases for factors 2-7 handle all numbers not divisible by 11
		// or another larger prime. The slower loop version is used after that.
		// If you use a number of threads with large prime factors you get
		// what you deserve.
		if ( factors->f == 2 )
		{
			if ( factors->n % 2 == 0 )
			{
				factors->n /= 2;
				return 2;
			}
			factors->f = 3;
		}
		else if ( factors->f == 3 )
		{
			if ( factors->n % 3 == 0 )
			{
				factors->n /= 3;
				return 3;
			}
			factors->f = 5;
		}
		else if ( factors->f == 5 )
		{
			if ( factors->n % 5 == 0 )
			{
				factors->n /= 5;
				return 5;
			}
			factors->f = 7;
		}
		else if ( factors->f == 7 )
		{
			if ( factors->n % 7 == 0 )
			{
				factors->n /= 7;
				return 7;
			}
			factors->f = 11;
		}
		else
		{
			if ( factors->n % factors->f == 0 )
			{
				factors->n /= factors->f;
				return factors->f;
			}
			factors->f++;
		}
	}

	// To get here we must be out of prime factors, leaving only n (if it is
	// prime) or an endless string of 1s.
	dim_t tmp = factors->n;
	factors->n = 1;
	return tmp;
}

bool bli_is_prime( dim_t n )
{
	bli_prime_factors_t factors;

	bli_prime_factorization( n, &factors );

	dim_t f = bli_next_prime_factor( &factors );

	if ( f == n ) return TRUE;
	else          return FALSE;
}

void bli_thread_partition_2x2
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     )
{
	// Partition a number of threads into two factors nt1 and nt2 such that
	// nt1/nt2 ~= work1/work2. There is a fast heuristic algorithm and a
	// slower optimal algorithm (which minimizes |nt1*work2 - nt2*work1|).

	// Return early small prime numbers of threads.
	if ( n_thread < 4 )
	{
		*nt1 = ( work1 >= work2 ? n_thread : 1 );
		*nt2 = ( work1 <  work2 ? n_thread : 1 );

		return;
	}

#if 1
	bli_thread_partition_2x2_fast( n_thread, work1, work2, nt1, nt2 );
#else
	bli_thread_partition_2x2_slow( n_thread, work1, work2, nt1, nt2 );
#endif
}

//#define PRINT_FACTORS

void bli_thread_partition_2x2_fast
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     )
{
	// Compute with these local variables until the end of the function, at
	// which time we will save the values back to nt1 and nt2.
	dim_t tn1 = 1;
	dim_t tn2 = 1;

	// Both algorithms need the prime factorization of n_thread.
	bli_prime_factors_t factors;
	bli_prime_factorization( n_thread, &factors );

	// Fast algorithm: assign prime factors in increasing order to whichever
	// partition has more work to do. The work is divided by the number of
	// threads assigned at each iteration. This algorithm is sub-optimal in
	// some cases. We attempt to mitigate the cases that involve at least one
	// factor of 2. For example, in the partitioning of 12 with equal work
	// this algorithm tentatively finds 6x2. This factorization involves a
	// factor of 2 that can be reallocated, allowing us to convert it to the
	// optimal solution of 4x3. But some cases cannot be corrected this way
	// because they do not contain a factor of 2. For example, this algorithm
	// factors 105 (with equal work) into 21x5 whereas 7x15 would be optimal.

	#ifdef PRINT_FACTORS
	printf( "w1 w2 = %d %d (initial)\n", (int)work1, (int)work2 );
	#endif

	dim_t f;
	while ( ( f = bli_next_prime_factor( &factors ) ) > 1 )
	{
		#ifdef PRINT_FACTORS
		printf( "w1 w2 = %4d %4d nt1 nt2 = %d %d ... f = %d\n",
		        (int)work1, (int)work2, (int)tn1, (int)tn2, (int)f );
		#endif

		if ( work1 > work2 ) { work1 /= f; tn1 *= f; }
		else                 { work2 /= f; tn2 *= f; }
	}

	#ifdef PRINT_FACTORS
	printf( "w1 w2 = %4d %4d nt1 nt2 = %d %d\n",
	        (int)work1, (int)work2, (int)tn1, (int)tn2 );
	#endif

	// Sometimes the last factor applied is prime. For example, on a square
	// matrix, we tentatively arrive (from the logic above) at:
	// - a 2x6 factorization when given 12 ways of parallelism
	// - a 2x10 factorization when given 20 ways of parallelism
	// - a 2x14 factorization when given 28 ways of parallelism
	// These factorizations are suboptimal under the assumption that we want
	// the parallelism to be as balanced as possible. Below, we make a final
	// attempt at rebalancing nt1 and nt2 by checking to see if the gap between
	// work1 and work2 is narrower if we reallocate a factor of 2.
	if ( work1 > work2 )
	{
		// Example: nt = 12
		//          w1 w2 (initial)   = 3600 3600; nt1 nt2 =  1 1
		//          w1 w2 (tentative) = 1800  600; nt1 nt2 =  2 6
		//          w1 w2 (ideal)     =  900 1200; nt1 nt2 =  4 3
		if ( tn2 % 2 == 0 )
		{
			dim_t diff     =          work1   - work2;
			dim_t diff_mod = bli_abs( work1/2 - work2*2 );

			if ( diff_mod < diff ) { tn1 *= 2; tn2 /= 2; }
		}
	}
	else if ( work1 < work2 )
	{
		// Example: nt = 40
		//          w1 w2 (initial)   = 3600 3600; nt1 nt2 =  1 1
		//          w1 w2 (tentative) =  360  900; nt1 nt2 = 10 4
		//          w1 w2 (ideal)     =  720  450; nt1 nt2 =  5 8
		if ( tn1 % 2 == 0 )
		{
			dim_t diff     =          work2   - work1;
			dim_t diff_mod = bli_abs( work2/2 - work1*2 );

			if ( diff_mod < diff ) { tn1 /= 2; tn2 *= 2; }
		}
	}

	#ifdef PRINT_FACTORS
	printf( "w1 w2 = %4d %4d nt1 nt2 = %d %d (final)\n",
	        (int)work1, (int)work2, (int)tn1, (int)tn2 );
	#endif

	// Save the final result.
	*nt1 = tn1;
	*nt2 = tn2;
}

#include "limits.h"

void bli_thread_partition_2x2_slow
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     )
{
	// Slow algorithm: exhaustively constructs all factor pairs of n_thread and
	// chooses the best one.

	// Compute with these local variables until the end of the function, at
	// which time we will save the values back to nt1 and nt2.
	dim_t tn1 = 1;
	dim_t tn2 = 1;

	// Both algorithms need the prime factorization of n_thread.
	bli_prime_factors_t factors;
	bli_prime_factorization( n_thread, &factors );

	// Eight prime factors handles n_thread up to 223092870.
	dim_t fact[8];
	dim_t mult[8];

	// There is always at least one prime factor, so use if for initialization.
	dim_t nfact = 1;
	fact[0] = bli_next_prime_factor( &factors );
	mult[0] = 1;

	// Collect the remaining prime factors, accounting for multiplicity of
	// repeated factors.
	dim_t f;
	while ( ( f = bli_next_prime_factor( &factors ) ) > 1 )
	{
		if ( f == fact[nfact-1] )
		{
			mult[nfact-1]++;
		}
		else
		{
			nfact++;
			fact[nfact-1] = f;
			mult[nfact-1] = 1;
		}
	}

	// Now loop over all factor pairs. A single factor pair is denoted by how
	// many of each prime factor are included in the first factor (ntaken).
	dim_t ntake[8] = {0};
	dim_t min_diff = INT_MAX;

	// Loop over how many prime factors to assign to the first factor in the
	// pair, for each prime factor. The total number of iterations is
	// \Prod_{i=0}^{nfact-1} mult[i].
	bool done = FALSE;
	while ( !done )
	{
		dim_t x = 1;
		dim_t y = 1;

		// Form the factors by integer exponentiation and accumulation.
		for ( dim_t i = 0 ; i < nfact ; i++ )
		{
			x *= bli_ipow( fact[i], ntake[i] );
			y *= bli_ipow( fact[i], mult[i]-ntake[i] );
		}

		// Check if this factor pair is optimal by checking
		// |nt1*work2 - nt2*work1|.
		dim_t diff = llabs( x*work2 - y*work1 );
		if ( diff < min_diff )
		{
			min_diff = diff;
			tn1 = x;
			tn2 = y;
		}

		// Go to the next factor pair by doing an "odometer loop".
		for ( dim_t i = 0 ; i < nfact ; i++ )
		{
			if ( ++ntake[i] > mult[i] )
			{
				ntake[i] = 0;
				if ( i == nfact-1 ) done = TRUE;
				else continue;
			}
			break;
		}
	}

	// Save the final result.
	*nt1 = tn1;
	*nt2 = tn2;
}

#if 0
void bli_thread_partition_2x2_orig
     (
       dim_t           n_thread,
       dim_t           work1,
       dim_t           work2,
       dim_t* restrict nt1,
       dim_t* restrict nt2
     )
{
	// Copy nt1 and nt2 to local variables and then compute with those local
	// variables until the end of the function, at which time we will save the
	// values back to nt1 and nt2.
	dim_t tn1; // = *nt1;
	dim_t tn2; // = *nt2;

    // Partition a number of threads into two factors nt1 and nt2 such that
    // nt1/nt2 ~= work1/work2. There is a fast heuristic algorithm and a
    // slower optimal algorithm (which minimizes |nt1*work2 - nt2*work1|).

    // Return early small prime numbers of threads.
    if ( n_thread < 4 )
    {
        tn1 = ( work1 >= work2 ? n_thread : 1 );
        tn2 = ( work1 <  work2 ? n_thread : 1 );

		return;
    }

    tn1 = 1;
    tn2 = 1;

    // Both algorithms need the prime factorization of n_thread.
    bli_prime_factors_t factors;
    bli_prime_factorization( n_thread, &factors );

#if 1

    // Fast algorithm: assign prime factors in increasing order to whichever
    // partition has more work to do. The work is divided by the number of
    // threads assigned at each iteration. This algorithm is sub-optimal in
	// some cases. We attempt to mitigate the cases that involve at least one
	// factor of 2. For example, in the partitioning of 12 with equal work
	// this algorithm tentatively finds 6x2. This factorization involves a
	// factor of 2 that can be reallocated, allowing us to convert it to the
	// optimal solution of 4x3. But some cases cannot be corrected this way
	// because they do not contain a factor of 2. For example, this algorithm
	// factors 105 (with equal work) into 21x5 whereas 7x15 would be optimal.

	//printf( "w1 w2 = %d %d (initial)\n", (int)work1, (int)work2 );

    dim_t f;
    while ( ( f = bli_next_prime_factor( &factors ) ) > 1 )
    {
		//printf( "w1 w2 = %4d %4d nt1 nt2 = %d %d ... f = %d\n", (int)work1, (int)work2, (int)tn1, (int)tn2, (int)f );

        if ( work1 > work2 )
        {
            work1 /= f;
            tn1 *= f;
        }
        else
        {
            work2 /= f;
            tn2 *= f;
        }
    }

	//printf( "w1 w2 = %4d %4d nt1 nt2 = %d %d\n", (int)work1, (int)work2, (int)tn1, (int)tn2 );

	// Sometimes the last factor applied is prime. For example, on a square
	// matrix, we tentatively arrive (from the logic above) at:
	// - a 2x6 factorization when given 12 ways of parallelism
	// - a 2x10 factorization when given 20 ways of parallelism
	// - a 2x14 factorization when given 28 ways of parallelism
	// These factorizations are suboptimal under the assumption that we want
	// the parallelism to be as balanced as possible. Below, we make a final
	// attempt at rebalancing nt1 and nt2 by checking to see if the gap between
	// work1 and work2 is narrower if we reallocate a factor of 2.
	if ( work1 > work2 )
	{
		// Example: nt = 12
		//          w1 w2 (initial)   = 3600 3600; nt1 nt2 =  1 1
		//          w1 w2 (tentative) = 1800  600; nt1 nt2 =  2 6
		//          w1 w2 (ideal)     =  900 1200; nt1 nt2 =  4 3
		if ( tn2 % 2 == 0 )
		{
			dim_t diff     =          work1   - work2;
			dim_t diff_mod = bli_abs( work1/2 - work2*2 );

			if ( diff_mod < diff ) { tn1 *= 2; tn2 /= 2; }
		}
	}
	else if ( work1 < work2 )
	{
		// Example: nt = 40
		//          w1 w2 (initial)   = 3600 3600; nt1 nt2 =  1 1
		//          w1 w2 (tentative) =  360  900; nt1 nt2 = 10 4
		//          w1 w2 (ideal)     =  720  450; nt1 nt2 =  5 8
		if ( tn1 % 2 == 0 )
		{
			dim_t diff     =          work2   - work1;
			dim_t diff_mod = bli_abs( work2/2 - work1*2 );

			if ( diff_mod < diff ) { tn1 /= 2; tn2 *= 2; }
		}
	}

	//printf( "w1 w2 = %4d %4d nt1 nt2 = %d %d (final)\n", (int)work1, (int)work2, (int)tn1, (int)tn2 );

#else

    // Slow algorithm: exhaustively constructs all factor pairs of n_thread and
    // chooses the best one.

    // Eight prime factors handles n_thread up to 223092870.
    dim_t fact[8];
    dim_t mult[8];

    // There is always at least one prime factor, so use if for initialization.
    dim_t nfact = 1;
    fact[0] = bli_next_prime_factor( &factors );
    mult[0] = 1;

    // Collect the remaining prime factors, accounting for multiplicity of
    // repeated factors.
    dim_t f;
    while ( ( f = bli_next_prime_factor( &factors ) ) > 1 )
    {
        if ( f == fact[nfact-1] )
        {
            mult[nfact-1]++;
        }
        else
        {
            nfact++;
            fact[nfact-1] = f;
            mult[nfact-1] = 1;
        }
    }

    // Now loop over all factor pairs. A single factor pair is denoted by how
    // many of each prime factor are included in the first factor (ntaken).
    dim_t ntake[8] = {0};
    dim_t min_diff = INT_MAX;

    // Loop over how many prime factors to assign to the first factor in the
    // pair, for each prime factor. The total number of iterations is
    // \Prod_{i=0}^{nfact-1} mult[i].
    bool   done = FALSE;
    while ( !done )
    {
        dim_t x = 1;
        dim_t y = 1;

        // Form the factors by integer exponentiation and accumulation.
        for  (dim_t i = 0 ; i < nfact ; i++ )
        {
            x *= bli_ipow( fact[i], ntake[i] );
            y *= bli_ipow( fact[i], mult[i]-ntake[i] );
        }

        // Check if this factor pair is optimal by checking
        // |nt1*work2 - nt2*work1|.
        dim_t diff = llabs( x*work2 - y*work1 );
        if ( diff < min_diff )
        {
            min_diff = diff;
            tn1 = x;
            tn2 = y;
        }

        // Go to the next factor pair by doing an "odometer loop".
        for ( dim_t i = 0 ; i < nfact ; i++ )
        {
            if ( ++ntake[i] > mult[i] )
            {
                ntake[i] = 0;
                if ( i == nfact-1 ) done = TRUE;
                else continue;
            }
            break;
        }
    }

#endif


	// Save the final result.
	*nt1 = tn1;
	*nt2 = tn2;
}
#endif

// -----------------------------------------------------------------------------

dim_t bli_gcd( dim_t x, dim_t y )
{
	while ( y != 0 )
	{
		dim_t t = y;
		y = x % y;
		x = t;
	}
	return x;
}

dim_t bli_lcm( dim_t x, dim_t y)
{
	return x * y / bli_gcd( x, y );
}

dim_t bli_ipow( dim_t base, dim_t power )
{
	dim_t p = 1;

	for ( dim_t mask = 0x1 ; mask <= power ; mask <<= 1 )
	{
		if ( power & mask ) p *= base;
		base *= base;
	}

	return p;
}

// -----------------------------------------------------------------------------

dim_t bli_thread_get_jc_nt( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_jc_ways( &global_rntm );
}

dim_t bli_thread_get_pc_nt( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_pc_ways( &global_rntm );
}

dim_t bli_thread_get_ic_nt( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_ic_ways( &global_rntm );
}

dim_t bli_thread_get_jr_nt( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_jr_ways( &global_rntm );
}

dim_t bli_thread_get_ir_nt( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_ir_ways( &global_rntm );
}

dim_t bli_thread_get_num_threads( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_num_threads( &global_rntm );
}

// ----------------------------------------------------------------------------

void bli_thread_set_ways( dim_t jc, dim_t pc, dim_t ic, dim_t jr, dim_t ir )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	// Acquire the mutex protecting global_rntm.
	bli_pthread_mutex_lock( &global_rntm_mutex );

	bli_rntm_set_ways_only( jc, pc, ic, jr, ir, &global_rntm );

	// Release the mutex protecting global_rntm.
	bli_pthread_mutex_unlock( &global_rntm_mutex );
}

void bli_thread_set_num_threads( dim_t n_threads )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	// Acquire the mutex protecting global_rntm.
	bli_pthread_mutex_lock( &global_rntm_mutex );

	bli_rntm_set_num_threads_only( n_threads, &global_rntm );

	// Release the mutex protecting global_rntm.
	bli_pthread_mutex_unlock( &global_rntm_mutex );
}

// ----------------------------------------------------------------------------

void bli_thread_init_rntm_from_env
     (
       rntm_t* rntm
     )
{
	// NOTE: We don't need to acquire the global_rntm_mutex here because this
	// function is only called from bli_thread_init(), which is only called
	// by bli_init_once().

	bool  auto_factor = FALSE;
	dim_t nt;
	dim_t jc, pc, ic, jr, ir;

#ifdef BLIS_ENABLE_MULTITHREADING

	// Try to read BLIS_NUM_THREADS first.
	nt = bli_env_get_var( "BLIS_NUM_THREADS", -1 );

	// If BLIS_NUM_THREADS was not set, try to read OMP_NUM_THREADS.
	if ( nt == -1 )
		nt = bli_env_get_var( "OMP_NUM_THREADS", -1 );

	// Read the environment variables for the number of threads (ways
	// of parallelism) for each individual loop.
	jc = bli_env_get_var( "BLIS_JC_NT", -1 );
	pc = bli_env_get_var( "BLIS_PC_NT", -1 );
	ic = bli_env_get_var( "BLIS_IC_NT", -1 );
	jr = bli_env_get_var( "BLIS_JR_NT", -1 );
	ir = bli_env_get_var( "BLIS_IR_NT", -1 );

	// If any BLIS_*_NT environment variable was set, then we ignore the
	// value of BLIS_NUM_THREADS or OMP_NUM_THREADS and use the
	// BLIS_*_NT values instead (with unset variables being treated as if
	// they contained 1).
	if ( jc != -1 || pc != -1 || ic != -1 || jr != -1 || ir != -1 )
	{
		if ( jc == -1 ) jc = 1;
		if ( pc == -1 ) pc = 1;
		if ( ic == -1 ) ic = 1;
		if ( jr == -1 ) jr = 1;
		if ( ir == -1 ) ir = 1;

		// Unset the value for nt.
		nt = -1;
	}

	// By this time, one of the following conditions holds:
	// - nt is -1 and the ways for each loop are -1.
	// - nt is -1 and the ways for each loop are all set.
	// - nt is set and the ways for each loop are -1.

	// If nt is set (ie: not -1), then we know we will perform an automatic
	// thread factorization (later, in bli_rntm.c).
	if ( nt != -1 ) auto_factor = TRUE;

#else

	// When multithreading is disabled, always set the rntm_t ways
	// values to 1.
	nt = -1;
	jc = pc = ic = jr = ir = 1;

#endif

	// Save the results back in the runtime object.
	bli_rntm_set_auto_factor_only( auto_factor, rntm );
	bli_rntm_set_num_threads_only( nt, rntm );
	bli_rntm_set_ways_only( jc, pc, ic, jr, ir, rntm );

#if 0
	printf( "bli_thread_init_rntm_from_env()\n" );
	bli_rntm_print( rntm );
#endif
}

