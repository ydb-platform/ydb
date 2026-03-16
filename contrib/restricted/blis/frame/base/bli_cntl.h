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


/*
// -- Control tree node definition --

struct cntl_s
{
	// Basic fields (usually required).
	opid_t         family;
	bszid_t        bszid;
	void_fp        var_func;
	struct cntl_s* sub_prenode;
	struct cntl_s* sub_node;

	// Optional fields (needed only by some operations such as packm).
	// NOTE: first field of params must be a uint64_t containing the size
	// of the struct.
	void*          params;

	// Internal fields that track "cached" data.
	mem_t          pack_mem;
};
typedef struct cntl_s cntl_t;
*/


// -- Control tree prototypes --

BLIS_EXPORT_BLIS cntl_t* bli_cntl_create_node
     (
       rntm_t* rntm,
       opid_t  family,
       bszid_t bszid,
       void_fp var_func,
       void*   params,
       cntl_t* sub_node
     );

BLIS_EXPORT_BLIS void bli_cntl_free_node
     (
       rntm_t* rntm,
       cntl_t* cntl
     );

BLIS_EXPORT_BLIS void bli_cntl_clear_node
     (
       cntl_t* cntl
     );

// -----------------------------------------------------------------------------

BLIS_EXPORT_BLIS void bli_cntl_free
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     );

BLIS_EXPORT_BLIS void bli_cntl_free_w_thrinfo
     (
       rntm_t*    rntm,
       cntl_t*    cntl,
       thrinfo_t* thread
     );

BLIS_EXPORT_BLIS void bli_cntl_free_wo_thrinfo
     (
       rntm_t*    rntm,
       cntl_t*    cntl
     );

BLIS_EXPORT_BLIS cntl_t* bli_cntl_copy
     (
       rntm_t* rntm,
       cntl_t* cntl
     );

BLIS_EXPORT_BLIS void bli_cntl_mark_family
     (
       opid_t  family,
       cntl_t* cntl
     );

// -----------------------------------------------------------------------------

dim_t bli_cntl_calc_num_threads_in
     (
       rntm_t* rntm,
       cntl_t* cntl
     );

// -----------------------------------------------------------------------------

// cntl_t query (fields only)

BLIS_INLINE opid_t bli_cntl_family( cntl_t* cntl )
{
	return cntl->family;
}

BLIS_INLINE bszid_t bli_cntl_bszid( cntl_t* cntl )
{
	return cntl->bszid;
}

BLIS_INLINE void_fp bli_cntl_var_func( cntl_t* cntl )
{
	return cntl->var_func;
}

BLIS_INLINE cntl_t* bli_cntl_sub_prenode( cntl_t* cntl )
{
	return cntl->sub_prenode;
}

BLIS_INLINE cntl_t* bli_cntl_sub_node( cntl_t* cntl )
{
	return cntl->sub_node;
}

BLIS_INLINE void* bli_cntl_params( cntl_t* cntl )
{
	return cntl->params;
}

BLIS_INLINE uint64_t bli_cntl_params_size( cntl_t* cntl )
{
	// The first 64 bytes is always the size of the params structure.
	return *( ( uint64_t* )(cntl->params) );
}

BLIS_INLINE mem_t* bli_cntl_pack_mem( cntl_t* cntl )
{
	return &(cntl->pack_mem);
}

// cntl_t query (complex)

BLIS_INLINE bool bli_cntl_is_null( cntl_t* cntl )
{
	return ( bool )
	       ( cntl == NULL );
}

BLIS_INLINE bool bli_cntl_is_leaf( cntl_t* cntl )
{
	return ( bool )
	       ( bli_cntl_sub_node( cntl ) == NULL );
}

BLIS_INLINE bool bli_cntl_does_part( cntl_t* cntl )
{
	return ( bool )
	       ( bli_cntl_bszid( cntl ) != BLIS_NO_PART );
}

// cntl_t modification

BLIS_INLINE void bli_cntl_set_family( opid_t family, cntl_t* cntl )
{
	cntl->family = family;
}

BLIS_INLINE void bli_cntl_set_bszid( bszid_t bszid, cntl_t* cntl )
{
	cntl->bszid = bszid;
}

BLIS_INLINE void bli_cntl_set_var_func( void_fp var_func, cntl_t* cntl )
{
	cntl->var_func = var_func;
}

BLIS_INLINE void bli_cntl_set_sub_prenode( cntl_t* sub_prenode, cntl_t* cntl )
{
	cntl->sub_prenode = sub_prenode;
}

BLIS_INLINE void bli_cntl_set_sub_node( cntl_t* sub_node, cntl_t* cntl )
{
	cntl->sub_node = sub_node;
}

BLIS_INLINE void bli_cntl_set_params( void* params, cntl_t* cntl )
{
	cntl->params = params;
}

BLIS_INLINE void bli_cntl_set_pack_mem( mem_t* pack_mem, cntl_t* cntl )
{
	cntl->pack_mem = *pack_mem;
}

