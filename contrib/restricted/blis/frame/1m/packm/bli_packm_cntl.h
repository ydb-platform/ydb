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

struct packm_params_s
{
	uint64_t      size; // size field must be present and come first.
	packm_var_oft var_func;
	bszid_t       bmid_m;
	bszid_t       bmid_n;
	bool          does_invert_diag;
	bool          rev_iter_if_upper;
	bool          rev_iter_if_lower;
	pack_t        pack_schema;
	packbuf_t     pack_buf_type;
};
typedef struct packm_params_s packm_params_t;

BLIS_INLINE packm_var_oft bli_cntl_packm_params_var_func( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->var_func;
}

BLIS_INLINE bszid_t bli_cntl_packm_params_bmid_m( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->bmid_m;
}

BLIS_INLINE bszid_t bli_cntl_packm_params_bmid_n( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->bmid_n;
}

BLIS_INLINE bool bli_cntl_packm_params_does_invert_diag( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->does_invert_diag;
}

BLIS_INLINE bool bli_cntl_packm_params_rev_iter_if_upper( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->rev_iter_if_upper;
}

BLIS_INLINE bool bli_cntl_packm_params_rev_iter_if_lower( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->rev_iter_if_lower;
}

BLIS_INLINE pack_t bli_cntl_packm_params_pack_schema( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->pack_schema;
}

BLIS_INLINE packbuf_t bli_cntl_packm_params_pack_buf_type( cntl_t* cntl )
{
	packm_params_t* ppp = ( packm_params_t* )cntl->params; return ppp->pack_buf_type;
}

// -----------------------------------------------------------------------------

cntl_t* bli_packm_cntl_create_node
     (
       rntm_t*   rntm,
       void_fp   var_func,
       void_fp   packm_var_func,
       bszid_t   bmid_m,
       bszid_t   bmid_n,
       bool      does_invert_diag,
       bool      rev_iter_if_upper,
       bool      rev_iter_if_lower,
       pack_t    pack_schema,
       packbuf_t pack_buf_type,
       cntl_t*   sub_node
     );

