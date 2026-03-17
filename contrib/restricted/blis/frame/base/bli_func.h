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

// -----------------------------------------------------------------------------

// func_t query

BLIS_INLINE void_fp bli_func_get_dt
     (
       num_t   dt,
       func_t* func
     )
{
    return func->ptr[ dt ];
}

// func_t modification

BLIS_INLINE void bli_func_set_dt
     (
       void_fp fp,
       num_t   dt,
       func_t* func
     )
{
    func->ptr[ dt ] = fp;
}

BLIS_INLINE void bli_func_copy_dt
     (
       num_t dt_src, func_t* func_src,
       num_t dt_dst, func_t* func_dst
     )
{
	void_fp fp = bli_func_get_dt( dt_src, func_src );

	bli_func_set_dt( fp, dt_dst, func_dst );
}

// -----------------------------------------------------------------------------

func_t* bli_func_create
     (
       void_fp ptr_s,
       void_fp ptr_d,
       void_fp ptr_c,
       void_fp ptr_z
     );

void bli_func_init
     (
       func_t* f,
       void_fp ptr_s,
       void_fp ptr_d,
       void_fp ptr_c,
       void_fp ptr_z
     );

void bli_func_init_null
     (
       func_t* f
     );

void bli_func_free( func_t* f );

// -----------------------------------------------------------------------------

bool bli_func_is_null_dt( num_t   dt,
                          func_t* f );
bool bli_func_is_null( func_t* f );

