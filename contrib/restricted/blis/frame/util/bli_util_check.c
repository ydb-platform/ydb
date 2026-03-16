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

//
// Define object-based check functions.
//

#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       obj_t*  x, \
       obj_t*  asum  \
     ) \
{ \
	bli_utilv_xa_check( x, asum ); \
}

GENFRONT( asumv )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       obj_t*  x  \
     ) \
{ \
	bli_utilm_mkhst_check( x ); \
}

GENFRONT( mkherm )
GENFRONT( mksymm )
GENFRONT( mktrim )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       obj_t*  x, \
       obj_t*  norm  \
     ) \
{ \
	bli_utilv_norm_check( x, norm ); \
}

GENFRONT( norm1v )
GENFRONT( normfv )
GENFRONT( normiv )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       obj_t*  x, \
       obj_t*  norm  \
     ) \
{ \
	bli_utilm_norm_check( x, norm ); \
}

GENFRONT( norm1m )
GENFRONT( normfm )
GENFRONT( normim )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       FILE*  file, \
       char*  s1, \
       obj_t* x, \
       char*  format, \
       char*  s2  \
     ) \
{ \
	bli_utilm_fprint_check( file, s1, x, format, s2 ); \
}

GENFRONT( fprintv )
GENFRONT( fprintm )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       obj_t*  x  \
     ) \
{ \
	bli_utilm_rand_check( x ); \
}

GENFRONT( randv )
GENFRONT( randnv )
GENFRONT( randm )
GENFRONT( randnm )


#undef  GENFRONT
#define GENFRONT( opname ) \
\
void PASTEMAC(opname,_check) \
     ( \
       obj_t*  x, \
       obj_t*  scale, \
       obj_t*  sumsq  \
     ) \
{ \
	bli_utilv_sumsqv_check( x, scale, sumsq ); \
}

GENFRONT( sumsqv )


// -----------------------------------------------------------------------------

void bli_utilv_xa_check
     (
       obj_t*  x,
       obj_t*  asum
     )
{
	err_t e_val;

	// Check object datatypes.

	e_val = bli_check_floating_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( asum );
	bli_check_error_code( e_val );

	// Check object dimensions.

	e_val = bli_check_vector_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_scalar_object( asum );
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).

	e_val = bli_check_object_buffer( x );
	bli_check_error_code( e_val );

	e_val = bli_check_object_buffer( asum );
	bli_check_error_code( e_val );
}

void bli_utilm_mkhst_check
     (
       obj_t*  a
     )
{
	err_t e_val;

	// Check object datatypes.

	e_val = bli_check_floating_object( a );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( a );
	bli_check_error_code( e_val );

	// Check object dimensions.

	e_val = bli_check_matrix_object( a );
	bli_check_error_code( e_val );

	e_val = bli_check_square_object( a );
	bli_check_error_code( e_val );

	e_val = bli_check_object_diag_offset_equals( a, 0 );
	bli_check_error_code( e_val );

	// Check matrix storage.

	e_val = bli_check_upper_or_lower_object( a );
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).

	e_val = bli_check_object_buffer( a );
	bli_check_error_code( e_val );
}

void bli_utilv_norm_check
     (
       obj_t*  x,
       obj_t*  norm
     )
{
	err_t e_val;

	// Check object datatypes.

	e_val = bli_check_floating_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_noninteger_object( norm );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( norm );
	bli_check_error_code( e_val );

	e_val = bli_check_object_real_proj_of( x, norm );
	bli_check_error_code( e_val );

	// Check object dimensions.

	e_val = bli_check_vector_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_scalar_object( norm );
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).

	e_val = bli_check_object_buffer( x );
	bli_check_error_code( e_val );

	e_val = bli_check_object_buffer( norm );
	bli_check_error_code( e_val );
}


void bli_utilm_norm_check
     (
       obj_t*  x,
       obj_t*  norm
     )
{
	err_t e_val;

	// Check object datatypes.

	e_val = bli_check_floating_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_noninteger_object( norm );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( norm );
	bli_check_error_code( e_val );

	e_val = bli_check_object_real_proj_of( x, norm );
	bli_check_error_code( e_val );

	// Check object dimensions.

	e_val = bli_check_matrix_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_scalar_object( norm );
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).

	e_val = bli_check_object_buffer( x );
	bli_check_error_code( e_val );

	e_val = bli_check_object_buffer( norm );
	bli_check_error_code( e_val );
}

void bli_utilm_fprint_check
     (
       FILE*  file,
       char*  s1,
       obj_t* x,
       char*  format,
       char*  s2
     )
{
	err_t e_val;

	// Check argument pointers.
	
	e_val = bli_check_null_pointer( file );
	bli_check_error_code( e_val );

	e_val = bli_check_null_pointer( s1 );
	bli_check_error_code( e_val );

	e_val = bli_check_null_pointer( s2 ); 
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).

	e_val = bli_check_object_buffer( x ); 
	bli_check_error_code( e_val );
}

void bli_utilm_rand_check
     (
       obj_t* x
     )
{
	err_t e_val;

	// Check object datatypes.

	e_val = bli_check_noninteger_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( x );
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).

	e_val = bli_check_object_buffer( x );
	bli_check_error_code( e_val );
}

void bli_utilv_sumsqv_check
     (
       obj_t*  x,
       obj_t*  scale,
       obj_t*  sumsq
     )
{
	err_t e_val;

	// Check object datatypes.

	e_val = bli_check_floating_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( scale );
	bli_check_error_code( e_val );

	e_val = bli_check_nonconstant_object( sumsq );
	bli_check_error_code( e_val );

	// Check object dimensions.

	e_val = bli_check_vector_object( x );
	bli_check_error_code( e_val );

	e_val = bli_check_scalar_object( scale );
	bli_check_error_code( e_val );
	
	e_val = bli_check_scalar_object( sumsq );
	bli_check_error_code( e_val );

	// Check object buffers (for non-NULLness).
	
	e_val = bli_check_object_buffer( x );
	bli_check_error_code( e_val );
	
	e_val = bli_check_object_buffer( scale );
	bli_check_error_code( e_val );

	e_val = bli_check_object_buffer( sumsq );
	bli_check_error_code( e_val );
}

