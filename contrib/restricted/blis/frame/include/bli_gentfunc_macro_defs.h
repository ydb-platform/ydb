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


#ifndef BLIS_GENTFUNC_MACRO_DEFS_H
#define BLIS_GENTFUNC_MACRO_DEFS_H

//
// -- MACROS TO INSERT FUNCTION-GENERATING MACROS ------------------------------
//



// -- Macros for generating BLAS routines --------------------------------------


// -- Basic one-operand macro --


#define INSERT_GENTFUNC_BLAS( blasname, blisname ) \
\
GENTFUNC( float,    s, blasname, blisname ) \
GENTFUNC( double,   d, blasname, blisname ) \
GENTFUNC( scomplex, c, blasname, blisname ) \
GENTFUNC( dcomplex, z, blasname, blisname )


// -- Basic one-operand macro with real domain only --


#define INSERT_GENTFUNCRO_BLAS( blasname, blisname ) \
\
GENTFUNCRO( float,    s, blasname, blisname ) \
GENTFUNCRO( double,   d, blasname, blisname )


// -- Basic one-operand macro with complex domain only and real projection --


#define INSERT_GENTFUNCCO_BLAS( blasname, blisname ) \
\
GENTFUNCCO( scomplex, float,  c, s, blasname, blisname ) \
GENTFUNCCO( dcomplex, double, z, d, blasname, blisname )


// -- Basic one-operand macro with conjugation (real funcs only, used only for dot, ger) --


#define INSERT_GENTFUNCDOTR_BLAS( blasname, blisname ) \
\
GENTFUNCDOT( float,    s,  , BLIS_NO_CONJUGATE, blasname, blisname ) \
GENTFUNCDOT( double,   d,  , BLIS_NO_CONJUGATE, blasname, blisname )


// -- Basic one-operand macro with conjugation (complex funcs only, used only for dot, ger) --


#define INSERT_GENTFUNCDOTC_BLAS( blasname, blisname ) \
\
GENTFUNCDOT( scomplex, c, c, BLIS_CONJUGATE,    blasname, blisname ) \
GENTFUNCDOT( scomplex, c, u, BLIS_NO_CONJUGATE, blasname, blisname ) \
GENTFUNCDOT( dcomplex, z, c, BLIS_CONJUGATE,    blasname, blisname ) \
GENTFUNCDOT( dcomplex, z, u, BLIS_NO_CONJUGATE, blasname, blisname )


// -- Basic one-operand macro with conjugation (used only for dot, ger) --


#define INSERT_GENTFUNCDOT_BLAS( blasname, blisname ) \
\
INSERT_GENTFUNCDOTR_BLAS( blasname, blisname ) \
INSERT_GENTFUNCDOTC_BLAS( blasname, blisname )


// -- Basic one-operand macro with real projection --


#define INSERT_GENTFUNCR_BLAS( rblasname, cblasname, blisname ) \
\
GENTFUNCR( float,    float,  s, s, rblasname, blisname ) \
GENTFUNCR( double,   double, d, d, rblasname, blisname ) \
GENTFUNCR( scomplex, float,  c, s, cblasname, blisname ) \
GENTFUNCR( dcomplex, double, z, d, cblasname, blisname )


// -- Alternate two-operand macro (one char for complex, one for real proj) --


#define INSERT_GENTFUNCR2_BLAS( blasname, blisname ) \
\
GENTFUNCR2( float,    float,  s,  , blasname, blisname ) \
GENTFUNCR2( double,   double, d,  , blasname, blisname ) \
GENTFUNCR2( scomplex, float,  c, s, blasname, blisname ) \
GENTFUNCR2( dcomplex, double, z, d, blasname, blisname )


// -- Extended two-operand macro (used only for scal) --


#define INSERT_GENTFUNCSCAL_BLAS( blasname, blisname ) \
\
GENTFUNCSCAL( float,    float,    s,  , blasname, blisname ) \
GENTFUNCSCAL( double,   double,   d,  , blasname, blisname ) \
GENTFUNCSCAL( scomplex, scomplex, c,  , blasname, blisname ) \
GENTFUNCSCAL( dcomplex, dcomplex, z,  , blasname, blisname ) \
GENTFUNCSCAL( scomplex, float,    c, s, blasname, blisname ) \
GENTFUNCSCAL( dcomplex, double,   z, d, blasname, blisname )




// -- Macros for functions with one operand ------------------------------------


// -- Basic one-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC_BASIC0( tfuncname ) \
\
GENTFUNC( float,    s, tfuncname ) \
GENTFUNC( double,   d, tfuncname ) \
GENTFUNC( scomplex, c, tfuncname ) \
GENTFUNC( dcomplex, z, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC_BASIC( tfuncname, varname ) \
\
GENTFUNC( float,    s, tfuncname, varname ) \
GENTFUNC( double,   d, tfuncname, varname ) \
GENTFUNC( scomplex, c, tfuncname, varname ) \
GENTFUNC( dcomplex, z, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC_BASIC2( tfuncname, varname1, varname2 ) \
\
GENTFUNC( float,    s, tfuncname, varname1, varname2 ) \
GENTFUNC( double,   d, tfuncname, varname1, varname2 ) \
GENTFUNC( scomplex, c, tfuncname, varname1, varname2 ) \
GENTFUNC( dcomplex, z, tfuncname, varname1, varname2 )

// -- (three auxiliary arguments) --

#define INSERT_GENTFUNC_BASIC3( tfuncname, varname1, varname2, varname3 ) \
\
GENTFUNC( float,    s, tfuncname, varname1, varname2, varname3 ) \
GENTFUNC( double,   d, tfuncname, varname1, varname2, varname3 ) \
GENTFUNC( scomplex, c, tfuncname, varname1, varname2, varname3 ) \
GENTFUNC( dcomplex, z, tfuncname, varname1, varname2, varname3 )

// -- (four auxiliary arguments) --

#define INSERT_GENTFUNC_BASIC4( tfuncname, varname1, varname2, varname3, varname4 ) \
\
GENTFUNC( float,    s, tfuncname, varname1, varname2, varname3, varname4 ) \
GENTFUNC( double,   d, tfuncname, varname1, varname2, varname3, varname4 ) \
GENTFUNC( scomplex, c, tfuncname, varname1, varname2, varname3, varname4 ) \
GENTFUNC( dcomplex, z, tfuncname, varname1, varname2, varname3, varname4 )



// -- Basic one-operand with real projection --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNCR_BASIC0( tfuncname ) \
\
GENTFUNCR( float,    float,  s, s, tfuncname ) \
GENTFUNCR( double,   double, d, d, tfuncname ) \
GENTFUNCR( scomplex, float,  c, s, tfuncname ) \
GENTFUNCR( dcomplex, double, z, d, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNCR_BASIC( tfuncname, varname ) \
\
GENTFUNCR( float,    float,  s, s, tfuncname, varname ) \
GENTFUNCR( double,   double, d, d, tfuncname, varname ) \
GENTFUNCR( scomplex, float,  c, s, tfuncname, varname ) \
GENTFUNCR( dcomplex, double, z, d, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNCR_BASIC2( tfuncname, varname1, varname2 ) \
\
GENTFUNCR( float,    float,  s, s, tfuncname, varname1, varname2 ) \
GENTFUNCR( double,   double, d, d, tfuncname, varname1, varname2 ) \
GENTFUNCR( scomplex, float,  c, s, tfuncname, varname1, varname2 ) \
GENTFUNCR( dcomplex, double, z, d, tfuncname, varname1, varname2 )

// -- (three auxiliary arguments) --

#define INSERT_GENTFUNCR_BASIC3( tfuncname, varname1, varname2, varname3  ) \
\
GENTFUNCR( float,    float,  s, s, tfuncname, varname1, varname2, varname3 ) \
GENTFUNCR( double,   double, d, d, tfuncname, varname1, varname2, varname3 ) \
GENTFUNCR( scomplex, float,  c, s, tfuncname, varname1, varname2, varname3 ) \
GENTFUNCR( dcomplex, double, z, d, tfuncname, varname1, varname2, varname3 )

// -- (four auxiliary arguments) --

#define INSERT_GENTFUNCR_BASIC4( tfuncname, varname1, varname2, varname3, varname4  ) \
\
GENTFUNCR( float,    float,  s, s, tfuncname, varname1, varname2, varname3, varname4 ) \
GENTFUNCR( double,   double, d, d, tfuncname, varname1, varname2, varname3, varname4 ) \
GENTFUNCR( scomplex, float,  c, s, tfuncname, varname1, varname2, varname3, varname4 ) \
GENTFUNCR( dcomplex, double, z, d, tfuncname, varname1, varname2, varname3, varname4 )



// -- Basic one-operand macro with real domain only --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNCRO_BASIC0( tfuncname ) \
\
GENTFUNCRO( float,  s, tfuncname ) \
GENTFUNCRO( double, d, tfuncname ) \

// -- (one auxiliary argument) --

#define INSERT_GENTFUNCRO_BASIC( tfuncname, varname ) \
\
GENTFUNCRO( float,  s, tfuncname, varname ) \
GENTFUNCRO( double, d, tfuncname, varname ) \



// -- Basic one-operand macro with complex domain only and real projection --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNCCO_BASIC0( tfuncname ) \
\
GENTFUNCCO( scomplex, float,  c, s, tfuncname ) \
GENTFUNCCO( dcomplex, double, z, d, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNCCO_BASIC( tfuncname, varname ) \
\
GENTFUNCCO( scomplex, float,  c, s, tfuncname, varname ) \
GENTFUNCCO( dcomplex, double, z, d, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNCCO_BASIC2( tfuncname, varname1, varname2 ) \
\
GENTFUNCCO( scomplex, float,  c, s, tfuncname, varname1, varname2 ) \
GENTFUNCCO( dcomplex, double, z, d, tfuncname, varname1, varname2 )

// -- (three auxiliary arguments) --

#define INSERT_GENTFUNCCO_BASIC3( tfuncname, varname1, varname2, varname3 ) \
\
GENTFUNCCO( scomplex, float,  c, s, tfuncname, varname1, varname2, varname3 ) \
GENTFUNCCO( dcomplex, double, z, d, tfuncname, varname1, varname2, varname3 )



// -- Basic one-operand macro with integer instance --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC_BASIC0_I( tfuncname ) \
\
GENTFUNC( float,    s, tfuncname ) \
GENTFUNC( double,   d, tfuncname ) \
GENTFUNC( scomplex, c, tfuncname ) \
GENTFUNC( dcomplex, z, tfuncname ) \
GENTFUNC( gint_t,   i, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC_BASIC_I( tfuncname, varname ) \
\
GENTFUNC( float,    s, tfuncname, varname ) \
GENTFUNC( double,   d, tfuncname, varname ) \
GENTFUNC( scomplex, c, tfuncname, varname ) \
GENTFUNC( dcomplex, z, tfuncname, varname ) \
GENTFUNC( gint_t,   i, tfuncname, varname )



// -- Basic one-operand with integer projection --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNCI_BASIC0( tfuncname ) \
\
GENTFUNCI( float,    gint_t, s, i, tfuncname ) \
GENTFUNCI( double,   gint_t, d, i, tfuncname ) \
GENTFUNCI( scomplex, gint_t, c, i, tfuncname ) \
GENTFUNCI( dcomplex, gint_t, z, i, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNCI_BASIC( tfuncname, varname ) \
\
GENTFUNCI( float,    gint_t, s, i, tfuncname, varname ) \
GENTFUNCI( double,   gint_t, d, i, tfuncname, varname ) \
GENTFUNCI( scomplex, gint_t, c, i, tfuncname, varname ) \
GENTFUNCI( dcomplex, gint_t, z, i, tfuncname, varname )



// -- Basic one-operand with real and integer projections --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNCRI_BASIC0( tfuncname ) \
\
GENTFUNCRI( float,    float,  gint_t, s, s, i, tfuncname ) \
GENTFUNCRI( double,   double, gint_t, d, d, i, tfuncname ) \
GENTFUNCRI( scomplex, float,  gint_t, c, s, i, tfuncname ) \
GENTFUNCRI( dcomplex, double, gint_t, z, d, i, tfuncname )




// -- Macros for functions with two primary operands ---------------------------


// -- Basic two-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2_BASIC0( tfuncname ) \
\
GENTFUNC2( float,    float,    s, s, tfuncname ) \
GENTFUNC2( double,   double,   d, d, tfuncname ) \
GENTFUNC2( scomplex, scomplex, c, c, tfuncname ) \
GENTFUNC2( dcomplex, dcomplex, z, z, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2_BASIC( tfuncname, varname ) \
\
GENTFUNC2( float,    float,    s, s, tfuncname, varname ) \
GENTFUNC2( double,   double,   d, d, tfuncname, varname ) \
GENTFUNC2( scomplex, scomplex, c, c, tfuncname, varname ) \
GENTFUNC2( dcomplex, dcomplex, z, z, tfuncname, varname )



// -- Mixed domain two-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2_MIX_D0( tfuncname ) \
\
GENTFUNC2( float,    scomplex, s, c, tfuncname ) \
GENTFUNC2( scomplex, float,    c, s, tfuncname ) \
\
GENTFUNC2( double,   dcomplex, d, z, tfuncname ) \
GENTFUNC2( dcomplex, double,   z, d, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2_MIX_D( tfuncname, varname ) \
\
GENTFUNC2( float,    scomplex, s, c, tfuncname, varname ) \
GENTFUNC2( scomplex, float,    c, s, tfuncname, varname ) \
\
GENTFUNC2( double,   dcomplex, d, z, tfuncname, varname ) \
GENTFUNC2( dcomplex, double,   z, d, tfuncname, varname )



// -- Mixed precision two-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2_MIX_P0( tfuncname ) \
\
GENTFUNC2( float,    double,   s, d, tfuncname ) \
GENTFUNC2( float,    dcomplex, s, z, tfuncname ) \
\
GENTFUNC2( double,   float,    d, s, tfuncname ) \
GENTFUNC2( double,   scomplex, d, c, tfuncname ) \
\
GENTFUNC2( scomplex, double,   c, d, tfuncname ) \
GENTFUNC2( scomplex, dcomplex, c, z, tfuncname ) \
\
GENTFUNC2( dcomplex, float,    z, s, tfuncname ) \
GENTFUNC2( dcomplex, scomplex, z, c, tfuncname ) \

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2_MIX_P( tfuncname, varname ) \
\
GENTFUNC2( float,    double,   s, d, tfuncname, varname ) \
GENTFUNC2( float,    dcomplex, s, z, tfuncname, varname ) \
\
GENTFUNC2( double,   float,    d, s, tfuncname, varname ) \
GENTFUNC2( double,   scomplex, d, c, tfuncname, varname ) \
\
GENTFUNC2( scomplex, double,   c, d, tfuncname, varname ) \
GENTFUNC2( scomplex, dcomplex, c, z, tfuncname, varname ) \
\
GENTFUNC2( dcomplex, float,    z, s, tfuncname, varname ) \
GENTFUNC2( dcomplex, scomplex, z, c, tfuncname, varname ) \



// -- Mixed domain/precision (all) two-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2_MIXDP0( tfuncname ) \
\
GENTFUNC2( float,    double,   s, d, tfuncname ) \
GENTFUNC2( float,    scomplex, s, c, tfuncname ) \
GENTFUNC2( float,    dcomplex, s, z, tfuncname ) \
\
GENTFUNC2( double,   float,    d, s, tfuncname ) \
GENTFUNC2( double,   scomplex, d, c, tfuncname ) \
GENTFUNC2( double,   dcomplex, d, z, tfuncname ) \
\
GENTFUNC2( scomplex, float,    c, s, tfuncname ) \
GENTFUNC2( scomplex, double,   c, d, tfuncname ) \
GENTFUNC2( scomplex, dcomplex, c, z, tfuncname ) \
\
GENTFUNC2( dcomplex, float,    z, s, tfuncname ) \
GENTFUNC2( dcomplex, double,   z, d, tfuncname ) \
GENTFUNC2( dcomplex, scomplex, z, c, tfuncname )


// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2_MIX_DP( tfuncname, varname ) \
\
GENTFUNC2( float,    double,   s, d, tfuncname, varname ) \
GENTFUNC2( float,    scomplex, s, c, tfuncname, varname ) \
GENTFUNC2( float,    dcomplex, s, z, tfuncname, varname ) \
\
GENTFUNC2( double,   float,    d, s, tfuncname, varname ) \
GENTFUNC2( double,   scomplex, d, c, tfuncname, varname ) \
GENTFUNC2( double,   dcomplex, d, z, tfuncname, varname ) \
\
GENTFUNC2( scomplex, float,    c, s, tfuncname, varname ) \
GENTFUNC2( scomplex, double,   c, d, tfuncname, varname ) \
GENTFUNC2( scomplex, dcomplex, c, z, tfuncname, varname ) \
\
GENTFUNC2( dcomplex, float,    z, s, tfuncname, varname ) \
GENTFUNC2( dcomplex, double,   z, d, tfuncname, varname ) \
GENTFUNC2( dcomplex, scomplex, z, c, tfuncname, varname )



// -- Basic two-operand with real projection of second operand --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2R_BASIC0( tfuncname ) \
\
GENTFUNC2R( float,    float,    float,    s, s, s, tfuncname ) \
GENTFUNC2R( double,   double,   double,   d, d, d, tfuncname ) \
GENTFUNC2R( scomplex, scomplex, float,    c, c, s, tfuncname ) \
GENTFUNC2R( dcomplex, dcomplex, double,   z, z, d, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2R_BASIC( tfuncname, varname ) \
\
GENTFUNC2R( float,    float,    float,    s, s, s, tfuncname, varname ) \
GENTFUNC2R( double,   double,   double,   d, d, d, tfuncname, varname ) \
GENTFUNC2R( scomplex, scomplex, float,    c, c, s, tfuncname, varname ) \
GENTFUNC2R( dcomplex, dcomplex, double,   z, z, d, tfuncname, varname )



// -- Mixed domain two-operand with real projection of second operand --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2R_MIX_D0( tfuncname ) \
\
GENTFUNC2R( float,    scomplex, float,    s, c, s, tfuncname ) \
GENTFUNC2R( scomplex, float,    float,    c, s, s, tfuncname ) \
\
GENTFUNC2R( double,   dcomplex, double,   d, z, d, tfuncname ) \
GENTFUNC2R( dcomplex, double,   double,   z, d, d, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2R_MIX_D( tfuncname, varname ) \
\
GENTFUNC2R( float,    scomplex, float,    s, c, s, tfuncname, varname ) \
GENTFUNC2R( scomplex, float,    float,    c, s, s, tfuncname, varname ) \
\
GENTFUNC2R( double,   dcomplex, double,   d, z, d, tfuncname, varname ) \
GENTFUNC2R( dcomplex, double,   double,   z, d, d, tfuncname, varname )



// -- Mixed precision two-operand with real projection of second operand --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2R_MIX_P0( tfuncname ) \
\
GENTFUNC2R( float,    double,   double,   s, d, d, tfuncname ) \
GENTFUNC2R( float,    dcomplex, double,   s, z, d, tfuncname ) \
\
GENTFUNC2R( double,   float,    float,    d, s, s, tfuncname ) \
GENTFUNC2R( double,   scomplex, float,    d, c, s, tfuncname ) \
\
GENTFUNC2R( scomplex, double,   double,   c, d, d, tfuncname ) \
GENTFUNC2R( scomplex, dcomplex, double,   c, z, d, tfuncname ) \
\
GENTFUNC2R( dcomplex, float,    float,    z, s, s, tfuncname ) \
GENTFUNC2R( dcomplex, scomplex, float,    z, c, s, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2R_MIX_P( tfuncname, varname ) \
\
GENTFUNC2R( float,    double,   double,   s, d, d, tfuncname, varname ) \
GENTFUNC2R( float,    dcomplex, double,   s, z, d, tfuncname, varname ) \
\
GENTFUNC2R( double,   float,    float,    d, s, s, tfuncname, varname ) \
GENTFUNC2R( double,   scomplex, float,    d, c, s, tfuncname, varname ) \
\
GENTFUNC2R( scomplex, double,   double,   c, d, d, tfuncname, varname ) \
GENTFUNC2R( scomplex, dcomplex, double,   c, z, d, tfuncname, varname ) \
\
GENTFUNC2R( dcomplex, float,    float,    z, s, s, tfuncname, varname ) \
GENTFUNC2R( dcomplex, scomplex, float,    z, c, s, tfuncname, varname )



// -- Mixed domain/precision (all) two-operand macro with real projection of second operand --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC2R_MIXDP0( tfuncname ) \
\
GENTFUNC2R( float,    double,   double,   s, d, d, tfuncname ) \
GENTFUNC2R( float,    scomplex, float,    s, c, s, tfuncname ) \
GENTFUNC2R( float,    dcomplex, double,   s, z, d, tfuncname ) \
\
GENTFUNC2R( double,   float,    float,    d, s, s, tfuncname ) \
GENTFUNC2R( double,   scomplex, float,    d, c, s, tfuncname ) \
GENTFUNC2R( double,   dcomplex, double,   d, z, d, tfuncname ) \
\
GENTFUNC2R( scomplex, float,    float,    c, s, s, tfuncname ) \
GENTFUNC2R( scomplex, double,   double,   c, d, d, tfuncname ) \
GENTFUNC2R( scomplex, dcomplex, double,   c, z, d, tfuncname ) \
\
GENTFUNC2R( dcomplex, float,    float,    z, s, s, tfuncname ) \
GENTFUNC2R( dcomplex, double,   double,   z, d, d, tfuncname ) \
GENTFUNC2R( dcomplex, scomplex, float,    z, c, s, tfuncname ) \

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC2R_MIX_DP( tfuncname, varname ) \
\
GENTFUNC2R( float,    double,   double,   s, d, d, tfuncname, varname ) \
GENTFUNC2R( float,    scomplex, float,    s, c, s, tfuncname, varname ) \
GENTFUNC2R( float,    dcomplex, double,   s, z, d, tfuncname, varname ) \
\
GENTFUNC2R( double,   float,    float,    d, s, s, tfuncname, varname ) \
GENTFUNC2R( double,   scomplex, float,    d, c, s, tfuncname, varname ) \
GENTFUNC2R( double,   dcomplex, double,   d, z, d, tfuncname, varname ) \
\
GENTFUNC2R( scomplex, float,    float,    c, s, s, tfuncname, varname ) \
GENTFUNC2R( scomplex, double,   double,   c, d, d, tfuncname, varname ) \
GENTFUNC2R( scomplex, dcomplex, double,   c, z, d, tfuncname, varname ) \
\
GENTFUNC2R( dcomplex, float,    float,    z, s, s, tfuncname, varname ) \
GENTFUNC2R( dcomplex, double,   double,   z, d, d, tfuncname, varname ) \
GENTFUNC2R( dcomplex, scomplex, float,    z, c, s, tfuncname, varname ) \




// -- Macros for functions with three primary operands -------------------------


// -- Basic three-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC3_BASIC0( tfuncname ) \
\
GENTFUNC3( float,    float,    float,    s, s, s, tfuncname ) \
GENTFUNC3( double,   double,   double,   d, d, d, tfuncname ) \
GENTFUNC3( scomplex, scomplex, scomplex, c, c, c, tfuncname ) \
GENTFUNC3( dcomplex, dcomplex, dcomplex, z, z, z, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC3_BASIC( tfuncname, varname ) \
\
GENTFUNC3( float,    float,    float,    s, s, s, tfuncname, varname ) \
GENTFUNC3( double,   double,   double,   d, d, d, tfuncname, varname ) \
GENTFUNC3( scomplex, scomplex, scomplex, c, c, c, tfuncname, varname ) \
GENTFUNC3( dcomplex, dcomplex, dcomplex, z, z, z, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC3_BASIC2( tfuncname, varname1, varname2 ) \
\
GENTFUNC3( float,    float,    float,    s, s, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   double,   double,   d, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, scomplex, scomplex, c, c, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, dcomplex, dcomplex, z, z, z, tfuncname, varname1, varname2 )



// -- Mixed domain three-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC3_MIX_D0( tfuncname ) \
\
GENTFUNC3( float,    float,    scomplex, s, s, c, tfuncname ) \
GENTFUNC3( float,    scomplex, float,    s, c, s, tfuncname ) \
GENTFUNC3( float,    scomplex, scomplex, s, c, c, tfuncname ) \
\
GENTFUNC3( double,   double,   dcomplex, d, d, z, tfuncname ) \
GENTFUNC3( double,   dcomplex, double,   d, z, d, tfuncname ) \
GENTFUNC3( double,   dcomplex, dcomplex, d, z, z, tfuncname ) \
\
GENTFUNC3( scomplex, float,    float,    c, s, s, tfuncname ) \
GENTFUNC3( scomplex, float,    scomplex, c, s, c, tfuncname ) \
GENTFUNC3( scomplex, scomplex, float,    c, c, s, tfuncname ) \
\
GENTFUNC3( dcomplex, double,   double,   z, d, d, tfuncname ) \
GENTFUNC3( dcomplex, double,   dcomplex, z, d, z, tfuncname ) \
GENTFUNC3( dcomplex, dcomplex, double,   z, z, d, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC3_MIX_D( tfuncname, varname ) \
\
GENTFUNC3( float,    float,    scomplex, s, s, c, tfuncname, varname ) \
GENTFUNC3( float,    scomplex, float,    s, c, s, tfuncname, varname ) \
GENTFUNC3( float,    scomplex, scomplex, s, c, c, tfuncname, varname ) \
\
GENTFUNC3( double,   double,   dcomplex, d, d, z, tfuncname, varname ) \
GENTFUNC3( double,   dcomplex, double,   d, z, d, tfuncname, varname ) \
GENTFUNC3( double,   dcomplex, dcomplex, d, z, z, tfuncname, varname ) \
\
GENTFUNC3( scomplex, float,    float,    c, s, s, tfuncname, varname ) \
GENTFUNC3( scomplex, float,    scomplex, c, s, c, tfuncname, varname ) \
GENTFUNC3( scomplex, scomplex, float,    c, c, s, tfuncname, varname ) \
\
GENTFUNC3( dcomplex, double,   double,   z, d, d, tfuncname, varname ) \
GENTFUNC3( dcomplex, double,   dcomplex, z, d, z, tfuncname, varname ) \
GENTFUNC3( dcomplex, dcomplex, double,   z, z, d, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC3_MIX_D2( tfuncname, varname1, varname2 ) \
\
GENTFUNC3( float,    float,    scomplex, s, s, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    scomplex, float,    s, c, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    scomplex, scomplex, s, c, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( double,   double,   dcomplex, d, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   dcomplex, double,   d, z, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   dcomplex, dcomplex, d, z, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( scomplex, float,    float,    c, s, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, float,    scomplex, c, s, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, scomplex, float,    c, c, s, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( dcomplex, double,   double,   z, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, double,   dcomplex, z, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, dcomplex, double,   z, z, d, tfuncname, varname1, varname2 )



// -- Mixed precision three-operand macro --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC3_MIX_P0( tfuncname ) \
\
GENTFUNC3( float,    float,    double,   s, s, d, tfuncname ) \
GENTFUNC3( float,    float,    dcomplex, s, s, z, tfuncname ) \
\
GENTFUNC3( float,    double,   float,    s, d, s, tfuncname ) \
GENTFUNC3( float,    double,   double,   s, d, d, tfuncname ) \
GENTFUNC3( float,    double,   scomplex, s, d, c, tfuncname ) \
GENTFUNC3( float,    double,   dcomplex, s, d, z, tfuncname ) \
\
GENTFUNC3( float,    scomplex, double,   s, c, d, tfuncname ) \
GENTFUNC3( float,    scomplex, dcomplex, s, c, z, tfuncname ) \
\
GENTFUNC3( float,    dcomplex, float,    s, z, s, tfuncname ) \
GENTFUNC3( float,    dcomplex, double,   s, z, d, tfuncname ) \
GENTFUNC3( float,    dcomplex, scomplex, s, z, c, tfuncname ) \
GENTFUNC3( float,    dcomplex, dcomplex, s, z, z, tfuncname ) \
\
\
GENTFUNC3( double,   float,    float,    d, s, s, tfuncname ) \
GENTFUNC3( double,   float,    double,   d, s, d, tfuncname ) \
GENTFUNC3( double,   float,    scomplex, d, s, c, tfuncname ) \
GENTFUNC3( double,   float,    dcomplex, d, s, z, tfuncname ) \
\
GENTFUNC3( double,   double,   float,    d, d, s, tfuncname ) \
GENTFUNC3( double,   double,   scomplex, d, d, c, tfuncname ) \
\
GENTFUNC3( double,   scomplex, float,    d, c, s, tfuncname ) \
GENTFUNC3( double,   scomplex, double,   d, c, d, tfuncname ) \
GENTFUNC3( double,   scomplex, scomplex, d, c, c, tfuncname ) \
GENTFUNC3( double,   scomplex, dcomplex, d, c, z, tfuncname ) \
\
GENTFUNC3( double,   dcomplex, float,    d, z, s, tfuncname ) \
GENTFUNC3( double,   dcomplex, scomplex, d, z, c, tfuncname ) \
\
\
GENTFUNC3( scomplex, float,    double,   c, s, d, tfuncname ) \
GENTFUNC3( scomplex, float,    dcomplex, c, s, z, tfuncname ) \
\
GENTFUNC3( scomplex, double,   float,    c, d, s, tfuncname ) \
GENTFUNC3( scomplex, double,   double,   c, d, d, tfuncname ) \
GENTFUNC3( scomplex, double,   scomplex, c, d, c, tfuncname ) \
GENTFUNC3( scomplex, double,   dcomplex, c, d, z, tfuncname ) \
\
GENTFUNC3( scomplex, scomplex, double,   c, c, d, tfuncname ) \
GENTFUNC3( scomplex, scomplex, dcomplex, c, c, z, tfuncname ) \
\
GENTFUNC3( scomplex, dcomplex, float,    c, z, s, tfuncname ) \
GENTFUNC3( scomplex, dcomplex, double,   c, z, d, tfuncname ) \
GENTFUNC3( scomplex, dcomplex, scomplex, c, z, c, tfuncname ) \
GENTFUNC3( scomplex, dcomplex, dcomplex, c, z, z, tfuncname ) \
\
\
GENTFUNC3( dcomplex, float,    float,    z, s, s, tfuncname ) \
GENTFUNC3( dcomplex, float,    double,   z, s, d, tfuncname ) \
GENTFUNC3( dcomplex, float,    scomplex, z, s, c, tfuncname ) \
GENTFUNC3( dcomplex, float,    dcomplex, z, s, z, tfuncname ) \
\
GENTFUNC3( dcomplex, double,   float,    z, d, s, tfuncname ) \
GENTFUNC3( dcomplex, double,   scomplex, z, d, c, tfuncname ) \
\
GENTFUNC3( dcomplex, scomplex, float,    z, c, s, tfuncname ) \
GENTFUNC3( dcomplex, scomplex, double,   z, c, d, tfuncname ) \
GENTFUNC3( dcomplex, scomplex, scomplex, z, c, c, tfuncname ) \
GENTFUNC3( dcomplex, scomplex, dcomplex, z, c, z, tfuncname ) \
\
GENTFUNC3( dcomplex, dcomplex, float,    z, z, s, tfuncname ) \
GENTFUNC3( dcomplex, dcomplex, scomplex, z, z, c, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC3_MIX_P( tfuncname, varname ) \
\
GENTFUNC3( float,    float,    double,   s, s, d, tfuncname, varname ) \
GENTFUNC3( float,    float,    dcomplex, s, s, z, tfuncname, varname ) \
\
GENTFUNC3( float,    double,   float,    s, d, s, tfuncname, varname ) \
GENTFUNC3( float,    double,   double,   s, d, d, tfuncname, varname ) \
GENTFUNC3( float,    double,   scomplex, s, d, c, tfuncname, varname ) \
GENTFUNC3( float,    double,   dcomplex, s, d, z, tfuncname, varname ) \
\
GENTFUNC3( float,    scomplex, double,   s, c, d, tfuncname, varname ) \
GENTFUNC3( float,    scomplex, dcomplex, s, c, z, tfuncname, varname ) \
\
GENTFUNC3( float,    dcomplex, float,    s, z, s, tfuncname, varname ) \
GENTFUNC3( float,    dcomplex, double,   s, z, d, tfuncname, varname ) \
GENTFUNC3( float,    dcomplex, scomplex, s, z, c, tfuncname, varname ) \
GENTFUNC3( float,    dcomplex, dcomplex, s, z, z, tfuncname, varname ) \
\
\
GENTFUNC3( double,   float,    float,    d, s, s, tfuncname, varname ) \
GENTFUNC3( double,   float,    double,   d, s, d, tfuncname, varname ) \
GENTFUNC3( double,   float,    scomplex, d, s, c, tfuncname, varname ) \
GENTFUNC3( double,   float,    dcomplex, d, s, z, tfuncname, varname ) \
\
GENTFUNC3( double,   double,   float,    d, d, s, tfuncname, varname ) \
GENTFUNC3( double,   double,   scomplex, d, d, c, tfuncname, varname ) \
\
GENTFUNC3( double,   scomplex, float,    d, c, s, tfuncname, varname ) \
GENTFUNC3( double,   scomplex, double,   d, c, d, tfuncname, varname ) \
GENTFUNC3( double,   scomplex, scomplex, d, c, c, tfuncname, varname ) \
GENTFUNC3( double,   scomplex, dcomplex, d, c, z, tfuncname, varname ) \
\
GENTFUNC3( double,   dcomplex, float,    d, z, s, tfuncname, varname ) \
GENTFUNC3( double,   dcomplex, scomplex, d, z, c, tfuncname, varname ) \
\
\
GENTFUNC3( scomplex, float,    double,   c, s, d, tfuncname, varname ) \
GENTFUNC3( scomplex, float,    dcomplex, c, s, z, tfuncname, varname ) \
\
GENTFUNC3( scomplex, double,   float,    c, d, s, tfuncname, varname ) \
GENTFUNC3( scomplex, double,   double,   c, d, d, tfuncname, varname ) \
GENTFUNC3( scomplex, double,   scomplex, c, d, c, tfuncname, varname ) \
GENTFUNC3( scomplex, double,   dcomplex, c, d, z, tfuncname, varname ) \
\
GENTFUNC3( scomplex, scomplex, double,   c, c, d, tfuncname, varname ) \
GENTFUNC3( scomplex, scomplex, dcomplex, c, c, z, tfuncname, varname ) \
\
GENTFUNC3( scomplex, dcomplex, float,    c, z, s, tfuncname, varname ) \
GENTFUNC3( scomplex, dcomplex, double,   c, z, d, tfuncname, varname ) \
GENTFUNC3( scomplex, dcomplex, scomplex, c, z, c, tfuncname, varname ) \
GENTFUNC3( scomplex, dcomplex, dcomplex, c, z, z, tfuncname, varname ) \
\
\
GENTFUNC3( dcomplex, float,    float,    z, s, s, tfuncname, varname ) \
GENTFUNC3( dcomplex, float,    double,   z, s, d, tfuncname, varname ) \
GENTFUNC3( dcomplex, float,    scomplex, z, s, c, tfuncname, varname ) \
GENTFUNC3( dcomplex, float,    dcomplex, z, s, z, tfuncname, varname ) \
\
GENTFUNC3( dcomplex, double,   float,    z, d, s, tfuncname, varname ) \
GENTFUNC3( dcomplex, double,   scomplex, z, d, c, tfuncname, varname ) \
\
GENTFUNC3( dcomplex, scomplex, float,    z, c, s, tfuncname, varname ) \
GENTFUNC3( dcomplex, scomplex, double,   z, c, d, tfuncname, varname ) \
GENTFUNC3( dcomplex, scomplex, scomplex, z, c, c, tfuncname, varname ) \
GENTFUNC3( dcomplex, scomplex, dcomplex, z, c, z, tfuncname, varname ) \
\
GENTFUNC3( dcomplex, dcomplex, float,    z, z, s, tfuncname, varname ) \
GENTFUNC3( dcomplex, dcomplex, scomplex, z, z, c, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC3_MIX_P2( tfuncname, varname1, varname2 ) \
\
GENTFUNC3( float,    float,    double,   s, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    float,    dcomplex, s, s, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( float,    double,   float,    s, d, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    double,   double,   s, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    double,   scomplex, s, d, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    double,   dcomplex, s, d, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( float,    scomplex, double,   s, c, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    scomplex, dcomplex, s, c, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( float,    dcomplex, float,    s, z, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    dcomplex, double,   s, z, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    dcomplex, scomplex, s, z, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( float,    dcomplex, dcomplex, s, z, z, tfuncname, varname1, varname2 ) \
\
\
GENTFUNC3( double,   float,    float,    d, s, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   float,    double,   d, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   float,    scomplex, d, s, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   float,    dcomplex, d, s, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( double,   double,   float,    d, d, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   double,   scomplex, d, d, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( double,   scomplex, float,    d, c, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   scomplex, double,   d, c, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   scomplex, scomplex, d, c, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   scomplex, dcomplex, d, c, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( double,   dcomplex, float,    d, z, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( double,   dcomplex, scomplex, d, z, c, tfuncname, varname1, varname2 ) \
\
\
GENTFUNC3( scomplex, float,    double,   c, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, float,    dcomplex, c, s, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( scomplex, double,   float,    c, d, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, double,   double,   c, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, double,   scomplex, c, d, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, double,   dcomplex, c, d, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( scomplex, scomplex, double,   c, c, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, scomplex, dcomplex, c, c, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( scomplex, dcomplex, float,    c, z, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, dcomplex, double,   c, z, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, dcomplex, scomplex, c, z, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( scomplex, dcomplex, dcomplex, c, z, z, tfuncname, varname1, varname2 ) \
\
\
GENTFUNC3( dcomplex, float,    float,    z, s, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, float,    double,   z, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, float,    scomplex, z, s, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, float,    dcomplex, z, s, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( dcomplex, double,   float,    z, d, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, double,   scomplex, z, d, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( dcomplex, scomplex, float,    z, c, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, scomplex, double,   z, c, d, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, scomplex, scomplex, z, c, c, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, scomplex, dcomplex, z, c, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3( dcomplex, dcomplex, float,    z, z, s, tfuncname, varname1, varname2 ) \
GENTFUNC3( dcomplex, dcomplex, scomplex, z, z, c, tfuncname, varname1, varname2 )



// -- Basic three-operand with union of operands 1 and 2 --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC3U12_BASIC0( tfuncname ) \
\
GENTFUNC3U12( float,    float,    float,    float,    s, s, s, s, tfuncname ) \
GENTFUNC3U12( double,   double,   double,   double,   d, d, d, d, tfuncname ) \
GENTFUNC3U12( scomplex, scomplex, scomplex, scomplex, c, c, c, c, tfuncname ) \
GENTFUNC3U12( dcomplex, dcomplex, dcomplex, dcomplex, z, z, z, z, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC3U12_BASIC( tfuncname, varname ) \
\
GENTFUNC3U12( float,    float,    float,    float,    s, s, s, s, tfuncname, varname ) \
GENTFUNC3U12( double,   double,   double,   double,   d, d, d, d, tfuncname, varname ) \
GENTFUNC3U12( scomplex, scomplex, scomplex, scomplex, c, c, c, c, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, dcomplex, dcomplex, dcomplex, z, z, z, z, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC3U12_BASIC2( tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( float,    float,    float,    float,    s, s, s, s, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   double,   double,   double,   d, d, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, scomplex, scomplex, scomplex, c, c, c, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, dcomplex, dcomplex, dcomplex, z, z, z, z, tfuncname, varname1, varname2 )



// -- Mixed domain three-operand with union of operands 1 and 2 --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC3U12_MIX_D0( tfuncname ) \
\
GENTFUNC3U12( float,    float,    scomplex, float,    s, s, c, s, tfuncname ) \
GENTFUNC3U12( float,    scomplex, float,    scomplex, s, c, s, c, tfuncname ) \
GENTFUNC3U12( float,    scomplex, scomplex, scomplex, s, c, c, c, tfuncname ) \
\
GENTFUNC3U12( double,   double,   dcomplex, double,   d, d, z, d, tfuncname ) \
GENTFUNC3U12( double,   dcomplex, double,   dcomplex, d, z, d, z, tfuncname ) \
GENTFUNC3U12( double,   dcomplex, dcomplex, dcomplex, d, z, z, z, tfuncname ) \
\
GENTFUNC3U12( scomplex, float,    float,    scomplex, c, s, s, c, tfuncname ) \
GENTFUNC3U12( scomplex, float,    scomplex, scomplex, c, s, c, c, tfuncname ) \
GENTFUNC3U12( scomplex, scomplex, float,    scomplex, c, c, s, c, tfuncname ) \
\
GENTFUNC3U12( dcomplex, double,   double,   dcomplex, z, d, d, z, tfuncname ) \
GENTFUNC3U12( dcomplex, double,   dcomplex, dcomplex, z, d, z, z, tfuncname ) \
GENTFUNC3U12( dcomplex, dcomplex, double,   dcomplex, z, z, d, z, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC3U12_MIX_D( tfuncname, varname ) \
\
GENTFUNC3U12( float,    float,    scomplex, float,    s, s, c, s, tfuncname, varname ) \
GENTFUNC3U12( float,    scomplex, float,    scomplex, s, c, s, c, tfuncname, varname ) \
GENTFUNC3U12( float,    scomplex, scomplex, scomplex, s, c, c, c, tfuncname, varname ) \
\
GENTFUNC3U12( double,   double,   dcomplex, double,   d, d, z, d, tfuncname, varname ) \
GENTFUNC3U12( double,   dcomplex, double,   dcomplex, d, z, d, z, tfuncname, varname ) \
GENTFUNC3U12( double,   dcomplex, dcomplex, dcomplex, d, z, z, z, tfuncname, varname ) \
\
GENTFUNC3U12( scomplex, float,    float,    scomplex, c, s, s, c, tfuncname, varname ) \
GENTFUNC3U12( scomplex, float,    scomplex, scomplex, c, s, c, c, tfuncname, varname ) \
GENTFUNC3U12( scomplex, scomplex, float,    scomplex, c, c, s, c, tfuncname, varname ) \
\
GENTFUNC3U12( dcomplex, double,   double,   dcomplex, z, d, d, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, double,   dcomplex, dcomplex, z, d, z, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, dcomplex, double,   dcomplex, z, z, d, z, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC3U12_MIX_D2( tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( float,    float,    scomplex, float,    s, s, c, s, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    scomplex, float,    scomplex, s, c, s, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    scomplex, scomplex, scomplex, s, c, c, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( double,   double,   dcomplex, double,   d, d, z, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   dcomplex, double,   dcomplex, d, z, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   dcomplex, dcomplex, dcomplex, d, z, z, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( scomplex, float,    float,    scomplex, c, s, s, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, float,    scomplex, scomplex, c, s, c, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, scomplex, float,    scomplex, c, c, s, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( dcomplex, double,   double,   dcomplex, z, d, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, double,   dcomplex, dcomplex, z, d, z, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, dcomplex, double,   dcomplex, z, z, d, z, tfuncname, varname1, varname2 )



// -- Mixed precision three-operand with union of operands 1 and 2 --

// -- (no auxiliary arguments) --

#define INSERT_GENTFUNC3U12_MIX_P0( tfuncname ) \
\
GENTFUNC3U12( float,    float,    double,   float,    s, s, d, s, tfuncname ) \
GENTFUNC3U12( float,    float,    dcomplex, float,    s, s, z, s, tfuncname ) \
\
GENTFUNC3U12( float,    double,   float,    double,   s, d, s, d, tfuncname ) \
GENTFUNC3U12( float,    double,   double,   double,   s, d, d, d, tfuncname ) \
GENTFUNC3U12( float,    double,   scomplex, double,   s, d, c, d, tfuncname ) \
GENTFUNC3U12( float,    double,   dcomplex, double,   s, d, z, d, tfuncname ) \
\
GENTFUNC3U12( float,    scomplex, double,   scomplex, s, c, d, c, tfuncname ) \
GENTFUNC3U12( float,    scomplex, dcomplex, scomplex, s, c, z, c, tfuncname ) \
\
GENTFUNC3U12( float,    dcomplex, float,    dcomplex, s, z, s, z, tfuncname ) \
GENTFUNC3U12( float,    dcomplex, double,   dcomplex, s, z, d, z, tfuncname ) \
GENTFUNC3U12( float,    dcomplex, scomplex, dcomplex, s, z, c, z, tfuncname ) \
GENTFUNC3U12( float,    dcomplex, dcomplex, dcomplex, s, z, z, z, tfuncname ) \
\
\
GENTFUNC3U12( double,   float,    float,    double,   d, s, s, d, tfuncname ) \
GENTFUNC3U12( double,   float,    double,   double,   d, s, d, d, tfuncname ) \
GENTFUNC3U12( double,   float,    scomplex, double,   d, s, c, d, tfuncname ) \
GENTFUNC3U12( double,   float,    dcomplex, double,   d, s, z, d, tfuncname ) \
\
GENTFUNC3U12( double,   double,   float,    double,   d, d, s, d, tfuncname ) \
GENTFUNC3U12( double,   double,   scomplex, double,   d, d, c, d, tfuncname ) \
\
GENTFUNC3U12( double,   scomplex, float,    dcomplex, d, c, s, z, tfuncname ) \
GENTFUNC3U12( double,   scomplex, double,   dcomplex, d, c, d, z, tfuncname ) \
GENTFUNC3U12( double,   scomplex, scomplex, dcomplex, d, c, c, z, tfuncname ) \
GENTFUNC3U12( double,   scomplex, dcomplex, dcomplex, d, c, z, z, tfuncname ) \
\
GENTFUNC3U12( double,   dcomplex, float,    dcomplex, d, z, s, z, tfuncname ) \
GENTFUNC3U12( double,   dcomplex, scomplex, dcomplex, d, z, c, z, tfuncname ) \
\
\
GENTFUNC3U12( scomplex, float,    double,   scomplex, c, s, d, c, tfuncname ) \
GENTFUNC3U12( scomplex, float,    dcomplex, scomplex, c, s, z, c, tfuncname ) \
\
GENTFUNC3U12( scomplex, double,   float,    dcomplex, c, d, s, z, tfuncname ) \
GENTFUNC3U12( scomplex, double,   double,   dcomplex, c, d, d, z, tfuncname ) \
GENTFUNC3U12( scomplex, double,   scomplex, dcomplex, c, d, c, z, tfuncname ) \
GENTFUNC3U12( scomplex, double,   dcomplex, dcomplex, c, d, z, z, tfuncname ) \
\
GENTFUNC3U12( scomplex, scomplex, double,   scomplex, c, c, d, c, tfuncname ) \
GENTFUNC3U12( scomplex, scomplex, dcomplex, scomplex, c, c, z, c, tfuncname ) \
\
GENTFUNC3U12( scomplex, dcomplex, float,    dcomplex, c, z, s, z, tfuncname ) \
GENTFUNC3U12( scomplex, dcomplex, double,   dcomplex, c, z, d, z, tfuncname ) \
GENTFUNC3U12( scomplex, dcomplex, scomplex, dcomplex, c, z, c, z, tfuncname ) \
GENTFUNC3U12( scomplex, dcomplex, dcomplex, dcomplex, c, z, z, z, tfuncname ) \
\
\
GENTFUNC3U12( dcomplex, float,    float,    dcomplex, z, s, s, z, tfuncname ) \
GENTFUNC3U12( dcomplex, float,    double,   dcomplex, z, s, d, z, tfuncname ) \
GENTFUNC3U12( dcomplex, float,    scomplex, dcomplex, z, s, c, z, tfuncname ) \
GENTFUNC3U12( dcomplex, float,    dcomplex, dcomplex, z, s, z, z, tfuncname ) \
\
GENTFUNC3U12( dcomplex, double,   float,    dcomplex, z, d, s, z, tfuncname ) \
GENTFUNC3U12( dcomplex, double,   scomplex, dcomplex, z, d, c, z, tfuncname ) \
\
GENTFUNC3U12( dcomplex, scomplex, float,    dcomplex, z, c, s, z, tfuncname ) \
GENTFUNC3U12( dcomplex, scomplex, double,   dcomplex, z, c, d, z, tfuncname ) \
GENTFUNC3U12( dcomplex, scomplex, scomplex, dcomplex, z, c, c, z, tfuncname ) \
GENTFUNC3U12( dcomplex, scomplex, dcomplex, dcomplex, z, c, z, z, tfuncname ) \
\
GENTFUNC3U12( dcomplex, dcomplex, float,    dcomplex, z, z, s, z, tfuncname ) \
GENTFUNC3U12( dcomplex, dcomplex, scomplex, dcomplex, z, z, c, z, tfuncname )

// -- (one auxiliary argument) --

#define INSERT_GENTFUNC3U12_MIX_P( tfuncname, varname ) \
\
GENTFUNC3U12( float,    float,    double,   float,    s, s, d, s, tfuncname, varname ) \
GENTFUNC3U12( float,    float,    dcomplex, float,    s, s, z, s, tfuncname, varname ) \
\
GENTFUNC3U12( float,    double,   float,    double,   s, d, s, d, tfuncname, varname ) \
GENTFUNC3U12( float,    double,   double,   double,   s, d, d, d, tfuncname, varname ) \
GENTFUNC3U12( float,    double,   scomplex, double,   s, d, c, d, tfuncname, varname ) \
GENTFUNC3U12( float,    double,   dcomplex, double,   s, d, z, d, tfuncname, varname ) \
\
GENTFUNC3U12( float,    scomplex, double,   scomplex, s, c, d, c, tfuncname, varname ) \
GENTFUNC3U12( float,    scomplex, dcomplex, scomplex, s, c, z, c, tfuncname, varname ) \
\
GENTFUNC3U12( float,    dcomplex, float,    dcomplex, s, z, s, z, tfuncname, varname ) \
GENTFUNC3U12( float,    dcomplex, double,   dcomplex, s, z, d, z, tfuncname, varname ) \
GENTFUNC3U12( float,    dcomplex, scomplex, dcomplex, s, z, c, z, tfuncname, varname ) \
GENTFUNC3U12( float,    dcomplex, dcomplex, dcomplex, s, z, z, z, tfuncname, varname ) \
\
\
GENTFUNC3U12( double,   float,    float,    double,   d, s, s, d, tfuncname, varname ) \
GENTFUNC3U12( double,   float,    double,   double,   d, s, d, d, tfuncname, varname ) \
GENTFUNC3U12( double,   float,    scomplex, double,   d, s, c, d, tfuncname, varname ) \
GENTFUNC3U12( double,   float,    dcomplex, double,   d, s, z, d, tfuncname, varname ) \
\
GENTFUNC3U12( double,   double,   float,    double,   d, d, s, d, tfuncname, varname ) \
GENTFUNC3U12( double,   double,   scomplex, double,   d, d, c, d, tfuncname, varname ) \
\
GENTFUNC3U12( double,   scomplex, float,    dcomplex, d, c, s, z, tfuncname, varname ) \
GENTFUNC3U12( double,   scomplex, double,   dcomplex, d, c, d, z, tfuncname, varname ) \
GENTFUNC3U12( double,   scomplex, scomplex, dcomplex, d, c, c, z, tfuncname, varname ) \
GENTFUNC3U12( double,   scomplex, dcomplex, dcomplex, d, c, z, z, tfuncname, varname ) \
\
GENTFUNC3U12( double,   dcomplex, float,    dcomplex, d, z, s, z, tfuncname, varname ) \
GENTFUNC3U12( double,   dcomplex, scomplex, dcomplex, d, z, c, z, tfuncname, varname ) \
\
\
GENTFUNC3U12( scomplex, float,    double,   scomplex, c, s, d, c, tfuncname, varname ) \
GENTFUNC3U12( scomplex, float,    dcomplex, scomplex, c, s, z, c, tfuncname, varname ) \
\
GENTFUNC3U12( scomplex, double,   float,    dcomplex, c, d, s, z, tfuncname, varname ) \
GENTFUNC3U12( scomplex, double,   double,   dcomplex, c, d, d, z, tfuncname, varname ) \
GENTFUNC3U12( scomplex, double,   scomplex, dcomplex, c, d, c, z, tfuncname, varname ) \
GENTFUNC3U12( scomplex, double,   dcomplex, dcomplex, c, d, z, z, tfuncname, varname ) \
\
GENTFUNC3U12( scomplex, scomplex, double,   scomplex, c, c, d, c, tfuncname, varname ) \
GENTFUNC3U12( scomplex, scomplex, dcomplex, scomplex, c, c, z, c, tfuncname, varname ) \
\
GENTFUNC3U12( scomplex, dcomplex, float,    dcomplex, c, z, s, z, tfuncname, varname ) \
GENTFUNC3U12( scomplex, dcomplex, double,   dcomplex, c, z, d, z, tfuncname, varname ) \
GENTFUNC3U12( scomplex, dcomplex, scomplex, dcomplex, c, z, c, z, tfuncname, varname ) \
GENTFUNC3U12( scomplex, dcomplex, dcomplex, dcomplex, c, z, z, z, tfuncname, varname ) \
\
\
GENTFUNC3U12( dcomplex, float,    float,    dcomplex, z, s, s, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, float,    double,   dcomplex, z, s, d, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, float,    scomplex, dcomplex, z, s, c, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, float,    dcomplex, dcomplex, z, s, z, z, tfuncname, varname ) \
\
GENTFUNC3U12( dcomplex, double,   float,    dcomplex, z, d, s, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, double,   scomplex, dcomplex, z, d, c, z, tfuncname, varname ) \
\
GENTFUNC3U12( dcomplex, scomplex, float,    dcomplex, z, c, s, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, scomplex, double,   dcomplex, z, c, d, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, scomplex, scomplex, dcomplex, z, c, c, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, scomplex, dcomplex, dcomplex, z, c, z, z, tfuncname, varname ) \
\
GENTFUNC3U12( dcomplex, dcomplex, float,    dcomplex, z, z, s, z, tfuncname, varname ) \
GENTFUNC3U12( dcomplex, dcomplex, scomplex, dcomplex, z, z, c, z, tfuncname, varname )

// -- (two auxiliary arguments) --

#define INSERT_GENTFUNC3U12_MIX_P2( tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( float,    float,    double,   float,    s, s, d, s, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    float,    dcomplex, float,    s, s, z, s, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( float,    double,   float,    double,   s, d, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    double,   double,   double,   s, d, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    double,   scomplex, double,   s, d, c, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    double,   dcomplex, double,   s, d, z, d, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( float,    scomplex, double,   scomplex, s, c, d, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    scomplex, dcomplex, scomplex, s, c, z, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( float,    dcomplex, float,    dcomplex, s, z, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    dcomplex, double,   dcomplex, s, z, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    dcomplex, scomplex, dcomplex, s, z, c, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( float,    dcomplex, dcomplex, dcomplex, s, z, z, z, tfuncname, varname1, varname2 ) \
\
\
GENTFUNC3U12( double,   float,    float,    double,   d, s, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   float,    double,   double,   d, s, d, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   float,    scomplex, double,   d, s, c, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   float,    dcomplex, double,   d, s, z, d, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( double,   double,   float,    double,   d, d, s, d, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   double,   scomplex, double,   d, d, c, d, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( double,   scomplex, float,    dcomplex, d, c, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   scomplex, double,   dcomplex, d, c, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   scomplex, scomplex, dcomplex, d, c, c, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   scomplex, dcomplex, dcomplex, d, c, z, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( double,   dcomplex, float,    dcomplex, d, z, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( double,   dcomplex, scomplex, dcomplex, d, z, c, z, tfuncname, varname1, varname2 ) \
\
\
GENTFUNC3U12( scomplex, float,    double,   scomplex, c, s, d, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, float,    dcomplex, scomplex, c, s, z, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( scomplex, double,   float,    dcomplex, c, d, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, double,   double,   dcomplex, c, d, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, double,   scomplex, dcomplex, c, d, c, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, double,   dcomplex, dcomplex, c, d, z, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( scomplex, scomplex, double,   scomplex, c, c, d, c, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, scomplex, dcomplex, scomplex, c, c, z, c, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( scomplex, dcomplex, float,    dcomplex, c, z, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, dcomplex, double,   dcomplex, c, z, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, dcomplex, scomplex, dcomplex, c, z, c, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( scomplex, dcomplex, dcomplex, dcomplex, c, z, z, z, tfuncname, varname1, varname2 ) \
\
\
GENTFUNC3U12( dcomplex, float,    float,    dcomplex, z, s, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, float,    double,   dcomplex, z, s, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, float,    scomplex, dcomplex, z, s, c, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, float,    dcomplex, dcomplex, z, s, z, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( dcomplex, double,   float,    dcomplex, z, d, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, double,   scomplex, dcomplex, z, d, c, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( dcomplex, scomplex, float,    dcomplex, z, c, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, scomplex, double,   dcomplex, z, c, d, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, scomplex, scomplex, dcomplex, z, c, c, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, scomplex, dcomplex, dcomplex, z, c, z, z, tfuncname, varname1, varname2 ) \
\
GENTFUNC3U12( dcomplex, dcomplex, float,    dcomplex, z, z, s, z, tfuncname, varname1, varname2 ) \
GENTFUNC3U12( dcomplex, dcomplex, scomplex, dcomplex, z, z, c, z, tfuncname, varname1, varname2 )


#endif
