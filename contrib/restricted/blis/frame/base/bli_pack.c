/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018, Advanced Micro Devices, Inc.

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

// The global rntm_t structure. (The definition resides in bli_rntm.c.)
extern rntm_t global_rntm;

// A mutex to allow synchronous access to global_rntm. (The definition
// resides in bli_rntm.c.)
extern bli_pthread_mutex_t global_rntm_mutex;

// -----------------------------------------------------------------------------

void bli_pack_init( void )
{
	// Read the environment variables and use them to initialize the
	// global runtime object.
	bli_pack_init_rntm_from_env( &global_rntm );
}

void bli_pack_finalize( void )
{
}

// -----------------------------------------------------------------------------

dim_t bli_pack_get_pack_a( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_pack_a( &global_rntm );
}

// -----------------------------------------------------------------------------

dim_t bli_pack_get_pack_b( void )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	return bli_rntm_pack_b( &global_rntm );
}

// ----------------------------------------------------------------------------

void bli_pack_set_pack_a( bool pack_a )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	// Acquire the mutex protecting global_rntm.
	bli_pthread_mutex_lock( &global_rntm_mutex );

	bli_rntm_set_pack_a( pack_a, &global_rntm );

	// Release the mutex protecting global_rntm.
	bli_pthread_mutex_unlock( &global_rntm_mutex );
}

// ----------------------------------------------------------------------------

void bli_pack_set_pack_b( bool pack_b )
{
	// We must ensure that global_rntm has been initialized.
	bli_init_once();

	// Acquire the mutex protecting global_rntm.
	bli_pthread_mutex_lock( &global_rntm_mutex );

	bli_rntm_set_pack_a( pack_b, &global_rntm );

	// Release the mutex protecting global_rntm.
	bli_pthread_mutex_unlock( &global_rntm_mutex );
}

// ----------------------------------------------------------------------------

void bli_pack_init_rntm_from_env
     (
       rntm_t* rntm
     )
{
	// NOTE: We don't need to acquire the global_rntm_mutex here because this
	// function is only called from bli_pack_init(), which is only called
	// by bli_init_once().

	bool pack_a;
	bool pack_b;

#if 1 //def BLIS_ENABLE_SELECTIVE_PACKING

	// Try to read BLIS_PACK_A and BLIS_PACK_B. For each variable, default to
	// -1 if it is unset.
	gint_t pack_a_env = bli_env_get_var( "BLIS_PACK_A", -1 );
	gint_t pack_b_env = bli_env_get_var( "BLIS_PACK_B", -1 );

	// Enforce the default behavior first, then check for affirmative FALSE, and
	// finally assume anything else is TRUE.
	if      ( pack_a_env == -1 ) pack_a = FALSE; // default behavior
	else if ( pack_a_env ==  0 ) pack_a = FALSE; // zero is FALSE
	else                         pack_a = TRUE;  // anything else is TRUE

	if      ( pack_b_env == -1 ) pack_b = FALSE; // default behavior
	else if ( pack_b_env ==  0 ) pack_b = FALSE; // zero is FALSE
	else                         pack_b = TRUE;  // anything else is TRUE

#else

	pack_a = TRUE;
	pack_b = TRUE;

#endif

	// Save the results back in the runtime object.
	bli_rntm_set_pack_a( pack_a, rntm );
	bli_rntm_set_pack_b( pack_b, rntm );

#if 0
	printf( "bli_pack_init_rntm_from_env()\n" );
	bli_rntm_print( rntm );
#endif
}

