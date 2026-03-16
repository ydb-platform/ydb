/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2018-2019, Advanced Micro Devices, Inc.

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

#ifdef BLIS_CONFIGURETIME_CPUID
  #define BLIS_INLINE static
  #define BLIS_EXPORT_BLIS
  #include "bli_system.h"
  #include "bli_type_defs.h"
  #include "bli_arch.h"
  #include "bli_cpuid.h"
  #include "bli_env.h"
#else
  #include "blis.h"
#endif

// -----------------------------------------------------------------------------

// The arch_t id for the currently running hardware. We initialize to -1,
// which will be overwritten upon calling bli_arch_set_id().
static arch_t id = -1;

arch_t bli_arch_query_id( void )
{
	bli_arch_set_id_once();

	// Simply return the id that was previously cached.
	return id;
}

// -----------------------------------------------------------------------------

// A pthread structure used in pthread_once(). pthread_once() is guaranteed to
// execute exactly once among all threads that pass in this control object.
static bli_pthread_once_t once_id = BLIS_PTHREAD_ONCE_INIT;

void bli_arch_set_id_once( void )
{
#ifndef BLIS_CONFIGURETIME_CPUID
	bli_pthread_once( &once_id, bli_arch_set_id );
#endif
}

// -----------------------------------------------------------------------------

void bli_arch_set_id( void )
{
	// Check the environment variable BLIS_ARCH_DEBUG to see if the user
	// requested that we echo the result of the subconfiguration selection.
	bool do_logging = bli_env_get_var( "BLIS_ARCH_DEBUG", 0 );
	bli_arch_set_logging( do_logging );

	// Check the environment variable BLIS_ARCH_TYPE to see if the user
	// requested that we use a specific subconfiguration.
	dim_t req_id = bli_env_get_var( "BLIS_ARCH_TYPE", -1 );

#ifndef BLIS_CONFIGURETIME_CPUID
	if ( req_id != -1 )
	{
		// BLIS_ARCH_TYPE was set. Cautiously check whether its value is usable.

		// If req_id was set to an invalid arch_t value (ie: outside the range
		// [0,BLIS_NUM_ARCHS-1]), output an error message and abort.
		if ( bli_error_checking_is_enabled() )
		{
			err_t e_val = bli_check_valid_arch_id( req_id );
			bli_check_error_code( e_val );
		}

		// At this point, we know that req_id is in the valid range, but we
		// don't yet know if it refers to a context that was actually
		// initialized. Query the address of an internal context data structure
		// corresponding to req_id. This pointer will be NULL if the associated
		// subconfig is not available.
		cntx_t** req_cntx = bli_gks_lookup_id( req_id );

		// This function checks the context pointer and aborts with a useful
		// error message if the pointer is found to be NULL.
		if ( bli_error_checking_is_enabled() )
		{
			err_t e_val = bli_check_initialized_gks_cntx( req_cntx );
			bli_check_error_code( e_val );
		}

		// Finally, we can be confident that req_id (1) is in range and (2)
		// refers to a context that has been initialized.
		id = req_id;
	}
	else
#endif
	{
		// BLIS_ARCH_TYPE was unset. Proceed with normal subconfiguration
		// selection behavior.

		// Architecture families.
		#if defined BLIS_FAMILY_INTEL64 || \
		    defined BLIS_FAMILY_AMD64   || \
		    defined BLIS_FAMILY_X86_64  || \
		    defined BLIS_FAMILY_ARM64   || \
		    defined BLIS_FAMILY_ARM32
		id = bli_cpuid_query_id();
		#endif

		// Intel microarchitectures.
		#ifdef BLIS_FAMILY_SKX
		id = BLIS_ARCH_SKX;
		#endif
		#ifdef BLIS_FAMILY_KNL
		id = BLIS_ARCH_KNL;
		#endif
		#ifdef BLIS_FAMILY_KNC
		id = BLIS_ARCH_KNC;
		#endif
		#ifdef BLIS_FAMILY_HASWELL
		id = BLIS_ARCH_HASWELL;
		#endif
		#ifdef BLIS_FAMILY_SANDYBRIDGE
		id = BLIS_ARCH_SANDYBRIDGE;
		#endif
		#ifdef BLIS_FAMILY_PENRYN
		id = BLIS_ARCH_PENRYN;
		#endif

		// AMD microarchitectures.
		#ifdef BLIS_FAMILY_ZEN2
		id = BLIS_ARCH_ZEN2;
		#endif
		#ifdef BLIS_FAMILY_ZEN
		id = BLIS_ARCH_ZEN;
		#endif
		#ifdef BLIS_FAMILY_EXCAVATOR
		id = BLIS_ARCH_EXCAVATOR;
		#endif
		#ifdef BLIS_FAMILY_STEAMROLLER
		id = BLIS_ARCH_STEAMROLLER;
		#endif
		#ifdef BLIS_FAMILY_PILEDRIVER
		id = BLIS_ARCH_PILEDRIVER;
		#endif
		#ifdef BLIS_FAMILY_BULLDOZER
		id = BLIS_ARCH_BULLDOZER;
		#endif

		// ARM microarchitectures.
		#ifdef BLIS_FAMILY_THUNDERX2
		id = BLIS_ARCH_THUNDERX2;
		#endif
		#ifdef BLIS_FAMILY_CORTEXA57
		id = BLIS_ARCH_CORTEXA57;
		#endif
		#ifdef BLIS_FAMILY_CORTEXA53
		id = BLIS_ARCH_CORTEXA53;
		#endif
		#ifdef BLIS_FAMILY_CORTEXA15
		id = BLIS_ARCH_CORTEXA15;
		#endif
		#ifdef BLIS_FAMILY_CORTEXA9
		id = BLIS_ARCH_CORTEXA9;
		#endif

		// IBM microarchitectures.
		#ifdef BLIS_FAMILY_POWER10
		id = BLIS_ARCH_POWER10;
		#endif
		#ifdef BLIS_FAMILY_POWER9
		id = BLIS_ARCH_POWER9;
		#endif
		#ifdef BLIS_FAMILY_POWER7
		id = BLIS_ARCH_POWER7;
		#endif
		#ifdef BLIS_FAMILY_BGQ
		id = BLIS_ARCH_BGQ;
		#endif

		// Generic microarchitecture.
		#ifdef BLIS_FAMILY_GENERIC
		id = BLIS_ARCH_GENERIC;
		#endif
	}

	if ( bli_arch_get_logging() )
		fprintf( stderr, "libblis: selecting sub-configuration '%s'.\n",
				 bli_arch_string( id ) );

	//printf( "blis_arch_query_id(): id = %u\n", id );
	//exit(1);
}

// -----------------------------------------------------------------------------

// NOTE: This string array must be kept up-to-date with the arch_t
// enumeration that is typedef'ed in bli_type_defs.h. That is, the
// index order of each string should correspond to the implied/assigned
// enum value given to the corresponding BLIS_ARCH_ value.
static char* config_name[ BLIS_NUM_ARCHS ] =
{
    "skx",
    "knl",
    "knc",
    "haswell",
    "sandybridge",
    "penryn",

    "zen2",
    "zen",
    "excavator",
    "steamroller",
    "piledriver",
    "bulldozer",

    "thunderx2",
    "cortexa57",
    "cortexa53",
    "cortexa15",
    "cortexa9",

    "power10",
    "power9",
    "power7",
    "bgq",

    "generic"
};

char* bli_arch_string( arch_t id )
{
	return config_name[ id ];
}

// -----------------------------------------------------------------------------

static bool arch_dolog = 0;

void bli_arch_set_logging( bool dolog )
{
	arch_dolog = dolog;
}

bool bli_arch_get_logging( void )
{
	return arch_dolog;
}

void bli_arch_log( char* fmt, ... )
{
	char prefix[] = "libblis: ";
	int  n_chars  = strlen( prefix ) + strlen( fmt ) + 1;

	if ( bli_arch_get_logging() && fmt )
	{
		char* prefix_fmt = malloc( n_chars );

		snprintf( prefix_fmt, n_chars, "%s%s", prefix, fmt );

		va_list ap;
		va_start( ap, fmt );
		vfprintf( stderr, prefix_fmt, ap );
		va_end( ap );

		free( prefix_fmt );
	}
}

