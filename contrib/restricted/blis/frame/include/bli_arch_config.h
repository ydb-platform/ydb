/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2016, Hewlett Packard Enterprise Development LP
   Copyright (C) 2019 - 2020, Advanced Micro Devices, Inc.

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

#ifndef BLIS_ARCH_CONFIG_H
#define BLIS_ARCH_CONFIG_H

//
// -- Context initialization prototypes ----------------------------------------
//

// -- Intel64 architectures --
#ifdef BLIS_CONFIG_SKX
CNTX_INIT_PROTS( skx )
#endif
#ifdef BLIS_CONFIG_KNL
CNTX_INIT_PROTS( knl )
#endif
#ifdef BLIS_CONFIG_KNC
CNTX_INIT_PROTS( knc )
#endif
#ifdef BLIS_CONFIG_HASWELL
CNTX_INIT_PROTS( haswell )
#endif
#ifdef BLIS_CONFIG_SANDYBRIDGE
CNTX_INIT_PROTS( sandybridge )
#endif
#ifdef BLIS_CONFIG_PENRYN
CNTX_INIT_PROTS( penryn )
#endif

// -- AMD64 architectures --
#ifdef BLIS_CONFIG_ZEN2
CNTX_INIT_PROTS( zen2 )
#endif
#ifdef BLIS_CONFIG_ZEN
CNTX_INIT_PROTS( zen )
#endif
#ifdef BLIS_CONFIG_EXCAVATOR
CNTX_INIT_PROTS( excavator )
#endif
#ifdef BLIS_CONFIG_STEAMROLLER
CNTX_INIT_PROTS( steamroller )
#endif
#ifdef BLIS_CONFIG_PILEDRIVER
CNTX_INIT_PROTS( piledriver )
#endif
#ifdef BLIS_CONFIG_BULLDOZER
CNTX_INIT_PROTS( bulldozer )
#endif

// -- ARM architectures --

#ifdef BLIS_CONFIG_THUNDERX2
CNTX_INIT_PROTS( thunderx2 )
#endif
#ifdef BLIS_CONFIG_CORTEXA57
CNTX_INIT_PROTS( cortexa57 )
#endif
#ifdef BLIS_CONFIG_CORTEXA53
CNTX_INIT_PROTS( cortexa53 )
#endif
#ifdef BLIS_CONFIG_CORTEXA15
CNTX_INIT_PROTS( cortexa15 )
#endif
#ifdef BLIS_CONFIG_CORTEXA9
CNTX_INIT_PROTS( cortexa9 )
#endif

// -- IBM Power --

#ifdef BLIS_CONFIG_POWER10
CNTX_INIT_PROTS( power10 )
#endif
#ifdef BLIS_CONFIG_POWER9
CNTX_INIT_PROTS( power9 )
#endif
#ifdef BLIS_CONFIG_POWER7
CNTX_INIT_PROTS( power7 )
#endif

// -- IBM BG/Q --

#ifdef BLIS_CONFIG_BGQ
CNTX_INIT_PROTS( bgq )
#endif

// -- Generic --

#ifdef BLIS_CONFIG_GENERIC
CNTX_INIT_PROTS( generic )
#endif


//
// -- Architecture family-specific headers -------------------------------------
//

// -- x86_64 families --

#ifdef BLIS_FAMILY_INTEL64
#error #include "bli_family_intel64.h"
#endif
#ifdef BLIS_FAMILY_AMD64
#error #include "bli_family_amd64.h"
#endif
#ifdef BLIS_FAMILY_X86_64
#include "bli_family_x86_64.h"
#endif

// -- Intel64 architectures --
#ifdef BLIS_FAMILY_SKX
#error #include "bli_family_skx.h"
#endif
#ifdef BLIS_FAMILY_KNL
#error #include "bli_family_knl.h"
#endif
#ifdef BLIS_FAMILY_KNC
#error #include "bli_family_knc.h"
#endif
#ifdef BLIS_FAMILY_HASWELL
#error #include "bli_family_haswell.h"
#endif
#ifdef BLIS_FAMILY_SANDYBRIDGE
#error #include "bli_family_sandybridge.h"
#endif
#ifdef BLIS_FAMILY_PENRYN
#error #include "bli_family_penryn.h"
#endif

// -- AMD64 architectures --

#ifdef BLIS_FAMILY_ZEN2
#error #include "bli_family_zen2.h"
#endif
#ifdef BLIS_FAMILY_ZEN
#error #include "bli_family_zen.h"
#endif
#ifdef BLIS_FAMILY_EXCAVATOR
#error #include "bli_family_excavator.h"
#endif
#ifdef BLIS_FAMILY_STEAMROLLER
#error #include "bli_family_steamroller.h"
#endif
#ifdef BLIS_FAMILY_PILEDRIVER
#error #include "bli_family_piledriver.h"
#endif
#ifdef BLIS_FAMILY_BULLDOZER
#error #include "bli_family_bulldozer.h"
#endif

// -- ARM architectures --

#ifdef BLIS_FAMILY_THUNDERX2
#error #include "bli_family_thunderx2.h"
#endif
#ifdef BLIS_FAMILY_CORTEXA57
#error #include "bli_family_cortexa57.h"
#endif
#ifdef BLIS_FAMILY_CORTEXA53
#error #include "bli_family_cortexa53.h"
#endif
#ifdef BLIS_FAMILY_CORTEXA15
#error #include "bli_family_cortexa15.h"
#endif
#ifdef BLIS_FAMILY_CORTEXA9
#error #include "bli_family_cortexa9.h"
#endif

// -- IBM Power --

#ifdef BLIS_FAMILY_POWER10
#error #include "bli_family_power10.h"
#endif
#ifdef BLIS_FAMILY_POWER9
#error #include "bli_family_power9.h"
#endif
#ifdef BLIS_FAMILY_POWER7
#error #include "bli_family_power7.h"
#endif

// -- IBM BG/Q --

#ifdef BLIS_FAMILY_BGQ
#error #include "bli_family_bgq.h"
#endif

// -- Generic --

#ifdef BLIS_FAMILY_GENERIC
#include "bli_family_generic.h"
#endif


//
// -- kernel set prototypes ----------------------------------------------------
//

// -- Intel64 architectures --
#ifdef BLIS_KERNELS_SKX
#error #include "bli_kernels_skx.h"
#endif
#ifdef BLIS_KERNELS_KNL
#error #include "bli_kernels_knl.h"
#endif
#ifdef BLIS_KERNELS_KNC
#error #include "bli_kernels_knc.h"
#endif
#ifdef BLIS_KERNELS_HASWELL
#error #include "bli_kernels_haswell.h"
#endif
#ifdef BLIS_KERNELS_SANDYBRIDGE
#error #include "bli_kernels_sandybridge.h"
#endif
#ifdef BLIS_KERNELS_PENRYN
#error #include "bli_kernels_penryn.h"
#endif

// -- AMD64 architectures --

#ifdef BLIS_KERNELS_ZEN2
#error #include "bli_kernels_zen2.h"
#endif
#ifdef BLIS_KERNELS_ZEN
#error #include "bli_kernels_zen.h"
#endif
//#ifdef BLIS_KERNELS_EXCAVATOR
//#include "bli_kernels_excavator.h"
//#endif
//#ifdef BLIS_KERNELS_STEAMROLLER
//#include "bli_kernels_steamroller.h"
//#endif
#ifdef BLIS_KERNELS_PILEDRIVER
#error #include "bli_kernels_piledriver.h"
#endif
#ifdef BLIS_KERNELS_BULLDOZER
#error #include "bli_kernels_bulldozer.h"
#endif

// -- ARM architectures --

#ifdef BLIS_KERNELS_ARMSVE
#error #include "bli_kernels_armsve.h"
#endif
#ifdef BLIS_KERNELS_ARMV8A
#error #include "bli_kernels_armv8a.h"
#endif
#ifdef BLIS_KERNELS_ARMV7A
#error #include "bli_kernels_armv7a.h"
#endif

// -- IBM Power --

#ifdef BLIS_KERNELS_POWER10
#error #include "bli_kernels_power10.h"
#endif
#ifdef BLIS_KERNELS_POWER9
#error #include "bli_kernels_power9.h"
#endif
#ifdef BLIS_KERNELS_POWER7
#error #include "bli_kernels_power7.h"
#endif

// -- IBM BG/Q --

#ifdef BLIS_KERNELS_BGQ
#error #include "bli_kernels_bgq.h"
#endif



#endif

