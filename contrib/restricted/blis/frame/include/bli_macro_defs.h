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

#ifndef BLIS_MACRO_DEFS_H
#define BLIS_MACRO_DEFS_H


// -- Undefine restrict for C++ and C89/90 --

#ifdef __cplusplus
  // Language is C++; define restrict as nothing.
  #ifndef restrict
  #define restrict
  #endif
#elif __STDC_VERSION__ >= 199901L
  // Language is C99 (or later); do nothing since restrict is recognized.
#else
  // Language is pre-C99; define restrict as nothing.
  #ifndef restrict
  #define restrict
  #endif
#endif


// -- Define typeof() operator if using non-GNU compiler --

#ifndef __GNUC__
  #define typeof __typeof__
#else
  #ifndef typeof
  #define typeof __typeof__
  #endif
#endif


// -- BLIS Thread Local Storage Keyword --

// __thread for TLS is supported by GCC, CLANG, ICC, and IBMC.
// There is a small risk here as __GNUC__ can also be defined by some other
// compiler (other than ICC and CLANG which we know define it) that
// doesn't support __thread, as __GNUC__ is not quite unique to GCC.
// But the possibility of someone using such non-main-stream compiler
// for building BLIS is low.
#if defined(__GNUC__) || defined(__clang__) || defined(__ICC) || defined(__IBMC__)
  #define BLIS_THREAD_LOCAL __thread
#else
  #define BLIS_THREAD_LOCAL
#endif


// -- BLIS constructor/destructor function attribute --

// __attribute__((constructor/destructor)) is supported by GCC only.
// There is a small risk here as __GNUC__ can also be defined by some other
// compiler (other than ICC and CLANG which we know define it) that
// doesn't support this, as __GNUC__ is not quite unique to GCC.
// But the possibility of someone using such non-main-stream compiler
// for building BLIS is low.

#if defined(__ICC) || defined(__INTEL_COMPILER)
  // ICC defines __GNUC__ but doesn't support this
  #define BLIS_ATTRIB_CTOR
  #define BLIS_ATTRIB_DTOR
#elif defined(__clang__)
  // CLANG supports __attribute__, but its documentation doesn't
  // mention support for constructor/destructor. Compiling with
  // clang and testing shows that it does support.
  #define BLIS_ATTRIB_CTOR __attribute__((constructor))
  #define BLIS_ATTRIB_DTOR __attribute__((destructor))
#elif defined(__GNUC__)
  #define BLIS_ATTRIB_CTOR __attribute__((constructor))
  #define BLIS_ATTRIB_DTOR __attribute__((destructor))
#else
  #define BLIS_ATTRIB_CTOR
  #define BLIS_ATTRIB_DTOR
#endif


// -- Concatenation macros --

#define BLIS_FUNC_PREFIX_STR       "bli"

// We add an extra layer the definitions of these string-pasting macros
// because sometimes it is needed if, for example, one of the PASTE
// macros is invoked with an "op" argument that is itself a macro.

#define PASTEMAC0_(op)             bli_ ## op
#define PASTEMAC0(op)              PASTEMAC0_(op)

#define PASTEMAC_(ch,op)           bli_ ## ch  ## op
#define PASTEMAC(ch,op)            PASTEMAC_(ch,op)

#define PASTEMAC2_(ch1,ch2,op)     bli_ ## ch1 ## ch2 ## op
#define PASTEMAC2(ch1,ch2,op)      PASTEMAC2_(ch1,ch2,op)

#define PASTEMAC3_(ch1,ch2,ch3,op) bli_ ## ch1 ## ch2 ## ch3 ## op
#define PASTEMAC3(ch1,ch2,ch3,op)  PASTEMAC3_(ch1,ch2,ch3,op)

#define PASTEMAC4_(ch1,ch2,ch3,ch4,op) bli_ ## ch1 ## ch2 ## ch3 ## ch4 ## op
#define PASTEMAC4(ch1,ch2,ch3,ch4,op)  PASTEMAC4_(ch1,ch2,ch3,ch4,op)

#define PASTEMAC5_(ch1,ch2,ch3,ch4,ch5,op) bli_ ## ch1 ## ch2 ## ch3 ## ch4 ## ch5 ## op
#define PASTEMAC5(ch1,ch2,ch3,ch4,ch5,op)  PASTEMAC5_(ch1,ch2,ch3,ch4,ch5,op)

#define PASTEMAC6_(ch1,ch2,ch3,ch4,ch5,ch6,op) bli_ ## ch1 ## ch2 ## ch3 ## ch4 ## ch5 ## ch6 ## op
#define PASTEMAC6(ch1,ch2,ch3,ch4,ch5,ch6,op)  PASTEMAC6_(ch1,ch2,ch3,ch4,ch5,ch6,op)

#define PASTEBLACHK_(op)           bla_ ## op ## _check
#define PASTEBLACHK(op)            PASTEBLACHK_(op)

#define PASTECH0_(op)              op
#define PASTECH0(op)               PASTECH0_(op)

#define PASTECH_(ch,op)            ch ## op
#define PASTECH(ch,op)             PASTECH_(ch,op)

#define PASTECH2_(ch1,ch2,op)      ch1 ## ch2 ## op
#define PASTECH2(ch1,ch2,op)       PASTECH2_(ch1,ch2,op)

#define PASTECH3_(ch1,ch2,ch3,op)  ch1 ## ch2 ## ch3 ## op
#define PASTECH3(ch1,ch2,ch3,op)   PASTECH3_(ch1,ch2,ch3,op)

#define MKSTR(s1)                  #s1
#define STRINGIFY_INT( s )         MKSTR( s )

// Fortran-77 name-mangling macros.
#define PASTEF770(name)                                      name ## _
#define PASTEF77(ch1,name)                     ch1        ## name ## _
#define PASTEF772(ch1,ch2,name)                ch1 ## ch2 ## name ## _
#define PASTEF773(ch1,ch2,ch3,name)     ch1 ## ch2 ## ch3 ## name ## _

// -- Include other groups of macros

#include "bli_genarray_macro_defs.h"
#include "bli_gentdef_macro_defs.h"
#include "bli_gentfunc_macro_defs.h"
#include "bli_gentprot_macro_defs.h"

#include "bli_misc_macro_defs.h"
#include "bli_param_macro_defs.h"
#include "bli_obj_macro_defs.h"
#include "bli_complex_macro_defs.h"
#include "bli_scalar_macro_defs.h"
#include "bli_error_macro_defs.h"
#include "bli_blas_macro_defs.h"
#include "bli_builtin_macro_defs.h"

#include "bli_oapi_macro_defs.h"
#include "bli_tapi_macro_defs.h"


#endif
