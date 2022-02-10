/** \file
 * Basic type and constant definitions for ANTLR3 Runtime.
 */
#ifndef	_ANTLR3DEFS_HPP
#define	_ANTLR3DEFS_HPP

// [The "BSD licence"]
// Copyright (c) 2005-2009 Gokulakannan Somasundaram, ElectronDB

//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// not used in C++ target (kept for "historical" reasons, the generated code still uses this)
#define	ANTLR_SIZE_HINT 0U

/* Work out what operating system/compiler this is. We just do this once
 * here and use an internal symbol after this.
 */
#ifdef	_WIN64
# define	ANTLR_USE_64BIT
#endif

#ifdef	_WIN32

#ifndef WIN32_LEAN_AND_MEAN
#define	WIN32_LEAN_AND_MEAN
#endif

/* Allow VC 8 (vs2005) and above to use 'secure' versions of various functions such as sprintf
 */
#ifndef	_CRT_SECURE_NO_DEPRECATE 
#define	_CRT_SECURE_NO_DEPRECATE 
#endif

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include    <winsock2.h>

#define	ANTLR_INLINE	        __inline

typedef FILE *	    ANTLR_FDSC;

typedef struct sockaddr_in	ANTLR_SOCKADDRT, * pANTLR_SOCKADDRT;	// Type used for socket address declaration
typedef struct sockaddr		ANTLR_SOCKADDRC, * pANTLR_SOCKADDRC;	// Type used for cast on accept()

#define	ANTLR_CLOSESOCKET	closesocket

#else // Un*x

#ifdef __LP64__
#define ANTLR_USE_64BIT
#endif

#define ANTLR_INLINE   inline

typedef int SOCKET;
typedef FILE *	    ANTLR_FDSC;

#endif

// Standard integer types (since C++11)  (should work with MSVC 2010/2013, gcc, clang)
//
typedef std::int32_t     ANTLR_CHAR;
typedef std::uint32_t    ANTLR_UCHAR;

typedef std::int8_t	     ANTLR_INT8;
typedef std::int16_t	 ANTLR_INT16;
typedef std::int32_t	 ANTLR_INT32;
typedef std::int64_t	 ANTLR_INT64;

typedef std::uint8_t     ANTLR_UINT8;
typedef std::uint16_t    ANTLR_UINT16;
typedef std::uint32_t    ANTLR_UINT32;
typedef std::uint64_t    ANTLR_UINT64;
typedef std::uint64_t    ANTLR_BITWORD;

#ifdef	ANTLR_USE_64BIT
#define ANTLR_UINT64_CAST(ptr)	(ANTLR_UINT64)(ptr))
#define	ANTLR_UINT32_CAST(ptr) (ANTLR_UINT32)((ANTLR_UINT64)(ptr))
typedef ANTLR_INT64		ANTLR_MARKER;
typedef ANTLR_UINT64		ANTLR_INTKEY;
#else
#define ANTLR_UINT64_CAST(ptr) (ANTLR_UINT64)((ANTLR_UINT32)(ptr))
#define	ANTLR_UINT32_CAST(ptr)	(ANTLR_UINT32)(ptr)
typedef	ANTLR_INT32		ANTLR_MARKER;
typedef ANTLR_UINT32		ANTLR_INTKEY;
#endif

#define	ANTLR_UINT64_LIT(lit)  lit##ULL

#endif	/* _ANTLR3DEFS_H	*/
