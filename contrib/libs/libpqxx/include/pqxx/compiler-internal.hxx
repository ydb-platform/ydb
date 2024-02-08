/** Compiler deficiency workarounds for compiling libpqxx itself.
 *
 * DO NOT INCLUDE THIS FILE when building client programs.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_COMPILER_INTERNAL
#define PQXX_H_COMPILER_INTERNAL


// Workarounds & definitions needed to compile libpqxx into a library
#include "pqxx/config-internal-compiler.h"

#ifdef _WIN32

#ifdef PQXX_SHARED
#undef  PQXX_LIBEXPORT
#define PQXX_LIBEXPORT	__declspec(dllexport)
#define PQXX_PRIVATE	__declspec()
#endif	// PQXX_SHARED

#ifdef _MSC_VER
#pragma warning (disable: 4251 4275 4273)
#pragma warning (disable: 4355)
#pragma warning (disable: 4996) // Deprecation warning, e.g. about strncpy().
#endif

#elif defined(__GNUC__) && defined(PQXX_HAVE_GCC_VISIBILITY)	// !_WIN32

#define PQXX_LIBEXPORT __attribute__ ((visibility("default")))
#define PQXX_PRIVATE __attribute__ ((visibility("hidden")))

#endif	// __GNUC__ && PQXX_HAVE_GCC_VISIBILITY


#include "pqxx/compiler-public.hxx"

#endif
