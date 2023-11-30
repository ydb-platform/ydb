/** Compiler deficiency workarounds for libpqxx clients.
 *
 * Copyright (c) 2000-2019, Jeroen T. Vermeulen.
 *
 * See COPYING for copyright license.  If you did not receive a file called
 * COPYING with this source code, please notify the distributor of this mistake,
 * or contact the author.
 */
#ifndef PQXX_H_COMPILER_PUBLIC
#define PQXX_H_COMPILER_PUBLIC

// Workarounds & definitions that need to be included even in library's headers
#include "pqxx/config-public-compiler.h"

// Some compilers, Visual Studio in particular, don't seem to support the
// standard's ISO-646 keywords out of the box.
#include <ciso646>


#if defined(__GNUC__) && defined(PQXX_HAVE_GCC_CONST)
/// Declare function without effects and without reading anything but its args.
#define PQXX_CONST __attribute__ ((const))
#else
#define PQXX_CONST
#endif

#if defined(PQXX_HAVE_DEPRECATED)
/// Mark an item as deprecated.
#define PQXX_DEPRECATED [[deprecated]]
#elif defined(__GNUC__) && defined(PQXX_HAVE_GCC_DEPRECATED)
#define PQXX_DEPRECATED __attribute__ ((deprecated))
#else
#define PQXX_DEPRECATED
#endif

#if defined(__GNUC__) && defined(PQXX_HAVE_GCC_PURE)
/// Declare function "pure": no side effects, only reads globals and its args.
#define PQXX_PURE __attribute__ ((pure))
#else
#define PQXX_PURE
#endif


// Workarounds for Windows
#ifdef _WIN32

/* For now, export DLL symbols if _DLL is defined.  This is done automatically
 * by the compiler when linking to the dynamic version of the runtime library,
 * according to "gzh"
 */
#if !defined(PQXX_LIBEXPORT) && defined(PQXX_SHARED)
#define PQXX_LIBEXPORT __declspec(dllimport)
#endif	// !PQXX_LIBEXPORT && PQXX_SHARED


// Workarounds for Microsoft Visual C++
#ifdef _MSC_VER

// Suppress vtables on abstract classes.
#define PQXX_NOVTABLE __declspec(novtable)

// Automatically link with the appropriate libpq (static or dynamic, debug or
// release).  The default is to use the release DLL.  Define PQXX_PQ_STATIC to
// link to a static version of libpq, and _DEBUG to link to a debug version.
// The two may be combined.
#if defined(PQXX_AUTOLINK)
#if defined(PQXX_PQ_STATIC)
#ifdef _DEBUG
#pragma comment(lib, "libpqd")
#else
#pragma comment(lib, "libpq")
#endif
#else
#ifdef _DEBUG
#pragma comment(lib, "libpqddll")
#else
#pragma comment(lib, "libpqdll")
#endif
#endif
#endif

// If we're not compiling libpqxx itself, automatically link with the
// appropriate libpqxx library.  To link with the libpqxx DLL, define
// PQXX_SHARED; the default is to link with the static library.  A static link
// is the recommended practice.
//
// The preprocessor macro PQXX_INTERNAL is used to detect whether we
// are compiling the libpqxx library itself.  When you compile the library
// yourself using your own project file, make sure to include this macro.
#if defined(PQXX_AUTOLINK) && !defined(PQXX_INTERNAL)
  #ifdef PQXX_SHARED
    #ifdef _DEBUG
      #pragma comment(lib, "libpqxxD")
    #else
      #pragma comment(lib, "libpqxx")
    #endif
  #else // !PQXX_SHARED
    #ifdef _DEBUG
      #pragma comment(lib, "libpqxx_staticD")
    #else
      #pragma comment(lib, "libpqxx_static")
    #endif
  #endif
#endif

#endif	// _MSC_VER
#endif	// _WIN32


#ifndef PQXX_LIBEXPORT
#define PQXX_LIBEXPORT
#endif

#ifndef PQXX_PRIVATE
#define PQXX_PRIVATE
#endif

#ifndef PQXX_NOVTABLE
#define PQXX_NOVTABLE
#endif

#endif
