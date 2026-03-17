//
// Foundation.h
//
// Library: Foundation
// Package: Core
// Module:  Foundation
//
// Basic definitions for the POCO Foundation library.
// This file must be the first file included by every other Foundation
// header file.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Foundation_INCLUDED
#define DB_Foundation_Foundation_INCLUDED


//
// Include library configuration
//
#include "DBPoco/Config.h"


//
// Ensure that POCO_DLL is default unless POCO_STATIC is defined
//


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Foundation_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Foundation_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(Foundation_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define Foundation_API __attribute__((visibility("default")))
#    else
#        define Foundation_API
#    endif
#endif


//
// Automatically link Foundation library.
//


//
// Include platform-specific definitions
//
#include "DBPoco/Platform.h"
#if   defined(DB_POCO_OS_FAMILY_UNIX)
#    include "DBPoco/Platform_POSIX.h"
#endif


//
// Include alignment settings early
//
#include "DBPoco/Alignment.h"

//
// Cleanup inconsistencies
//


//
// DB_POCO_JOIN
//
// The following piece of macro magic joins the two
// arguments together, even when one of the arguments is
// itself a macro (see 16.3.1 in C++ standard).  The key
// is that macro expansion of macro arguments does not
// occur in DB_POCO_DO_JOIN2 but does in DB_POCO_DO_JOIN.
//
#define DB_POCO_JOIN(X, Y) DB_POCO_DO_JOIN(X, Y)
#define DB_POCO_DO_JOIN(X, Y) DB_POCO_DO_JOIN2(X, Y)
#define DB_POCO_DO_JOIN2(X, Y) X##Y


//
// DB_POCO_DEPRECATED
//
// A macro expanding to a compiler-specific clause to
// mark a class or function as deprecated.
//
#if defined(DB_POCO_NO_DEPRECATED)
#    define DB_POCO_DEPRECATED
#elif defined(_GNUC_)
#    define DB_POCO_DEPRECATED __attribute__((deprecated))
#else
#    define DB_POCO_DEPRECATED __attribute__((deprecated))
#endif


//
// Pull in basic definitions
//
#include <string>
#include "DBPoco/Bugcheck.h"
#include "DBPoco/Types.h"


#endif // DB_Foundation_Foundation_INCLUDED
