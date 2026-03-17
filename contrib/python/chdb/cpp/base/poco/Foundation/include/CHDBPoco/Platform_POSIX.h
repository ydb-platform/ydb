//
// Platform_POSIX.h
//
// Library: Foundation
// Package: Core
// Module:  Platform
//
// Platform and architecture identification macros
// and platform-specific definitions for various POSIX platforms
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_Platform_POSIX_INCLUDED
#define CHDB_Foundation_Platform_POSIX_INCLUDED


//
// PA-RISC based HP-UX platforms have some issues...
//
#if defined(hpux) || defined(_hpux)
#    if defined(__hppa) || defined(__hppa__)
#        define CHDB_POCO_NO_SYS_SELECT_H 1
#    endif
#endif


//
// Thread-safety of local static initialization
//
#ifndef CHDB_POCO_LOCAL_STATIC_INIT_IS_THREADSAFE
#    define CHDB_POCO_LOCAL_STATIC_INIT_IS_THREADSAFE 1
#endif


//
// No syslog.h on QNX/BB10
//
#if defined(__QNXNTO__)
#    define CHDB_POCO_NO_SYSLOGCHANNEL
#endif


#endif // CHDB_Foundation_Platform_POSIX_INCLUDED
