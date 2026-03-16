//
// Config.h
//
// Library: Foundation
// Package: Core
// Module:  Foundation
//
// Feature configuration for the POCO libraries.
//
// Copyright (c) 2006-2016, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_Config_INCLUDED
#define CHDB_Foundation_Config_INCLUDED


// Define to enable Windows Unicode (UTF-8) support
// NOTE: As of POCO C++ Libraries release 1.6.0, compiling POCO
// without CHDB_POCO_WIN32_UTF8 defined on Windows is deprecated.
#define CHDB_POCO_WIN32_UTF8


// Define to enable C++11 support
// #define CHDB_POCO_ENABLE_CPP11


// Define to disable implicit linking
// #define CHDB_POCO_NO_AUTOMATIC_LIBS


// Define to disable automatic initialization
// Defining this will disable ALL automatic
// initialization framework-wide (e.g. Net
// on Windows, all Data back-ends, etc).
//
// #define CHDB_POCO_NO_AUTOMATIC_LIB_INIT


// Define to disable FPEnvironment support
// #define CHDB_POCO_NO_FPENVIRONMENT


// Define if std::wstring is not available
// #define CHDB_POCO_NO_WSTRING


// Define to disable shared memory
// #define CHDB_POCO_NO_SHAREDMEMORY


// Define if no <locale> header is available (such as on WinCE)
#define CHDB_POCO_NO_LOCALE


// Define to desired default thread stack size
// Zero means OS default
#ifndef CHDB_POCO_THREAD_STACK_SIZE
#    define CHDB_POCO_THREAD_STACK_SIZE 0
#endif


// Define to override system-provided
// minimum thread priority value on POSIX
// platforms (returned by CHDBPoco::Thread::getMinOSPriority()).
// #define CHDB_POCO_THREAD_PRIORITY_MIN 0


// Define to override system-provided
// maximum thread priority value on POSIX
// platforms (returned by CHDBPoco::Thread::getMaxOSPriority()).
// #define CHDB_POCO_THREAD_PRIORITY_MAX 31


// Define to disable small object optimization. If not
// defined, Any and Dynamic::Var (and similar optimization
// candidates) will be auto-allocated on the stack in
// cases when value holder fits into CHDB_POCO_SMALL_OBJECT_SIZE
// (see below).
//
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !!! NOTE: Any/Dynamic::Var SOO will NOT work reliably   !!!
// !!! without C++11 (std::aligned_storage in particular). !!!
// !!! Only comment this out if your compiler has support  !!!
// !!! for std::aligned_storage.                           !!!
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//
#ifndef POCO_ENABLE_SOO
#    define CHDB_POCO_NO_SOO
#endif


// Small object size in bytes. When assigned to Any or Var,
// objects larger than this value will be allocated on the heap,
// while those smaller will be placement new-ed into an
// internal buffer.
#if !defined(CHDB_POCO_SMALL_OBJECT_SIZE) && !defined(CHDB_POCO_NO_SOO)
#    define CHDB_POCO_SMALL_OBJECT_SIZE 32
#endif


// Define to disable compilation of DirectoryWatcher
// on platforms with no inotify.
// #define CHDB_POCO_NO_INOTIFY


// Following are options to remove certain features
// to reduce library/executable size for smaller
// embedded platforms. By enabling these options,
// the size of a statically executable can be
// reduced by a few 100 Kbytes.


// No automatic registration of FileChannel in
// LoggingFactory - avoids FileChannel and friends
// being linked to executable.
// #define CHDB_POCO_NO_FILECHANNEL


// No automatic registration of SplitterChannel in
// LoggingFactory - avoids SplitterChannel being
// linked to executable.
// #define CHDB_POCO_NO_SPLITTERCHANNEL


// No automatic registration of SyslogChannel in
// LoggingFactory - avoids SyslogChannel being
// linked to executable on Unix/Linux systems.
// #define CHDB_POCO_NO_SYSLOGCHANNEL


// Define to enable MSVC secure warnings
// #define CHDB_POCO_MSVC_SECURE_WARNINGS


// No support for INI file configurations in
// CHDBPoco::Util::Application.
// #define CHDB_POCO_UTIL_NO_INIFILECONFIGURATION


// No support for JSON configuration in
// CHDBPoco::Util::Application. Avoids linking of JSON
// library and saves a few 100 Kbytes.
// #define CHDB_POCO_UTIL_NO_JSONCONFIGURATION


// No support for XML configuration in
// CHDBPoco::Util::Application. Avoids linking of XML
// library and saves a few 100 Kbytes.
// #define CHDB_POCO_UTIL_NO_XMLCONFIGURATION


// No IPv6 support
// Define to disable IPv6
// #define CHDB_POCO_NET_NO_IPv6


// Windows CE has no locale support


// Enable the poco_debug_* and poco_trace_* macros
// even if the _DEBUG variable is not set.
// This allows the use of these macros in a release version.
// #define CHDB_POCO_LOG_DEBUG


// OpenSSL on Windows
//
// Poco has its own OpenSSL build system.
// See <https://github.com/pocoproject/openssl/blob/master/README.md>
// for details.
//
// These options are Windows only.
//
// To disable the use of Poco-provided OpenSSL binaries,
// define CHDB_POCO_EXTERNAL_OPENSSL.
//
// Possible values:
//   CHDB_POCO_EXTERNAL_OPENSSL_SLPRO:
//     Automatically link OpenSSL libraries from OpenSSL Windows installer provided
//     by Shining Light Productions <http://slproweb.com/products/Win32OpenSSL.html>
//     The (global) library search path must be set accordingly.
//   CHDB_POCO_EXTERNAL_OPENSSL_DEFAULT:
//     Automatically link OpenSSL libraries from standard OpenSSL Windows build.
//     The (global) library search path must be set accordingly.
//   empty or other value:
//     Do not link any OpenSSL libraries automatically. You will have to edit the
//     Visual C++ project files for Crypto and NetSSL_OpenSSL.
// #define CHDB_POCO_EXTERNAL_OPENSSL CHDB_POCO_EXTERNAL_OPENSSL_SLPRO


// Define to prevent changing the suffix for shared libraries
// to "d.so", "d.dll", etc. for _DEBUG builds in CHDBPoco::SharedLibrary.
// #define CHDB_POCO_NO_SHARED_LIBRARY_DEBUG_SUFFIX


// Disarm CHDB_POCO_DEPRECATED macro.
// #define CHDB_POCO_NO_DEPRECATED


#endif // CHDB_Foundation_Config_INCLUDED
