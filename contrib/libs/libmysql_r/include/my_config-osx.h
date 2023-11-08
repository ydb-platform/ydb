/* Copyright (c) 2009, 2019, Oracle and/or its affiliates. All rights reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License, version 2.0,
 as published by the Free Software Foundation.

 This program is also distributed with certain software (including
 but not limited to OpenSSL) that is licensed under separate terms,
 as designated in a particular file or component or in included license
 documentation.  The authors of MySQL hereby grant you an additional
 permission to link the program and your derivative works with the
 separately licensed software that they have included with MySQL.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License, version 2.0, for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef MY_CONFIG_H
#define MY_CONFIG_H

/*
 * From configure.cmake, in order of appearance
 */
#define HAVE_LLVM_LIBCPP 1

/* Libraries */
/* #undef HAVE_LIBM */
/* #undef HAVE_LIBNSL */
/* #undef HAVE_LIBCRYPT */
/* #undef HAVE_LIBSOCKET */
/* #undef HAVE_LIBDL */
/* #undef HAVE_LIBRT */
/* #undef HAVE_LIBWRAP */
/* #undef HAVE_LIBWRAP_PROTOTYPES */

/* Header files */
#define HAVE_ALLOCA_H 1
#define HAVE_ARPA_INET_H 1
#define HAVE_DLFCN_H 1
#define HAVE_EXECINFO_H 1
/* #undef HAVE_FPU_CONTROL_H */
#define HAVE_GRP_H 1
/* #undef HAVE_IEEEFP_H */
#define HAVE_LANGINFO_H 1
/* #undef HAVE_LSAN_INTERFACE_H */
/* #undef HAVE_MALLOC_H */
#define HAVE_NETINET_IN_H 1
#define HAVE_POLL_H 1
#define HAVE_PWD_H 1
#define HAVE_STRINGS_H 1
#define HAVE_SYS_CDEFS_H 1
#define HAVE_SYS_IOCTL_H 1
#define HAVE_SYS_MMAN_H 1
/* #undef HAVE_SYS_PRCTL_H */
#define HAVE_SYS_RESOURCE_H 1
#define HAVE_SYS_SELECT_H 1
#define HAVE_SYS_SOCKET_H 1
#define HAVE_TERM_H 1
#define HAVE_TERMIOS_H 1
/* #undef HAVE_TERMIO_H */
#define HAVE_UNISTD_H 1
#define HAVE_SYS_WAIT_H 1
#define HAVE_SYS_PARAM_H 1
#define HAVE_FNMATCH_H 1
#define HAVE_SYS_UN_H 1
#define HAVE_VIS_H 1
/* #undef HAVE_SASL_SASL_H */

/* Libevent */
/* #undef HAVE_DEVPOLL */
/* #undef HAVE_SYS_DEVPOLL_H */
/* #undef HAVE_SYS_EPOLL_H */
#define HAVE_TAILQFOREACH 1

/* Functions */
/* #undef HAVE_ALIGNED_MALLOC */
#define HAVE_BACKTRACE 1
#define HAVE_INDEX 1
#define HAVE_CHOWN 1
/* #undef HAVE_CUSERID */
/* #undef HAVE_DIRECTIO */
#define HAVE_FTRUNCATE 1
#define HAVE_FCHMOD 1
#define HAVE_FCNTL 1
#define HAVE_FDATASYNC 1
/* #undef HAVE_DECL_FDATASYNC */
/* #undef HAVE_FEDISABLEEXCEPT */
#define HAVE_FSEEKO 1
#define HAVE_FSYNC 1
/* #undef HAVE_GETHRTIME */
#define HAVE_GETNAMEINFO 1
#define HAVE_GETPASS 1
/* #undef HAVE_GETPASSPHRASE */
#define HAVE_GETPWNAM 1
#define HAVE_GETPWUID 1
#define HAVE_GETRLIMIT 1
#define HAVE_GETRUSAGE 1
#define HAVE_INITGROUPS 1
#define HAVE_ISSETUGID 1
#define HAVE_GETUID 1
#define HAVE_GETEUID 1
#define HAVE_GETGID 1
#define HAVE_GETEGID 1
/* #undef HAVE_LSAN_DO_RECOVERABLE_LEAK_CHECK */
#define HAVE_MADVISE 1
/* #undef HAVE_MALLOC_INFO */
/* #undef HAVE_MEMRCHR */
#define HAVE_MLOCK 1
#define HAVE_MLOCKALL 1
/* #undef HAVE_MMAP64 */
#define HAVE_POLL 1
/* #undef HAVE_POSIX_FALLOCATE */
#define HAVE_POSIX_MEMALIGN 1
#define HAVE_PREAD 1
/* #undef HAVE_PTHREAD_CONDATTR_SETCLOCK */
/* #undef HAVE_PTHREAD_GETAFFINITY_NP */
#define HAVE_PTHREAD_SIGMASK 1
/* #undef HAVE_SETFD */
#define HAVE_SIGACTION 1
#define HAVE_SLEEP 1
#define HAVE_STPCPY 1
#define HAVE_STPNCPY 1
#define HAVE_STRLCPY 1
#define HAVE_STRLCAT 1
#define HAVE_STRSIGNAL 1
#define HAVE_FGETLN 1
#define HAVE_STRSEP 1
/* #undef HAVE_TELL */
#define HAVE_VASPRINTF 1
/* #undef HAVE_MEMALIGN */
#define HAVE_NL_LANGINFO 1
/* #undef HAVE_HTONLL */
/* #undef HAVE_EPOLL */
/* #undef HAVE_EVENT_PORTS */
#define HAVE_INET_NTOP 1
#define HAVE_WORKING_KQUEUE 1
#define HAVE_TIMERADD 1
#define HAVE_TIMERCLEAR 1
#define HAVE_TIMERCMP 1
#define HAVE_TIMERISSET 1

/* WL2373 */
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_TIMES_H 1
#define HAVE_TIMES 1
#define HAVE_GETTIMEOFDAY 1

/* Symbols */
#define HAVE_LRAND48 1
#define GWINSZ_IN_SYS_IOCTL 1
#define FIONREAD_IN_SYS_IOCTL 1
#define FIONREAD_IN_SYS_FILIO 1
/* #undef HAVE_MADV_DONTDUMP */
/* #undef HAVE_O_TMPFILE */

#define HAVE_ISINF 1

#define HAVE_KQUEUE 1
/* #undef HAVE_SETNS */
#define HAVE_KQUEUE_TIMERS 1
/* #undef HAVE_POSIX_TIMERS */

/* Endianess */
/* #undef WORDS_BIGENDIAN */

/* Type sizes */
#define SIZEOF_VOIDP     8
#define SIZEOF_CHARP     8
#define SIZEOF_LONG      8
#define SIZEOF_SHORT     2
#define SIZEOF_INT       4
#define SIZEOF_LONG_LONG 8
#define SIZEOF_OFF_T     8
#define SIZEOF_TIME_T    8
/* #undef HAVE_ULONG */
#define HAVE_U_INT32_T 1
#define HAVE_TM_GMTOFF 1

/* Support for tagging symbols with __attribute__((visibility("hidden"))) */
#define HAVE_VISIBILITY_HIDDEN 1

/* Code tests*/
#define HAVE_CLOCK_GETTIME 1
#define HAVE_CLOCK_REALTIME 1
#define STACK_DIRECTION -1
#define TIME_WITH_SYS_TIME 1
/* #undef NO_FCNTL_NONBLOCK */
#define HAVE_PAUSE_INSTRUCTION 1
/* #undef HAVE_FAKE_PAUSE_INSTRUCTION */
/* #undef HAVE_HMT_PRIORITY_INSTRUCTION */
#define HAVE_ABI_CXA_DEMANGLE 1
#define HAVE_BUILTIN_UNREACHABLE 1
#define HAVE_BUILTIN_EXPECT 1
#define HAVE_BUILTIN_STPCPY 1
#define HAVE_GCC_ATOMIC_BUILTINS 1
#define HAVE_GCC_SYNC_BUILTINS 1
/* #undef HAVE_VALGRIND */
#define HAVE_SYS_GETTID 1
/* #undef HAVE_PTHREAD_GETTHREADID_NP */
#define HAVE_PTHREAD_THREADID_NP 1
/* #undef HAVE_INTEGER_PTHREAD_SELF */
/* #undef HAVE_PTHREAD_SETNAME_NP */

/* IPV6 */
/* #undef HAVE_NETINET_IN6_H */
/* #undef HAVE_STRUCT_IN6_ADDR */

/*
 * Platform specific CMake files
 */
#define MACHINE_TYPE "x86_64"
/* #undef LINUX_ALPINE */
/* #undef LINUX_SUSE */
/* #undef HAVE_LINUX_LARGE_PAGES */
/* #undef HAVE_SOLARIS_LARGE_PAGES */
/* #undef HAVE_SOLARIS_ATOMIC */
#define SYSTEM_TYPE "osx10.12"
/* This should mean case insensitive file system */
/* #undef FN_NO_CASE_SENSE */

/*
 * From main CMakeLists.txt
 */
#define MAX_INDEXES 64U
/* #undef WITH_INNODB_MEMCACHED */
/* #undef ENABLE_MEMCACHED_SASL */
/* #undef ENABLE_MEMCACHED_SASL_PWDB */
#define ENABLED_PROFILING 1
/* #undef HAVE_ASAN */
/* #undef HAVE_LSAN */
/* #undef HAVE_UBSAN */
/* #undef HAVE_TSAN */
/* #undef ENABLED_LOCAL_INFILE */

/* Lock Order */
/* #undef WITH_LOCK_ORDER */

/* Character sets and collations */
#define DEFAULT_MYSQL_HOME "/tmp/nds-install-mysql80"
#define SHAREDIR "/tmp/nds-install-mysql80/share/mysql"
#define DEFAULT_BASEDIR "/tmp/nds-install-mysql80"
#define MYSQL_DATADIR "/var/lib/mysql"
#define MYSQL_KEYRINGDIR "/tmp/nds-install-mysql80/keyring"
#define DEFAULT_CHARSET_HOME "/tmp/nds-install-mysql80"
#define PLUGINDIR "/tmp/nds-install-mysql80/lib/mysql/plugin"
#define DEFAULT_SYSCONFDIR "/tmp/nds-install-mysql80/etc"
#define DEFAULT_TMPDIR P_tmpdir
/*
 * Readline
 */
#define HAVE_MBSTATE_T
#define HAVE_LANGINFO_CODESET
#define HAVE_WCSDUP
#define HAVE_WCHAR_T 1
#define HAVE_WINT_T 1
/* #undef HAVE_CURSES_H */
/* #undef HAVE_NCURSES_H */
#define USE_LIBEDIT_INTERFACE 1
#define HAVE_HIST_ENTRY 1
#define USE_NEW_EDITLINE_INTERFACE 1

/*
 * Libedit
 */
/* #undef HAVE_DECL_TGOTO */

/*
 * Character sets
 */
#define MYSQL_DEFAULT_CHARSET_NAME "utf8mb4"
#define MYSQL_DEFAULT_COLLATION_NAME "utf8mb4_0900_ai_ci"

/*
 * Performance schema
 */
#define WITH_PERFSCHEMA_STORAGE_ENGINE 1
/* #undef DISABLE_PSI_THREAD */
/* #undef DISABLE_PSI_MUTEX */
/* #undef DISABLE_PSI_RWLOCK */
/* #undef DISABLE_PSI_COND */
/* #undef DISABLE_PSI_FILE */
/* #undef DISABLE_PSI_TABLE */
/* #undef DISABLE_PSI_SOCKET */
/* #undef DISABLE_PSI_STAGE */
/* #undef DISABLE_PSI_STATEMENT */
/* #undef DISABLE_PSI_SP */
/* #undef DISABLE_PSI_PS */
/* #undef DISABLE_PSI_IDLE */
/* #undef DISABLE_PSI_ERROR */
/* #undef DISABLE_PSI_STATEMENT_DIGEST */
/* #undef DISABLE_PSI_METADATA */
/* #undef DISABLE_PSI_MEMORY */
/* #undef DISABLE_PSI_TRANSACTION */

/*
 * MySQL version
 */
#define MYSQL_VERSION_MAJOR 8
#define MYSQL_VERSION_MINOR 0
#define MYSQL_VERSION_PATCH 17
#define MYSQL_VERSION_EXTRA ""
#define PACKAGE "mysql"
#define PACKAGE_VERSION "8.0.17"
#define VERSION "8.0.17"
#define PROTOCOL_VERSION 10

/*
 * CPU info
 */
#define CPU_LEVEL1_DCACHE_LINESIZE 64

/*
 * NDB
 */
/* #undef WITH_NDBCLUSTER_STORAGE_ENGINE */
/* #undef HAVE_PTHREAD_SETSCHEDPARAM */

/*
 * Other
 */
/* #undef EXTRA_DEBUG */

/*
 * Hardcoded values needed by libevent/NDB/memcached
 */
#define HAVE_FCNTL_H 1
#define HAVE_GETADDRINFO 1
#define HAVE_INTTYPES_H 1
/* libevent's select.c is not Windows compatible */
#ifndef _WIN32
#define HAVE_SELECT 1
#endif
#define HAVE_SIGNAL_H 1
#define HAVE_STDARG_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRTOK_R 1
#define HAVE_STRTOLL 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_TYPES_H 1
#define SIZEOF_CHAR 1

/*
 * Needed by libevent
 */
#define HAVE_SOCKLEN_T 1

/* For --secure-file-priv */
#define DEFAULT_SECURE_FILE_PRIV_DIR "NULL"
/* #undef HAVE_LIBNUMA */

/* For default value of --early_plugin_load */
/* #undef DEFAULT_EARLY_PLUGIN_LOAD */

/* For default value of --partial_revokes */
#define DEFAULT_PARTIAL_REVOKES 0

#define SO_EXT ".so"

#endif
