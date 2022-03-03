#pragma once

#include "pg_config-linux.h"

/* Define to the type of arg 1 of 'accept' */
#define ACCEPT_TYPE_ARG1 unsigned int

/* Define to the type of arg 3 of 'accept' */
#define ACCEPT_TYPE_ARG3 int

/* Define to the return type of 'accept' */
#define ACCEPT_TYPE_RETURN unsigned int PASCAL

/* Define to 1 if you have the `clock_gettime' function. */
#undef HAVE_CLOCK_GETTIME 

/* Define to 1 if your compiler handles computed gotos. */
#undef HAVE_COMPUTED_GOTO 

/* Define to 1 if you have the `crypt' function. */
#undef HAVE_CRYPT 

/* Define to 1 if you have the <crypt.h> header file. */
#undef HAVE_CRYPT_H 

/* Define to 1 if you have the declaration of `posix_fadvise', and to 0 if you
 *      don't. */ 
#undef HAVE_DECL_POSIX_FADVISE

/* Define to 1 if you have the declaration of `RTLD_GLOBAL', and to 0 if you
   don't. */
#undef HAVE_DECL_RTLD_GLOBAL

/* Define to 1 if you have the declaration of `RTLD_NOW', and to 0 if you
   don't. */
#undef HAVE_DECL_RTLD_NOW

/* Define to 1 if you have the declaration of `strlcat', and to 0 if you
   don't. */
#define HAVE_DECL_STRLCAT 0
  
/* Define to 1 if you have the declaration of `strlcpy', and to 0 if you
   don't. */ 
#define HAVE_DECL_STRLCPY 0  

/* Define to 1 if you have the `dlopen' function. */
#undef HAVE_DLOPEN 

/* Define to 1 if you have the `fdatasync' function. */
#undef HAVE_FDATASYNC

/* Define to 1 if you have __atomic_compare_exchange_n(int *, int *, int). */
#undef HAVE_GCC__ATOMIC_INT32_CAS

/* Define to 1 if you have __atomic_compare_exchange_n(int64 *, int64 *,
   int64). */
#undef HAVE_GCC__ATOMIC_INT64_CAS

/* Define to 1 if you have __sync_lock_test_and_set(char *) and friends. */
#undef HAVE_GCC__SYNC_CHAR_TAS

/* Define to 1 if you have the `getaddrinfo' function. */
#undef HAVE_GETADDRINFO 

/* Define to 1 if you have the `gethostbyname_r' function. */
#undef HAVE_GETHOSTBYNAME_R 

/* Define to 1 if you have the `getopt' function. */
#undef HAVE_GETOPT

/* Define to 1 if you have the <getopt.h> header file. */
#undef HAVE_GETOPT_H

/* Define to 1 if you have the `getopt_long' function. */
#undef HAVE_GETOPT_LONG

/* Define to 1 if you have the `getpwuid_r' function. */
#undef HAVE_GETPWUID_R 

/* Define to 1 if you have the `getrlimit' function. */
#undef HAVE_RLIMIT

/* Define to 1 if you have the `getrusage' function. */
#undef HAVE_GETRUSAGE

/* Define to 1 if you have the `inet_aton' function. */
#undef HAVE_INET_ATON 

/* Define to 1 if you have the <inttypes.h> header file. */
#undef HAVE_INTTYPES_H

/* Define to 1 if you have the <langinfo.h> header file. */
#undef HAVE_LANGINFO_H

/* Define to 1 if you have the `crypto' library (-lcrypto). */
#undef HAVE_LIBCRYPTO 

/* Define to 1 if `long int' works and is 64 bits. */
#define HAVE_LONG_INT_64 1

/* Define to 1 if the system has the type `long long int'. */
#define HAVE_LONG_LONG_INT 1

/* Define to 1 if you have the `mbstowcs_l' function. */
#define HAVE_MBSTOWCS_L 1

/* Define to 1 if the system has the type `MINIDUMP_TYPE'. */
#define HAVE_MINIDUMP_TYPE 1

/* Define to 1 if you have the `mkdtemp' function. */
#undef HAVE_MKDTEMP

/* Define to 1 if you have the <netinet/tcp.h> header file. */
#undef HAVE_NETINET_TCP_H 

/* Define to 1 if you have the `poll' function. */
#undef HAVE_POLL 

/* Define to 1 if you have the <poll.h> header file. */
#undef HAVE_POLL_H 

/* Define to 1 if you have the `posix_fadvise' function. */
#undef HAVE_POSIX_FADVISE

/* Define to 1 if you have the `posix_fallocate' function. */
#undef HAVE_POSIX_FALLOCATE 

/* Define to 1 if you have the `ppoll' function. */
#undef HAVE_PPOLL

/* Define to 1 if you have the `pread' function. */
#undef HAVE_PREAD 

/* Define if you have POSIX threads libraries and header files. */
#undef HAVE_PTHREAD

/* Have PTHREAD_PRIO_INHERIT. */
#define HAVE_PTHREAD_PRIO_INHERIT 1

/* Define to 1 if you have the `pwrite' function. */
#undef HAVE_PWRITE 

/* Define to 1 if you have the `readlink' function. */
#undef HAVE_READLINK 

/* Define to 1 if you have the global variable
   'rl_completion_append_character'. */
#undef HAVE_RL_COMPLETION_APPEND_CHARACTER 

/* Define to 1 if you have the `rl_completion_matches' function. */
#undef HAVE_RL_COMPLETION_MATCHES 

/* Define to 1 if you have the `rl_filename_completion_function' function. */
#undef HAVE_RL_FILENAME_COMPLETION_FUNCTION 

/* Define to 1 if you have the `setsid' function. */
#undef HAVE_SETSID 

/* Define to 1 if you have the `strchrnul' function. */
#undef HAVE_STRCHRNUL

/* Define to 1 if you have the `strerror_r' function. */
#undef HAVE_STRERROR_R 

/* Define to 1 if you have the <strings.h> header file. */
#undef HAVE_STRINGS_H

/* Define to 1 if you have the `strsignal' function. */
#undef HAVE_STRSIGNAL 

/* Define to 1 if the system has the type `struct option'. */
#undef HAVE_STRUCT_OPTION

/* Define to 1 if `tm_zone' is member of `struct tm'. */
#undef HAVE_STRUCT_TM_TM_ZONE 

/* Define to 1 if you have the `sync_file_range' function. */
#undef HAVE_SYNC_FILE_RANGE 

/* Define to 1 if you have the syslog interface. */
#undef HAVE_SYSLOG 

/* Define to 1 if you have the <sys/ipc.h> header file. */
#undef HAVE_SYS_IPC_H 

/* Define to 1 if you have the <sys/prctl.h> header file. */
#undef HAVE_SYS_PRCTL_H 

/* Define to 1 if you have the <sys/select.h> header file. */
#undef HAVE_SYS_SELECT_H 

/* Define to 1 if you have the <sys/sem.h> header file. */
#undef HAVE_SYS_SEM_H

/* Define to 1 if you have the <sys/shm.h> header file. */
#undef HAVE_SYS_SHM_H 

/* Define to 1 if you have the <sys/un.h> header file. */
#undef HAVE_SYS_UN_H 

/* Define to 1 if you have the <termios.h> header file. */
#undef HAVE_TERMIOS_H 

/* Define to 1 if your `struct tm' has `tm_zone'. Deprecated, use
   `HAVE_STRUCT_TM_TM_ZONE' instead. */
#undef HAVE_TM_ZONE 

/* Define to 1 if your compiler understands `typeof' or something similar. */
#undef HAVE_TYPEOF

/* Define to 1 if you have the external array `tzname'. */
#undef HAVE_TZNAME 

/* Define to 1 if you have unix sockets. */
#undef HAVE_UNIX_SOCKETS 

/* Define to 1 if you have the `unsetenv' function. */
#undef HAVE_UNSETENV 

/* Define to 1 if you have the `uselocale' function. */
#undef HAVE_USELOCALE 

/* Define to 1 if you have the `utimes' function. */
#undef HAVE_UTIMES 

/* Define to 1 if you have the `wcstombs_l' function. */
#define HAVE_WCSTOMBS_L 1

/* Define to 1 if the assembler supports X86_64's POPCNTQ instruction. */
#undef HAVE_X86_64_POPCNTQ 

/* Define to 1 if the system has the type `_Bool'. */
#undef HAVE__BOOL

/* Define to 1 if your compiler understands __builtin_bswap16. */
#undef HAVE__BUILTIN_BSWAP16 

/* Define to 1 if your compiler understands __builtin_bswap32. */
#undef HAVE__BUILTIN_BSWAP32 

/* Define to 1 if your compiler understands __builtin_bswap64. */
#undef HAVE__BUILTIN_BSWAP64 

/* Define to 1 if your compiler understands __builtin_clz. */
#undef HAVE__BUILTIN_CLZ 

/* Define to 1 if your compiler understands __builtin_constant_p. */
#undef HAVE__BUILTIN_CONSTANT_P 

/* Define to 1 if your compiler understands __builtin_ctz. */
#undef HAVE__BUILTIN_CTZ

/* Define to 1 if your compiler understands __builtin_$op_overflow. */
#undef HAVE__BUILTIN_OP_OVERFLOW 

/* Define to 1 if your compiler understands __builtin_popcount. */
#undef HAVE__BUILTIN_POPCOUNT

/* Define to 1 if your compiler understands __builtin_types_compatible_p. */
#undef HAVE__BUILTIN_TYPES_COMPATIBLE_P 

/* Define to 1 if your compiler understands __builtin_unreachable. */
#undef HAVE__BUILTIN_UNREACHABLE

/* Define to 1 if you have the `_configthreadlocale' function. */
#define HAVE__CONFIGTHREADLOCALE 1

/* Define to 1 if you have __cpuid. */
#define HAVE__CPUID 1

/* Define to 1 if you have __get_cpuid. */
#undef HAVE__GET_CPUID

/* Define to the appropriate printf length modifier for 64-bit ints. */
#define INT64_MODIFIER "ll"

/* Define to 1 if `locale_t' requires <xlocale.h>. */
/* #undef LOCALE_T_IN_XLOCALE */

/* Define to the name of a signed 128-bit integer type. */
#undef PG_INT128_TYPE

/* Define to the name of a signed 64-bit integer type. */
#define PG_INT64_TYPE long long int

/* The size of `size_t', as computed by sizeof. */
#ifdef _WIN64
#define SIZEOF_SIZE_T 8
#else
#define SIZEOF_SIZE_T 4
#endif

/* The size of `void *', as computed by sizeof. */
#ifndef _WIN64
#define SIZEOF_VOID_P 8
#else
#define SIZEOF_VOID_P 4
#endif

/* Define to select named POSIX semaphores. */
#undef USE_NAMED_POSIX_SEMAPHORES 

/* Define to build with systemd support. (--with-systemd) */
#undef USE_SYSTEMD

/* Define to select unnamed POSIX semaphores. */
#undef USE_UNNAMED_POSIX_SEMAPHORES 

/* Define to use native Windows API for random number generation */
#define USE_WIN32_RANDOM 1

/* Define to select Win32-style semaphores. */
#define USE_WIN32_SEMAPHORES 1

#define pg_restrict __restrict
