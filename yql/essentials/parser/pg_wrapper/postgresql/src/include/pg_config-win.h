#pragma once

#include "pg_config-linux.h"

/* Define to 1 if you have the <strings.h> header file. */
#undef HAVE_STRINGS_H

/* Define to 1 if you have the <sys/un.h> header file. */
#undef HAVE_SYS_UN_H

/* Define to 1 if you have the <termios.h> header file. */
#undef HAVE_TERMIOS_H

/* Define to 1 if you have the `inet_aton' function. */
#define HAVE_INET_ATON 1

/* Define to 1 if you have the <sys/select.h> header file. */
#undef HAVE_SYS_SELECT_H

/* Define to 1 if you have the <sys/prctl.h> header file. */
#undef HAVE_SYS_PRCTL_H

/* Define to 1 if you have the <netinet/tcp.h> header file. */
#undef HAVE_NETINET_TCP_H

/* Define to 1 if you have the <sys/resource.h> header file. */
#undef HAVE_SYS_RESOURCE_H

/* Define to 1 if you have the `dlopen' function. */
#undef HAVE_DLOPEN

/* Define to 1 if you have the syslog interface. */
#undef HAVE_SYSLOG

/* Define to 1 if you have the <getopt.h> header file. */
#undef HAVE_GETOPT_H

/* Define to 1 if you have the <execinfo.h> header file. */
#undef HAVE_EXECINFO_H

/* Define to 1 if you have the `sync_file_range' function. */
#undef HAVE_SYNC_FILE_RANGE

/* Define to 1 if you have the <sys/epoll.h> header file. */
#undef HAVE_SYS_EPOLL_H

/* Define to 1 if you have the <poll.h> header file. */
#undef HAVE_POLL_H

/* Define to 1 if you have the `poll' function. */
#undef HAVE_POLL

/* Define to 1 if you have the <sys/ipc.h> header file. */
#undef HAVE_SYS_IPC_H

/* Define to 1 if you have the <sys/personality.h> header file. */
#undef HAVE_SYS_PERSONALITY_H

/* Define to 1 if you have the <langinfo.h> header file. */
#undef HAVE_LANGINFO_H

/* Define to 1 if you have the <sys/shm.h> header file. */
#undef HAVE_SYS_SHM_H

/* Define to 1 if the assembler supports X86_64's POPCNTQ instruction. */
#undef HAVE_X86_64_POPCNTQ

/* Define to 1 if you have the declaration of `RTLD_GLOBAL', and to 0 if you
   don't. */
#undef HAVE_DECL_RTLD_GLOBAL

/* Define to 1 if you have the declaration of `RTLD_NOW', and to 0 if you
   don't. */
#undef HAVE_DECL_RTLD_NOW

/* Define to 1 if you have the `getrlimit' function. */
#undef HAVE_GETRLIMIT

/* Define to 1 if you have the `getrusage' function. */
#undef HAVE_GETRUSAGE

/* Define to 1 if your compiler understands _Static_assert. */
#undef HAVE__STATIC_ASSERT

/* Define to 1 if you have the global variable 'int opterr'. */
#undef HAVE_INT_OPTERR

/* Define to 1 if you have the `strchrnul' function. */
#undef HAVE_STRCHRNUL

/* Define to 1 if you have the `backtrace_symbols' function. */
#undef HAVE_BACKTRACE_SYMBOLS

/* Define to 1 if you have the `setsid' function. */
#undef HAVE_SETSID

/* Define to 1 if you have the `fdatasync' function. */
#undef HAVE_FDATASYNC

/* Define to 1 if the system has the type `locale_t'. */
#undef HAVE_LOCALE_T

/* Define to 1 if you have the `strerror_r' function. */
#undef HAVE_STRERROR_R

/* Define to 1 if you have the `getaddrinfo' function. */
#undef HAVE_GETADDRINFO

/* Define to use OpenSSL for random number generation */
#undef USE_OPENSSL_RANDOM

/* Define to use native Windows API for random number generation */
#define USE_WIN32_RANDOM 1

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

/* Define to 1 if your compiler understands `typeof' or something similar. */
#undef HAVE_TYPEOF

/* Define to 1 if your compiler handles computed gotos. */
#undef HAVE_COMPUTED_GOTO

/* Define to 1 if you have the `readlink' function. */
#undef HAVE_READLINK

/* Define to 1 if you have the `syncfs' function. */
#undef HAVE_SYNCFS


/* PostgreSQL's CFLAG for windows. */
#define WIN32_STACK_RLIMIT 4194304

/* NO GOOD WAY WAS FOUND TO PROVIDE DEFINITIONS FOR random AND srandom. */
/* rand is PostgreSQL's implementation of random from stdlib.h for windows. */
#define random rand

/* srand is PostgreSQL's implementation of srandom from stdlib.h for windows. */
#define srandom srand

#define BLCKSZ 8192

#undef HAVE_POSIX_FALLOCATE

