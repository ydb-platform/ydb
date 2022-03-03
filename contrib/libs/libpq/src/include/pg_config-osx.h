#pragma once

#include "pg_config-linux.h"

/* Define to 1 if you have the `append_history' function. */
#undef HAVE_APPEND_HISTORY

/* Define to 1 if you have the `ASN1_STRING_get0_data' function. */
#undef HAVE_ASN1_STRING_GET0_DATA 

/* Define to 1 if you have the `copyfile' function. */
#define HAVE_COPYFILE 1

/* Define to 1 if you have the <copyfile.h> header file. */
#define HAVE_COPYFILE_H 1

/* Define to 1 if you have the `CRYPTO_lock' function. */
#define HAVE_CRYPTO_LOCK 1

/* Define to 1 if you have the <crypt.h> header file. */
#undef HAVE_CRYPT_H 

/* Define to 1 if you have the declaration of `fdatasync', and to 0 if you
   don't. */
#undef HAVE_DECL_FDATASYNC
#define HAVE_DECL_FDATASYNC 0

/* Define to 1 if you have the declaration of `F_FULLFSYNC', and to 0 if you
   don't. */
#undef HAVE_DECL_F_FULLFSYNC
#define HAVE_DECL_F_FULLFSYNC 1

/* Define to 1 if you have the declaration of `snprintf', and to 0 if you
   don't. */
#define HAVE_DECL_SNPRINTF 1

/* Define to 1 if you have the declaration of `strlcat', and to 0 if you
   don't. */
#undef HAVE_DECL_STRLCAT
#define HAVE_DECL_STRLCAT 1 

/* Define to 1 if you have the declaration of `strlcpy', and to 0 if you
   don't. */ 
#undef HAVE_DECL_STRLCPY
#define HAVE_DECL_STRLCPY 1

/* Define to 1 if you have the declaration of `sys_siglist', and to 0 if you
   don't. */
#define HAVE_DECL_SYS_SIGLIST 1

/* Define to 1 if you have the declaration of `vsnprintf', and to 0 if you
   don't. */
#define HAVE_DECL_VSNPRINTF 1

/* Define to 1 if you have the <dld.h> header file. */
#undef HAVE_DLD_H

/* Define to 1 if you have the `fls' function. */
#define HAVE_FLS 1

/* Define to 1 if you have the `gethostbyname_r' function. */
#undef HAVE_GETHOSTBYNAME_R 

/* Define to 1 if you have the `getpeereid' function. */
#define HAVE_GETPEEREID 1

/* Define to 1 if you have the `mbstowcs_l' function. */
#define HAVE_MBSTOWCS_L 1

/* Define to 1 if you have the `posix_fadvise' function. */
#undef HAVE_POSIX_FADVISE 

/* Define to 1 if you have the `posix_fallocate' function. */
#undef HAVE_POSIX_FALLOCATE 

/* Define to 1 if you have the `ppoll' function. */
#undef HAVE_PPOLL

/* Define to 1 if you have the `pread' function. */
#undef HAVE_PREAD

/* Define to 1 if you have the `pthread_is_threaded_np' function. */
#define HAVE_PTHREAD_IS_THREADED_NP 1

/* Define to 1 if you have the `pwrite' function. */
#undef HAVE_PWRITE

/* Define to 1 if you have the `rl_reset_screen_size' function. */
#undef HAVE_RL_RESET_SCREEN_SIZE 

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1

/* Define to 1 if you have the `strchrnul' function. */                
#undef HAVE_STRCHRNUL

/* Define to 1 if you have the `strerror_r' function. */
#define HAVE_STRERROR_R 1

/* Define to 1 if you have the `strlcat' function. */
#define HAVE_STRLCAT 1

/* Define to 1 if you have the `strlcpy' function. */
#define HAVE_STRLCPY 1

/* Define to 1 if `sa_len' is a member of `struct sockaddr'. */
#define HAVE_STRUCT_SOCKADDR_SA_LEN 1

/* Define to 1 if `ss_len' is a member of `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE_SS_LEN 1

/* Define to 1 if you have the `sync_file_range' function. */
#undef HAVE_SYNC_FILE_RANGE 

/* Define to 1 if you have the <sys/epoll.h> header file. */
#undef HAVE_SYS_EPOLL_H 

/* Define to 1 if you have the <sys/sockio.h> header file. */
#define HAVE_SYS_SOCKIO_H 1

/* Define to 1 if you have the <sys/ucred.h> header file. */
#define HAVE_SYS_UCRED_H 1

/* Define to 1 if you have the `towlower' function. */
#define HAVE_TOWLOWER 1

/* Define to 1 if you have the <uuid.h> header file. */
/* #undef HAVE_UUID_H */

/* Define to 1 if you have OSSP UUID support. */
/* #undef HAVE_UUID_OSSP */

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1

/* Define to 1 if you have the `wcstombs_l' function. */
#define HAVE_WCSTOMBS_L 1

/* Define to 1 if `locale_t' requires <xlocale.h>. */
#define LOCALE_T_IN_XLOCALE 1

/* Define to gnu_printf if compiler supports it, else printf. */
#undef PG_PRINTF_ATTRIBUTE
#define PG_PRINTF_ATTRIBUTE printf

/* Define to 1 if strerror_r() returns int. */
#define STRERROR_R_INT 1

/* Define to build with systemd support. (--with-systemd) */
/* #undef USE_SYSTEMD */

/* Define to select SysV-style semaphores. */
#define USE_SYSV_SEMAPHORES 1

/* Define to select unnamed POSIX semaphores. */
/* #undef USE_UNNAMED_POSIX_SEMAPHORES */

/* Define to 1 if `wcstombs_l' requires <xlocale.h>. */
#define WCSTOMBS_L_IN_XLOCALE 1
