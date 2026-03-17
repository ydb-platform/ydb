/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2010 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * This file is included at the bottom of opal_config.h, and is
 * therefore a) after all the #define's that were output from
 * configure, and b) included in most/all files in Open MPI.
 *
 * Since this file is *only* ever included by opal_config.h, and
 * opal_config.h already has #ifndef/#endif protection, there is no
 * need to #ifndef/#endif protection here.
 */

#ifndef OPAL_CONFIG_H
#error "opal_config_bottom.h should only be included from opal_config.h"
#endif

/*
 * If we build a static library, Visual C define the _LIB symbol. In the
 * case of a shared library _USERDLL get defined.
 *
 * OMPI_BUILDING and _LIB define how opal_config.h
 * handles configuring all of Open MPI's "compatibility" code.  Both
 * constants will always be defined by the end of opal_config.h.
 *
 * OMPI_BUILDING affects how much compatibility code is included by
 * opal_config.h.  It will always be 1 or 0.  The user can set the
 * value before including either mpi.h or opal_config.h and it will be
 * respected.  If opal_config.h is included before mpi.h, it will
 * default to 1.  If mpi.h is included before opal_config.h, it will
 * default to 0.
 */
#ifndef OMPI_BUILDING
#define OMPI_BUILDING 1
#endif

/*
 * Flex is trying to include the unistd.h file. As there is no configure
 * option or this, the flex generated files will try to include the file
 * even on platforms without unistd.h. Therefore, if we
 * know this file is not available, we can prevent flex from including it.
 */
#ifndef HAVE_UNISTD_H
#define YY_NO_UNISTD_H
#endif

/***********************************************************************
 *
 * code that should be in ompi_config_bottom.h regardless of build
 * status
 *
 **********************************************************************/

/*
 * BEGIN_C_DECLS should be used at the beginning of your declarations,
 * so that C++ compilers don't mangle their names.  Use END_C_DECLS at
 * the end of C declarations.
 */
#undef BEGIN_C_DECLS
#undef END_C_DECLS
#if defined(c_plusplus) || defined(__cplusplus)
# define BEGIN_C_DECLS extern "C" {
# define END_C_DECLS }
#else
#define BEGIN_C_DECLS          /* empty */
#define END_C_DECLS            /* empty */
#endif

/**
 * The attribute definition should be included before any potential
 * usage.
 */
#if OPAL_HAVE_ATTRIBUTE_ALIGNED
#    define __opal_attribute_aligned__(a)    __attribute__((__aligned__(a)))
#    define __opal_attribute_aligned_max__   __attribute__((__aligned__))
#else
#    define __opal_attribute_aligned__(a)
#    define __opal_attribute_aligned_max__
#endif

#if OPAL_HAVE_ATTRIBUTE_ALWAYS_INLINE
#    define __opal_attribute_always_inline__ __attribute__((__always_inline__))
#else
#    define __opal_attribute_always_inline__
#endif

#if OPAL_HAVE_ATTRIBUTE_COLD
#    define __opal_attribute_cold__          __attribute__((__cold__))
#else
#    define __opal_attribute_cold__
#endif

#if OPAL_HAVE_ATTRIBUTE_CONST
#    define __opal_attribute_const__         __attribute__((__const__))
#else
#    define __opal_attribute_const__
#endif

#if OPAL_HAVE_ATTRIBUTE_DEPRECATED
#    define __opal_attribute_deprecated__    __attribute__((__deprecated__))
#else
#    define __opal_attribute_deprecated__
#endif

#if OPAL_HAVE_ATTRIBUTE_FORMAT
#    define __opal_attribute_format__(a,b,c) __attribute__((__format__(a, b, c)))
#else
#    define __opal_attribute_format__(a,b,c)
#endif

/* Use this __atribute__ on function-ptr declarations, only */
#if OPAL_HAVE_ATTRIBUTE_FORMAT_FUNCPTR
#    define __opal_attribute_format_funcptr__(a,b,c) __attribute__((__format__(a, b, c)))
#else
#    define __opal_attribute_format_funcptr__(a,b,c)
#endif

#if OPAL_HAVE_ATTRIBUTE_HOT
#    define __opal_attribute_hot__           __attribute__((__hot__))
#else
#    define __opal_attribute_hot__
#endif

#if OPAL_HAVE_ATTRIBUTE_MALLOC
#    define __opal_attribute_malloc__        __attribute__((__malloc__))
#else
#    define __opal_attribute_malloc__
#endif

#if OPAL_HAVE_ATTRIBUTE_MAY_ALIAS
#    define __opal_attribute_may_alias__     __attribute__((__may_alias__))
#else
#    define __opal_attribute_may_alias__
#endif

#if OPAL_HAVE_ATTRIBUTE_NO_INSTRUMENT_FUNCTION
#    define __opal_attribute_no_instrument_function__  __attribute__((__no_instrument_function__))
#else
#    define __opal_attribute_no_instrument_function__
#endif

#if OPAL_HAVE_ATTRIBUTE_NOINLINE
#    define __opal_attribute_noinline__  __attribute__((__noinline__))
#else
#    define __opal_attribute_noinline__
#endif

#if OPAL_HAVE_ATTRIBUTE_NONNULL
#    define __opal_attribute_nonnull__(a)    __attribute__((__nonnull__(a)))
#    define __opal_attribute_nonnull_all__   __attribute__((__nonnull__))
#else
#    define __opal_attribute_nonnull__(a)
#    define __opal_attribute_nonnull_all__
#endif

#if OPAL_HAVE_ATTRIBUTE_NORETURN
#    define __opal_attribute_noreturn__      __attribute__((__noreturn__))
#else
#    define __opal_attribute_noreturn__
#endif

/* Use this __atribute__ on function-ptr declarations, only */
#if OPAL_HAVE_ATTRIBUTE_NORETURN_FUNCPTR
#    define __opal_attribute_noreturn_funcptr__  __attribute__((__noreturn__))
#else
#    define __opal_attribute_noreturn_funcptr__
#endif

#if OPAL_HAVE_ATTRIBUTE_PACKED
#    define __opal_attribute_packed__        __attribute__((__packed__))
#else
#    define __opal_attribute_packed__
#endif

#if OPAL_HAVE_ATTRIBUTE_PURE
#    define __opal_attribute_pure__          __attribute__((__pure__))
#else
#    define __opal_attribute_pure__
#endif

#if OPAL_HAVE_ATTRIBUTE_SENTINEL
#    define __opal_attribute_sentinel__      __attribute__((__sentinel__))
#else
#    define __opal_attribute_sentinel__
#endif

#if OPAL_HAVE_ATTRIBUTE_UNUSED
#    define __opal_attribute_unused__        __attribute__((__unused__))
#else
#    define __opal_attribute_unused__
#endif

#if OPAL_HAVE_ATTRIBUTE_VISIBILITY
#    define __opal_attribute_visibility__(a) __attribute__((__visibility__(a)))
#else
#    define __opal_attribute_visibility__(a)
#endif

#if OPAL_HAVE_ATTRIBUTE_WARN_UNUSED_RESULT
#    define __opal_attribute_warn_unused_result__ __attribute__((__warn_unused_result__))
#else
#    define __opal_attribute_warn_unused_result__
#endif

#if OPAL_HAVE_ATTRIBUTE_WEAK_ALIAS
#    define __opal_attribute_weak_alias__(a) __attribute__((__weak__, __alias__(a)))
#else
#    define __opal_attribute_weak_alias__(a)
#endif

#if OPAL_HAVE_ATTRIBUTE_DESTRUCTOR
#    define __opal_attribute_destructor__    __attribute__((__destructor__))
#else
#    define __opal_attribute_destructor__
#endif

#if OPAL_HAVE_ATTRIBUTE_OPTNONE
#    define __opal_attribute_optnone__    __attribute__((__optnone__))
#else
#    define __opal_attribute_optnone__
#endif

#if OPAL_HAVE_ATTRIBUTE_EXTENSION
#    define __opal_attribute_extension__    __extension__
#else
#    define __opal_attribute_extension__
#endif

#  if OPAL_C_HAVE_VISIBILITY
#    define OPAL_DECLSPEC           __opal_attribute_visibility__("default")
#    define OPAL_MODULE_DECLSPEC    __opal_attribute_visibility__("default")
#  else
#    define OPAL_DECLSPEC
#    define OPAL_MODULE_DECLSPEC
#  endif

#if !defined(__STDC_LIMIT_MACROS) && (defined(c_plusplus) || defined (__cplusplus))
/* When using a C++ compiler, the max / min value #defines for std
   types are only included if __STDC_LIMIT_MACROS is set before
   including stdint.h */
#define __STDC_LIMIT_MACROS
#endif
#include "opal_stdint.h"

/***********************************************************************
 *
 * Code that is only for when building Open MPI or utilities that are
 * using the internals of Open MPI.  It should not be included when
 * building MPI applications
 *
 **********************************************************************/
#if OMPI_BUILDING

/*
 * Maximum size of a filename path.
 */
#include <limits.h>
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#if defined(PATH_MAX)
#define OPAL_PATH_MAX   (PATH_MAX + 1)
#elif defined(_POSIX_PATH_MAX)
#define OPAL_PATH_MAX   (_POSIX_PATH_MAX + 1)
#else
#define OPAL_PATH_MAX   256
#endif

/*
 * Set the compile-time path-separator on this system and variable separator
 */
#define OPAL_PATH_SEP "/"
#define OPAL_ENV_SEP  ':'

#if defined(MAXHOSTNAMELEN)
#define OPAL_MAXHOSTNAMELEN (MAXHOSTNAMELEN + 1)
#elif defined(HOST_NAME_MAX)
#define OPAL_MAXHOSTNAMELEN (HOST_NAME_MAX + 1)
#else
/* SUSv2 guarantees that "Host names are limited to 255 bytes". */
#define OPAL_MAXHOSTNAMELEN (255 + 1)
#endif

/*
 * Do we want memory debugging?
 *
 * A few scenarios:
 *
 * 1. In the OMPI C library: we want these defines in all cases
 * 2. In the OMPI C++ bindings: we do not want them
 * 3. In the OMPI C++ executables: we do want them
 *
 * So for 1, everyone must include <opal_config.h> first.  For 2, the
 * C++ bindings will never include <opal_config.h> -- they will only
 * include <mpi.h>, which includes <opal_config.h>, but after
 * setting OMPI_BUILDING to 0  For 3, it's the same as 1 -- just include
 * <opal_config.h> first.
 *
 * Give code that needs to include opal_config.h but really can't have
 * this stuff enabled (like the memory manager code) a way to turn us
 * off
 */
#if OPAL_ENABLE_MEM_DEBUG && !defined(OPAL_DISABLE_ENABLE_MEM_DEBUG)

/* It is safe to include opal/util/malloc.h here because a) it will only
   happen when we are building OMPI and therefore have a full OMPI
   source tree [including headers] available, and b) we guaranteed to
   *not* to include anything else via opal/util/malloc.h, so we won't
   have Cascading Includes Of Death. */
#    include "opal/util/malloc.h"
#    if defined(malloc)
#        undef malloc
#    endif
#    define malloc(size) opal_malloc((size), __FILE__, __LINE__)
#    if defined(calloc)
#        undef calloc
#    endif
#    define calloc(nmembers, size) opal_calloc((nmembers), (size), __FILE__, __LINE__)
#    if defined(realloc)
#        undef realloc
#    endif
#    define realloc(ptr, size) opal_realloc((ptr), (size), __FILE__, __LINE__)
#    if defined(free)
#        undef free
#    endif
#    define free(ptr) opal_free((ptr), __FILE__, __LINE__)

/*
 * If we're mem debugging, make the OPAL_DEBUG_ZERO resolve to memset
 */
#    include <string.h>
#    define OPAL_DEBUG_ZERO(obj) memset(&(obj), 0, sizeof(obj))
#else
#    define OPAL_DEBUG_ZERO(obj)
#endif

/*
 * printf functions for portability (only when building Open MPI)
 */
#if !defined(HAVE_VASPRINTF) || !defined(HAVE_VSNPRINTF)
#include <stdarg.h>
#include <stdlib.h>
#endif

#if !defined(HAVE_ASPRINTF) || !defined(HAVE_SNPRINTF) || !defined(HAVE_VASPRINTF) || !defined(HAVE_VSNPRINTF)
#include "opal/util/printf.h"
#endif

#ifndef HAVE_ASPRINTF
# define asprintf opal_asprintf
#endif

#ifndef HAVE_SNPRINTF
# define snprintf opal_snprintf
#endif

#ifndef HAVE_VASPRINTF
# define vasprintf opal_vasprintf
#endif

#ifndef HAVE_VSNPRINTF
# define vsnprintf opal_vsnprintf
#endif

/*
 * Some platforms (Solaris) have a broken qsort implementation.  Work
 * around by using our own.
 */
#if OPAL_HAVE_BROKEN_QSORT
#ifdef qsort
#undef qsort
#endif

#error #include "opal/util/qsort.h"
#define qsort opal_qsort
#endif

/*
 * On some homogenous big-iron machines (Sandia's Red Storm), there
 * are no htonl and friends.  If that's the case, provide stubs.  I
 * would hope we never find a platform that doesn't have these macros
 * and would want to talk to the outside world... On other platforms
 * we fail to detect them correctly.
 */
#if !defined(HAVE_UNIX_BYTESWAP)
static inline uint32_t htonl(uint32_t hostvar) { return hostvar; }
static inline uint32_t ntohl(uint32_t netvar) { return netvar; }
static inline uint16_t htons(uint16_t hostvar) { return hostvar; }
static inline uint16_t ntohs(uint16_t netvar) { return netvar; }
#endif

/*
 * Define __func__-preprocessor directive if the compiler does not
 * already define it.  Define it to __FILE__ so that we at least have
 * a clue where the developer is trying to indicate where the error is
 * coming from (assuming that __func__ is typically used for
 * printf-style debugging).
 */
#if defined(HAVE_DECL___FUNC__) && !HAVE_DECL___FUNC__
#define __func__ __FILE__
#endif

#define IOVBASE_TYPE  void

/* ensure the bool type is defined as it is used everywhere */
#include <stdbool.h>

/**
 * If we generate our own bool type, we need a special way to cast the result
 * in such a way to keep the compilers silent.
 */
#  define OPAL_INT_TO_BOOL(VALUE)  (bool)(VALUE)

/**
 * Top level define to check 2 things: a) if we want ipv6 support, and
 * b) the underlying system supports ipv6.  Having one #define for
 * this makes it simpler to check throughout the code base.
 */
#if OPAL_ENABLE_IPV6 && defined(HAVE_STRUCT_SOCKADDR_IN6)
#define OPAL_ENABLE_IPV6 1
#else
#define OPAL_ENABLE_IPV6 0
#endif

#if !defined(HAVE_STRUCT_SOCKADDR_STORAGE) && defined(HAVE_STRUCT_SOCKADDR_IN)
#define sockaddr_storage sockaddr
#define ss_family sa_family
#endif

/* Compatibility structure so that we don't have to have as many
   #if checks in the code base */
#if !defined(HAVE_STRUCT_SOCKADDR_IN6) && defined(HAVE_STRUCT_SOCKADDR_IN)
#define sockaddr_in6 sockaddr_in
#define sin6_len sin_len
#define sin6_family sin_family
#define sin6_port sin_port
#define sin6_addr sin_addr
#endif

#if !HAVE_DECL_AF_UNSPEC
#define AF_UNSPEC 0
#endif
#if !HAVE_DECL_PF_UNSPEC
#define PF_UNSPEC 0
#endif
#if !HAVE_DECL_AF_INET6
#define AF_INET6 AF_UNSPEC
#endif
#if !HAVE_DECL_PF_INET6
#define PF_INET6 PF_UNSPEC
#endif

#if defined(__APPLE__) && defined(HAVE_INTTYPES_H)
/* Prior to Mac OS X 10.3, the length modifier "ll" wasn't
   supported, but "q" was for long long.  This isn't ANSI
   C and causes a warning when using PRI?64 macros.  We
   don't support versions prior to OS X 10.3, so we dont'
   need such backward compatibility.  Instead, redefine
   the macros to be "ll", which is ANSI C and doesn't
   cause a compiler warning. */
#include <inttypes.h>
#if defined(__PRI_64_LENGTH_MODIFIER__)
#undef __PRI_64_LENGTH_MODIFIER__
#define __PRI_64_LENGTH_MODIFIER__ "ll"
#endif
#if defined(__SCN_64_LENGTH_MODIFIER__)
#undef __SCN_64_LENGTH_MODIFIER__
#define __SCN_64_LENGTH_MODIFIER__ "ll"
#endif
#endif

#ifdef MCS_VXWORKS
/* VXWorks puts some common functions in oddly named headers.  Rather
   than update all the places the functions are used, which would be a
   maintenance disatster, just update here... */
#ifdef HAVE_IOLIB_H
/* pipe(), ioctl() */
#error #include <ioLib.h>
#endif
#ifdef HAVE_SOCKLIB_H
/* socket() */
#error #include <sockLib.h>
#endif
#ifdef HAVE_HOSTLIB_H
/* gethostname() */
#error #include <hostLib.h>
#endif
#endif

/* If we're in C++, then just undefine restrict and then define it to
   nothing.  "restrict" is not part of the C++ language, and we don't
   have a corresponding AC_CXX_RESTRICT to figure out what the C++
   compiler supports. */
#if defined(c_plusplus) || defined(__cplusplus)
#undef restrict
#define restrict
#endif

#else

/* For a similar reason to what is listed in opal_config_top.h, we
   want to protect others from the autoconf/automake-generated
   PACKAGE_<foo> macros in opal_config.h.  We can't put these undef's
   directly in opal_config.h because they'll be turned into #defines'
   via autoconf.

   So put them here in case any only else includes OMPI/ORTE/OPAL's
   config.h files. */

#undef PACKAGE_BUGREPORT
#undef PACKAGE_NAME
#undef PACKAGE_STRING
#undef PACKAGE_TARNAME
#undef PACKAGE_VERSION
#undef PACKAGE_URL
#undef HAVE_CONFIG_H

#endif /* OMPI_BUILDING */
