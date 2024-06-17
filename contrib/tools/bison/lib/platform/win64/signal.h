/* DO NOT EDIT! GENERATED AUTOMATICALLY! */
/* A GNU-like <signal.h>.

   Copyright (C) 2006-2013 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

#if __GNUC__ >= 3

#endif


#if defined __need_sig_atomic_t || defined __need_sigset_t || defined _GL_ALREADY_INCLUDING_SIGNAL_H || (defined _SIGNAL_H && !defined __SIZEOF_PTHREAD_MUTEX_T)
/* Special invocation convention:
   - Inside glibc header files.
   - On glibc systems we have a sequence of nested includes
     <signal.h> -> <ucontext.h> -> <signal.h>.
     In this situation, the functions are not yet declared, therefore we cannot
     provide the C++ aliases.
   - On glibc systems with GCC 4.3 we have a sequence of nested includes
     <csignal> -> </usr/include/signal.h> -> <sys/ucontext.h> -> <signal.h>.
     In this situation, some of the functions are not yet declared, therefore
     we cannot provide the C++ aliases.  */

#if _MSC_VER >= 1900
#include <../ucrt/signal.h>
#else
#error #include <../include/signal.h>
#endif

#else
/* Normal invocation convention.  */

#ifndef _GL_M4_SIGNAL_H

#define _GL_ALREADY_INCLUDING_SIGNAL_H

/* Define pid_t, uid_t.
   Also, mingw defines sigset_t not in <signal.h>, but in <sys/types.h>.
   On Solaris 10, <signal.h> includes <sys/types.h>, which eventually includes
   us; so include <sys/types.h> now, before the second inclusion guard.  */
#include <sys/types.h>

/* The include_next requires a split double-inclusion guard.  */
#if _MSC_VER >= 1900
#include <../ucrt/signal.h>
#else
#error #include <../include/signal.h>
#endif

#undef _GL_ALREADY_INCLUDING_SIGNAL_H

#ifndef _GL_M4_SIGNAL_H
#define _GL_M4_SIGNAL_H

/* Mac OS X 10.3, FreeBSD 6.4, OpenBSD 3.8, OSF/1 4.0, Solaris 2.6 declare
   pthread_sigmask in <pthread.h>, not in <signal.h>.
   But avoid namespace pollution on glibc systems.*/
#if (0 || defined GNULIB_POSIXCHECK) \
    && ((defined __APPLE__ && defined __MACH__) || defined __FreeBSD__ || defined __OpenBSD__ || defined __osf__ || defined __sun) \
    && ! defined __GLIBC__
# include <pthread.h>
#endif

/* The definitions of _GL_FUNCDECL_RPL etc. are copied here.  */
#ifndef _GL_CXXDEFS_H
#define _GL_CXXDEFS_H

/* The three most frequent use cases of these macros are:

   * For providing a substitute for a function that is missing on some
     platforms, but is declared and works fine on the platforms on which
     it exists:

       #if @GNULIB_FOO@
       # if !@HAVE_FOO@
       _GL_FUNCDECL_SYS (foo, ...);
       # endif
       _GL_CXXALIAS_SYS (foo, ...);
       _GL_CXXALIASWARN (foo);
       #elif defined GNULIB_POSIXCHECK
       ...
       #endif

   * For providing a replacement for a function that exists on all platforms,
     but is broken/insufficient and needs to be replaced on some platforms:

       #if @GNULIB_FOO@
       # if @REPLACE_FOO@
       #  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
       #   undef foo
       #   define foo rpl_foo
       #  endif
       _GL_FUNCDECL_RPL (foo, ...);
       _GL_CXXALIAS_RPL (foo, ...);
       # else
       _GL_CXXALIAS_SYS (foo, ...);
       # endif
       _GL_CXXALIASWARN (foo);
       #elif defined GNULIB_POSIXCHECK
       ...
       #endif

   * For providing a replacement for a function that exists on some platforms
     but is broken/insufficient and needs to be replaced on some of them and
     is additionally either missing or undeclared on some other platforms:

       #if @GNULIB_FOO@
       # if @REPLACE_FOO@
       #  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
       #   undef foo
       #   define foo rpl_foo
       #  endif
       _GL_FUNCDECL_RPL (foo, ...);
       _GL_CXXALIAS_RPL (foo, ...);
       # else
       #  if !@HAVE_FOO@   or   if !@HAVE_DECL_FOO@
       _GL_FUNCDECL_SYS (foo, ...);
       #  endif
       _GL_CXXALIAS_SYS (foo, ...);
       # endif
       _GL_CXXALIASWARN (foo);
       #elif defined GNULIB_POSIXCHECK
       ...
       #endif
*/

/* _GL_EXTERN_C declaration;
   performs the declaration with C linkage.  */
#if defined __cplusplus
# define _GL_EXTERN_C extern "C"
#else
# define _GL_EXTERN_C extern
#endif

/* _GL_FUNCDECL_RPL (func, rettype, parameters_and_attributes);
   declares a replacement function, named rpl_func, with the given prototype,
   consisting of return type, parameters, and attributes.
   Example:
     _GL_FUNCDECL_RPL (open, int, (const char *filename, int flags, ...)
                                  _GL_ARG_NONNULL ((1)));
 */
#define _GL_FUNCDECL_RPL(func,rettype,parameters_and_attributes) \
  _GL_FUNCDECL_RPL_1 (rpl_##func, rettype, parameters_and_attributes)
#define _GL_FUNCDECL_RPL_1(rpl_func,rettype,parameters_and_attributes) \
  _GL_EXTERN_C rettype rpl_func parameters_and_attributes

/* _GL_FUNCDECL_SYS (func, rettype, parameters_and_attributes);
   declares the system function, named func, with the given prototype,
   consisting of return type, parameters, and attributes.
   Example:
     _GL_FUNCDECL_SYS (open, int, (const char *filename, int flags, ...)
                                  _GL_ARG_NONNULL ((1)));
 */
#define _GL_FUNCDECL_SYS(func,rettype,parameters_and_attributes) \
  _GL_EXTERN_C rettype func parameters_and_attributes

/* _GL_CXXALIAS_RPL (func, rettype, parameters);
   declares a C++ alias called GNULIB_NAMESPACE::func
   that redirects to rpl_func, if GNULIB_NAMESPACE is defined.
   Example:
     _GL_CXXALIAS_RPL (open, int, (const char *filename, int flags, ...));
 */
#define _GL_CXXALIAS_RPL(func,rettype,parameters) \
  _GL_CXXALIAS_RPL_1 (func, rpl_##func, rettype, parameters)
#if defined __cplusplus && defined GNULIB_NAMESPACE
# define _GL_CXXALIAS_RPL_1(func,rpl_func,rettype,parameters) \
    namespace GNULIB_NAMESPACE                                \
    {                                                         \
      rettype (*const func) parameters = ::rpl_func;          \
    }                                                         \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#else
# define _GL_CXXALIAS_RPL_1(func,rpl_func,rettype,parameters) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

/* _GL_CXXALIAS_RPL_CAST_1 (func, rpl_func, rettype, parameters);
   is like  _GL_CXXALIAS_RPL_1 (func, rpl_func, rettype, parameters);
   except that the C function rpl_func may have a slightly different
   declaration.  A cast is used to silence the "invalid conversion" error
   that would otherwise occur.  */
#if defined __cplusplus && defined GNULIB_NAMESPACE
# define _GL_CXXALIAS_RPL_CAST_1(func,rpl_func,rettype,parameters) \
    namespace GNULIB_NAMESPACE                                     \
    {                                                              \
      rettype (*const func) parameters =                           \
        reinterpret_cast<rettype(*)parameters>(::rpl_func);        \
    }                                                              \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#else
# define _GL_CXXALIAS_RPL_CAST_1(func,rpl_func,rettype,parameters) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

/* _GL_CXXALIAS_SYS (func, rettype, parameters);
   declares a C++ alias called GNULIB_NAMESPACE::func
   that redirects to the system provided function func, if GNULIB_NAMESPACE
   is defined.
   Example:
     _GL_CXXALIAS_SYS (open, int, (const char *filename, int flags, ...));
 */
#if defined __cplusplus && defined GNULIB_NAMESPACE
  /* If we were to write
       rettype (*const func) parameters = ::func;
     like above in _GL_CXXALIAS_RPL_1, the compiler could optimize calls
     better (remove an indirection through a 'static' pointer variable),
     but then the _GL_CXXALIASWARN macro below would cause a warning not only
     for uses of ::func but also for uses of GNULIB_NAMESPACE::func.  */
# define _GL_CXXALIAS_SYS(func,rettype,parameters) \
    namespace GNULIB_NAMESPACE                     \
    {                                              \
      static rettype (*func) parameters = ::func;  \
    }                                              \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#else
# define _GL_CXXALIAS_SYS(func,rettype,parameters) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

/* _GL_CXXALIAS_SYS_CAST (func, rettype, parameters);
   is like  _GL_CXXALIAS_SYS (func, rettype, parameters);
   except that the C function func may have a slightly different declaration.
   A cast is used to silence the "invalid conversion" error that would
   otherwise occur.  */
#if defined __cplusplus && defined GNULIB_NAMESPACE
# define _GL_CXXALIAS_SYS_CAST(func,rettype,parameters) \
    namespace GNULIB_NAMESPACE                          \
    {                                                   \
      static rettype (*func) parameters =               \
        reinterpret_cast<rettype(*)parameters>(::func); \
    }                                                   \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#else
# define _GL_CXXALIAS_SYS_CAST(func,rettype,parameters) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

/* _GL_CXXALIAS_SYS_CAST2 (func, rettype, parameters, rettype2, parameters2);
   is like  _GL_CXXALIAS_SYS (func, rettype, parameters);
   except that the C function is picked among a set of overloaded functions,
   namely the one with rettype2 and parameters2.  Two consecutive casts
   are used to silence the "cannot find a match" and "invalid conversion"
   errors that would otherwise occur.  */
#if defined __cplusplus && defined GNULIB_NAMESPACE
  /* The outer cast must be a reinterpret_cast.
     The inner cast: When the function is defined as a set of overloaded
     functions, it works as a static_cast<>, choosing the designated variant.
     When the function is defined as a single variant, it works as a
     reinterpret_cast<>. The parenthesized cast syntax works both ways.  */
# define _GL_CXXALIAS_SYS_CAST2(func,rettype,parameters,rettype2,parameters2) \
    namespace GNULIB_NAMESPACE                                                \
    {                                                                         \
      static rettype (*func) parameters =                                     \
        reinterpret_cast<rettype(*)parameters>(                               \
          (rettype2(*)parameters2)(::func));                                  \
    }                                                                         \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#else
# define _GL_CXXALIAS_SYS_CAST2(func,rettype,parameters,rettype2,parameters2) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

/* _GL_CXXALIASWARN (func);
   causes a warning to be emitted when ::func is used but not when
   GNULIB_NAMESPACE::func is used.  func must be defined without overloaded
   variants.  */
#if defined __cplusplus && defined GNULIB_NAMESPACE
# define _GL_CXXALIASWARN(func) \
   _GL_CXXALIASWARN_1 (func, GNULIB_NAMESPACE)
# define _GL_CXXALIASWARN_1(func,namespace) \
   _GL_CXXALIASWARN_2 (func, namespace)
/* To work around GCC bug <http://gcc.gnu.org/bugzilla/show_bug.cgi?id=43881>,
   we enable the warning only when not optimizing.  */
# if !__OPTIMIZE__
#  define _GL_CXXALIASWARN_2(func,namespace) \
    _GL_WARN_ON_USE (func, \
                     "The symbol ::" #func " refers to the system function. " \
                     "Use " #namespace "::" #func " instead.")
# elif __GNUC__ >= 3 && GNULIB_STRICT_CHECKING
#  define _GL_CXXALIASWARN_2(func,namespace) \
     extern __typeof__ (func) func
# else
#  define _GL_CXXALIASWARN_2(func,namespace) \
     _GL_EXTERN_C int _gl_cxxalias_dummy
# endif
#else
# define _GL_CXXALIASWARN(func) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

/* _GL_CXXALIASWARN1 (func, rettype, parameters_and_attributes);
   causes a warning to be emitted when the given overloaded variant of ::func
   is used but not when GNULIB_NAMESPACE::func is used.  */
#if defined __cplusplus && defined GNULIB_NAMESPACE
# define _GL_CXXALIASWARN1(func,rettype,parameters_and_attributes) \
   _GL_CXXALIASWARN1_1 (func, rettype, parameters_and_attributes, \
                        GNULIB_NAMESPACE)
# define _GL_CXXALIASWARN1_1(func,rettype,parameters_and_attributes,namespace) \
   _GL_CXXALIASWARN1_2 (func, rettype, parameters_and_attributes, namespace)
/* To work around GCC bug <http://gcc.gnu.org/bugzilla/show_bug.cgi?id=43881>,
   we enable the warning only when not optimizing.  */
# if !__OPTIMIZE__
#  define _GL_CXXALIASWARN1_2(func,rettype,parameters_and_attributes,namespace) \
    _GL_WARN_ON_USE_CXX (func, rettype, parameters_and_attributes, \
                         "The symbol ::" #func " refers to the system function. " \
                         "Use " #namespace "::" #func " instead.")
# elif __GNUC__ >= 3 && GNULIB_STRICT_CHECKING
#  define _GL_CXXALIASWARN1_2(func,rettype,parameters_and_attributes,namespace) \
     extern __typeof__ (func) func
# else
#  define _GL_CXXALIASWARN1_2(func,rettype,parameters_and_attributes,namespace) \
     _GL_EXTERN_C int _gl_cxxalias_dummy
# endif
#else
# define _GL_CXXALIASWARN1(func,rettype,parameters_and_attributes) \
    _GL_EXTERN_C int _gl_cxxalias_dummy
#endif

#endif /* _GL_CXXDEFS_H */

/* The definition of _GL_ARG_NONNULL is copied here.  */
/* _GL_ARG_NONNULL((n,...,m)) tells the compiler and static analyzer tools
   that the values passed as arguments n, ..., m must be non-NULL pointers.
   n = 1 stands for the first argument, n = 2 for the second argument etc.  */
#ifndef _GL_ARG_NONNULL
# if (__GNUC__ == 3 && __GNUC_MINOR__ >= 3) || __GNUC__ > 3
#  define _GL_ARG_NONNULL(params) __attribute__ ((__nonnull__ params))
# else
#  define _GL_ARG_NONNULL(params)
# endif
#endif

/* The definition of _GL_WARN_ON_USE is copied here.  */
#ifndef _GL_WARN_ON_USE

# if 4 < __GNUC__ || (__GNUC__ == 4 && 3 <= __GNUC_MINOR__)
/* A compiler attribute is available in gcc versions 4.3.0 and later.  */
#  define _GL_WARN_ON_USE(function, message) \
extern __typeof__ (function) function __attribute__ ((__warning__ (message)))
# elif __GNUC__ >= 3 && GNULIB_STRICT_CHECKING
/* Verify the existence of the function.  */
#  define _GL_WARN_ON_USE(function, message) \
extern __typeof__ (function) function
# else /* Unsupported.  */
#  define _GL_WARN_ON_USE(function, message) \
_GL_WARN_EXTERN_C int _gl_warn_on_use
# endif
#endif

/* _GL_WARN_ON_USE_CXX (function, rettype, parameters_and_attributes, "string")
   is like _GL_WARN_ON_USE (function, "string"), except that the function is
   declared with the given prototype, consisting of return type, parameters,
   and attributes.
   This variant is useful for overloaded functions in C++. _GL_WARN_ON_USE does
   not work in this case.  */
#ifndef _GL_WARN_ON_USE_CXX
# if 4 < __GNUC__ || (__GNUC__ == 4 && 3 <= __GNUC_MINOR__)
#  define _GL_WARN_ON_USE_CXX(function,rettype,parameters_and_attributes,msg) \
extern rettype function parameters_and_attributes \
     __attribute__ ((__warning__ (msg)))
# elif __GNUC__ >= 3 && GNULIB_STRICT_CHECKING
/* Verify the existence of the function.  */
#  define _GL_WARN_ON_USE_CXX(function,rettype,parameters_and_attributes,msg) \
extern rettype function parameters_and_attributes
# else /* Unsupported.  */
#  define _GL_WARN_ON_USE_CXX(function,rettype,parameters_and_attributes,msg) \
_GL_WARN_EXTERN_C int _gl_warn_on_use
# endif
#endif

/* _GL_WARN_EXTERN_C declaration;
   performs the declaration with C linkage.  */
#ifndef _GL_WARN_EXTERN_C
# if defined __cplusplus
#  define _GL_WARN_EXTERN_C extern "C"
# else
#  define _GL_WARN_EXTERN_C extern
# endif
#endif

/* On AIX, sig_atomic_t already includes volatile.  C99 requires that
   'volatile sig_atomic_t' ignore the extra modifier, but C89 did not.
   Hence, redefine this to a non-volatile type as needed.  */
#if ! 1
# if !GNULIB_defined_sig_atomic_t
typedef int rpl_sig_atomic_t;
#  undef sig_atomic_t
#  define sig_atomic_t rpl_sig_atomic_t
#  define GNULIB_defined_sig_atomic_t 1
# endif
#endif

/* A set or mask of signals.  */
#if !0
# if !GNULIB_defined_sigset_t
typedef unsigned int sigset_t;
#  define GNULIB_defined_sigset_t 1
# endif
#endif

/* Define sighandler_t, the type of signal handlers.  A GNU extension.  */
#if !0
# ifdef __cplusplus
extern "C" {
# endif
# if !GNULIB_defined_sighandler_t
typedef void (*sighandler_t) (int);
#  define GNULIB_defined_sighandler_t 1
# endif
# ifdef __cplusplus
}
# endif
#endif


#if 1
# ifndef SIGPIPE
/* Define SIGPIPE to a value that does not overlap with other signals.  */
#  define SIGPIPE 13
#  define GNULIB_defined_SIGPIPE 1
/* To actually use SIGPIPE, you also need the gnulib modules 'sigprocmask',
   'write', 'stdio'.  */
# endif
#endif


/* Maximum signal number + 1.  */
#ifndef NSIG
# if defined __TANDEM
#  define NSIG 32
# endif
#endif


#if 0
# if 0
#  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
#   undef pthread_sigmask
#   define pthread_sigmask rpl_pthread_sigmask
#  endif
_GL_FUNCDECL_RPL (pthread_sigmask, int,
                  (int how, const sigset_t *new_mask, sigset_t *old_mask));
_GL_CXXALIAS_RPL (pthread_sigmask, int,
                  (int how, const sigset_t *new_mask, sigset_t *old_mask));
# else
#  if !1
_GL_FUNCDECL_SYS (pthread_sigmask, int,
                  (int how, const sigset_t *new_mask, sigset_t *old_mask));
#  endif
_GL_CXXALIAS_SYS (pthread_sigmask, int,
                  (int how, const sigset_t *new_mask, sigset_t *old_mask));
# endif
_GL_CXXALIASWARN (pthread_sigmask);
#elif defined GNULIB_POSIXCHECK
# undef pthread_sigmask
# if HAVE_RAW_DECL_PTHREAD_SIGMASK
_GL_WARN_ON_USE (pthread_sigmask, "pthread_sigmask is not portable - "
                 "use gnulib module pthread_sigmask for portability");
# endif
#endif


#if 1
# if 1
#  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
#   undef raise
#   define raise rpl_raise
#  endif
_GL_FUNCDECL_RPL (raise, int, (int sig));
_GL_CXXALIAS_RPL (raise, int, (int sig));
# else
#  if !1
_GL_FUNCDECL_SYS (raise, int, (int sig));
#  endif
_GL_CXXALIAS_SYS (raise, int, (int sig));
# endif
_GL_CXXALIASWARN (raise);
#elif defined GNULIB_POSIXCHECK
# undef raise
/* Assume raise is always declared.  */
_GL_WARN_ON_USE (raise, "raise can crash on native Windows - "
                 "use gnulib module raise for portability");
#endif


#if 1
# if !0

#  ifndef GNULIB_defined_signal_blocking
#   define GNULIB_defined_signal_blocking 1
#  endif

/* Maximum signal number + 1.  */
#  ifndef NSIG
#   define NSIG 32
#  endif

/* This code supports only 32 signals.  */
#  if !GNULIB_defined_verify_NSIG_constraint
typedef int verify_NSIG_constraint[NSIG <= 32 ? 1 : -1];
#   define GNULIB_defined_verify_NSIG_constraint 1
#  endif

# endif

/* When also using extern inline, suppress the use of static inline in
   standard headers of problematic Apple configurations, as Libc at
   least through Libc-825.26 (2013-04-09) mishandles it; see, e.g.,
   <http://lists.gnu.org/archive/html/bug-gnulib/2012-12/msg00023.html>.
   Perhaps Apple will fix this some day.  */
#if (defined _GL_EXTERN_INLINE_IN_USE && defined __APPLE__ \
     && (defined __i386__ || defined __x86_64__))
# undef sigaddset
# undef sigdelset
# undef sigemptyset
# undef sigfillset
# undef sigismember
#endif

/* Test whether a given signal is contained in a signal set.  */
# if 0
/* This function is defined as a macro on Mac OS X.  */
#  if defined __cplusplus && defined GNULIB_NAMESPACE
#   undef sigismember
#  endif
# else
_GL_FUNCDECL_SYS (sigismember, int, (const sigset_t *set, int sig)
                                    _GL_ARG_NONNULL ((1)));
# endif
_GL_CXXALIAS_SYS (sigismember, int, (const sigset_t *set, int sig));
_GL_CXXALIASWARN (sigismember);

/* Initialize a signal set to the empty set.  */
# if 0
/* This function is defined as a macro on Mac OS X.  */
#  if defined __cplusplus && defined GNULIB_NAMESPACE
#   undef sigemptyset
#  endif
# else
_GL_FUNCDECL_SYS (sigemptyset, int, (sigset_t *set) _GL_ARG_NONNULL ((1)));
# endif
_GL_CXXALIAS_SYS (sigemptyset, int, (sigset_t *set));
_GL_CXXALIASWARN (sigemptyset);

/* Add a signal to a signal set.  */
# if 0
/* This function is defined as a macro on Mac OS X.  */
#  if defined __cplusplus && defined GNULIB_NAMESPACE
#   undef sigaddset
#  endif
# else
_GL_FUNCDECL_SYS (sigaddset, int, (sigset_t *set, int sig)
                                  _GL_ARG_NONNULL ((1)));
# endif
_GL_CXXALIAS_SYS (sigaddset, int, (sigset_t *set, int sig));
_GL_CXXALIASWARN (sigaddset);

/* Remove a signal from a signal set.  */
# if 0
/* This function is defined as a macro on Mac OS X.  */
#  if defined __cplusplus && defined GNULIB_NAMESPACE
#   undef sigdelset
#  endif
# else
_GL_FUNCDECL_SYS (sigdelset, int, (sigset_t *set, int sig)
                                  _GL_ARG_NONNULL ((1)));
# endif
_GL_CXXALIAS_SYS (sigdelset, int, (sigset_t *set, int sig));
_GL_CXXALIASWARN (sigdelset);

/* Fill a signal set with all possible signals.  */
# if 0
/* This function is defined as a macro on Mac OS X.  */
#  if defined __cplusplus && defined GNULIB_NAMESPACE
#   undef sigfillset
#  endif
# else
_GL_FUNCDECL_SYS (sigfillset, int, (sigset_t *set) _GL_ARG_NONNULL ((1)));
# endif
_GL_CXXALIAS_SYS (sigfillset, int, (sigset_t *set));
_GL_CXXALIASWARN (sigfillset);

/* Return the set of those blocked signals that are pending.  */
# if !0
_GL_FUNCDECL_SYS (sigpending, int, (sigset_t *set) _GL_ARG_NONNULL ((1)));
# endif
_GL_CXXALIAS_SYS (sigpending, int, (sigset_t *set));
_GL_CXXALIASWARN (sigpending);

/* If OLD_SET is not NULL, put the current set of blocked signals in *OLD_SET.
   Then, if SET is not NULL, affect the current set of blocked signals by
   combining it with *SET as indicated in OPERATION.
   In this implementation, you are not allowed to change a signal handler
   while the signal is blocked.  */
# if !0
#  define SIG_BLOCK   0  /* blocked_set = blocked_set | *set; */
#  define SIG_SETMASK 1  /* blocked_set = *set; */
#  define SIG_UNBLOCK 2  /* blocked_set = blocked_set & ~*set; */
_GL_FUNCDECL_SYS (sigprocmask, int,
                  (int operation, const sigset_t *set, sigset_t *old_set));
# endif
_GL_CXXALIAS_SYS (sigprocmask, int,
                  (int operation, const sigset_t *set, sigset_t *old_set));
_GL_CXXALIASWARN (sigprocmask);

/* Install the handler FUNC for signal SIG, and return the previous
   handler.  */
# ifdef __cplusplus
extern "C" {
# endif
# if !GNULIB_defined_function_taking_int_returning_void_t
typedef void (*_gl_function_taking_int_returning_void_t) (int);
#  define GNULIB_defined_function_taking_int_returning_void_t 1
# endif
# ifdef __cplusplus
}
# endif
# if !0
#  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
#   define signal rpl_signal
#  endif
_GL_FUNCDECL_RPL (signal, _gl_function_taking_int_returning_void_t,
                  (int sig, _gl_function_taking_int_returning_void_t func));
_GL_CXXALIAS_RPL (signal, _gl_function_taking_int_returning_void_t,
                  (int sig, _gl_function_taking_int_returning_void_t func));
# else
_GL_CXXALIAS_SYS (signal, _gl_function_taking_int_returning_void_t,
                  (int sig, _gl_function_taking_int_returning_void_t func));
# endif
_GL_CXXALIASWARN (signal);

# if !0 && GNULIB_defined_SIGPIPE
/* Raise signal SIGPIPE.  */
_GL_EXTERN_C int _gl_raise_SIGPIPE (void);
# endif

#elif defined GNULIB_POSIXCHECK
# undef sigaddset
# if HAVE_RAW_DECL_SIGADDSET
_GL_WARN_ON_USE (sigaddset, "sigaddset is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
# undef sigdelset
# if HAVE_RAW_DECL_SIGDELSET
_GL_WARN_ON_USE (sigdelset, "sigdelset is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
# undef sigemptyset
# if HAVE_RAW_DECL_SIGEMPTYSET
_GL_WARN_ON_USE (sigemptyset, "sigemptyset is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
# undef sigfillset
# if HAVE_RAW_DECL_SIGFILLSET
_GL_WARN_ON_USE (sigfillset, "sigfillset is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
# undef sigismember
# if HAVE_RAW_DECL_SIGISMEMBER
_GL_WARN_ON_USE (sigismember, "sigismember is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
# undef sigpending
# if HAVE_RAW_DECL_SIGPENDING
_GL_WARN_ON_USE (sigpending, "sigpending is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
# undef sigprocmask
# if HAVE_RAW_DECL_SIGPROCMASK
_GL_WARN_ON_USE (sigprocmask, "sigprocmask is unportable - "
                 "use the gnulib module sigprocmask for portability");
# endif
#endif /* 1 */


#if 1
# if !0

#  if !0

#   if !GNULIB_defined_siginfo_types

/* Present to allow compilation, but unsupported by gnulib.  */
union sigval
{
  int sival_int;
  void *sival_ptr;
};

/* Present to allow compilation, but unsupported by gnulib.  */
struct siginfo_t
{
  int si_signo;
  int si_code;
  int si_errno;
  pid_t si_pid;
  uid_t si_uid;
  void *si_addr;
  int si_status;
  long si_band;
  union sigval si_value;
};
typedef struct siginfo_t siginfo_t;

#    define GNULIB_defined_siginfo_types 1
#   endif

#  endif /* !0 */

/* We assume that platforms which lack the sigaction() function also lack
   the 'struct sigaction' type, and vice versa.  */

#  if !GNULIB_defined_struct_sigaction

struct sigaction
{
  union
  {
    void (*_sa_handler) (int);
    /* Present to allow compilation, but unsupported by gnulib.  POSIX
       says that implementations may, but not must, make sa_sigaction
       overlap with sa_handler, but we know of no implementation where
       they do not overlap.  */
    void (*_sa_sigaction) (int, siginfo_t *, void *);
  } _sa_func;
  sigset_t sa_mask;
  /* Not all POSIX flags are supported.  */
  int sa_flags;
};
#   define sa_handler _sa_func._sa_handler
#   define sa_sigaction _sa_func._sa_sigaction
/* Unsupported flags are not present.  */
#   define SA_RESETHAND 1
#   define SA_NODEFER 2
#   define SA_RESTART 4

#   define GNULIB_defined_struct_sigaction 1
#  endif

_GL_FUNCDECL_SYS (sigaction, int, (int, const struct sigaction *restrict,
                                   struct sigaction *restrict));

# elif !1

#  define sa_sigaction sa_handler

# endif /* !0, !1 */

_GL_CXXALIAS_SYS (sigaction, int, (int, const struct sigaction *restrict,
                                   struct sigaction *restrict));
_GL_CXXALIASWARN (sigaction);

#elif defined GNULIB_POSIXCHECK
# undef sigaction
# if HAVE_RAW_DECL_SIGACTION
_GL_WARN_ON_USE (sigaction, "sigaction is unportable - "
                 "use the gnulib module sigaction for portability");
# endif
#endif

/* Some systems don't have SA_NODEFER.  */
#ifndef SA_NODEFER
# define SA_NODEFER 0
#endif


#endif /* _GL_M4_SIGNAL_H */
#endif /* _GL_M4_SIGNAL_H */
#endif
