/* DO NOT EDIT! GENERATED AUTOMATICALLY! */
/* A POSIX <locale.h>.
   Copyright (C) 2007-2013 Free Software Foundation, Inc.

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


#ifdef _GL_ALREADY_INCLUDING_LOCALE_H

/* Special invocation conventions to handle Solaris header files
   (through Solaris 10) when combined with gettext's libintl.h.  */

#if _MSC_VER >= 1900
#include <../ucrt/locale.h>
#else
#error #include <../include/locale.h>
#endif

#else
/* Normal invocation convention.  */

#ifndef _GL_M4_LOCALE_H

#define _GL_ALREADY_INCLUDING_LOCALE_H

/* The include_next requires a split double-inclusion guard.  */
#if _MSC_VER >= 1900
#include <../ucrt/locale.h>
#else
#error #include <../include/locale.h>
#endif

#undef _GL_ALREADY_INCLUDING_LOCALE_H

#ifndef _GL_M4_LOCALE_H
#define _GL_M4_LOCALE_H

/* NetBSD 5.0 mis-defines NULL.  */
#include <stddef.h>

/* Mac OS X 10.5 defines the locale_t type in <xlocale.h>.  */
#if 0
# include <xlocale.h>
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

/* The LC_MESSAGES locale category is specified in POSIX, but not in ISO C.
   On systems that don't define it, use the same value as GNU libintl.  */
#if !defined LC_MESSAGES
# define LC_MESSAGES 1729
#endif

/* Bionic libc's 'struct lconv' is just a dummy.  */
#if 1
# define lconv rpl_lconv
struct lconv
{
  /* All 'char *' are actually 'const char *'.  */

  /* Members that depend on the LC_NUMERIC category of the locale.  See
     <http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap07.html#tag_07_03_04> */

  /* Symbol used as decimal point.  */
  char *decimal_point;
  /* Symbol used to separate groups of digits to the left of the decimal
     point.  */
  char *thousands_sep;
  /* Definition of the size of groups of digits to the left of the decimal
     point.  */
  char *grouping;

  /* Members that depend on the LC_MONETARY category of the locale.  See
     <http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap07.html#tag_07_03_03> */

  /* Symbol used as decimal point.  */
  char *mon_decimal_point;
  /* Symbol used to separate groups of digits to the left of the decimal
     point.  */
  char *mon_thousands_sep;
  /* Definition of the size of groups of digits to the left of the decimal
     point.  */
  char *mon_grouping;
  /* Sign used to indicate a value >= 0.  */
  char *positive_sign;
  /* Sign used to indicate a value < 0.  */
  char *negative_sign;

  /* For formatting local currency.  */
  /* Currency symbol (3 characters) followed by separator (1 character).  */
  char *currency_symbol;
  /* Number of digits after the decimal point.  */
  char frac_digits;
  /* For values >= 0: 1 if the currency symbol precedes the number, 0 if it
     comes after the number.  */
  char p_cs_precedes;
  /* For values >= 0: Position of the sign.  */
  char p_sign_posn;
  /* For values >= 0: Placement of spaces between currency symbol, sign, and
     number.  */
  char p_sep_by_space;
  /* For values < 0: 1 if the currency symbol precedes the number, 0 if it
     comes after the number.  */
  char n_cs_precedes;
  /* For values < 0: Position of the sign.  */
  char n_sign_posn;
  /* For values < 0: Placement of spaces between currency symbol, sign, and
     number.  */
  char n_sep_by_space;

  /* For formatting international currency.  */
  /* Currency symbol (3 characters) followed by separator (1 character).  */
  char *int_curr_symbol;
  /* Number of digits after the decimal point.  */
  char int_frac_digits;
  /* For values >= 0: 1 if the currency symbol precedes the number, 0 if it
     comes after the number.  */
  char int_p_cs_precedes;
  /* For values >= 0: Position of the sign.  */
  char int_p_sign_posn;
  /* For values >= 0: Placement of spaces between currency symbol, sign, and
     number.  */
  char int_p_sep_by_space;
  /* For values < 0: 1 if the currency symbol precedes the number, 0 if it
     comes after the number.  */
  char int_n_cs_precedes;
  /* For values < 0: Position of the sign.  */
  char int_n_sign_posn;
  /* For values < 0: Placement of spaces between currency symbol, sign, and
     number.  */
  char int_n_sep_by_space;
};
#endif

#if 1
# if 1
#  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
#   undef localeconv
#   define localeconv rpl_localeconv
#  endif
_GL_FUNCDECL_RPL (localeconv, struct lconv *, (void));
_GL_CXXALIAS_RPL (localeconv, struct lconv *, (void));
# else
_GL_CXXALIAS_SYS (localeconv, struct lconv *, (void));
# endif
_GL_CXXALIASWARN (localeconv);
#elif 1
# undef localeconv
# define localeconv localeconv_used_without_requesting_gnulib_module_localeconv
#elif defined GNULIB_POSIXCHECK
# undef localeconv
# if HAVE_RAW_DECL_LOCALECONV
_GL_WARN_ON_USE (localeconv,
                 "localeconv returns too few information on some platforms - "
                 "use gnulib module localeconv for portability");
# endif
#endif

#if IN_M4_GNULIB_TESTS
# if 1
#  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
#   undef setlocale
#   define setlocale rpl_setlocale
#   define GNULIB_defined_setlocale 1
#  endif
_GL_FUNCDECL_RPL (setlocale, char *, (int category, const char *locale));
_GL_CXXALIAS_RPL (setlocale, char *, (int category, const char *locale));
# else
_GL_CXXALIAS_SYS (setlocale, char *, (int category, const char *locale));
# endif
_GL_CXXALIASWARN (setlocale);
#elif defined GNULIB_POSIXCHECK
# undef setlocale
# if HAVE_RAW_DECL_SETLOCALE
_GL_WARN_ON_USE (setlocale, "setlocale works differently on native Windows - "
                 "use gnulib module setlocale for portability");
# endif
#endif

#if 0
# if 0
#  if !(defined __cplusplus && defined GNULIB_NAMESPACE)
#   undef duplocale
#   define duplocale rpl_duplocale
#  endif
_GL_FUNCDECL_RPL (duplocale, locale_t, (locale_t locale) _GL_ARG_NONNULL ((1)));
_GL_CXXALIAS_RPL (duplocale, locale_t, (locale_t locale));
# else
#  if 1
_GL_CXXALIAS_SYS (duplocale, locale_t, (locale_t locale));
#  endif
# endif
# if 1
_GL_CXXALIASWARN (duplocale);
# endif
#elif defined GNULIB_POSIXCHECK
# undef duplocale
# if HAVE_RAW_DECL_DUPLOCALE
_GL_WARN_ON_USE (duplocale, "duplocale is buggy on some glibc systems - "
                 "use gnulib module duplocale for portability");
# endif
#endif

#endif /* _GL_M4_LOCALE_H */
#endif /* ! _GL_ALREADY_INCLUDING_LOCALE_H */
#endif /* _GL_M4_LOCALE_H */
