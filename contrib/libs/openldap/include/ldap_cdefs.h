/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 * 
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */
/* LDAP C Defines */

#ifndef _LDAP_CDEFS_H
#define _LDAP_CDEFS_H

#if defined(__cplusplus) || defined(c_plusplus)
#	define LDAP_BEGIN_DECL	extern "C" {
#	define LDAP_END_DECL	}
#else
#	define LDAP_BEGIN_DECL	/* begin declarations */
#	define LDAP_END_DECL	/* end declarations */
#endif

#if !defined(LDAP_NO_PROTOTYPES) && ( defined(LDAP_NEEDS_PROTOTYPES) || \
	defined(__STDC__) || defined(__cplusplus) || defined(c_plusplus) )

	/* ANSI C or C++ */
#	define LDAP_P(protos)	protos
#	define LDAP_CONCAT1(x,y)	x ## y
#	define LDAP_CONCAT(x,y)	LDAP_CONCAT1(x,y)
#	define LDAP_STRING(x)	#x /* stringify without expanding x */
#	define LDAP_XSTRING(x)	LDAP_STRING(x) /* expand x, then stringify */

#ifndef LDAP_CONST
#	define LDAP_CONST	const
#endif

#else /* no prototypes */

	/* traditional C */
#	define LDAP_P(protos)	()
#	define LDAP_CONCAT(x,y)	x/**/y
#	define LDAP_STRING(x)	"x"

#ifndef LDAP_CONST
#	define LDAP_CONST	/* no const */
#endif

#endif /* no prototypes */

#if (__GNUC__) * 1000 + (__GNUC_MINOR__) >= 2006
#	define LDAP_GCCATTR(attrs)	__attribute__(attrs)
#else
#	define LDAP_GCCATTR(attrs)
#endif

/*
 * Support for Windows DLLs.
 *
 * When external source code includes header files for dynamic libraries,
 * the external source code is "importing" DLL symbols into its resulting
 * object code. On Windows, symbols imported from DLLs must be explicitly
 * indicated in header files with the __declspec(dllimport) directive.
 * This is not totally necessary for functions because the compiler
 * (gcc or MSVC) will generate stubs when this directive is absent.
 * However, this is required for imported variables.
 *
 * The LDAP libraries, i.e. liblber and libldap, can be built as
 * static or shared, based on configuration. Just about all other source
 * code in OpenLDAP use these libraries. If the LDAP libraries
 * are configured as shared, 'configure' defines the LDAP_LIBS_DYNAMIC
 * macro. When other source files include LDAP library headers, the
 * LDAP library symbols will automatically be marked as imported. When
 * the actual LDAP libraries are being built, the symbols will not
 * be marked as imported because the LBER_LIBRARY or LDAP_LIBRARY macros
 * will be respectively defined.
 *
 * Any project outside of OpenLDAP with source code wanting to use
 * LDAP dynamic libraries should explicitly define LDAP_LIBS_DYNAMIC.
 * This will ensure that external source code appropriately marks symbols
 * that will be imported.
 *
 * The slapd executable, itself, can be used as a dynamic library.
 * For example, if a backend module is compiled as shared, it will
 * import symbols from slapd. When this happens, the slapd symbols
 * must be marked as imported in header files that the backend module
 * includes. Remember that slapd links with various static libraries.
 * If the LDAP libraries were configured as static, their object
 * code is also part of the monolithic slapd executable. Thus, when
 * a backend module imports symbols from slapd, it imports symbols from
 * all of the static libraries in slapd as well. Thus, the SLAP_IMPORT
 * macro, when defined, will appropriately mark symbols as imported.
 * This macro should be used by shared backend modules as well as any
 * other external source code that imports symbols from the slapd
 * executable as if it were a DLL.
 *
 * Note that we don't actually have to worry about using the
 * __declspec(dllexport) directive anywhere. This is because both
 * MSVC and Mingw provide alternate (more effective) methods for exporting
 * symbols out of binaries, i.e. the use of a DEF file.
 *
 * NOTE ABOUT BACKENDS: Backends can be configured as static or dynamic.
 * When a backend is configured as dynamic, slapd will load the backend
 * explicitly and populate function pointer structures by calling
 * the backend's well-known initialization function. Because of this
 * procedure, slapd never implicitly imports symbols from dynamic backends.
 * This makes it unnecessary to tag various backend functions with the
 * __declspec(dllimport) directive. This is because neither slapd nor
 * any other external binary should ever be implicitly loading a backend
 * dynamic module.
 *
 * Backends are supposed to be self-contained. However, it appears that
 * back-meta DOES implicitly import symbols from back-ldap. This means
 * that the __declspec(dllimport) directive should be marked on back-ldap
 * functions (in its header files) if and only if we're compiling for
 * windows AND back-ldap has been configured as dynamic AND back-meta
 * is the client of back-ldap. When client is slapd, there is no effect
 * since slapd does not implicitly import symbols.
 *
 * TODO(?): Currently, back-meta nor back-ldap is supported for Mingw32.
 * Thus, there's no need to worry about this right now. This is something that
 * may or may not have to be addressed in the future.
 */

/* LBER library */
#if defined(_WIN32) && \
    ((defined(LDAP_LIBS_DYNAMIC) && !defined(LBER_LIBRARY)) || \
     (!defined(LDAP_LIBS_DYNAMIC) && defined(SLAPD_IMPORT)))
#	define LBER_F(type)		extern __declspec(dllimport) type
#	define LBER_V(type)		extern __declspec(dllimport) type
#else
#	define LBER_F(type)		extern type
#	define LBER_V(type)		extern type
#endif

/* LDAP library */
#if defined(_WIN32) && \
    ((defined(LDAP_LIBS_DYNAMIC) && !defined(LDAP_LIBRARY)) || \
     (!defined(LDAP_LIBS_DYNAMIC) && defined(SLAPD_IMPORT)))
#	define LDAP_F(type)		extern __declspec(dllimport) type
#	define LDAP_V(type)		extern __declspec(dllimport) type
#else
#	define LDAP_F(type)		extern type
#	define LDAP_V(type)		extern type
#endif

/* AVL library */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_AVL_F(type)		extern __declspec(dllimport) type
#	define LDAP_AVL_V(type)		extern __declspec(dllimport) type
#else
#	define LDAP_AVL_F(type)		extern type
#	define LDAP_AVL_V(type)		extern type
#endif

/* LDIF library */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_LDIF_F(type)	extern __declspec(dllimport) type
#	define LDAP_LDIF_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_LDIF_F(type)	extern type
#	define LDAP_LDIF_V(type)	extern type
#endif

/* LUNICODE library */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_LUNICODE_F(type)	extern __declspec(dllimport) type
#	define LDAP_LUNICODE_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_LUNICODE_F(type)	extern type
#	define LDAP_LUNICODE_V(type)	extern type
#endif

/* LUTIL library */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_LUTIL_F(type)	extern __declspec(dllimport) type
#	define LDAP_LUTIL_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_LUTIL_F(type)	extern type
#	define LDAP_LUTIL_V(type)	extern type
#endif

/* REWRITE library */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_REWRITE_F(type)	extern __declspec(dllimport) type
#	define LDAP_REWRITE_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_REWRITE_F(type)	extern type
#	define LDAP_REWRITE_V(type)	extern type
#endif

/* SLAPD (as a dynamic library exporting symbols) */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_SLAPD_F(type)	extern __declspec(dllimport) type
#	define LDAP_SLAPD_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_SLAPD_F(type)	extern type
#	define LDAP_SLAPD_V(type)	extern type
#endif

/* SLAPD (as a dynamic library exporting symbols) */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define LDAP_SLAPI_F(type)	extern __declspec(dllimport) type
#	define LDAP_SLAPI_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_SLAPI_F(type)	extern type
#	define LDAP_SLAPI_V(type)	extern type
#endif

/* SLAPD (as a dynamic library exporting symbols) */
#if defined(_WIN32) && defined(SLAPD_IMPORT)
#	define SLAPI_F(type)		extern __declspec(dllimport) type
#	define SLAPI_V(type)		extern __declspec(dllimport) type
#else
#	define SLAPI_F(type)		extern type
#	define SLAPI_V(type)		extern type
#endif

/*
 * C library. Mingw32 links with the dynamic C run-time library by default,
 * so the explicit definition of CSTATIC will keep dllimport from
 * being defined, if desired.
 *
 * MSVC defines the _DLL macro when the compiler is invoked with /MD or /MDd,
 * which means the resulting object code will be linked with the dynamic
 * C run-time library.
 *
 * Technically, it shouldn't be necessary to redefine any functions that
 * the headers for the C library should already contain. Nevertheless, this
 * is here as a safe-guard.
 *
 * TODO: Determine if these macros ever get expanded for Windows. If not,
 * the declspec expansion can probably be removed.
 */
#if (defined(__MINGW32__) && !defined(CSTATIC)) || \
    (defined(_MSC_VER) && defined(_DLL))
#	define LDAP_LIBC_F(type)	extern __declspec(dllimport) type
#	define LDAP_LIBC_V(type)	extern __declspec(dllimport) type
#else
#	define LDAP_LIBC_F(type)	extern type
#	define LDAP_LIBC_V(type)	extern type
#endif

#endif /* _LDAP_CDEFS_H */
