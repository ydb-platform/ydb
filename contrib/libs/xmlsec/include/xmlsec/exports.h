/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Export macro declarations for Win32 platform.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_EXPORTS_H__
#define __XMLSEC_EXPORTS_H__

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/* There are many variations of Windows */
#if !defined(XMLSEC_WINDOWS)

#if defined(WIN32) || defined(_WIN32) || defined(WINDOWS) || defined(_WINDOWS)
#define XMLSEC_WINDOWS 1
#elif defined(_MSC_VER)
#define XMLSEC_WINDOWS 1
#elif defined(__MINGW32__) || defined(__MINGW64__)
#define XMLSEC_WINDOWS 1
#elif defined(__CYGWIN__)
#define XMLSEC_WINDOWS 1
#endif /* */

#endif /* !defined(XMLSEC_WINDOWS) */

/* Now, the export fun begins. The following we must do for the
   Windows platform. */

#if !defined XMLSEC_EXPORT
#  if defined(XMLSEC_WINDOWS)
     /* if we compile libxmlsec itself: */
#    if defined(IN_XMLSEC)
#      if !defined(XMLSEC_STATIC)
#        define XMLSEC_EXPORT __declspec(dllexport)
#      else
#        define XMLSEC_EXPORT extern
#      endif
     /* if a client program includes this file: */
#    else /* defined(IN_XMLSEC) */
#      if !defined(XMLSEC_STATIC)
#        define XMLSEC_EXPORT __declspec(dllimport)
#      else
#        define XMLSEC_EXPORT
#      endif
#    endif /* defined(IN_XMLSEC) */
   /* This holds on all other platforms/compilers, which are easier to
      handle in regard to this. */
#  else /* defined(XMLSEC_WINDOWS) */
#    define XMLSEC_EXPORT
#  endif /* defined(XMLSEC_WINDOWS) */
#endif /* !defined XMLSEC_EXPORT */

#if !defined XMLSEC_CRYPTO_EXPORT
#  if defined(XMLSEC_WINDOWS)
     /* if we compile libxmlsec itself: */
#    if defined(IN_XMLSEC_CRYPTO)
#      if !defined(XMLSEC_STATIC)
#        define XMLSEC_CRYPTO_EXPORT __declspec(dllexport)
#      else
#        define XMLSEC_CRYPTO_EXPORT extern
#      endif
     /* if a client program includes this file: */
#    else /* defined(IN_XMLSEC_CRYPTO) */
#      if !defined(XMLSEC_STATIC)
#        define XMLSEC_CRYPTO_EXPORT __declspec(dllimport)
#      else
#        define XMLSEC_CRYPTO_EXPORT
#      endif
#    endif /* defined(IN_XMLSEC_CRYPTO) */
   /* This holds on all other platforms/compilers, which are easier to
      handle in regard to this. */
#  else  /* defined(XMLSEC_WINDOWS) */
#    define XMLSEC_CRYPTO_EXPORT
#  endif  /* defined(XMLSEC_WINDOWS) */
#endif /* !defined XMLSEC_CRYPTO_EXPORT */

#if !defined XMLSEC_EXPORT_VAR
#  if defined(XMLSEC_WINDOWS)
     /* if we compile libxmlsec itself: */
#    if defined(IN_XMLSEC)
#      if !defined(XMLSEC_STATIC)
#        define XMLSEC_EXPORT_VAR __declspec(dllexport) extern
#      else
#        define XMLSEC_EXPORT_VAR extern
#      endif
     /* if we compile libxmlsec-crypto itself: */
#    elif defined(IN_XMLSEC_CRYPTO)
#        define XMLSEC_EXPORT_VAR extern
     /* if a client program includes this file: */
#    else
#      if !defined(XMLSEC_STATIC)
#        define XMLSEC_EXPORT_VAR __declspec(dllimport) extern
#      else
#        define XMLSEC_EXPORT_VAR extern
#      endif
#    endif
   /* This holds on all other platforms/compilers, which are easier to
      handle in regard to this. */
#  else /* defined(XMLSEC_WINDOWS) */
#    define XMLSEC_EXPORT_VAR extern
#  endif  /* defined(XMLSEC_WINDOWS) */
#endif

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* __XMLSEC_EXPORTS_H__ */


