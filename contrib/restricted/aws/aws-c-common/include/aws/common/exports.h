#ifndef AWS_COMMON_EXPORTS_H
#define AWS_COMMON_EXPORTS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#if defined(AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined(_WIN32)
#    ifdef AWS_COMMON_USE_IMPORT_EXPORT
#        ifdef AWS_COMMON_EXPORTS
#            define AWS_COMMON_API __declspec(dllexport)
#        else
#            define AWS_COMMON_API __declspec(dllimport)
#        endif /* AWS_COMMON_EXPORTS */
#    else
#        define AWS_COMMON_API
#    endif /* AWS_COMMON_USE_IMPORT_EXPORT */

#else /* defined (AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined (_WIN32) */

#    if defined(AWS_COMMON_USE_IMPORT_EXPORT) && defined(AWS_COMMON_EXPORTS)
#        define AWS_COMMON_API __attribute__((visibility("default")))
#    else
#        define AWS_COMMON_API
#    endif

#endif /* defined (AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined (_WIN32) */

#ifdef AWS_NO_STATIC_IMPL
#    define AWS_STATIC_IMPL AWS_COMMON_API
#endif

#ifndef AWS_STATIC_IMPL
/*
 * In order to allow us to export our inlinable methods in a DLL/.so, we have a designated .c
 * file where this AWS_STATIC_IMPL macro will be redefined to be non-static.
 */
#    define AWS_STATIC_IMPL static inline
#endif

#endif /* AWS_COMMON_EXPORTS_H */
