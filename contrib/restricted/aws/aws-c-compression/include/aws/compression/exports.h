#ifndef AWS_COMPRESSION_EXPORTS_H
#define AWS_COMPRESSION_EXPORTS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#if defined(AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined(_WIN32)
#    ifdef AWS_COMPRESSION_USE_IMPORT_EXPORT
#        ifdef AWS_COMPRESSION_EXPORTS
#            define AWS_COMPRESSION_API __declspec(dllexport)
#        else
#            define AWS_COMPRESSION_API __declspec(dllimport)
#        endif /* AWS_COMPRESSION_EXPORTS */
#    else
#        define AWS_COMPRESSION_API
#    endif /* AWS_COMPRESSION_USE_IMPORT_EXPORT */

#else /* defined (AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined (_WIN32) */
#    if defined(AWS_COMPRESSION_USE_IMPORT_EXPORT) && defined(AWS_COMPRESSION_EXPORTS)
#        define AWS_COMPRESSION_API __attribute__((visibility("default")))
#    else
#        define AWS_COMPRESSION_API
#    endif

#endif /* defined (AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined (_WIN32) */

#endif /* AWS_COMPRESSION_EXPORTS_H */
