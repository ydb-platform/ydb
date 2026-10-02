#ifndef AWS_EVENT_STREAM_EXPORTS_H_
#define AWS_EVENT_STREAM_EXPORTS_H_
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#if defined(AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined(_WIN32)
#    ifdef AWS_EVENT_STREAM_USE_IMPORT_EXPORT
#        ifdef AWS_EVENT_STREAM_EXPORTS
#            define AWS_EVENT_STREAM_API __declspec(dllexport)
#        else
#            define AWS_EVENT_STREAM_API __declspec(dllimport)
#        endif /* AWS_EVENT_STREAM_EXPORTS */
#    else
#        define AWS_EVENT_STREAM_API
#    endif /* AWS_EVENT_STREAM_USE_IMPORT_EXPORT */

#else /* defined (AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined (_WIN32) */

#    if defined(AWS_EVENT_STREAM_USE_IMPORT_EXPORT) && defined(AWS_EVENT_STREAM_EXPORTS)
#        define AWS_EVENT_STREAM_API __attribute__((visibility("default")))
#    else
#        define AWS_EVENT_STREAM_API
#    endif

#endif /* defined (AWS_CRT_USE_WINDOWS_DLL_SEMANTICS) || defined (_WIN32) */

#endif /* AWS_EVENT_STREAM_EXPORTS_H */
