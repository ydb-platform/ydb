/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/SDKConfig.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/MemorySystemInterface.h>
#include <aws/crt/StlAllocator.h>

#include <memory>
#include <cstdlib>

namespace Aws
{
#ifdef USE_AWS_MEMORY_MANAGEMENT

    template< typename T > using Allocator = Aws::Crt::StlAllocator<T>;

#ifdef __ANDROID__
#if _GLIBCXX_FULLY_DYNAMIC_STRING == 0
    template< typename T >
    bool operator ==(const Allocator< T >& lhs, const Allocator< T >& rhs)
    {
        AWS_UNREFERENCED_PARAM(lhs);
        AWS_UNREFERENCED_PARAM(rhs);

        return false;
    }
#endif // _GLIBCXX_FULLY_DYNAMIC_STRING == 0
#endif // __ANDROID__

#else

    template< typename T > using Allocator = std::allocator<T>;

#endif // USE_AWS_MEMORY_MANAGEMENT
    /**
     * Creates a shared_ptr using AWS Allocator hooks.
     * allocationTag is for memory tracking purposes.
     */
    template<typename T, typename ...ArgTypes>
    std::shared_ptr<T> MakeShared(const char* allocationTag, ArgTypes&&... args)
    {
        AWS_UNREFERENCED_PARAM(allocationTag);

        return std::allocate_shared<T, Aws::Allocator<T>>(Aws::Allocator<T>(), std::forward<ArgTypes>(args)...);
    }


} // namespace Aws
