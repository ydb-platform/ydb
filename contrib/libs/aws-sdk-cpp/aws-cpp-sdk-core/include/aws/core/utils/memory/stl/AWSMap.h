/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/memory/stl/AWSAllocator.h>

#include <map>
#include <unordered_map>
#include <cstring>

namespace Aws
{

template< typename K, typename V > using Map = std::map< K, V, std::less< K >, Aws::Allocator< std::pair< const K, V > > >;
template< typename K, typename V > using UnorderedMap = std::unordered_map< K, V, std::hash< K >, std::equal_to< K >, Aws::Allocator< std::pair< const K, V > > >;
template< typename K, typename V > using MultiMap = std::multimap< K, V, std::less< K >, Aws::Allocator< std::pair< const K, V > > >;

struct CompareStrings
{
    bool operator()(const char* a, const char* b) const
    {
        return std::strcmp(a, b) < 0;
    }
};

template<typename V> using CStringMap = std::map<const char*, V, CompareStrings, Aws::Allocator<std::pair<const char*, V> > >;

} // namespace Aws
