// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
/**
  * This file provides forward declarations for Allocator.
  */

namespace CH
{

template <bool clear_memory_, bool mmap_populate = false>
class Allocator;

template <typename Base, size_t N = 64, size_t Alignment = 1>
class AllocatorWithStackMemory;

}
