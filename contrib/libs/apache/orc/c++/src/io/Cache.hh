/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <future>
#include <utility>
#include <vector>

namespace orc {

  struct ReadRange {
    uint64_t offset;
    uint64_t length;

    ReadRange() = default;
    ReadRange(uint64_t offset, uint64_t length) : offset(offset), length(length) {}

    friend bool operator==(const ReadRange& left, const ReadRange& right) {
      return (left.offset == right.offset && left.length == right.length);
    }
    friend bool operator!=(const ReadRange& left, const ReadRange& right) {
      return !(left == right);
    }

    bool contains(const ReadRange& other) const {
      return (offset <= other.offset && offset + length >= other.offset + other.length);
    }
  };

  struct ReadRangeCombiner {
    const uint64_t holeSizeLimit;
    const uint64_t rangeSizeLimit;

    std::vector<ReadRange> coalesce(std::vector<ReadRange> ranges) const;

    static std::vector<ReadRange> coalesceReadRanges(std::vector<ReadRange> ranges,
                                                     uint64_t holeSizeLimit,
                                                     uint64_t rangeSizeLimit);
  };

  using Buffer = DataBuffer<char>;
  using BufferPtr = std::shared_ptr<Buffer>;

  struct RangeCacheEntry {
    ReadRange range;
    BufferPtr buffer;
    std::shared_future<void> future;  // use shared_future in case of multiple get calls

    RangeCacheEntry() = default;
    RangeCacheEntry(const ReadRange& range, BufferPtr buffer, std::future<void> future)
        : range(range), buffer(std::move(buffer)), future(std::move(future).share()) {}

    friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
      return left.range.offset < right.range.offset;
    }
  };

  struct BufferSlice {
    BufferPtr buffer = nullptr;
    uint64_t offset = 0;
    uint64_t length = 0;
  };

  /// A read cache designed to hide IO latencies when reading.
  class ReadRangeCache {
   public:
    /// Construct a read cache with given options
    explicit ReadRangeCache(InputStream* stream, CacheOptions options, MemoryPool* memoryPool,
                            ReaderMetrics* metrics = nullptr)
        : stream_(stream),
          options_(std::move(options)),
          memoryPool_(memoryPool),
          metrics_(metrics) {}

    ~ReadRangeCache() = default;

    /// Cache the given ranges in the background.
    ///
    /// The caller must ensure that the ranges do not overlap with each other,
    /// nor with previously cached ranges.  Otherwise, behaviour will be undefined.
    void cache(std::vector<ReadRange> ranges);

    /// Read a range previously given to Cache().
    BufferSlice read(const ReadRange& range);

    /// Evict cache entries with its range before given boundary.
    void evictEntriesBefore(uint64_t boundary);

   private:
    std::vector<RangeCacheEntry> makeCacheEntries(const std::vector<ReadRange>& ranges) const;

    InputStream* stream_;
    CacheOptions options_;
    // Ordered by offset (so as to find a matching region by binary search)
    std::vector<RangeCacheEntry> entries_;
    MemoryPool* memoryPool_;
    ReaderMetrics* metrics_;
  };

}  // namespace orc
