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

#include <cstddef>
#include <memory>
#include <string_view>

#ifdef BUILD_SPARSEHASH
#error #include <sparsehash/dense_hash_map>
#else
#include <unordered_map>
#endif

#include "RLE.hh"

namespace google::protobuf {
  class Arena;
}

namespace orc {
  /**
   * Implementation of increasing sorted string dictionary
   */
  class SortedStringDictionary {
   public:
    SortedStringDictionary();

    ~SortedStringDictionary();

    // insert a new string into dictionary, return its insertion order
    size_t insert(const char* str, size_t len);

    // reserve space for dictionary entries
    void reserve(size_t size);

    // write dictionary data & length to output buffer and
    // reorder input index buffer from insertion order to dictionary order
    void flush(AppendOnlyBufferedStream* dataStream, RleEncoder* lengthEncoder,
               std::vector<int64_t>& idxBuffer) const;

    // get dict entries in insertion order
    std::vector<std::string_view> getEntriesInInsertionOrder() const;

    // return count of entries
    size_t size() const;

    // return capacity of dictionary
    size_t capacity() const;

    // return total length of strings in the dictioanry
    uint64_t length() const;

    void clear();

   private:
#ifdef BUILD_SPARSEHASH
    // map from string to its insertion order index
    google::dense_hash_map<std::string_view, size_t> keyToIndex_;
#else
    std::unordered_map<std::string_view, size_t> keyToIndex_;
#endif

    uint64_t totalLength_;

    // use friend class here to avoid being bothered by const function calls
    friend class StringColumnWriter;
    friend class CharColumnWriter;
    friend class VarCharColumnWriter;
    // store indexes of insertion order in the dictionary for not-null rows
    std::vector<int64_t> idxInDictBuffer_;

    std::unique_ptr<google::protobuf::Arena> arena_;
  };

}  // namespace orc
