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
#include <string>

#ifdef BUILD_SPARSEHASH
#error #include <sparsehash/dense_hash_map>
#else
#include <unordered_map>
#endif

#include "RLE.hh"

namespace orc {
  /**
   * Implementation of increasing sorted string dictionary
   */
  class SortedStringDictionary {
   public:
    struct DictEntry {
      DictEntry(const char* str, size_t len) : data(std::make_unique<std::string>(str, len)) {}

      std::unique_ptr<std::string> data;
    };

    struct DictEntryWithIndex {
      DictEntryWithIndex(const char* str, size_t len, size_t index)
          : entry(str, len), index(index) {}

      DictEntry entry;
      size_t index;
    };

    SortedStringDictionary() : totalLength_(0) {
#ifdef BUILD_SPARSEHASH
      /// Need to set empty key otherwise dense_hash_map will not work correctly
      keyToIndex_.set_empty_key(std::string_view{});
#endif
    }

    // insert a new string into dictionary, return its insertion order
    size_t insert(const char* str, size_t len);

    // write dictionary data & length to output buffer
    void flush(AppendOnlyBufferedStream* dataStream, RleEncoder* lengthEncoder) const;

    // reorder input index buffer from insertion order to dictionary order
    void reorder(std::vector<int64_t>& idxBuffer) const;

    // get dict entries in insertion order
    void getEntriesInInsertionOrder(std::vector<const DictEntry*>&) const;

    // return count of entries
    size_t size() const;

    // return total length of strings in the dictioanry
    uint64_t length() const;

    void clear();

   private:
    struct LessThan {
      bool operator()(const DictEntryWithIndex& l, const DictEntryWithIndex& r) {
        return *l.entry.data < *r.entry.data;  // use std::string's operator<
      }
    };
    // store dictionary entries in insertion order
    mutable std::vector<DictEntryWithIndex> flatDict_;

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
  };

}  // namespace orc
