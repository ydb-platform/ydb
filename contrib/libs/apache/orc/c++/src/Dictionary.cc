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

#include "Dictionary.hh"

#include <google/protobuf/arena.h>
#include <memory>
#include <utility>

using google::protobuf::Arena;

namespace orc {
  SortedStringDictionary::SortedStringDictionary()
      : totalLength_(0), arena_(std::make_unique<Arena>()) {
#ifdef BUILD_SPARSEHASH
    /// Need to set empty key otherwise dense_hash_map will not work correctly
    keyToIndex_.set_empty_key(std::string_view{});
#endif
  }

  SortedStringDictionary::~SortedStringDictionary() = default;

  // insert a new string into dictionary, return its insertion order
  size_t SortedStringDictionary::insert(const char* str, size_t len) {
    size_t index = keyToIndex_.size();

    auto it = keyToIndex_.find(std::string_view{str, len});
    if (it != keyToIndex_.end()) {
      return it->second;
    } else {
      auto s = Arena::Create<std::string>(arena_.get(), str, len);
      keyToIndex_.emplace(std::string_view{s->data(), s->length()}, index);
      totalLength_ += len;
      return index;
    }
  }

  // reserve space for dictionary entries
  void SortedStringDictionary::reserve(size_t size) {
    keyToIndex_.reserve(size);
  }

  /**
   * Write dictionary data & length to output buffer and
   * reorder input index buffer from insertion order to dictionary order
   *
   * We require this function because string values are buffered by indexes
   * in their insertion order. Until the entire dictionary is complete can
   * we get their sorted indexes in the dictionary in that ORC specification
   * demands dictionary should be ordered. Therefore this function transforms
   * the indexes from insertion order to dictionary value order for final
   * output.
   */
  void SortedStringDictionary::flush(AppendOnlyBufferedStream* dataStream,
                                     RleEncoder* lengthEncoder,
                                     std::vector<int64_t>& idxBuffer) const {
    std::vector<std::pair<std::string_view, size_t>> flatDict;
    flatDict.reserve(keyToIndex_.size());
    for (auto [key, index] : keyToIndex_) {
      flatDict.emplace_back(key, index);
    }
    std::sort(flatDict.begin(), flatDict.end());

    for (const auto& [entry, _] : flatDict) {
      dataStream->write(entry.data(), entry.size());
      lengthEncoder->write(static_cast<int64_t>(entry.size()));
    }

    std::vector<size_t> mapping(flatDict.size());
    for (size_t i = 0; i < flatDict.size(); ++i) {
      mapping[flatDict[i].second] = i;
    }

    // do the transformation
    for (size_t i = 0; i != idxBuffer.size(); ++i) {
      idxBuffer[i] = static_cast<int64_t>(mapping[static_cast<size_t>(idxBuffer[i])]);
    }
  }

  // get dict entries in insertion order
  std::vector<std::string_view> SortedStringDictionary::getEntriesInInsertionOrder() const {
    std::vector<std::string_view> entries(keyToIndex_.size());
    for (auto [key, index] : keyToIndex_) {
      entries[index] = key;
    }
    return entries;
  }

  // return count of entries
  size_t SortedStringDictionary::size() const {
    return keyToIndex_.size();
  }

  // return capacity of dictionary
  size_t SortedStringDictionary::capacity() const {
    return keyToIndex_.bucket_count() * keyToIndex_.max_load_factor();
  }

  // return total length of strings in the dictioanry
  uint64_t SortedStringDictionary::length() const {
    return totalLength_;
  }

  void SortedStringDictionary::clear() {
    totalLength_ = 0;
    keyToIndex_.clear();
    arena_->Reset();
  }
}  // namespace orc
