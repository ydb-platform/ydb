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

namespace orc {

  // insert a new string into dictionary, return its insertion order
  size_t SortedStringDictionary::insert(const char* str, size_t len) {
    size_t index = flatDict_.size();

    auto it = keyToIndex_.find(std::string_view{str, len});
    if (it != keyToIndex_.end()) {
      return it->second;
    } else {
      flatDict_.emplace_back(str, len, index);
      totalLength_ += len;

      const auto& lastEntry = flatDict_.back().entry;
      keyToIndex_.emplace(std::string_view{lastEntry.data->data(), lastEntry.data->size()}, index);
      return index;
    }
  }

  // write dictionary data & length to output buffer
  void SortedStringDictionary::flush(AppendOnlyBufferedStream* dataStream,
                                     RleEncoder* lengthEncoder) const {
    std::sort(flatDict_.begin(), flatDict_.end(), LessThan());

    for (const auto& entryWithIndex : flatDict_) {
      dataStream->write(entryWithIndex.entry.data->data(), entryWithIndex.entry.data->size());
      lengthEncoder->write(static_cast<int64_t>(entryWithIndex.entry.data->size()));
    }
  }

  /**
   * Reorder input index buffer from insertion order to dictionary order
   *
   * We require this function because string values are buffered by indexes
   * in their insertion order. Until the entire dictionary is complete can
   * we get their sorted indexes in the dictionary in that ORC specification
   * demands dictionary should be ordered. Therefore this function transforms
   * the indexes from insertion order to dictionary value order for final
   * output.
   */
  void SortedStringDictionary::reorder(std::vector<int64_t>& idxBuffer) const {
    // iterate the dictionary to get mapping from insertion order to value order
    std::vector<size_t> mapping(flatDict_.size());
    for (size_t i = 0; i < flatDict_.size(); ++i) {
      mapping[flatDict_[i].index] = i;
    }

    // do the transformation
    for (size_t i = 0; i != idxBuffer.size(); ++i) {
      idxBuffer[i] = static_cast<int64_t>(mapping[static_cast<size_t>(idxBuffer[i])]);
    }
  }

  // get dict entries in insertion order
  void SortedStringDictionary::getEntriesInInsertionOrder(
      std::vector<const DictEntry*>& entries) const {
    /// flatDict_ is sorted in insertion order before [[SortedStringDictionary::flush]] is invoked.
    entries.resize(flatDict_.size());
    for (size_t i = 0; i < flatDict_.size(); ++i) {
      entries[i] = &(flatDict_[i].entry);
    }
  }

  // return count of entries
  size_t SortedStringDictionary::size() const {
    return flatDict_.size();
  }

  // return total length of strings in the dictioanry
  uint64_t SortedStringDictionary::length() const {
    return totalLength_;
  }

  void SortedStringDictionary::clear() {
    totalLength_ = 0;
    keyToIndex_.clear();
    flatDict_.clear();
  }
}  // namespace orc