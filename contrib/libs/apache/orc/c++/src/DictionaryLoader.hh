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

#ifndef ORC_DICTIONARY_LOADER_HH
#define ORC_DICTIONARY_LOADER_HH

#include "ColumnReader.hh"
#include "orc/Vector.hh"

namespace orc {

  /**
   * Load a string dictionary for a single column from a stripe.
   * This function reads the LENGTH and DICTIONARY_DATA streams and populates
   * the StringDictionary structure. It automatically uses ReadCache if available
   * through the StripeStreams interface.
   *
   * @param columnId the column ID to load the dictionary for
   * @param stripe the StripeStreams interface providing access to streams
   * @param pool the memory pool to use for allocating the dictionary
   * @return a shared pointer to the loaded StringDictionary, or nullptr if loading fails
   */
  std::shared_ptr<StringDictionary> loadStringDictionary(uint64_t columnId, StripeStreams& stripe,
                                                         MemoryPool& pool);

  // Helper function to convert encoding kind to RLE version
  inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
    switch (static_cast<int64_t>(kind)) {
      case proto::ColumnEncoding_Kind_DIRECT:
      case proto::ColumnEncoding_Kind_DICTIONARY:
        return RleVersion_1;
      case proto::ColumnEncoding_Kind_DIRECT_V2:
      case proto::ColumnEncoding_Kind_DICTIONARY_V2:
        return RleVersion_2;
      default:
        throw ParseError("Unknown encoding in convertRleVersion");
    }
  }

}  // namespace orc

#endif
