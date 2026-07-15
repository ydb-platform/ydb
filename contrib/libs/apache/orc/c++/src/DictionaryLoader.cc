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

#include "DictionaryLoader.hh"
#include "RLE.hh"

namespace orc {

  namespace {

    // Helper function to read data fully from a stream
    void readFully(char* buffer, int64_t bufferSize, SeekableInputStream* stream) {
      int64_t posn = 0;
      while (posn < bufferSize) {
        const void* chunk;
        int length;
        if (!stream->Next(&chunk, &length)) {
          throw ParseError("bad read in readFully");
        }
        if (posn + length > bufferSize) {
          throw ParseError("Corrupt dictionary blob");
        }
        memcpy(buffer + posn, chunk, static_cast<size_t>(length));
        posn += length;
      }
    }

  }  // namespace

  std::shared_ptr<StringDictionary> loadStringDictionary(uint64_t columnId, StripeStreams& stripe,
                                                         MemoryPool& pool) {
    // Get encoding information
    proto::ColumnEncoding encoding = stripe.getEncoding(columnId);
    RleVersion rleVersion = convertRleVersion(encoding.kind());
    uint32_t dictSize = encoding.dictionary_size();

    // Create the dictionary object
    auto dictionary = std::make_shared<StringDictionary>(pool);

    // Read LENGTH stream to get dictionary entry lengths
    std::unique_ptr<SeekableInputStream> stream =
        stripe.getStream(columnId, proto::Stream_Kind_LENGTH, false);
    if (dictSize > 0 && stream == nullptr) {
      std::stringstream ss;
      ss << "LENGTH stream not found in StringDictionaryColumn for column " << columnId;
      throw ParseError(ss.str());
    }
    std::unique_ptr<RleDecoder> lengthDecoder =
        createRleDecoder(std::move(stream), false, rleVersion, pool, stripe.getReaderMetrics());

    // Decode dictionary entry lengths
    dictionary->dictionaryOffset.resize(dictSize + 1);
    int64_t* lengthArray = dictionary->dictionaryOffset.data();
    lengthDecoder->next(lengthArray + 1, dictSize, nullptr);
    lengthArray[0] = 0;

    // Convert lengths to cumulative offsets
    for (uint32_t i = 1; i < dictSize + 1; ++i) {
      if (lengthArray[i] < 0) {
        std::stringstream ss;
        ss << "Negative dictionary entry length for column " << columnId;
        throw ParseError(ss.str());
      }
      lengthArray[i] += lengthArray[i - 1];
    }

    int64_t blobSize = lengthArray[dictSize];

    // Read DICTIONARY_DATA stream to get dictionary content
    dictionary->dictionaryBlob.resize(static_cast<uint64_t>(blobSize));
    std::unique_ptr<SeekableInputStream> blobStream =
        stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA, false);
    if (blobSize > 0 && blobStream == nullptr) {
      std::stringstream ss;
      ss << "DICTIONARY_DATA stream not found in StringDictionaryColumn for column " << columnId;
      throw ParseError(ss.str());
    }

    // Read the dictionary blob
    readFully(dictionary->dictionaryBlob.data(), blobSize, blobStream.get());

    return dictionary;
  }

}  // namespace orc