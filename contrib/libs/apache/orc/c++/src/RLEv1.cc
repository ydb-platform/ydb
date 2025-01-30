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

#include "RLEv1.hh"
#include "Adaptor.hh"
#include "Compression.hh"
#include "Utils.hh"
#include "orc/Exceptions.hh"

#include <algorithm>

namespace orc {

  const uint64_t MINIMUM_REPEAT = 3;
  const uint64_t MAXIMUM_REPEAT = 127 + MINIMUM_REPEAT;

  const int64_t BASE_128_MASK = 0x7f;

  const int64_t MAX_DELTA = 127;
  const int64_t MIN_DELTA = -128;
  const uint64_t MAX_LITERAL_SIZE = 128;

  RleEncoderV1::RleEncoderV1(std::unique_ptr<BufferedOutputStream> outStream, bool hasSigned)
      : RleEncoder(std::move(outStream), hasSigned) {
    literals = new int64_t[MAX_LITERAL_SIZE];
    delta_ = 0;
    repeat_ = false;
    tailRunLength_ = 0;
  }

  RleEncoderV1::~RleEncoderV1() {
    delete[] literals;
  }

  void RleEncoderV1::writeValues() {
    if (numLiterals != 0) {
      if (repeat_) {
        writeByte(static_cast<char>(static_cast<uint64_t>(numLiterals) - MINIMUM_REPEAT));
        writeByte(static_cast<char>(delta_));
        if (isSigned) {
          writeVslong(literals[0]);
        } else {
          writeVulong(literals[0]);
        }
      } else {
        writeByte(static_cast<char>(-numLiterals));
        for (size_t i = 0; i < numLiterals; ++i) {
          if (isSigned) {
            writeVslong(literals[i]);
          } else {
            writeVulong(literals[i]);
          }
        }
      }
      repeat_ = false;
      numLiterals = 0;
      tailRunLength_ = 0;
    }
  }

  uint64_t RleEncoderV1::flush() {
    finishEncode();
    uint64_t dataSize = outputStream->flush();
    return dataSize;
  }

  void RleEncoderV1::write(int64_t value) {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
      tailRunLength_ = 1;
    } else if (repeat_) {
      if (value == literals[0] + delta_ * static_cast<int64_t>(numLiterals)) {
        numLiterals += 1;
        if (numLiterals == MAXIMUM_REPEAT) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength_ = 1;
      }
    } else {
      if (tailRunLength_ == 1) {
        delta_ = value - literals[numLiterals - 1];
        if (delta_ < MIN_DELTA || delta_ > MAX_DELTA) {
          tailRunLength_ = 1;
        } else {
          tailRunLength_ = 2;
        }
      } else if (value == literals[numLiterals - 1] + delta_) {
        tailRunLength_ += 1;
      } else {
        delta_ = value - literals[numLiterals - 1];
        if (delta_ < MIN_DELTA || delta_ > MAX_DELTA) {
          tailRunLength_ = 1;
        } else {
          tailRunLength_ = 2;
        }
      }
      if (tailRunLength_ == MINIMUM_REPEAT) {
        if (numLiterals + 1 == MINIMUM_REPEAT) {
          repeat_ = true;
          numLiterals += 1;
        } else {
          numLiterals -= static_cast<int>(MINIMUM_REPEAT - 1);
          int64_t base = literals[numLiterals];
          writeValues();
          literals[0] = base;
          repeat_ = true;
          numLiterals = MINIMUM_REPEAT;
        }
      } else {
        literals[numLiterals++] = value;
        if (numLiterals == MAX_LITERAL_SIZE) {
          writeValues();
        }
      }
    }
  }

  void RleEncoderV1::finishEncode() {
    writeValues();
    RleEncoder::finishEncode();
  }

  signed char RleDecoderV1::readByte() {
    SCOPED_MINUS_STOPWATCH(metrics, DecodingLatencyUs);
    if (bufferStart_ == bufferEnd_) {
      int bufferLength;
      const void* bufferPointer;
      if (!inputStream_->Next(&bufferPointer, &bufferLength)) {
        throw ParseError("bad read in readByte");
      }
      bufferStart_ = static_cast<const char*>(bufferPointer);
      bufferEnd_ = bufferStart_ + bufferLength;
    }
    return static_cast<signed char>(*(bufferStart_++));
  }

  uint64_t RleDecoderV1::readLong() {
    uint64_t result = 0;
    int64_t offset = 0;
    signed char ch = readByte();
    if (ch >= 0) {
      result = static_cast<uint64_t>(ch);
    } else {
      result = static_cast<uint64_t>(ch) & BASE_128_MASK;
      while ((ch = readByte()) < 0) {
        offset += 7;
        result |= (static_cast<uint64_t>(ch) & BASE_128_MASK) << offset;
      }
      result |= static_cast<uint64_t>(ch) << (offset + 7);
    }
    return result;
  }

  void RleDecoderV1::skipLongs(uint64_t numValues) {
    while (numValues > 0) {
      if (readByte() >= 0) {
        --numValues;
      }
    }
  }

  void RleDecoderV1::readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues_ = static_cast<uint64_t>(-ch);
      repeating_ = false;
    } else {
      remainingValues_ = static_cast<uint64_t>(ch) + MINIMUM_REPEAT;
      repeating_ = true;
      delta_ = readByte();
      value_ = isSigned_ ? unZigZag(readLong()) : static_cast<int64_t>(readLong());
    }
  }

  void RleDecoderV1::reset() {
    remainingValues_ = 0;
    value_ = 0;
    bufferStart_ = nullptr;
    bufferEnd_ = nullptr;
    delta_ = 0;
    repeating_ = false;
  }

  RleDecoderV1::RleDecoderV1(std::unique_ptr<SeekableInputStream> input, bool hasSigned,
                             ReaderMetrics* metrics)
      : RleDecoder(metrics), inputStream_(std::move(input)), isSigned_(hasSigned) {
    reset();
  }

  void RleDecoderV1::seek(PositionProvider& location) {
    // move the input stream
    inputStream_->seek(location);
    // reset the decoder status and lazily call readHeader()
    reset();
    // skip ahead the given number of records
    skip(location.next());
  }

  void RleDecoderV1::skip(uint64_t numValues) {
    while (numValues > 0) {
      if (remainingValues_ == 0) {
        readHeader();
      }
      uint64_t count = std::min(numValues, remainingValues_);
      remainingValues_ -= count;
      numValues -= count;
      if (repeating_) {
        value_ += delta_ * static_cast<int64_t>(count);
      } else {
        skipLongs(count);
      }
    }
  }

  template <typename T>
  void RleDecoderV1::next(T* const data, const uint64_t numValues, const char* const notNull) {
    SCOPED_STOPWATCH(metrics, DecodingLatencyUs, DecodingCall);
    uint64_t position = 0;
    // skipNulls()
    if (notNull) {
      // Skip over null values.
      while (position < numValues && !notNull[position]) {
        ++position;
      }
    }
    while (position < numValues) {
      // If we are out of values, read more.
      if (remainingValues_ == 0) {
        readHeader();
      }
      // How many do we read out of this block?
      uint64_t count = std::min(numValues - position, remainingValues_);
      uint64_t consumed = 0;
      if (repeating_) {
        if (notNull) {
          for (uint64_t i = 0; i < count; ++i) {
            if (notNull[position + i]) {
              data[position + i] = static_cast<T>(value_ + static_cast<int64_t>(consumed) * delta_);
              consumed += 1;
            }
          }
        } else {
          for (uint64_t i = 0; i < count; ++i) {
            data[position + i] = static_cast<T>(value_ + static_cast<int64_t>(i) * delta_);
          }
          consumed = count;
        }
        value_ += static_cast<int64_t>(consumed) * delta_;
      } else {
        if (notNull) {
          for (uint64_t i = 0; i < count; ++i) {
            if (notNull[position + i]) {
              data[position + i] =
                  isSigned_ ? static_cast<T>(unZigZag(readLong())) : static_cast<T>(readLong());
              ++consumed;
            }
          }
        } else {
          if (isSigned_) {
            for (uint64_t i = 0; i < count; ++i) {
              data[position + i] = static_cast<T>(unZigZag(readLong()));
            }
          } else {
            for (uint64_t i = 0; i < count; ++i) {
              data[position + i] = static_cast<T>(readLong());
            }
          }
          consumed = count;
        }
      }
      remainingValues_ -= consumed;
      position += count;

      // skipNulls()
      if (notNull) {
        // Skip over null values.
        while (position < numValues && !notNull[position]) {
          ++position;
        }
      }
    }
  }

  void RleDecoderV1::next(int64_t* data, uint64_t numValues, const char* notNull) {
    next<int64_t>(data, numValues, notNull);
  }

  void RleDecoderV1::next(int32_t* data, uint64_t numValues, const char* notNull) {
    next<int32_t>(data, numValues, notNull);
  }

  void RleDecoderV1::next(int16_t* data, uint64_t numValues, const char* notNull) {
    next<int16_t>(data, numValues, notNull);
  }
}  // namespace orc
