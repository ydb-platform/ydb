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

#ifndef ORC_UTILS_HH
#define ORC_UTILS_HH

#include <atomic>
#include <chrono>
#include <stdexcept>

namespace orc {

  class AutoStopwatch {
    std::chrono::high_resolution_clock::time_point start_;
    std::atomic<uint64_t>* latencyUs_;
    std::atomic<uint64_t>* count_;
    bool minus_;

   public:
    AutoStopwatch(std::atomic<uint64_t>* latencyUs, std::atomic<uint64_t>* count,
                  bool minus = false)
        : latencyUs_(latencyUs), count_(count), minus_(minus) {
      if (latencyUs_) {
        start_ = std::chrono::high_resolution_clock::now();
      }
    }

    ~AutoStopwatch() {
      if (latencyUs_) {
        std::chrono::microseconds elapsedTime =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_);
        if (!minus_) {
          latencyUs_->fetch_add(static_cast<uint64_t>(elapsedTime.count()));
        } else {
          latencyUs_->fetch_sub(static_cast<uint64_t>(elapsedTime.count()));
        }
      }

      if (count_) {
        count_->fetch_add(1);
      }
    }
  };

#if ENABLE_METRICS
#define SCOPED_STOPWATCH(METRICS_PTR, LATENCY_VAR, COUNT_VAR)                           \
  AutoStopwatch measure((METRICS_PTR == nullptr ? nullptr : &METRICS_PTR->LATENCY_VAR), \
                        (METRICS_PTR == nullptr ? nullptr : &METRICS_PTR->COUNT_VAR))

#define SCOPED_MINUS_STOPWATCH(METRICS_PTR, LATENCY_VAR)                                         \
  AutoStopwatch measure((METRICS_PTR == nullptr ? nullptr : &METRICS_PTR->LATENCY_VAR), nullptr, \
                        true)
#else
#define SCOPED_STOPWATCH(METRICS_PTR, LATENCY_VAR, COUNT_VAR)
#define SCOPED_MINUS_STOPWATCH(METRICS_PTR, LATENCY_VAR)
#endif

  struct Utf8Utils {
    /**
     * Counts how many utf-8 chars of the input data
     */
    static uint64_t charLength(const char* data, uint64_t length) {
      uint64_t chars = 0;
      for (uint64_t i = 0; i < length; i++) {
        if (isUtfStartByte(data[i])) {
          chars++;
        }
      }
      return chars;
    }

    /**
     * Return the number of bytes required to read at most maxCharLength
     * characters in full from a utf-8 encoded byte array provided
     * by data. This does not validate utf-8 data, but
     * operates correctly on already valid utf-8 data.
     *
     * @param maxCharLength number of characters required
     * @param data the bytes of UTF-8
     * @param length the length of data to truncate
     */
    static uint64_t truncateBytesTo(uint64_t maxCharLength, const char* data, uint64_t length) {
      uint64_t chars = 0;
      if (length <= maxCharLength) {
        return length;
      }
      for (uint64_t i = 0; i < length; i++) {
        if (isUtfStartByte(data[i])) {
          chars++;
        }
        if (chars > maxCharLength) {
          return i;
        }
      }
      // everything fits
      return length;
    }

    /**
     * Checks if b is the first byte of a UTF-8 character.
     */
    inline static bool isUtfStartByte(char b) {
      return (b & 0xC0) != 0x80;
    }

    /**
     * Find the start of the last character that ends in the current string.
     * @param text the bytes of the utf-8
     * @param from the first byte location
     * @param until the last byte location
     * @return the index of the last character
     */
    static uint64_t findLastCharacter(const char* text, uint64_t from, uint64_t until) {
      uint64_t posn = until;
      /* we don't expect characters more than 5 bytes */
      while (posn >= from) {
        if (isUtfStartByte(text[posn])) {
          return posn;
        }
        posn -= 1;
      }
      /* beginning of a valid char not found */
      throw std::logic_error("Could not truncate string, beginning of a valid char not found");
    }
  };

}  // namespace orc

#endif
