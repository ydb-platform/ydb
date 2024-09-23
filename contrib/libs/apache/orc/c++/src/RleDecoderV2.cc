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

#include "Adaptor.hh"
#include "BpackingDefault.hh"
#if defined(ORC_HAVE_RUNTIME_AVX512)
#error #include "BpackingAvx512.hh"
#endif
#include "Compression.hh"
#include "Dispatch.hh"
#include "RLEV2Util.hh"
#include "RLEv2.hh"
#include "Utils.hh"

namespace orc {

  unsigned char RleDecoderV2::readByte() {
    SCOPED_MINUS_STOPWATCH(metrics, DecodingLatencyUs);
    if (bufferStart == bufferEnd) {
      int bufferLength;
      const void* bufferPointer;
      if (!inputStream->Next(&bufferPointer, &bufferLength)) {
        throw ParseError("bad read in RleDecoderV2::readByte");
      }
      bufferStart = const_cast<char*>(static_cast<const char*>(bufferPointer));
      bufferEnd = bufferStart + bufferLength;
    }

    unsigned char result = static_cast<unsigned char>(*bufferStart++);
    return result;
  }

  int64_t RleDecoderV2::readLongBE(uint64_t bsz) {
    int64_t ret = 0, val;
    uint64_t n = bsz;
    while (n > 0) {
      n--;
      val = readByte();
      ret |= (val << (n * 8));
    }
    return ret;
  }

  inline int64_t RleDecoderV2::readVslong() {
    return unZigZag(readVulong());
  }

  uint64_t RleDecoderV2::readVulong() {
    uint64_t ret = 0, b;
    uint64_t offset = 0;
    do {
      b = readByte();
      ret |= (0x7f & b) << offset;
      offset += 7;
    } while (b >= 0x80);
    return ret;
  }

  struct UnpackDynamicFunction {
    using FunctionType = decltype(&BitUnpack::readLongs);

    static std::vector<std::pair<DispatchLevel, FunctionType>> implementations() {
#if defined(ORC_HAVE_RUNTIME_AVX512)
      return {{DispatchLevel::NONE, BitUnpackDefault::readLongs},
              {DispatchLevel::AVX512, BitUnpackAVX512::readLongs}};
#else
      return {{DispatchLevel::NONE, BitUnpackDefault::readLongs}};
#endif
    }
  };

  void RleDecoderV2::readLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs) {
    static DynamicDispatch<UnpackDynamicFunction> dispatch;
    return dispatch.func(this, data, offset, len, fbs);
  }

  RleDecoderV2::RleDecoderV2(std::unique_ptr<SeekableInputStream> input, bool _isSigned,
                             MemoryPool& pool, ReaderMetrics* _metrics)
      : RleDecoder(_metrics),
        inputStream(std::move(input)),
        isSigned(_isSigned),
        firstByte(0),
        bufferStart(nullptr),
        bufferEnd(bufferStart),
        runLength(0),
        runRead(0),
        bitsLeft(0),
        curByte(0),
        unpackedPatch(pool, 0),
        literals(pool, MAX_LITERAL_SIZE) {
    // PASS
  }

  void RleDecoderV2::seek(PositionProvider& location) {
    // move the input stream
    inputStream->seek(location);
    // clear state
    bufferEnd = bufferStart = nullptr;
    runRead = runLength = 0;
    // skip ahead the given number of records
    skip(location.next());
  }

  void RleDecoderV2::skip(uint64_t numValues) {
    // simple for now, until perf tests indicate something encoding specific is
    // needed
    const uint64_t N = 64;
    int64_t dummy[N];

    while (numValues) {
      uint64_t nRead = std::min(N, numValues);
      next(dummy, nRead, nullptr);
      numValues -= nRead;
    }
  }

  template <typename T>
  void RleDecoderV2::next(T* const data, const uint64_t numValues, const char* const notNull) {
    SCOPED_STOPWATCH(metrics, DecodingLatencyUs, DecodingCall);
    uint64_t nRead = 0;

    while (nRead < numValues) {
      // Skip any nulls before attempting to read first byte.
      while (notNull && !notNull[nRead]) {
        if (++nRead == numValues) {
          return;  // ended with null values
        }
      }

      if (runRead == runLength) {
        resetRun();
        firstByte = readByte();
      }

      uint64_t offset = nRead, length = numValues - nRead;

      EncodingType enc = static_cast<EncodingType>((firstByte >> 6) & 0x03);
      switch (static_cast<int64_t>(enc)) {
        case SHORT_REPEAT:
          nRead += nextShortRepeats(data, offset, length, notNull);
          break;
        case DIRECT:
          nRead += nextDirect(data, offset, length, notNull);
          break;
        case PATCHED_BASE:
          nRead += nextPatched(data, offset, length, notNull);
          break;
        case DELTA:
          nRead += nextDelta(data, offset, length, notNull);
          break;
        default:
          throw ParseError("unknown encoding");
      }
    }
  }

  void RleDecoderV2::next(int64_t* data, uint64_t numValues, const char* notNull) {
    next<int64_t>(data, numValues, notNull);
  }

  void RleDecoderV2::next(int32_t* data, uint64_t numValues, const char* notNull) {
    next<int32_t>(data, numValues, notNull);
  }

  void RleDecoderV2::next(int16_t* data, uint64_t numValues, const char* notNull) {
    next<int16_t>(data, numValues, notNull);
  }

  template <typename T>
  uint64_t RleDecoderV2::nextShortRepeats(T* const data, uint64_t offset, uint64_t numValues,
                                          const char* const notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bytes
      uint64_t byteSize = (firstByte >> 3) & 0x07;
      byteSize += 1;

      runLength = firstByte & 0x07;
      // run lengths values are stored only after MIN_REPEAT value is met
      runLength += MIN_REPEAT;
      runRead = 0;

      // read the repeated value which is store using fixed bytes
      literals[0] = readLongBE(byteSize);

      if (isSigned) {
        literals[0] = unZigZag(static_cast<uint64_t>(literals[0]));
      }
    }

    uint64_t nRead = std::min(runLength - runRead, numValues);

    if (notNull) {
      for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
        if (notNull[pos]) {
          data[pos] = static_cast<T>(literals[0]);
          ++runRead;
        }
      }
    } else {
      for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
        data[pos] = static_cast<T>(literals[0]);
        ++runRead;
      }
    }

    return nRead;
  }

  template <typename T>
  uint64_t RleDecoderV2::nextDirect(T* const data, uint64_t offset, uint64_t numValues,
                                    const char* const notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bits
      unsigned char fbo = (firstByte >> 1) & 0x1f;
      uint32_t bitSize = decodeBitWidth(fbo);

      // extract the run length
      runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
      runLength |= readByte();
      // runs are one off
      runLength += 1;
      runRead = 0;

      readLongs(literals.data(), 0, runLength, bitSize);
      if (isSigned) {
        for (uint64_t i = 0; i < runLength; ++i) {
          literals[i] = unZigZag(static_cast<uint64_t>(literals[i]));
        }
      }
    }

    return copyDataFromBuffer(data, offset, numValues, notNull);
  }

  void RleDecoderV2::adjustGapAndPatch(uint32_t patchBitSize, int64_t patchMask, int64_t* resGap,
                                       int64_t* resPatch, uint64_t* patchIdx) {
    uint64_t idx = *patchIdx;
    uint64_t gap = static_cast<uint64_t>(unpackedPatch[idx]) >> patchBitSize;
    int64_t patch = unpackedPatch[idx] & patchMask;
    int64_t actualGap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (gap == 255 && patch == 0) {
      actualGap += 255;
      ++idx;
      gap = static_cast<uint64_t>(unpackedPatch[idx]) >> patchBitSize;
      patch = unpackedPatch[idx] & patchMask;
    }
    // add the left over gap
    actualGap += gap;

    *resGap = actualGap;
    *resPatch = patch;
    *patchIdx = idx;
  }

  template <typename T>
  uint64_t RleDecoderV2::nextPatched(T* const data, uint64_t offset, uint64_t numValues,
                                     const char* const notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bits
      unsigned char fbo = (firstByte >> 1) & 0x1f;
      uint32_t bitSize = decodeBitWidth(fbo);

      // extract the run length
      runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
      runLength |= readByte();
      // runs are one off
      runLength += 1;
      runRead = 0;

      // extract the number of bytes occupied by base
      uint64_t thirdByte = readByte();
      uint64_t byteSize = (thirdByte >> 5) & 0x07;
      // base width is one off
      byteSize += 1;

      // extract patch width
      uint32_t pwo = thirdByte & 0x1f;
      uint32_t patchBitSize = decodeBitWidth(pwo);

      // read fourth byte and extract patch gap width
      uint64_t fourthByte = readByte();
      uint32_t pgw = (fourthByte >> 5) & 0x07;
      // patch gap width is one off
      pgw += 1;

      // extract the length of the patch list
      size_t pl = fourthByte & 0x1f;
      if (pl == 0) {
        throw ParseError("Corrupt PATCHED_BASE encoded data (pl==0)!");
      }

      // read the next base width number of bytes to extract base value
      int64_t base = readLongBE(byteSize);
      int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
      // if mask of base value is 1 then base is negative value else positive
      if ((base & mask) != 0) {
        base = base & ~mask;
        base = -base;
      }

      readLongs(literals.data(), 0, runLength, bitSize);
      // any remaining bits are thrown out
      resetReadLongs();

      // TODO: something more efficient than resize
      unpackedPatch.resize(pl);
      // TODO: Skip corrupt?
      //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
      if ((patchBitSize + pgw) > 64) {
        throw ParseError(
            "Corrupt PATCHED_BASE encoded data "
            "(patchBitSize + pgw > 64)!");
      }
      uint32_t cfb = getClosestFixedBits(patchBitSize + pgw);
      readLongs(unpackedPatch.data(), 0, pl, cfb);
      // any remaining bits are thrown out
      resetReadLongs();

      // apply the patch directly when decoding the packed data
      int64_t patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);

      int64_t gap = 0;
      int64_t patch = 0;
      uint64_t patchIdx = 0;
      adjustGapAndPatch(patchBitSize, patchMask, &gap, &patch, &patchIdx);

      for (uint64_t i = 0; i < runLength; ++i) {
        if (static_cast<int64_t>(i) != gap) {
          // no patching required. add base to unpacked value to get final value
          literals[i] += base;
        } else {
          // extract the patch value
          int64_t patchedVal = literals[i] | (patch << bitSize);

          // add base to patched value
          literals[i] = base + patchedVal;

          // increment the patch to point to next entry in patch list
          ++patchIdx;

          if (patchIdx < unpackedPatch.size()) {
            adjustGapAndPatch(patchBitSize, patchMask, &gap, &patch, &patchIdx);

            // next gap is relative to the current gap
            gap += i;
          }
        }
      }
    }

    return copyDataFromBuffer(data, offset, numValues, notNull);
  }

  template <typename T>
  uint64_t RleDecoderV2::nextDelta(T* const data, uint64_t offset, uint64_t numValues,
                                   const char* const notNull) {
    if (runRead == runLength) {
      // extract the number of fixed bits
      unsigned char fbo = (firstByte >> 1) & 0x1f;
      uint32_t bitSize;
      if (fbo != 0) {
        bitSize = decodeBitWidth(fbo);
      } else {
        bitSize = 0;
      }

      // extract the run length
      runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
      runLength |= readByte();
      ++runLength;  // account for first value
      runRead = 0;

      int64_t prevValue;
      // read the first value stored as vint
      if (isSigned) {
        prevValue = readVslong();
      } else {
        prevValue = static_cast<int64_t>(readVulong());
      }

      literals[0] = prevValue;

      // read the fixed delta value stored as vint (deltas can be negative even
      // if all number are positive)
      int64_t deltaBase = readVslong();

      if (bitSize == 0) {
        // add fixed deltas to adjacent values
        for (uint64_t i = 1; i < runLength; ++i) {
          literals[i] = literals[i - 1] + deltaBase;
        }
      } else {
        prevValue = literals[1] = prevValue + deltaBase;
        if (runLength < 2) {
          std::stringstream ss;
          ss << "Illegal run length for delta encoding: " << runLength;
          throw ParseError(ss.str());
        }
        // write the unpacked values, add it to previous value and store final
        // value to result buffer. if the delta base value is negative then it
        // is a decreasing sequence else an increasing sequence.
        // read deltas using the literals buffer.
        readLongs(literals.data(), 2, runLength - 2, bitSize);
        if (deltaBase < 0) {
          for (uint64_t i = 2; i < runLength; ++i) {
            prevValue = literals[i] = prevValue - literals[i];
          }
        } else {
          for (uint64_t i = 2; i < runLength; ++i) {
            prevValue = literals[i] = prevValue + literals[i];
          }
        }
      }
    }

    return copyDataFromBuffer(data, offset, numValues, notNull);
  }

  template <typename T>
  uint64_t RleDecoderV2::copyDataFromBuffer(T* data, uint64_t offset, uint64_t numValues,
                                            const char* notNull) {
    uint64_t nRead = std::min(runLength - runRead, numValues);
    if (notNull) {
      for (uint64_t i = offset; i < (offset + nRead); ++i) {
        if (notNull[i]) {
          data[i] = static_cast<T>(literals[runRead++]);
        }
      }
    } else {
      for (uint64_t i = offset; i < (offset + nRead); ++i) {
        data[i] = static_cast<T>(literals[runRead++]);
      }
    }
    return nRead;
  }

}  // namespace orc
