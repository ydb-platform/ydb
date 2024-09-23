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

#include "BpackingDefault.hh"
#include "RLEv2.hh"
#include "Utils.hh"

namespace orc {

  UnpackDefault::UnpackDefault(RleDecoderV2* dec) : decoder(dec) {
    // PASS
  }

  UnpackDefault::~UnpackDefault() {
    // PASS
  }

  void UnpackDefault::unrolledUnpack4(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Make sure bitsLeft is 0 before the loop. bitsLeft can only be 0, 4, or 8.
      while (decoder->getBitsLeft() > 0 && curIdx < offset + len) {
        decoder->setBitsLeft(decoder->getBitsLeft() - 4);
        data[curIdx++] = (decoder->getCurByte() >> decoder->getBitsLeft()) & 15;
      }
      if (curIdx == offset + len) return;

      // Exhaust the buffer
      uint64_t numGroups = (offset + len - curIdx) / 2;
      numGroups = std::min(numGroups, static_cast<uint64_t>(decoder->bufLength()));
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      uint32_t localByte;
      for (uint64_t i = 0; i < numGroups; ++i) {
        localByte = *buffer++;
        data[curIdx] = (localByte >> 4) & 15;
        data[curIdx + 1] = localByte & 15;
        curIdx += 2;
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // readByte() will update 'bufferStart' and 'bufferEnd'
      decoder->setCurByte(decoder->readByte());
      decoder->setBitsLeft(8);
    }
  }

  void UnpackDefault::unrolledUnpack8(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength();
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        data[curIdx++] = *buffer++;
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // readByte() will update 'bufferStart' and 'bufferEnd'.
      data[curIdx++] = decoder->readByte();
    }
  }

  void UnpackDefault::unrolledUnpack16(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 2;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint16_t b0, b1;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint16_t>(*buffer);
        b1 = static_cast<uint16_t>(*(buffer + 1));
        buffer += 2;
        data[curIdx++] = (b0 << 8) | b1;
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      data[curIdx++] = (b0 << 8) | b1;
    }
  }

  void UnpackDefault::unrolledUnpack24(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 3;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint32_t b0, b1, b2;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint32_t>(*buffer);
        b1 = static_cast<uint32_t>(*(buffer + 1));
        b2 = static_cast<uint32_t>(*(buffer + 2));
        buffer += 3;
        data[curIdx++] = static_cast<int64_t>((b0 << 16) | (b1 << 8) | b2);
      }
      //////decoder->bufferStart += bufferNum * 3;
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      b2 = decoder->readByte();
      data[curIdx++] = static_cast<int64_t>((b0 << 16) | (b1 << 8) | b2);
    }
  }

  void UnpackDefault::unrolledUnpack32(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 4;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint32_t b0, b1, b2, b3;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint32_t>(*buffer);
        b1 = static_cast<uint32_t>(*(buffer + 1));
        b2 = static_cast<uint32_t>(*(buffer + 2));
        b3 = static_cast<uint32_t>(*(buffer + 3));
        buffer += 4;
        data[curIdx++] = static_cast<int64_t>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      b2 = decoder->readByte();
      b3 = decoder->readByte();
      data[curIdx++] = static_cast<int64_t>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
    }
  }

  void UnpackDefault::unrolledUnpack40(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 5;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint64_t b0, b1, b2, b3, b4;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint32_t>(*buffer);
        b1 = static_cast<uint32_t>(*(buffer + 1));
        b2 = static_cast<uint32_t>(*(buffer + 2));
        b3 = static_cast<uint32_t>(*(buffer + 3));
        b4 = static_cast<uint32_t>(*(buffer + 4));
        buffer += 5;
        data[curIdx++] =
            static_cast<int64_t>((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      b2 = decoder->readByte();
      b3 = decoder->readByte();
      b4 = decoder->readByte();
      data[curIdx++] = static_cast<int64_t>((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
    }
  }

  void UnpackDefault::unrolledUnpack48(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 6;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint64_t b0, b1, b2, b3, b4, b5;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint32_t>(*buffer);
        b1 = static_cast<uint32_t>(*(buffer + 1));
        b2 = static_cast<uint32_t>(*(buffer + 2));
        b3 = static_cast<uint32_t>(*(buffer + 3));
        b4 = static_cast<uint32_t>(*(buffer + 4));
        b5 = static_cast<uint32_t>(*(buffer + 5));
        buffer += 6;
        data[curIdx++] = static_cast<int64_t>((b0 << 40) | (b1 << 32) | (b2 << 24) | (b3 << 16) |
                                              (b4 << 8) | b5);
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      b2 = decoder->readByte();
      b3 = decoder->readByte();
      b4 = decoder->readByte();
      b5 = decoder->readByte();
      data[curIdx++] =
          static_cast<int64_t>((b0 << 40) | (b1 << 32) | (b2 << 24) | (b3 << 16) | (b4 << 8) | b5);
    }
  }

  void UnpackDefault::unrolledUnpack56(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 7;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint64_t b0, b1, b2, b3, b4, b5, b6;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint32_t>(*buffer);
        b1 = static_cast<uint32_t>(*(buffer + 1));
        b2 = static_cast<uint32_t>(*(buffer + 2));
        b3 = static_cast<uint32_t>(*(buffer + 3));
        b4 = static_cast<uint32_t>(*(buffer + 4));
        b5 = static_cast<uint32_t>(*(buffer + 5));
        b6 = static_cast<uint32_t>(*(buffer + 6));
        buffer += 7;
        data[curIdx++] = static_cast<int64_t>((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) |
                                              (b4 << 16) | (b5 << 8) | b6);
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      b2 = decoder->readByte();
      b3 = decoder->readByte();
      b4 = decoder->readByte();
      b5 = decoder->readByte();
      b6 = decoder->readByte();
      data[curIdx++] = static_cast<int64_t>((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) |
                                            (b4 << 16) | (b5 << 8) | b6);
    }
  }

  void UnpackDefault::unrolledUnpack64(int64_t* data, uint64_t offset, uint64_t len) {
    uint64_t curIdx = offset;
    while (curIdx < offset + len) {
      // Exhaust the buffer
      int64_t bufferNum = decoder->bufLength() / 8;
      bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
      uint64_t b0, b1, b2, b3, b4, b5, b6, b7;
      // Avoid updating 'bufferStart' inside the loop.
      auto* buffer = reinterpret_cast<unsigned char*>(decoder->getBufStart());
      for (int i = 0; i < bufferNum; ++i) {
        b0 = static_cast<uint32_t>(*buffer);
        b1 = static_cast<uint32_t>(*(buffer + 1));
        b2 = static_cast<uint32_t>(*(buffer + 2));
        b3 = static_cast<uint32_t>(*(buffer + 3));
        b4 = static_cast<uint32_t>(*(buffer + 4));
        b5 = static_cast<uint32_t>(*(buffer + 5));
        b6 = static_cast<uint32_t>(*(buffer + 6));
        b7 = static_cast<uint32_t>(*(buffer + 7));
        buffer += 8;
        data[curIdx++] = static_cast<int64_t>((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) |
                                              (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
      }
      decoder->setBufStart(reinterpret_cast<char*>(buffer));
      if (curIdx == offset + len) return;

      // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
      b0 = decoder->readByte();
      b1 = decoder->readByte();
      b2 = decoder->readByte();
      b3 = decoder->readByte();
      b4 = decoder->readByte();
      b5 = decoder->readByte();
      b6 = decoder->readByte();
      b7 = decoder->readByte();
      data[curIdx++] = static_cast<int64_t>((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) |
                                            (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
    }
  }

  void UnpackDefault::plainUnpackLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs) {
    for (uint64_t i = offset; i < (offset + len); i++) {
      uint64_t result = 0;
      uint64_t bitsLeftToRead = fbs;
      while (bitsLeftToRead > decoder->getBitsLeft()) {
        result <<= decoder->getBitsLeft();
        result |= decoder->getCurByte() & ((1 << decoder->getBitsLeft()) - 1);
        bitsLeftToRead -= decoder->getBitsLeft();
        decoder->setCurByte(decoder->readByte());
        decoder->setBitsLeft(8);
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        decoder->setBitsLeft(decoder->getBitsLeft() - static_cast<uint32_t>(bitsLeftToRead));
        result |= (decoder->getCurByte() >> decoder->getBitsLeft()) & ((1 << bitsLeftToRead) - 1);
      }
      data[i] = static_cast<int64_t>(result);
    }
  }

  void BitUnpackDefault::readLongs(RleDecoderV2* decoder, int64_t* data, uint64_t offset,
                                   uint64_t len, uint64_t fbs) {
    UnpackDefault unpackDefault(decoder);
    switch (fbs) {
      case 4:
        unpackDefault.unrolledUnpack4(data, offset, len);
        break;
      case 8:
        unpackDefault.unrolledUnpack8(data, offset, len);
        break;
      case 16:
        unpackDefault.unrolledUnpack16(data, offset, len);
        break;
      case 24:
        unpackDefault.unrolledUnpack24(data, offset, len);
        break;
      case 32:
        unpackDefault.unrolledUnpack32(data, offset, len);
        break;
      case 40:
        unpackDefault.unrolledUnpack40(data, offset, len);
        break;
      case 48:
        unpackDefault.unrolledUnpack48(data, offset, len);
        break;
      case 56:
        unpackDefault.unrolledUnpack56(data, offset, len);
        break;
      case 64:
        unpackDefault.unrolledUnpack64(data, offset, len);
        break;
      default:
        // Fallback to the default implementation for deprecated bit size.
        unpackDefault.plainUnpackLongs(data, offset, len, fbs);
        break;
    }
  }

}  // namespace orc
