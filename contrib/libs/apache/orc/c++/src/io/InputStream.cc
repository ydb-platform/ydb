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

#include "InputStream.hh"
#include "orc/Exceptions.hh"

#include <algorithm>
#include <iomanip>

namespace orc {

  void printBuffer(std::ostream& out, const char* buffer, uint64_t length) {
    const uint64_t width = 24;
    out << std::hex;
    for (uint64_t line = 0; line < (length + width - 1) / width; ++line) {
      out << std::setfill('0') << std::setw(7) << (line * width);
      for (uint64_t byte = 0; byte < width && line * width + byte < length; ++byte) {
        out << " " << std::setfill('0') << std::setw(2)
            << static_cast<uint64_t>(0xff & buffer[line * width + byte]);
      }
      out << "\n";
    }
    out << std::dec;
  }

  PositionProvider::PositionProvider(const std::list<uint64_t>& posns) {
    position_ = posns.begin();
  }

  uint64_t PositionProvider::next() {
    uint64_t result = *position_;
    ++position_;
    return result;
  }

  uint64_t PositionProvider::current() {
    return *position_;
  }

  SeekableInputStream::~SeekableInputStream() {
    // PASS
  }

  SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const unsigned char* values, uint64_t size,
                                                     uint64_t blkSize)
      : data_(reinterpret_cast<const char*>(values)) {
    length_ = size;
    position_ = 0;
    blockSize_ = blkSize == 0 ? length_ : static_cast<uint64_t>(blkSize);
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const char* values, uint64_t size,
                                                     uint64_t blkSize)
      : data_(values) {
    length_ = size;
    position_ = 0;
    blockSize_ = blkSize == 0 ? length_ : static_cast<uint64_t>(blkSize);
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int* size) {
    uint64_t currentSize = std::min(length_ - position_, blockSize_);
    if (currentSize > 0) {
      *buffer = data_ + position_;
      *size = static_cast<int>(currentSize);
      position_ += currentSize;
      return true;
    }
    *size = 0;
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount <= blockSize_ && unsignedCount <= position_) {
        position_ -= unsignedCount;
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount + position_ <= length_) {
        position_ += unsignedCount;
        return true;
      } else {
        position_ = length_;
      }
    }
    return false;
  }

  int64_t SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position_);
  }

  void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position_ = seekPosition.next();
  }

  std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "SeekableArrayInputStream " << position_ << " of " << length_;
    return result.str();
  }

  static uint64_t computeBlock(uint64_t request, uint64_t length) {
    return std::min(length, request == 0 ? 256 * 1024 : request);
  }

  SeekableFileInputStream::SeekableFileInputStream(InputStream* stream, uint64_t offset,
                                                   uint64_t byteCount, MemoryPool& pool,
                                                   uint64_t blockSize)
      : pool_(pool),
        input_(stream),
        start_(offset),
        length_(byteCount),
        blockSize_(computeBlock(blockSize, length_)) {
    position_ = 0;
    buffer_.reset(new DataBuffer<char>(pool_));
    pushBack_ = 0;
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int* size) {
    uint64_t bytesRead;
    if (pushBack_ != 0) {
      *data = buffer_->data() + (buffer_->size() - pushBack_);
      bytesRead = pushBack_;
    } else {
      bytesRead = std::min(length_ - position_, blockSize_);
      buffer_->resize(bytesRead);
      if (bytesRead > 0) {
        input_->read(buffer_->data(), bytesRead, start_ + position_);
        *data = static_cast<void*>(buffer_->data());
      }
    }
    position_ += bytesRead;
    pushBack_ = 0;
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
  }

  void SeekableFileInputStream::BackUp(int signedCount) {
    if (signedCount < 0) {
      throw std::logic_error("can't backup negative distances");
    }
    uint64_t count = static_cast<uint64_t>(signedCount);
    if (pushBack_ > 0) {
      throw std::logic_error("can't backup unless we just called Next");
    }
    if (count > blockSize_ || count > position_) {
      throw std::logic_error("can't backup that far");
    }
    pushBack_ = static_cast<uint64_t>(count);
    position_ -= pushBack_;
  }

  bool SeekableFileInputStream::Skip(int signedCount) {
    if (signedCount < 0) {
      return false;
    }
    uint64_t count = static_cast<uint64_t>(signedCount);
    position_ = std::min(position_ + count, length_);
    pushBack_ = 0;
    return position_ < length_;
  }

  int64_t SeekableFileInputStream::ByteCount() const {
    return static_cast<int64_t>(position_);
  }

  void SeekableFileInputStream::seek(PositionProvider& location) {
    position_ = location.next();
    if (position_ > length_) {
      position_ = length_;
      throw std::logic_error("seek too far");
    }
    pushBack_ = 0;
  }

  std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input_->getName() << " from " << start_ << " for " << length_;
    return result.str();
  }

}  // namespace orc
