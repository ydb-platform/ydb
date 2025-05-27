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

#include "OutputStream.hh"
#include "Utils.hh"
#include "orc/Exceptions.hh"

#include <sstream>

namespace orc {

  PositionRecorder::~PositionRecorder() {
    // PASS
  }

  BufferedOutputStream::BufferedOutputStream(MemoryPool& pool, OutputStream* outStream,
                                             uint64_t capacity, uint64_t blockSize,
                                             WriterMetrics* metrics)
      : outputStream_(outStream), blockSize_(blockSize), metrics_(metrics) {
    dataBuffer_.reset(new BlockBuffer(pool, blockSize_));
    dataBuffer_->reserve(capacity);
  }

  BufferedOutputStream::~BufferedOutputStream() {
    // PASS
  }

  bool BufferedOutputStream::Next(void** buffer, int* size) {
    auto block = dataBuffer_->getNextBlock();
    if (block.data == nullptr) {
      throw std::logic_error("Failed to get next buffer from block buffer.");
    }
    *buffer = block.data;
    *size = static_cast<int>(block.size);
    return true;
  }

  void BufferedOutputStream::BackUp(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount <= dataBuffer_->size()) {
        dataBuffer_->resize(dataBuffer_->size() - unsignedCount);
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  void BufferedOutputStream::finishStream() {
    // PASS
  }

  int64_t BufferedOutputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(dataBuffer_->size());
  }

  bool BufferedOutputStream::WriteAliasedRaw(const void*, int) {
    throw NotImplementedYet("WriteAliasedRaw is not supported.");
  }

  bool BufferedOutputStream::AllowsAliasing() const {
    return false;
  }

  std::string BufferedOutputStream::getName() const {
    std::ostringstream result;
    result << "BufferedOutputStream " << dataBuffer_->size() << " of " << dataBuffer_->capacity();
    return result.str();
  }

  uint64_t BufferedOutputStream::getSize() const {
    return dataBuffer_->size();
  }

  uint64_t BufferedOutputStream::flush() {
    uint64_t dataSize = dataBuffer_->size();
    // flush data buffer into outputStream
    if (dataSize > 0) {
      SCOPED_STOPWATCH(metrics_, IOBlockingLatencyUs, IOCount);
      dataBuffer_->writeTo(outputStream_, metrics_);
    }
    dataBuffer_->resize(0);
    return dataSize;
  }

  void BufferedOutputStream::suppress() {
    dataBuffer_->resize(0);
  }

  uint64_t BufferedOutputStream::getRawInputBufferSize() const {
    throw std::logic_error("getRawInputBufferSize is not supported.");
  }

  void AppendOnlyBufferedStream::write(const char* data, size_t size) {
    size_t dataOffset = 0;
    while (size > 0) {
      if (bufferOffset_ == bufferLength_) {
        if (!outStream_->Next(reinterpret_cast<void**>(&buffer_), &bufferLength_)) {
          throw std::logic_error("Failed to allocate buffer.");
        }
        bufferOffset_ = 0;
      }
      size_t len = std::min(static_cast<size_t>(bufferLength_ - bufferOffset_), size);
      memcpy(buffer_ + bufferOffset_, data + dataOffset, len);
      bufferOffset_ += static_cast<int>(len);
      dataOffset += len;
      size -= len;
    }
  }

  uint64_t AppendOnlyBufferedStream::getSize() const {
    return outStream_->getSize();
  }

  uint64_t AppendOnlyBufferedStream::flush() {
    finishStream();
    return outStream_->flush();
  }

  void AppendOnlyBufferedStream::recordPosition(PositionRecorder* recorder) const {
    uint64_t flushedSize = outStream_->getSize();
    uint64_t unusedBufferSize = static_cast<uint64_t>(bufferLength_ - bufferOffset_);
    if (outStream_->isCompressed()) {
      // start of the compression chunk in the stream
      recorder->add(flushedSize);
      // There are multiple blocks in the input buffer, but bufferPosition only records the
      // effective length of the last block. We need rawInputBufferSize to record the total length
      // of all variable blocks.
      recorder->add(outStream_->getRawInputBufferSize() - unusedBufferSize);
    } else {
      // byte offset of the start location
      recorder->add(flushedSize - unusedBufferSize);
    }
  }

  void AppendOnlyBufferedStream::finishStream() {
    outStream_->BackUp(bufferLength_ - bufferOffset_);
    outStream_->finishStream();
    bufferOffset_ = bufferLength_ = 0;
    buffer_ = nullptr;
  }

}  // namespace orc
