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

#include "StripeStream.hh"
#include "RLE.hh"
#include "Reader.hh"
#include "io/Cache.hh"
#include "orc/Exceptions.hh"

#include "wrap/coded-stream-wrapper.h"

namespace orc {

  StripeStreamsImpl::StripeStreamsImpl(const RowReaderImpl& reader, uint64_t index,
                                       const proto::StripeInformation& stripeInfo,
                                       const proto::StripeFooter& footer, uint64_t stripeStart,
                                       InputStream& input, const Timezone& writerTimezone,
                                       const Timezone& readerTimezone)
      : reader_(reader),
        stripeInfo_(stripeInfo),
        footer_(footer),
        stripeIndex_(index),
        stripeStart_(stripeStart),
        input_(input),
        writerTimezone_(writerTimezone),
        readerTimezone_(readerTimezone),
        readCache_(reader.getReadCache()) {
    // PASS
  }

  StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
  }

  StreamInformation::~StreamInformation() {
    // PASS
  }

  StripeInformation::~StripeInformation() {
    // PASS
  }

  StreamInformationImpl::~StreamInformationImpl() {
    // PASS
  }

  const std::vector<bool> StripeStreamsImpl::getSelectedColumns() const {
    return reader_.getSelectedColumns();
  }

  proto::ColumnEncoding StripeStreamsImpl::getEncoding(uint64_t columnId) const {
    return footer_.columns(static_cast<int>(columnId));
  }

  const Timezone& StripeStreamsImpl::getWriterTimezone() const {
    return writerTimezone_;
  }

  const Timezone& StripeStreamsImpl::getReaderTimezone() const {
    return readerTimezone_;
  }

  std::ostream* StripeStreamsImpl::getErrorStream() const {
    return reader_.getFileContents().errorStream;
  }

  std::unique_ptr<SeekableInputStream> StripeStreamsImpl::getStream(uint64_t columnId,
                                                                    proto::Stream_Kind kind,
                                                                    bool shouldStream) const {
    uint64_t offset = stripeStart_;
    uint64_t dataEnd =
        stripeInfo_.offset() + stripeInfo_.index_length() + stripeInfo_.data_length();
    MemoryPool* pool = reader_.getFileContents().pool;
    for (int i = 0; i < footer_.streams_size(); ++i) {
      const proto::Stream& stream = footer_.streams(i);
      if (stream.has_kind() && stream.kind() == kind &&
          stream.column() == static_cast<uint64_t>(columnId)) {
        uint64_t streamLength = stream.length();
        if (offset + streamLength > dataEnd) {
          std::stringstream msg;
          msg << "Malformed stream meta at stream index " << i << " in stripe " << stripeIndex_
              << ": streamOffset=" << offset << ", streamLength=" << streamLength
              << ", stripeOffset=" << stripeInfo_.offset()
              << ", stripeIndexLength=" << stripeInfo_.index_length()
              << ", stripeDataLength=" << stripeInfo_.data_length();
          throw ParseError(msg.str());
        }

        BufferSlice slice;
        if (readCache_) {
          ReadRange range{offset, streamLength};
          slice = readCache_->read(range);
        }

        uint64_t myBlock = shouldStream ? input_.getNaturalReadSize() : streamLength;
        std::unique_ptr<SeekableInputStream> seekableInput;
        if (slice.buffer) {
          seekableInput = std::make_unique<SeekableArrayInputStream>(
              slice.buffer->data() + slice.offset, slice.length);
        } else {
          seekableInput = std::make_unique<SeekableFileInputStream>(&input_, offset, streamLength,
                                                                    *pool, myBlock);
        }
        return createDecompressor(reader_.getCompression(), std::move(seekableInput),
                                  reader_.getCompressionSize(), *pool,
                                  reader_.getFileContents().readerMetrics);
      }
      offset += stream.length();
    }
    return nullptr;
  }

  MemoryPool& StripeStreamsImpl::getMemoryPool() const {
    return *reader_.getFileContents().pool;
  }

  ReaderMetrics* StripeStreamsImpl::getReaderMetrics() const {
    return reader_.getFileContents().readerMetrics;
  }

  bool StripeStreamsImpl::getThrowOnHive11DecimalOverflow() const {
    return reader_.getThrowOnHive11DecimalOverflow();
  }

  bool StripeStreamsImpl::isDecimalAsLong() const {
    return reader_.getIsDecimalAsLong();
  }

  int32_t StripeStreamsImpl::getForcedScaleOnHive11Decimal() const {
    return reader_.getForcedScaleOnHive11Decimal();
  }

  const SchemaEvolution* StripeStreamsImpl::getSchemaEvolution() const {
    return reader_.getSchemaEvolution();
  }

  void StripeInformationImpl::ensureStripeFooterLoaded() const {
    if (stripeFooter_.get() == nullptr) {
      std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
          compression_,
          std::make_unique<SeekableFileInputStream>(stream_, offset_ + indexLength_ + dataLength_,
                                                    footerLength_, memory_),
          blockSize_, memory_, metrics_);
      stripeFooter_ = std::make_unique<proto::StripeFooter>();
      if (!stripeFooter_->ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Failed to parse the stripe footer");
      }
    }
  }

  std::unique_ptr<StreamInformation> StripeInformationImpl::getStreamInformation(
      uint64_t streamId) const {
    ensureStripeFooterLoaded();
    uint64_t streamOffset = offset_;
    for (uint64_t s = 0; s < streamId; ++s) {
      streamOffset += stripeFooter_->streams(static_cast<int>(s)).length();
    }
    return std::make_unique<StreamInformationImpl>(
        streamOffset, stripeFooter_->streams(static_cast<int>(streamId)));
  }

}  // namespace orc
