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

#ifndef ORC_STRIPE_STREAM_HH
#define ORC_STRIPE_STREAM_HH

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "ColumnReader.hh"
#include "Timezone.hh"
#include "TypeImpl.hh"

namespace orc {

  class RowReaderImpl;
  class ReadRangeCache;

  /**
   * StripeStream Implementation
   */

  class StripeStreamsImpl : public StripeStreams {
   private:
    const RowReaderImpl& reader_;
    const proto::StripeInformation& stripeInfo_;
    const proto::StripeFooter& footer_;
    const uint64_t stripeIndex_;
    const uint64_t stripeStart_;
    InputStream& input_;
    const Timezone& writerTimezone_;
    const Timezone& readerTimezone_;
    std::shared_ptr<ReadRangeCache> readCache_;

   public:
    StripeStreamsImpl(const RowReaderImpl& reader, uint64_t index,
                      const proto::StripeInformation& stripeInfo, const proto::StripeFooter& footer,
                      uint64_t stripeStart, InputStream& input, const Timezone& writerTimezone,
                      const Timezone& readerTimezone);

    virtual ~StripeStreamsImpl() override;

    virtual const std::vector<bool> getSelectedColumns() const override;

    virtual proto::ColumnEncoding getEncoding(uint64_t columnId) const override;

    virtual std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId,
                                                           proto::Stream_Kind kind,
                                                           bool shouldStream) const override;

    MemoryPool& getMemoryPool() const override;

    ReaderMetrics* getReaderMetrics() const override;

    const Timezone& getWriterTimezone() const override;

    const Timezone& getReaderTimezone() const override;

    std::ostream* getErrorStream() const override;

    bool getThrowOnHive11DecimalOverflow() const override;

    bool isDecimalAsLong() const override;

    int32_t getForcedScaleOnHive11Decimal() const override;

    const SchemaEvolution* getSchemaEvolution() const override;
  };

  /**
   * StreamInformation Implementation
   */

  class StreamInformationImpl : public StreamInformation {
   private:
    StreamKind kind_;
    uint64_t column_;
    uint64_t offset_;
    uint64_t length_;

   public:
    StreamInformationImpl(uint64_t offset, const proto::Stream& stream)
        : kind_(static_cast<StreamKind>(stream.kind())),
          column_(stream.column()),
          offset_(offset),
          length_(stream.length()) {
      // PASS
    }

    ~StreamInformationImpl() override;

    StreamKind getKind() const override {
      return kind_;
    }

    uint64_t getColumnId() const override {
      return column_;
    }

    uint64_t getOffset() const override {
      return offset_;
    }

    uint64_t getLength() const override {
      return length_;
    }
  };

  /**
   * StripeInformation Implementation
   */

  class StripeInformationImpl : public StripeInformation {
    uint64_t offset_;
    uint64_t indexLength_;
    uint64_t dataLength_;
    uint64_t footerLength_;
    uint64_t numRows_;
    InputStream* stream_;
    MemoryPool& memory_;
    CompressionKind compression_;
    uint64_t blockSize_;
    mutable std::unique_ptr<proto::StripeFooter> stripeFooter_;
    ReaderMetrics* metrics_;
    void ensureStripeFooterLoaded() const;

   public:
    StripeInformationImpl(uint64_t offset, uint64_t indexLength, uint64_t dataLength,
                          uint64_t footerLength, uint64_t numRows, InputStream* stream,
                          MemoryPool& memory, CompressionKind compression, uint64_t blockSize,
                          ReaderMetrics* metrics)
        : offset_(offset),
          indexLength_(indexLength),
          dataLength_(dataLength),
          footerLength_(footerLength),
          numRows_(numRows),
          stream_(stream),
          memory_(memory),
          compression_(compression),
          blockSize_(blockSize),
          metrics_(metrics) {
      // PASS
    }

    virtual ~StripeInformationImpl() override {
      // PASS
    }

    uint64_t getOffset() const override {
      return offset_;
    }

    uint64_t getLength() const override {
      return indexLength_ + dataLength_ + footerLength_;
    }
    uint64_t getIndexLength() const override {
      return indexLength_;
    }

    uint64_t getDataLength() const override {
      return dataLength_;
    }

    uint64_t getFooterLength() const override {
      return footerLength_;
    }

    uint64_t getNumberOfRows() const override {
      return numRows_;
    }

    uint64_t getNumberOfStreams() const override {
      ensureStripeFooterLoaded();
      return static_cast<uint64_t>(stripeFooter_->streams_size());
    }

    std::unique_ptr<StreamInformation> getStreamInformation(uint64_t streamId) const override;

    ColumnEncodingKind getColumnEncoding(uint64_t colId) const override {
      ensureStripeFooterLoaded();
      return static_cast<ColumnEncodingKind>(
          stripeFooter_->columns(static_cast<int>(colId)).kind());
    }

    uint64_t getDictionarySize(uint64_t colId) const override {
      ensureStripeFooterLoaded();
      return static_cast<ColumnEncodingKind>(
          stripeFooter_->columns(static_cast<int>(colId)).dictionary_size());
    }

    const std::string& getWriterTimezone() const override {
      ensureStripeFooterLoaded();
      return stripeFooter_->writer_timezone();
    }
  };

}  // namespace orc

#endif
