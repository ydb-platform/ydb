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

#include "orc/Common.hh"
#include "orc/OrcFile.hh"

#include "ColumnWriter.hh"
#include "Timezone.hh"
#include "Utils.hh"

#include <memory>
#include <stdexcept>

namespace orc {

  struct WriterOptionsPrivate {
    uint64_t stripeSize;
    uint64_t compressionBlockSize;
    uint64_t rowIndexStride;
    CompressionKind compression;
    CompressionStrategy compressionStrategy;
    MemoryPool* memoryPool;
    double paddingTolerance;
    std::ostream* errorStream;
    FileVersion fileVersion;
    double dictionaryKeySizeThreshold;
    bool enableIndex;
    std::set<uint64_t> columnsUseBloomFilter;
    double bloomFilterFalsePositiveProb;
    BloomFilterVersion bloomFilterVersion;
    std::string timezone;
    WriterMetrics* metrics;
    bool useTightNumericVector;
    uint64_t outputBufferCapacity;
    uint64_t memoryBlockSize;
    bool alignBlockBoundToRowGroup;

    WriterOptionsPrivate() : fileVersion(FileVersion::v_0_12()) {  // default to Hive_0_12
      stripeSize = 64 * 1024 * 1024;                               // 64M
      compressionBlockSize = 64 * 1024;                            // 64K
      rowIndexStride = 10000;
      compression = CompressionKind_ZSTD;
      compressionStrategy = CompressionStrategy_SPEED;
      memoryPool = getDefaultPool();
      paddingTolerance = 0.0;
      errorStream = &std::cerr;
      dictionaryKeySizeThreshold = 0.0;
      enableIndex = true;
      bloomFilterFalsePositiveProb = 0.01;
      bloomFilterVersion = UTF8;
      // Writer timezone uses "GMT" by default to get rid of potential issues
      // introduced by moving timestamps between different timezones.
      // Explictly set the writer timezone if the use case depends on it.
      timezone = "GMT";
      metrics = nullptr;
      useTightNumericVector = false;
      outputBufferCapacity = 1024 * 1024;
      memoryBlockSize = 64 * 1024;  // 64K
      alignBlockBoundToRowGroup = false;
    }
  };

  WriterOptions::WriterOptions()
      : privateBits_(std::unique_ptr<WriterOptionsPrivate>(new WriterOptionsPrivate())) {
    // PASS
  }

  WriterOptions::WriterOptions(const WriterOptions& rhs)
      : privateBits_(std::unique_ptr<WriterOptionsPrivate>(
            new WriterOptionsPrivate(*(rhs.privateBits_.get())))) {
    // PASS
  }

  WriterOptions::WriterOptions(WriterOptions& rhs) {
    // swap privateBits with rhs
    privateBits_.swap(rhs.privateBits_);
  }

  WriterOptions& WriterOptions::operator=(const WriterOptions& rhs) {
    if (this != &rhs) {
      privateBits_.reset(new WriterOptionsPrivate(*(rhs.privateBits_.get())));
    }
    return *this;
  }

  WriterOptions::~WriterOptions() {
    // PASS
  }
  RleVersion WriterOptions::getRleVersion() const {
    if (privateBits_->fileVersion == FileVersion::v_0_11()) {
      return RleVersion_1;
    }

    return RleVersion_2;
  }

  WriterOptions& WriterOptions::setStripeSize(uint64_t size) {
    privateBits_->stripeSize = size;
    return *this;
  }

  uint64_t WriterOptions::getStripeSize() const {
    return privateBits_->stripeSize;
  }

  WriterOptions& WriterOptions::setCompressionBlockSize(uint64_t size) {
    if (size >= (1 << 23)) {
      throw std::invalid_argument("Compression block size cannot be greater or equal than 8M");
    }
    privateBits_->compressionBlockSize = size;
    return *this;
  }

  uint64_t WriterOptions::getCompressionBlockSize() const {
    return privateBits_->compressionBlockSize;
  }

  WriterOptions& WriterOptions::setRowIndexStride(uint64_t stride) {
    privateBits_->rowIndexStride = stride;
    privateBits_->enableIndex = (stride != 0);
    return *this;
  }

  uint64_t WriterOptions::getRowIndexStride() const {
    return privateBits_->rowIndexStride;
  }

  WriterOptions& WriterOptions::setDictionaryKeySizeThreshold(double val) {
    privateBits_->dictionaryKeySizeThreshold = val;
    return *this;
  }

  double WriterOptions::getDictionaryKeySizeThreshold() const {
    return privateBits_->dictionaryKeySizeThreshold;
  }

  WriterOptions& WriterOptions::setFileVersion(const FileVersion& version) {
    // Only Hive_0_11 and Hive_0_12 version are supported currently
    if (version.getMajor() == 0 && (version.getMinor() == 11 || version.getMinor() == 12)) {
      privateBits_->fileVersion = version;
      return *this;
    }
    if (version == FileVersion::UNSTABLE_PRE_2_0()) {
      *privateBits_->errorStream << "Warning: ORC files written in "
                                 << FileVersion::UNSTABLE_PRE_2_0().toString()
                                 << " will not be readable by other versions of the software."
                                 << " It is only for developer testing.\n";
      privateBits_->fileVersion = version;
      return *this;
    }
    throw std::logic_error("Unsupported file version specified.");
  }

  FileVersion WriterOptions::getFileVersion() const {
    return privateBits_->fileVersion;
  }

  WriterOptions& WriterOptions::setCompression(CompressionKind comp) {
    privateBits_->compression = comp;
    return *this;
  }

  CompressionKind WriterOptions::getCompression() const {
    return privateBits_->compression;
  }

  WriterOptions& WriterOptions::setCompressionStrategy(CompressionStrategy strategy) {
    privateBits_->compressionStrategy = strategy;
    return *this;
  }

  CompressionStrategy WriterOptions::getCompressionStrategy() const {
    return privateBits_->compressionStrategy;
  }

  bool WriterOptions::getAlignedBitpacking() const {
    return privateBits_->compressionStrategy == CompressionStrategy ::CompressionStrategy_SPEED;
  }

  WriterOptions& WriterOptions::setPaddingTolerance(double tolerance) {
    privateBits_->paddingTolerance = tolerance;
    return *this;
  }

  double WriterOptions::getPaddingTolerance() const {
    return privateBits_->paddingTolerance;
  }

  WriterOptions& WriterOptions::setMemoryPool(MemoryPool* memoryPool) {
    privateBits_->memoryPool = memoryPool;
    return *this;
  }

  MemoryPool* WriterOptions::getMemoryPool() const {
    return privateBits_->memoryPool;
  }

  WriterOptions& WriterOptions::setErrorStream(std::ostream& errStream) {
    privateBits_->errorStream = &errStream;
    return *this;
  }

  std::ostream* WriterOptions::getErrorStream() const {
    return privateBits_->errorStream;
  }

  bool WriterOptions::getEnableIndex() const {
    return privateBits_->enableIndex;
  }

  bool WriterOptions::getEnableDictionary() const {
    return privateBits_->dictionaryKeySizeThreshold > 0.0;
  }

  WriterOptions& WriterOptions::setColumnsUseBloomFilter(const std::set<uint64_t>& columns) {
    privateBits_->columnsUseBloomFilter = columns;
    return *this;
  }

  bool WriterOptions::isColumnUseBloomFilter(uint64_t column) const {
    return privateBits_->columnsUseBloomFilter.find(column) !=
           privateBits_->columnsUseBloomFilter.end();
  }

  WriterOptions& WriterOptions::setBloomFilterFPP(double fpp) {
    privateBits_->bloomFilterFalsePositiveProb = fpp;
    return *this;
  }

  double WriterOptions::getBloomFilterFPP() const {
    return privateBits_->bloomFilterFalsePositiveProb;
  }

  // delibrately not provide setter to write bloom filter version because
  // we only support UTF8 for now.
  BloomFilterVersion WriterOptions::getBloomFilterVersion() const {
    return privateBits_->bloomFilterVersion;
  }

  const Timezone& WriterOptions::getTimezone() const {
    return getTimezoneByName(privateBits_->timezone);
  }

  const std::string& WriterOptions::getTimezoneName() const {
    return privateBits_->timezone;
  }

  WriterOptions& WriterOptions::setTimezoneName(const std::string& zone) {
    privateBits_->timezone = zone;
    return *this;
  }

  WriterMetrics* WriterOptions::getWriterMetrics() const {
    return privateBits_->metrics;
  }

  WriterOptions& WriterOptions::setWriterMetrics(WriterMetrics* metrics) {
    privateBits_->metrics = metrics;
    return *this;
  }

  WriterOptions& WriterOptions::setUseTightNumericVector(bool useTightNumericVector) {
    privateBits_->useTightNumericVector = useTightNumericVector;
    return *this;
  }

  bool WriterOptions::getUseTightNumericVector() const {
    return privateBits_->useTightNumericVector;
  }

  WriterOptions& WriterOptions::setOutputBufferCapacity(uint64_t capacity) {
    privateBits_->outputBufferCapacity = capacity;
    return *this;
  }

  uint64_t WriterOptions::getOutputBufferCapacity() const {
    return privateBits_->outputBufferCapacity;
  }

  WriterOptions& WriterOptions::setMemoryBlockSize(uint64_t capacity) {
    privateBits_->memoryBlockSize = capacity;
    return *this;
  }

  uint64_t WriterOptions::getMemoryBlockSize() const {
    return privateBits_->memoryBlockSize;
  }

  WriterOptions& WriterOptions::setAlignBlockBoundToRowGroup(bool alignBlockBoundToRowGroup) {
    privateBits_->alignBlockBoundToRowGroup = alignBlockBoundToRowGroup;
    return *this;
  }

  bool WriterOptions::getAlignBlockBoundToRowGroup() const {
    return privateBits_->alignBlockBoundToRowGroup;
  }

  Writer::~Writer() {
    // PASS
  }

  class WriterImpl : public Writer {
   private:
    std::unique_ptr<ColumnWriter> columnWriter_;
    std::unique_ptr<BufferedOutputStream> compressionStream_;
    std::unique_ptr<BufferedOutputStream> bufferedStream_;
    std::unique_ptr<StreamsFactory> streamsFactory_;
    OutputStream* outStream_;
    WriterOptions options_;
    const Type& type_;
    uint64_t stripeRows_, totalRows_, indexRows_;
    uint64_t currentOffset_;
    proto::Footer fileFooter_;
    proto::PostScript postScript_;
    proto::StripeInformation stripeInfo_;
    proto::Metadata metadata_;

    static const char* magicId;
    static const WriterId writerId;
    bool useTightNumericVector_;
    int32_t stripesAtLastFlush_;
    uint64_t lastFlushOffset_;

   public:
    WriterImpl(const Type& type, OutputStream* stream, const WriterOptions& options);

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size) const override;

    void add(ColumnVectorBatch& rowsToAdd) override;

    void close() override;

    void addUserMetadata(const std::string& name, const std::string& value) override;

    uint64_t writeIntermediateFooter() override;

   private:
    void init();
    void initStripe();
    void writeStripe();
    void writeMetadata();
    void writeFileFooter();
    void writePostscript();
    void buildFooterType(const Type& t, proto::Footer& footer, uint32_t& index);
    static proto::CompressionKind convertCompressionKind(const CompressionKind& kind);
  };

  const char* WriterImpl::magicId = "ORC";

  const WriterId WriterImpl::writerId = WriterId::ORC_CPP_WRITER;

  WriterImpl::WriterImpl(const Type& t, OutputStream* stream, const WriterOptions& opts)
      : outStream_(stream), options_(opts), type_(t) {
    streamsFactory_ = createStreamsFactory(options_, outStream_);
    columnWriter_ = buildWriter(type_, *streamsFactory_, options_);
    stripeRows_ = totalRows_ = indexRows_ = 0;
    currentOffset_ = 0;
    stripesAtLastFlush_ = 0;
    lastFlushOffset_ = 0;

    useTightNumericVector_ = opts.getUseTightNumericVector();

    if (options_.getCompressionBlockSize() % options_.getMemoryBlockSize() != 0) {
      throw std::invalid_argument(
          "Compression block size must be a multiple of memory block size.");
    }

    // compression stream for stripe footer, file footer and metadata
    compressionStream_ = createCompressor(
        options_.getCompression(), outStream_, options_.getCompressionStrategy(),
        options_.getOutputBufferCapacity(), options_.getCompressionBlockSize(),
        options_.getMemoryBlockSize(), *options_.getMemoryPool(), options_.getWriterMetrics());

    // uncompressed stream for post script
    bufferedStream_.reset(new BufferedOutputStream(*options_.getMemoryPool(), outStream_,
                                                   1024,  // buffer capacity: 1024 bytes
                                                   options_.getCompressionBlockSize(),
                                                   options_.getWriterMetrics()));

    init();
  }

  std::unique_ptr<ColumnVectorBatch> WriterImpl::createRowBatch(uint64_t size) const {
    return type_.createRowBatch(size, *options_.getMemoryPool(), false, useTightNumericVector_);
  }

  void WriterImpl::add(ColumnVectorBatch& rowsToAdd) {
    if (options_.getEnableIndex()) {
      uint64_t pos = 0;
      uint64_t chunkSize = 0;
      uint64_t rowIndexStride = options_.getRowIndexStride();
      while (pos < rowsToAdd.numElements) {
        chunkSize = std::min(rowsToAdd.numElements - pos, rowIndexStride - indexRows_);
        columnWriter_->add(rowsToAdd, pos, chunkSize, nullptr);

        pos += chunkSize;
        indexRows_ += chunkSize;
        stripeRows_ += chunkSize;

        if (indexRows_ >= rowIndexStride) {
          if (options_.getAlignBlockBoundToRowGroup()) {
            columnWriter_->finishStreams();
          }
          columnWriter_->createRowIndexEntry();
          indexRows_ = 0;
        }
      }
    } else {
      stripeRows_ += rowsToAdd.numElements;
      columnWriter_->add(rowsToAdd, 0, rowsToAdd.numElements, nullptr);
    }

    if (columnWriter_->getEstimatedSize() >= options_.getStripeSize()) {
      writeStripe();
    }
  }

  void WriterImpl::close() {
    if (stripeRows_ > 0) {
      writeStripe();
    }
    writeMetadata();
    writeFileFooter();
    writePostscript();
    outStream_->close();
  }

  uint64_t WriterImpl::writeIntermediateFooter() {
    if (stripeRows_ > 0) {
      writeStripe();
    }
    if (stripesAtLastFlush_ != fileFooter_.stripes_size()) {
      writeMetadata();
      writeFileFooter();
      writePostscript();
      stripesAtLastFlush_ = fileFooter_.stripes_size();
      outStream_->flush();
      lastFlushOffset_ = outStream_->getLength();
      currentOffset_ = lastFlushOffset_;
      // init stripe now that we adjusted the currentOffset
      initStripe();
    }
    return lastFlushOffset_;
  }

  void WriterImpl::addUserMetadata(const std::string& name, const std::string& value) {
    proto::UserMetadataItem* userMetadataItem = fileFooter_.add_metadata();
    userMetadataItem->set_name(name);
    userMetadataItem->set_value(value);
  }

  void WriterImpl::init() {
    // Write file header
    const static size_t magicIdLength = strlen(WriterImpl::magicId);
    {
      SCOPED_STOPWATCH(options_.getWriterMetrics(), IOBlockingLatencyUs, IOCount);
      outStream_->write(WriterImpl::magicId, magicIdLength);
    }
    currentOffset_ += magicIdLength;

    // Initialize file footer
    fileFooter_.set_header_length(currentOffset_);
    fileFooter_.set_content_length(0);
    fileFooter_.set_number_of_rows(0);
    fileFooter_.set_row_index_stride(static_cast<uint32_t>(options_.getRowIndexStride()));
    fileFooter_.set_writer(writerId);
    fileFooter_.set_software_version(ORC_VERSION);

    uint32_t index = 0;
    buildFooterType(type_, fileFooter_, index);

    // Initialize post script
    postScript_.set_footer_length(0);
    postScript_.set_compression(WriterImpl::convertCompressionKind(options_.getCompression()));
    postScript_.set_compression_block_size(options_.getCompressionBlockSize());

    postScript_.add_version(options_.getFileVersion().getMajor());
    postScript_.add_version(options_.getFileVersion().getMinor());

    postScript_.set_writer_version(WriterVersion_ORC_135);
    postScript_.set_magic("ORC");

    // Initialize first stripe
    initStripe();
  }

  void WriterImpl::initStripe() {
    stripeInfo_.set_offset(currentOffset_);
    stripeInfo_.set_index_length(0);
    stripeInfo_.set_data_length(0);
    stripeInfo_.set_footer_length(0);
    stripeInfo_.set_number_of_rows(0);

    stripeRows_ = indexRows_ = 0;
  }

  void WriterImpl::writeStripe() {
    if (options_.getEnableIndex() && indexRows_ != 0) {
      columnWriter_->createRowIndexEntry();
      indexRows_ = 0;
    } else {
      columnWriter_->mergeRowGroupStatsIntoStripeStats();
    }

    // dictionary should be written before any stream is flushed
    columnWriter_->writeDictionary();

    std::vector<proto::Stream> streams;
    // write ROW_INDEX streams
    if (options_.getEnableIndex()) {
      columnWriter_->writeIndex(streams);
    }
    // write streams like PRESENT, DATA, etc.
    columnWriter_->flush(streams);

    // generate and write stripe footer
    proto::StripeFooter stripeFooter;
    for (uint32_t i = 0; i < streams.size(); ++i) {
      *stripeFooter.add_streams() = streams[i];
    }

    std::vector<proto::ColumnEncoding> encodings;
    columnWriter_->getColumnEncoding(encodings);

    for (uint32_t i = 0; i < encodings.size(); ++i) {
      *stripeFooter.add_columns() = encodings[i];
    }

    stripeFooter.set_writer_timezone(options_.getTimezoneName());

    // add stripe statistics to metadata
    proto::StripeStatistics* stripeStats = metadata_.add_stripe_stats();
    std::vector<proto::ColumnStatistics> colStats;
    columnWriter_->getStripeStatistics(colStats);
    for (uint32_t i = 0; i != colStats.size(); ++i) {
      *stripeStats->add_col_stats() = colStats[i];
    }
    // merge stripe stats into file stats and clear stripe stats
    columnWriter_->mergeStripeStatsIntoFileStats();

    if (!stripeFooter.SerializeToZeroCopyStream(compressionStream_.get())) {
      throw std::logic_error("Failed to write stripe footer.");
    }
    uint64_t footerLength = compressionStream_->flush();

    // calculate data length and index length
    uint64_t dataLength = 0;
    uint64_t indexLength = 0;
    for (uint32_t i = 0; i < streams.size(); ++i) {
      if (streams[i].kind() == proto::Stream_Kind_ROW_INDEX ||
          streams[i].kind() == proto::Stream_Kind_BLOOM_FILTER_UTF8) {
        indexLength += streams[i].length();
      } else {
        dataLength += streams[i].length();
      }
    }

    // update stripe info
    stripeInfo_.set_index_length(indexLength);
    stripeInfo_.set_data_length(dataLength);
    stripeInfo_.set_footer_length(footerLength);
    stripeInfo_.set_number_of_rows(stripeRows_);

    *fileFooter_.add_stripes() = stripeInfo_;

    currentOffset_ = currentOffset_ + indexLength + dataLength + footerLength;
    totalRows_ += stripeRows_;

    columnWriter_->reset();

    initStripe();
  }

  void WriterImpl::writeMetadata() {
    if (!metadata_.SerializeToZeroCopyStream(compressionStream_.get())) {
      throw std::logic_error("Failed to write metadata.");
    }
    postScript_.set_metadata_length(compressionStream_.get()->flush());
  }

  void WriterImpl::writeFileFooter() {
    fileFooter_.set_content_length(currentOffset_ - fileFooter_.header_length());
    fileFooter_.set_number_of_rows(totalRows_);

    // update file statistics
    std::vector<proto::ColumnStatistics> colStats;
    columnWriter_->getFileStatistics(colStats);
    fileFooter_.clear_statistics();
    for (uint32_t i = 0; i != colStats.size(); ++i) {
      *fileFooter_.add_statistics() = colStats[i];
    }

    if (!fileFooter_.SerializeToZeroCopyStream(compressionStream_.get())) {
      throw std::logic_error("Failed to write file footer.");
    }
    postScript_.set_footer_length(compressionStream_->flush());
  }

  void WriterImpl::writePostscript() {
    if (!postScript_.SerializeToZeroCopyStream(bufferedStream_.get())) {
      throw std::logic_error("Failed to write post script.");
    }
    unsigned char psLength = static_cast<unsigned char>(bufferedStream_->flush());
    SCOPED_STOPWATCH(options_.getWriterMetrics(), IOBlockingLatencyUs, IOCount);
    outStream_->write(&psLength, sizeof(unsigned char));
  }

  void WriterImpl::buildFooterType(const Type& t, proto::Footer& footer, uint32_t& index) {
    proto::Type protoType;
    protoType.set_maximum_length(static_cast<uint32_t>(t.getMaximumLength()));
    protoType.set_precision(static_cast<uint32_t>(t.getPrecision()));
    protoType.set_scale(static_cast<uint32_t>(t.getScale()));

    switch (t.getKind()) {
      case BOOLEAN: {
        protoType.set_kind(proto::Type_Kind_BOOLEAN);
        break;
      }
      case BYTE: {
        protoType.set_kind(proto::Type_Kind_BYTE);
        break;
      }
      case SHORT: {
        protoType.set_kind(proto::Type_Kind_SHORT);
        break;
      }
      case INT: {
        protoType.set_kind(proto::Type_Kind_INT);
        break;
      }
      case LONG: {
        protoType.set_kind(proto::Type_Kind_LONG);
        break;
      }
      case FLOAT: {
        protoType.set_kind(proto::Type_Kind_FLOAT);
        break;
      }
      case DOUBLE: {
        protoType.set_kind(proto::Type_Kind_DOUBLE);
        break;
      }
      case STRING: {
        protoType.set_kind(proto::Type_Kind_STRING);
        break;
      }
      case BINARY: {
        protoType.set_kind(proto::Type_Kind_BINARY);
        break;
      }
      case TIMESTAMP: {
        protoType.set_kind(proto::Type_Kind_TIMESTAMP);
        break;
      }
      case TIMESTAMP_INSTANT: {
        protoType.set_kind(proto::Type_Kind_TIMESTAMP_INSTANT);
        break;
      }
      case LIST: {
        protoType.set_kind(proto::Type_Kind_LIST);
        break;
      }
      case MAP: {
        protoType.set_kind(proto::Type_Kind_MAP);
        break;
      }
      case STRUCT: {
        protoType.set_kind(proto::Type_Kind_STRUCT);
        break;
      }
      case UNION: {
        protoType.set_kind(proto::Type_Kind_UNION);
        break;
      }
      case DECIMAL: {
        protoType.set_kind(proto::Type_Kind_DECIMAL);
        break;
      }
      case DATE: {
        protoType.set_kind(proto::Type_Kind_DATE);
        break;
      }
      case VARCHAR: {
        protoType.set_kind(proto::Type_Kind_VARCHAR);
        break;
      }
      case CHAR: {
        protoType.set_kind(proto::Type_Kind_CHAR);
        break;
      }
      case GEOMETRY: {
        protoType.set_kind(proto::Type_Kind_GEOMETRY);
        protoType.set_crs(t.getCrs());
        break;
      }
      case GEOGRAPHY: {
        protoType.set_kind(proto::Type_Kind_GEOGRAPHY);
        protoType.set_crs(t.getCrs());
        switch (t.getAlgorithm()) {
          case geospatial::EdgeInterpolationAlgorithm::SPHERICAL: {
            protoType.set_algorithm(proto::Type_EdgeInterpolationAlgorithm_SPHERICAL);
            break;
          }
          case orc::geospatial::EdgeInterpolationAlgorithm::VINCENTY: {
            protoType.set_algorithm(proto::Type_EdgeInterpolationAlgorithm_VINCENTY);
            break;
          }
          case orc::geospatial::EdgeInterpolationAlgorithm::THOMAS: {
            protoType.set_algorithm(proto::Type_EdgeInterpolationAlgorithm_VINCENTY);
            break;
          }
          case orc::geospatial::EdgeInterpolationAlgorithm::ANDOYER: {
            protoType.set_algorithm(proto::Type_EdgeInterpolationAlgorithm_ANDOYER);
            break;
          }
          case orc::geospatial::EdgeInterpolationAlgorithm::KARNEY: {
            protoType.set_algorithm(proto::Type_EdgeInterpolationAlgorithm_KARNEY);
            break;
          }
          default:
            throw std::invalid_argument("Unknown Algorithm.");
        }
        break;
      }
      default:
        throw std::logic_error("Unknown type.");
    }

    for (auto& key : t.getAttributeKeys()) {
      const auto& value = t.getAttributeValue(key);
      auto protoAttr = protoType.add_attributes();
      protoAttr->set_key(key);
      protoAttr->set_value(value);
    }

    int pos = static_cast<int>(index);
    *footer.add_types() = protoType;

    for (uint64_t i = 0; i < t.getSubtypeCount(); ++i) {
      // only add subtypes' field names if this type is STRUCT
      if (t.getKind() == STRUCT) {
        footer.mutable_types(pos)->add_field_names(t.getFieldName(i));
      }
      footer.mutable_types(pos)->add_subtypes(++index);
      buildFooterType(*t.getSubtype(i), footer, index);
    }
  }

  proto::CompressionKind WriterImpl::convertCompressionKind(const CompressionKind& kind) {
    return static_cast<proto::CompressionKind>(kind);
  }

  std::unique_ptr<Writer> createWriter(const Type& type, OutputStream* stream,
                                       const WriterOptions& options) {
    return std::unique_ptr<Writer>(new WriterImpl(type, stream, options));
  }

}  // namespace orc
