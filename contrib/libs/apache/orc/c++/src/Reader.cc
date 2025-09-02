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

#include "Reader.hh"
#include "Adaptor.hh"
#include "BloomFilter.hh"
#include "Options.hh"
#include "Statistics.hh"
#include "StripeStream.hh"
#include "Utils.hh"

#include "wrap/coded-stream-wrapper.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace orc {
  // ORC files writen by these versions of cpp writers have inconsistent bloom filter
  // hashing. Bloom filters of them should not be used.
  static const char* BAD_CPP_BLOOM_FILTER_VERSIONS[] = {
      "1.6.0", "1.6.1", "1.6.2", "1.6.3",  "1.6.4",  "1.6.5", "1.6.6",
      "1.6.7", "1.6.8", "1.6.9", "1.6.10", "1.6.11", "1.7.0"};

  ReaderMetrics* getDefaultReaderMetrics() {
    static ReaderMetrics internal;
    return &internal;
  }

  const RowReaderOptions::IdReadIntentMap EMPTY_IDREADINTENTMAP() {
    return {};
  }

  const WriterVersionImpl& WriterVersionImpl::VERSION_HIVE_8732() {
    static const WriterVersionImpl version(WriterVersion_HIVE_8732);
    return version;
  }

  uint64_t getCompressionBlockSize(const proto::PostScript& ps) {
    if (ps.has_compression_block_size()) {
      return ps.compression_block_size();
    } else {
      return 256 * 1024;
    }
  }

  CompressionKind convertCompressionKind(const proto::PostScript& ps) {
    if (ps.has_compression()) {
      return static_cast<CompressionKind>(ps.compression());
    } else {
      throw ParseError("Unknown compression type");
    }
  }

  std::string ColumnSelector::toDotColumnPath() {
    if (columns_.empty()) {
      return std::string();
    }
    std::ostringstream columnStream;
    std::copy(columns_.begin(), columns_.end(),
              std::ostream_iterator<std::string>(columnStream, "."));
    std::string columnPath = columnStream.str();
    return columnPath.substr(0, columnPath.length() - 1);
  }

  WriterVersion getWriterVersionImpl(const FileContents* contents) {
    if (!contents->postscript->has_writer_version()) {
      return WriterVersion_ORIGINAL;
    }
    return static_cast<WriterVersion>(contents->postscript->writer_version());
  }

  void ColumnSelector::selectChildren(std::vector<bool>& selectedColumns, const Type& type) {
    return selectChildren(selectedColumns, type, EMPTY_IDREADINTENTMAP());
  }

  void ColumnSelector::selectChildren(std::vector<bool>& selectedColumns, const Type& type,
                                      const RowReaderOptions::IdReadIntentMap& idReadIntentMap) {
    size_t id = static_cast<size_t>(type.getColumnId());
    TypeKind kind = type.getKind();
    if (!selectedColumns[id]) {
      selectedColumns[id] = true;
      bool selectChild = true;
      if (kind == TypeKind::LIST || kind == TypeKind::MAP || kind == TypeKind::UNION) {
        auto elem = idReadIntentMap.find(id);
        if (elem != idReadIntentMap.end() && elem->second == ReadIntent_OFFSETS) {
          selectChild = false;
        }
      }

      if (selectChild) {
        for (size_t c = id; c <= type.getMaximumColumnId(); ++c) {
          selectedColumns[c] = true;
        }
      }
    }
  }

  /**
   * Recurses over a type tree and selects the parents of every selected type.
   * @return true if any child was selected.
   */
  bool ColumnSelector::selectParents(std::vector<bool>& selectedColumns, const Type& type) {
    size_t id = static_cast<size_t>(type.getColumnId());
    bool result = selectedColumns[id];
    uint64_t numSubtypeSelected = 0;
    for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
      if (selectParents(selectedColumns, *type.getSubtype(c))) {
        result = true;
        numSubtypeSelected++;
      }
    }
    selectedColumns[id] = result;

    if (type.getKind() == TypeKind::UNION && selectedColumns[id]) {
      if (0 < numSubtypeSelected && numSubtypeSelected < type.getSubtypeCount()) {
        // Subtypes of UNION should be fully selected or not selected at all.
        // Override partial subtype selections with full selections.
        for (uint64_t c = 0; c < type.getSubtypeCount(); ++c) {
          selectChildren(selectedColumns, *type.getSubtype(c));
        }
      }
    }
    return result;
  }

  /**
   * Recurses over a type tree and build two maps
   * map<TypeName, TypeId>, map<TypeId, Type>
   */
  void ColumnSelector::buildTypeNameIdMap(const Type* type) {
    // map<type_id, Type*>
    idTypeMap_[type->getColumnId()] = type;

    if (STRUCT == type->getKind()) {
      for (size_t i = 0; i < type->getSubtypeCount(); ++i) {
        const std::string& fieldName = type->getFieldName(i);
        columns_.push_back(fieldName);
        nameIdMap_[toDotColumnPath()] = type->getSubtype(i)->getColumnId();
        buildTypeNameIdMap(type->getSubtype(i));
        columns_.pop_back();
      }
    } else {
      // other non-primitive type
      for (size_t j = 0; j < type->getSubtypeCount(); ++j) {
        buildTypeNameIdMap(type->getSubtype(j));
      }
    }
  }

  void ColumnSelector::updateSelected(std::vector<bool>& selectedColumns,
                                      const RowReaderOptions& options) {
    selectedColumns.assign(static_cast<size_t>(contents_->footer->types_size()), false);
    if (contents_->schema->getKind() == STRUCT && options.getIndexesSet()) {
      for (std::list<uint64_t>::const_iterator field = options.getInclude().begin();
           field != options.getInclude().end(); ++field) {
        updateSelectedByFieldId(selectedColumns, *field);
      }
    } else if (contents_->schema->getKind() == STRUCT && options.getNamesSet()) {
      for (std::list<std::string>::const_iterator field = options.getIncludeNames().begin();
           field != options.getIncludeNames().end(); ++field) {
        updateSelectedByName(selectedColumns, *field);
      }
    } else if (options.getTypeIdsSet()) {
      const RowReaderOptions::IdReadIntentMap idReadIntentMap = options.getIdReadIntentMap();
      for (std::list<uint64_t>::const_iterator typeId = options.getInclude().begin();
           typeId != options.getInclude().end(); ++typeId) {
        updateSelectedByTypeId(selectedColumns, *typeId, idReadIntentMap);
      }
    } else {
      // default is to select all columns
      std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    selectParents(selectedColumns, *contents_->schema.get());
    selectedColumns[0] = true;  // column 0 is selected by default
  }

  void ColumnSelector::updateSelectedByFieldId(std::vector<bool>& selectedColumns,
                                               uint64_t fieldId) {
    if (fieldId < contents_->schema->getSubtypeCount()) {
      selectChildren(selectedColumns, *contents_->schema->getSubtype(fieldId));
    } else {
      std::stringstream buffer;
      buffer << "Invalid column selected " << fieldId << " out of "
             << contents_->schema->getSubtypeCount();
      throw ParseError(buffer.str());
    }
  }

  void ColumnSelector::updateSelectedByTypeId(std::vector<bool>& selectedColumns, uint64_t typeId) {
    updateSelectedByTypeId(selectedColumns, typeId, EMPTY_IDREADINTENTMAP());
  }

  void ColumnSelector::updateSelectedByTypeId(
      std::vector<bool>& selectedColumns, uint64_t typeId,
      const RowReaderOptions::IdReadIntentMap& idReadIntentMap) {
    if (typeId < selectedColumns.size()) {
      const Type& type = *idTypeMap_[typeId];
      selectChildren(selectedColumns, type, idReadIntentMap);
    } else {
      std::stringstream buffer;
      buffer << "Invalid type id selected " << typeId << " out of " << selectedColumns.size();
      throw ParseError(buffer.str());
    }
  }

  void ColumnSelector::updateSelectedByName(std::vector<bool>& selectedColumns,
                                            const std::string& fieldName) {
    std::map<std::string, uint64_t>::const_iterator ite = nameIdMap_.find(fieldName);
    if (ite != nameIdMap_.end()) {
      updateSelectedByTypeId(selectedColumns, ite->second);
    } else {
      bool first = true;
      std::ostringstream ss;
      ss << "Invalid column selected " << fieldName << ". Valid names are ";
      for (auto it = nameIdMap_.begin(); it != nameIdMap_.end(); ++it) {
        if (!first) ss << ", ";
        ss << it->first;
        first = false;
      }
      throw ParseError(ss.str());
    }
  }

  ColumnSelector::ColumnSelector(const FileContents* contents) : contents_(contents) {
    buildTypeNameIdMap(contents_->schema.get());
  }

  RowReaderImpl::RowReaderImpl(std::shared_ptr<FileContents> contents, const RowReaderOptions& opts)
      : localTimezone_(getLocalTimezone()),
        contents_(contents),
        throwOnHive11DecimalOverflow_(opts.getThrowOnHive11DecimalOverflow()),
        forcedScaleOnHive11Decimal_(opts.getForcedScaleOnHive11Decimal()),
        footer_(contents_->footer.get()),
        firstRowOfStripe_(*contents_->pool, 0),
        enableEncodedBlock_(opts.getEnableLazyDecoding()),
        readerTimezone_(getTimezoneByName(opts.getTimezoneName())),
        schemaEvolution_(opts.getReadType(), contents_->schema.get()) {
    uint64_t numberOfStripes;
    numberOfStripes = static_cast<uint64_t>(footer_->stripes_size());
    currentStripe_ = numberOfStripes;
    lastStripe_ = 0;
    currentRowInStripe_ = 0;
    rowsInCurrentStripe_ = 0;
    numRowGroupsInStripeRange_ = 0;
    useTightNumericVector_ = opts.getUseTightNumericVector();
    throwOnSchemaEvolutionOverflow_ = opts.getThrowOnSchemaEvolutionOverflow();
    uint64_t rowTotal = 0;

    firstRowOfStripe_.resize(numberOfStripes);
    for (size_t i = 0; i < numberOfStripes; ++i) {
      firstRowOfStripe_[i] = rowTotal;
      proto::StripeInformation stripeInfo = footer_->stripes(static_cast<int>(i));
      rowTotal += stripeInfo.number_of_rows();
      bool isStripeInRange = stripeInfo.offset() >= opts.getOffset() &&
                             stripeInfo.offset() < opts.getOffset() + opts.getLength();
      if (isStripeInRange) {
        if (i < currentStripe_) {
          currentStripe_ = i;
        }
        if (i >= lastStripe_) {
          lastStripe_ = i + 1;
        }
        if (footer_->row_index_stride() > 0) {
          numRowGroupsInStripeRange_ +=
              (stripeInfo.number_of_rows() + footer_->row_index_stride() - 1) /
              footer_->row_index_stride();
        }
      }
    }
    firstStripe_ = currentStripe_;
    processingStripe_ = lastStripe_;

    if (currentStripe_ == 0) {
      previousRow_ = (std::numeric_limits<uint64_t>::max)();
    } else if (currentStripe_ == numberOfStripes) {
      previousRow_ = footer_->number_of_rows();
    } else {
      previousRow_ = firstRowOfStripe_[firstStripe_] - 1;
    }

    ColumnSelector column_selector(contents_.get());
    column_selector.updateSelected(selectedColumns_, opts);

    // prepare SargsApplier if SearchArgument is available
    if (opts.getSearchArgument() && footer_->row_index_stride() > 0) {
      sargs_ = opts.getSearchArgument();
      sargsApplier_.reset(
          new SargsApplier(*contents_->schema, sargs_.get(), footer_->row_index_stride(),
                           getWriterVersionImpl(contents.get()), contents_->readerMetrics));
    }

    skipBloomFilters_ = hasBadBloomFilters();
  }

  // Check if the file has inconsistent bloom filters.
  bool RowReaderImpl::hasBadBloomFilters() {
    // Only C++ writer in old releases could have bad bloom filters.
    if (footer_->writer() != ORC_CPP_WRITER) return false;
    // 'softwareVersion' is added in 1.5.13, 1.6.11, and 1.7.0.
    // 1.6.x releases before 1.6.11 won't have it. On the other side, the C++ writer
    // supports writing bloom filters since 1.6.0. So files written by the C++ writer
    // and with 'softwareVersion' unset would have bad bloom filters.
    if (!footer_->has_software_version()) return true;

    const std::string& fullVersion = footer_->software_version();
    std::string version;
    // Deal with snapshot versions, e.g. 1.6.12-SNAPSHOT.
    if (fullVersion.find('-') != std::string::npos) {
      version = fullVersion.substr(0, fullVersion.find('-'));
    } else {
      version = fullVersion;
    }
    for (const char* v : BAD_CPP_BLOOM_FILTER_VERSIONS) {
      if (version == v) {
        return true;
      }
    }
    return false;
  }

  CompressionKind RowReaderImpl::getCompression() const {
    return contents_->compression;
  }

  uint64_t RowReaderImpl::getCompressionSize() const {
    return contents_->blockSize;
  }

  const std::vector<bool> RowReaderImpl::getSelectedColumns() const {
    return selectedColumns_;
  }

  const Type& RowReaderImpl::getSelectedType() const {
    if (selectedSchema_.get() == nullptr) {
      selectedSchema_ = buildSelectedType(contents_->schema.get(), selectedColumns_);
    }
    return *(selectedSchema_.get());
  }

  uint64_t RowReaderImpl::getRowNumber() const {
    return previousRow_;
  }

  void RowReaderImpl::seekToRow(uint64_t rowNumber) {
    // Empty file
    if (lastStripe_ == 0) {
      return;
    }

    // If we are reading only a portion of the file
    // (bounded by firstStripe and lastStripe),
    // seeking before or after the portion of interest should return no data.
    // Implement this by setting previousRow to the number of rows in the file.

    // seeking past lastStripe
    uint64_t num_stripes = static_cast<uint64_t>(footer_->stripes_size());
    if ((lastStripe_ == num_stripes && rowNumber >= footer_->number_of_rows()) ||
        (lastStripe_ < num_stripes && rowNumber >= firstRowOfStripe_[lastStripe_])) {
      currentStripe_ = num_stripes;
      previousRow_ = footer_->number_of_rows();
      return;
    }

    uint64_t seekToStripe = 0;
    while (seekToStripe + 1 < lastStripe_ && firstRowOfStripe_[seekToStripe + 1] <= rowNumber) {
      seekToStripe++;
    }

    // seeking before the first stripe
    if (seekToStripe < firstStripe_) {
      currentStripe_ = num_stripes;
      previousRow_ = footer_->number_of_rows();
      return;
    }

    previousRow_ = rowNumber;
    auto rowIndexStride = footer_->row_index_stride();
    if (!isCurrentStripeInited() || currentStripe_ != seekToStripe || rowIndexStride == 0 ||
        currentStripeInfo_.index_length() == 0) {
      // current stripe is not initialized or
      // target stripe is not current stripe or
      // current stripe doesn't have row indexes
      currentStripe_ = seekToStripe;
      currentRowInStripe_ = rowNumber - firstRowOfStripe_[currentStripe_];
      startNextStripe();
      if (currentStripe_ >= lastStripe_) {
        return;
      }
    } else {
      currentRowInStripe_ = rowNumber - firstRowOfStripe_[currentStripe_];
      if (sargsApplier_) {
        // advance to selected row group if predicate pushdown is enabled
        currentRowInStripe_ =
            advanceToNextRowGroup(currentRowInStripe_, rowsInCurrentStripe_,
                                  footer_->row_index_stride(), sargsApplier_->getNextSkippedRows());
      }
    }

    uint64_t rowsToSkip = currentRowInStripe_;
    // seek to the target row group if row indexes exists
    if (rowIndexStride > 0 && currentStripeInfo_.index_length() > 0) {
      if (rowIndexes_.empty()) {
        loadStripeIndex();
      }
      // TODO(ORC-1175): process the failures of loadStripeIndex() call
      seekToRowGroup(static_cast<uint32_t>(rowsToSkip / rowIndexStride));
      // skip leading rows in the target row group
      rowsToSkip %= rowIndexStride;
    }
    // 'reader' is reset in startNextStripe(). It could be nullptr if 'rowsToSkip' is 0,
    // e.g. when startNextStripe() skips all remaining rows of the file.
    if (rowsToSkip > 0) {
      reader_->skip(rowsToSkip);
    }
  }

  void RowReaderImpl::loadStripeIndex() {
    // reset all previous row indexes
    rowIndexes_.clear();
    bloomFilterIndex_.clear();

    // obtain row indexes for selected columns
    uint64_t offset = currentStripeInfo_.offset();
    for (int i = 0; i < currentStripeFooter_.streams_size(); ++i) {
      const proto::Stream& pbStream = currentStripeFooter_.streams(i);
      uint64_t colId = pbStream.column();
      if (selectedColumns_[colId] && pbStream.has_kind() &&
          (pbStream.kind() == proto::Stream_Kind_ROW_INDEX ||
           pbStream.kind() == proto::Stream_Kind_BLOOM_FILTER_UTF8)) {
        std::unique_ptr<SeekableInputStream> inStream = createDecompressor(
            getCompression(),
            std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                contents_->stream.get(), offset, pbStream.length(), *contents_->pool)),
            getCompressionSize(), *contents_->pool, contents_->readerMetrics);

        if (pbStream.kind() == proto::Stream_Kind_ROW_INDEX) {
          proto::RowIndex rowIndex;
          if (!rowIndex.ParseFromZeroCopyStream(inStream.get())) {
            throw ParseError("Failed to parse the row index");
          }
          rowIndexes_[colId] = rowIndex;
        } else if (!skipBloomFilters_) {  // Stream_Kind_BLOOM_FILTER_UTF8
          proto::BloomFilterIndex pbBFIndex;
          if (!pbBFIndex.ParseFromZeroCopyStream(inStream.get())) {
            throw ParseError("Failed to parse bloom filter index");
          }
          BloomFilterIndex bfIndex;
          for (int j = 0; j < pbBFIndex.bloom_filter_size(); j++) {
            bfIndex.entries.push_back(BloomFilterUTF8Utils::deserialize(
                pbStream.kind(), currentStripeFooter_.columns(static_cast<int>(pbStream.column())),
                pbBFIndex.bloom_filter(j)));
          }
          // add bloom filters to result for one column
          bloomFilterIndex_[pbStream.column()] = bfIndex;
        }
      }
      offset += pbStream.length();
    }
  }

  void RowReaderImpl::seekToRowGroup(uint32_t rowGroupEntryId) {
    // store positions for selected columns
    std::list<std::list<uint64_t>> positions;
    // store position providers for selected colimns
    std::unordered_map<uint64_t, PositionProvider> positionProviders;

    for (auto rowIndex = rowIndexes_.cbegin(); rowIndex != rowIndexes_.cend(); ++rowIndex) {
      uint64_t colId = rowIndex->first;
      const proto::RowIndexEntry& entry =
          rowIndex->second.entry(static_cast<int32_t>(rowGroupEntryId));

      // copy index positions for a specific column
      positions.push_back({});
      auto& position = positions.back();
      for (int pos = 0; pos != entry.positions_size(); ++pos) {
        position.push_back(entry.positions(pos));
      }
      positionProviders.insert(std::make_pair(colId, PositionProvider(position)));
    }

    reader_->seekToRowGroup(positionProviders);
  }

  const FileContents& RowReaderImpl::getFileContents() const {
    return *contents_;
  }

  bool RowReaderImpl::getThrowOnHive11DecimalOverflow() const {
    return throwOnHive11DecimalOverflow_;
  }

  bool RowReaderImpl::getIsDecimalAsLong() const {
    return contents_->isDecimalAsLong;
  }

  int32_t RowReaderImpl::getForcedScaleOnHive11Decimal() const {
    return forcedScaleOnHive11Decimal_;
  }

  proto::StripeFooter getStripeFooter(const proto::StripeInformation& info,
                                      const FileContents& contents) {
    uint64_t stripeFooterStart = info.offset() + info.index_length() + info.data_length();
    uint64_t stripeFooterLength = info.footer_length();
    std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
        contents.compression,
        std::make_unique<SeekableFileInputStream>(contents.stream.get(), stripeFooterStart,
                                                  stripeFooterLength, *contents.pool),
        contents.blockSize, *contents.pool, contents.readerMetrics);
    proto::StripeFooter result;
    if (!result.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError(std::string("bad StripeFooter from ") + pbStream->getName());
    }
    // Verify StripeFooter in case it's corrupt
    if (result.columns_size() != contents.footer->types_size()) {
      std::stringstream msg;
      msg << "bad number of ColumnEncodings in StripeFooter: expected="
          << contents.footer->types_size() << ", actual=" << result.columns_size();
      throw ParseError(msg.str());
    }
    return result;
  }

  ReaderImpl::ReaderImpl(std::shared_ptr<FileContents> contents, const ReaderOptions& opts,
                         uint64_t fileLength, uint64_t postscriptLength)
      : contents_(std::move(contents)),
        options_(opts),
        fileLength_(fileLength),
        postscriptLength_(postscriptLength),
        footer_(contents_->footer.get()) {
    isMetadataLoaded_ = false;
    checkOrcVersion();
    numberOfStripes_ = static_cast<uint64_t>(footer_->stripes_size());
    contents_->schema = convertType(footer_->types(0), *footer_);
    contents_->blockSize = getCompressionBlockSize(*contents_->postscript);
    contents_->compression = convertCompressionKind(*contents_->postscript);
  }

  std::string ReaderImpl::getSerializedFileTail() const {
    proto::FileTail tail;
    proto::PostScript* mutable_ps = tail.mutable_postscript();
    mutable_ps->CopyFrom(*contents_->postscript);
    proto::Footer* mutableFooter = tail.mutable_footer();
    mutableFooter->CopyFrom(*footer_);
    tail.set_file_length(fileLength_);
    tail.set_postscript_length(postscriptLength_);
    TProtobufString result;
    if (!tail.SerializeToString(&result)) {
      throw ParseError("Failed to serialize file tail");
    }
    return result;
  }

  const ReaderOptions& ReaderImpl::getReaderOptions() const {
    return options_;
  }

  CompressionKind ReaderImpl::getCompression() const {
    return contents_->compression;
  }

  uint64_t ReaderImpl::getCompressionSize() const {
    return contents_->blockSize;
  }

  uint64_t ReaderImpl::getNumberOfStripes() const {
    return numberOfStripes_;
  }

  uint64_t ReaderImpl::getNumberOfStripeStatistics() const {
    if (!isMetadataLoaded_) {
      readMetadata();
    }
    return contents_->metadata == nullptr
               ? 0
               : static_cast<uint64_t>(contents_->metadata->stripe_stats_size());
  }

  std::unique_ptr<StripeInformation> ReaderImpl::getStripe(uint64_t stripeIndex) const {
    if (stripeIndex > getNumberOfStripes()) {
      throw std::logic_error("stripe index out of range");
    }
    proto::StripeInformation stripeInfo = footer_->stripes(static_cast<int>(stripeIndex));

    return std::unique_ptr<StripeInformation>(new StripeInformationImpl(
        stripeInfo.offset(), stripeInfo.index_length(), stripeInfo.data_length(),
        stripeInfo.footer_length(), stripeInfo.number_of_rows(), contents_->stream.get(),
        *contents_->pool, contents_->compression, contents_->blockSize, contents_->readerMetrics));
  }

  FileVersion ReaderImpl::getFormatVersion() const {
    if (contents_->postscript->version_size() != 2) {
      return FileVersion::v_0_11();
    }
    return {contents_->postscript->version(0), contents_->postscript->version(1)};
  }

  uint64_t ReaderImpl::getNumberOfRows() const {
    return footer_->number_of_rows();
  }

  WriterId ReaderImpl::getWriterId() const {
    if (footer_->has_writer()) {
      uint32_t id = footer_->writer();
      if (id > WriterId::CUDF_WRITER) {
        return WriterId::UNKNOWN_WRITER;
      } else {
        return static_cast<WriterId>(id);
      }
    }
    return WriterId::ORC_JAVA_WRITER;
  }

  uint32_t ReaderImpl::getWriterIdValue() const {
    if (footer_->has_writer()) {
      return footer_->writer();
    } else {
      return WriterId::ORC_JAVA_WRITER;
    }
  }

  std::string ReaderImpl::getSoftwareVersion() const {
    std::ostringstream buffer;
    buffer << writerIdToString(getWriterIdValue());
    if (footer_->has_software_version()) {
      buffer << " " << footer_->software_version();
    }
    return buffer.str();
  }

  WriterVersion ReaderImpl::getWriterVersion() const {
    return getWriterVersionImpl(contents_.get());
  }

  uint64_t ReaderImpl::getContentLength() const {
    return footer_->content_length();
  }

  uint64_t ReaderImpl::getStripeStatisticsLength() const {
    return contents_->postscript->metadata_length();
  }

  uint64_t ReaderImpl::getFileFooterLength() const {
    return contents_->postscript->footer_length();
  }

  uint64_t ReaderImpl::getFilePostscriptLength() const {
    return postscriptLength_;
  }

  uint64_t ReaderImpl::getFileLength() const {
    return fileLength_;
  }

  uint64_t ReaderImpl::getRowIndexStride() const {
    return footer_->row_index_stride();
  }

  const std::string& ReaderImpl::getStreamName() const {
    return contents_->stream->getName();
  }

  std::list<std::string> ReaderImpl::getMetadataKeys() const {
    std::list<std::string> result;
    for (int i = 0; i < footer_->metadata_size(); ++i) {
      result.push_back(footer_->metadata(i).name());
    }
    return result;
  }

  std::string ReaderImpl::getMetadataValue(const std::string& key) const {
    for (int i = 0; i < footer_->metadata_size(); ++i) {
      if (footer_->metadata(i).name() == key) {
        return footer_->metadata(i).value();
      }
    }
    throw std::range_error("key not found");
  }

  void ReaderImpl::getRowIndexStatistics(
      const proto::StripeInformation& stripeInfo, uint64_t stripeIndex,
      const proto::StripeFooter& currentStripeFooter,
      std::vector<std::vector<proto::ColumnStatistics>>* indexStats) const {
    int num_streams = currentStripeFooter.streams_size();
    uint64_t offset = stripeInfo.offset();
    uint64_t indexEnd = stripeInfo.offset() + stripeInfo.index_length();
    for (int i = 0; i < num_streams; i++) {
      const proto::Stream& stream = currentStripeFooter.streams(i);
      StreamKind streamKind = static_cast<StreamKind>(stream.kind());
      uint64_t length = static_cast<uint64_t>(stream.length());
      if (streamKind == StreamKind::StreamKind_ROW_INDEX) {
        if (offset + length > indexEnd) {
          std::stringstream msg;
          msg << "Malformed RowIndex stream meta in stripe " << stripeIndex
              << ": streamOffset=" << offset << ", streamLength=" << length
              << ", stripeOffset=" << stripeInfo.offset()
              << ", stripeIndexLength=" << stripeInfo.index_length();
          throw ParseError(msg.str());
        }
        std::unique_ptr<SeekableInputStream> pbStream =
            createDecompressor(contents_->compression,
                               std::unique_ptr<SeekableInputStream>(new SeekableFileInputStream(
                                   contents_->stream.get(), offset, length, *contents_->pool)),
                               contents_->blockSize, *(contents_->pool), contents_->readerMetrics);

        proto::RowIndex rowIndex;
        if (!rowIndex.ParseFromZeroCopyStream(pbStream.get())) {
          throw ParseError("Failed to parse RowIndex from stripe footer");
        }
        int num_entries = rowIndex.entry_size();
        size_t column = static_cast<size_t>(stream.column());
        for (int j = 0; j < num_entries; j++) {
          const proto::RowIndexEntry& entry = rowIndex.entry(j);
          (*indexStats)[column].push_back(entry.statistics());
        }
      }
      offset += length;
    }
  }

  bool ReaderImpl::hasMetadataValue(const std::string& key) const {
    for (int i = 0; i < footer_->metadata_size(); ++i) {
      if (footer_->metadata(i).name() == key) {
        return true;
      }
    }
    return false;
  }

  const Type& ReaderImpl::getType() const {
    return *(contents_->schema.get());
  }

  std::unique_ptr<StripeStatistics> ReaderImpl::getStripeStatistics(uint64_t stripeIndex,
                                                                    bool includeRowIndex) const {
    if (!isMetadataLoaded_) {
      readMetadata();
    }
    if (contents_->metadata == nullptr) {
      throw std::logic_error("No stripe statistics in file");
    }

    proto::StripeInformation currentStripeInfo = footer_->stripes(static_cast<int>(stripeIndex));
    proto::StripeFooter currentStripeFooter = getStripeFooter(currentStripeInfo, *contents_.get());

    const Timezone& writerTZ = currentStripeFooter.has_writer_timezone()
                                   ? getTimezoneByName(currentStripeFooter.writer_timezone())
                                   : getLocalTimezone();
    StatContext statContext(hasCorrectStatistics(), &writerTZ);

    if (!includeRowIndex) {
      return std::make_unique<StripeStatisticsImpl>(
          contents_->metadata->stripe_stats(static_cast<int>(stripeIndex)), statContext);
    }

    size_t num_cols = static_cast<size_t>(
        contents_->metadata->stripe_stats(static_cast<int>(stripeIndex)).col_stats_size());
    std::vector<std::vector<proto::ColumnStatistics>> indexStats(num_cols);

    getRowIndexStatistics(currentStripeInfo, stripeIndex, currentStripeFooter, &indexStats);

    return std::make_unique<StripeStatisticsWithRowGroupIndexImpl>(
        contents_->metadata->stripe_stats(static_cast<int>(stripeIndex)), indexStats, statContext);
  }

  std::unique_ptr<Statistics> ReaderImpl::getStatistics() const {
    StatContext statContext(hasCorrectStatistics());
    return std::make_unique<StatisticsImpl>(*footer_, statContext);
  }

  std::unique_ptr<ColumnStatistics> ReaderImpl::getColumnStatistics(uint32_t index) const {
    if (index >= static_cast<uint64_t>(footer_->statistics_size())) {
      throw std::logic_error("column index out of range");
    }
    proto::ColumnStatistics col = footer_->statistics(static_cast<int32_t>(index));

    StatContext statContext(hasCorrectStatistics());
    return std::unique_ptr<ColumnStatistics>(convertColumnStatistics(col, statContext));
  }

  void ReaderImpl::readMetadata() const {
    uint64_t metadataSize = contents_->postscript->metadata_length();
    uint64_t footerLength = contents_->postscript->footer_length();
    if (fileLength_ < metadataSize + footerLength + postscriptLength_ + 1) {
      std::stringstream msg;
      msg << "Invalid Metadata length: fileLength=" << fileLength_
          << ", metadataLength=" << metadataSize << ", footerLength=" << footerLength
          << ", postscriptLength=" << postscriptLength_;
      throw ParseError(msg.str());
    }
    uint64_t metadataStart = fileLength_ - metadataSize - footerLength - postscriptLength_ - 1;
    if (metadataSize != 0) {
      std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
          contents_->compression,
          std::make_unique<SeekableFileInputStream>(contents_->stream.get(), metadataStart,
                                                    metadataSize, *contents_->pool),
          contents_->blockSize, *contents_->pool, contents_->readerMetrics);
      contents_->metadata.reset(new proto::Metadata());
      if (!contents_->metadata->ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Failed to parse the metadata");
      }
    }
    isMetadataLoaded_ = true;
  }

  bool ReaderImpl::hasCorrectStatistics() const {
    return !WriterVersionImpl::VERSION_HIVE_8732().compareGT(getWriterVersion());
  }

  void ReaderImpl::checkOrcVersion() {
    FileVersion version = getFormatVersion();
    if (version != FileVersion(0, 11) && version != FileVersion(0, 12)) {
      *(options_.getErrorStream())
          << "Warning: ORC file " << contents_->stream->getName()
          << " was written in an unknown format version " << version.toString() << "\n";
    }
  }

  std::unique_ptr<RowReader> ReaderImpl::createRowReader() const {
    RowReaderOptions defaultOpts;
    return createRowReader(defaultOpts);
  }

  std::unique_ptr<RowReader> ReaderImpl::createRowReader(const RowReaderOptions& opts) const {
    if (opts.getSearchArgument() && !isMetadataLoaded_) {
      // load stripe statistics for PPD
      readMetadata();
    }
    return std::make_unique<RowReaderImpl>(contents_, opts);
  }

  uint64_t maxStreamsForType(const proto::Type& type) {
    switch (static_cast<int64_t>(type.kind())) {
      case proto::Type_Kind_STRUCT:
        return 1;
      case proto::Type_Kind_INT:
      case proto::Type_Kind_LONG:
      case proto::Type_Kind_SHORT:
      case proto::Type_Kind_FLOAT:
      case proto::Type_Kind_DOUBLE:
      case proto::Type_Kind_BOOLEAN:
      case proto::Type_Kind_BYTE:
      case proto::Type_Kind_DATE:
      case proto::Type_Kind_LIST:
      case proto::Type_Kind_MAP:
      case proto::Type_Kind_UNION:
        return 2;
      case proto::Type_Kind_BINARY:
      case proto::Type_Kind_DECIMAL:
      case proto::Type_Kind_TIMESTAMP:
      case proto::Type_Kind_TIMESTAMP_INSTANT:
        return 3;
      case proto::Type_Kind_CHAR:
      case proto::Type_Kind_STRING:
      case proto::Type_Kind_VARCHAR:
      case proto::Type_Kind_GEOMETRY:
      case proto::Type_Kind_GEOGRAPHY:
        return 4;
      default:
        return 0;
    }
  }

  uint64_t ReaderImpl::getMemoryUse(int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents_->footer->types_size()), true);
    return getMemoryUse(stripeIx, selectedColumns);
  }

  uint64_t ReaderImpl::getMemoryUseByFieldId(const std::list<uint64_t>& include, int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents_->footer->types_size()), false);
    ColumnSelector column_selector(contents_.get());
    if (contents_->schema->getKind() == STRUCT && include.begin() != include.end()) {
      for (std::list<uint64_t>::const_iterator field = include.begin(); field != include.end();
           ++field) {
        column_selector.updateSelectedByFieldId(selectedColumns, *field);
      }
    } else {
      // default is to select all columns
      std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    column_selector.selectParents(selectedColumns, *contents_->schema.get());
    selectedColumns[0] = true;  // column 0 is selected by default
    return getMemoryUse(stripeIx, selectedColumns);
  }

  uint64_t ReaderImpl::getMemoryUseByName(const std::list<std::string>& names, int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents_->footer->types_size()), false);
    ColumnSelector column_selector(contents_.get());
    if (contents_->schema->getKind() == STRUCT && names.begin() != names.end()) {
      for (std::list<std::string>::const_iterator field = names.begin(); field != names.end();
           ++field) {
        column_selector.updateSelectedByName(selectedColumns, *field);
      }
    } else {
      // default is to select all columns
      std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    column_selector.selectParents(selectedColumns, *contents_->schema.get());
    selectedColumns[0] = true;  // column 0 is selected by default
    return getMemoryUse(stripeIx, selectedColumns);
  }

  uint64_t ReaderImpl::getMemoryUseByTypeId(const std::list<uint64_t>& include, int stripeIx) {
    std::vector<bool> selectedColumns;
    selectedColumns.assign(static_cast<size_t>(contents_->footer->types_size()), false);
    ColumnSelector column_selector(contents_.get());
    if (include.begin() != include.end()) {
      for (std::list<uint64_t>::const_iterator field = include.begin(); field != include.end();
           ++field) {
        column_selector.updateSelectedByTypeId(selectedColumns, *field);
      }
    } else {
      // default is to select all columns
      std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    column_selector.selectParents(selectedColumns, *contents_->schema.get());
    selectedColumns[0] = true;  // column 0 is selected by default
    return getMemoryUse(stripeIx, selectedColumns);
  }

  uint64_t ReaderImpl::getMemoryUse(int stripeIx, std::vector<bool>& selectedColumns) {
    uint64_t maxDataLength = 0;

    if (stripeIx >= 0 && stripeIx < footer_->stripes_size()) {
      uint64_t stripe = footer_->stripes(stripeIx).data_length();
      if (maxDataLength < stripe) {
        maxDataLength = stripe;
      }
    } else {
      for (int i = 0; i < footer_->stripes_size(); i++) {
        uint64_t stripe = footer_->stripes(i).data_length();
        if (maxDataLength < stripe) {
          maxDataLength = stripe;
        }
      }
    }

    bool hasStringColumn = false;
    uint64_t nSelectedStreams = 0;
    for (int i = 0; !hasStringColumn && i < footer_->types_size(); i++) {
      if (selectedColumns[static_cast<size_t>(i)]) {
        const proto::Type& type = footer_->types(i);
        nSelectedStreams += maxStreamsForType(type);
        switch (static_cast<int64_t>(type.kind())) {
          case proto::Type_Kind_CHAR:
          case proto::Type_Kind_STRING:
          case proto::Type_Kind_VARCHAR:
          case proto::Type_Kind_BINARY: {
            hasStringColumn = true;
            break;
          }
          default: {
            break;
          }
        }
      }
    }

    /* If a string column is read, use stripe data_length as a memory estimate
     * because we don't know the dictionary size. Multiply by 2 because
     * a string column requires two buffers:
     * in the input stream and in the seekable input stream.
     * If no string column is read, estimate from the number of streams.
     */
    uint64_t memory = hasStringColumn
                          ? 2 * maxDataLength
                          : std::min(uint64_t(maxDataLength),
                                     nSelectedStreams * contents_->stream->getNaturalReadSize());

    // Do we need even more memory to read the footer or the metadata?
    if (memory < contents_->postscript->footer_length() + DIRECTORY_SIZE_GUESS) {
      memory = contents_->postscript->footer_length() + DIRECTORY_SIZE_GUESS;
    }
    if (memory < contents_->postscript->metadata_length()) {
      memory = contents_->postscript->metadata_length();
    }

    // Account for firstRowOfStripe.
    memory += static_cast<uint64_t>(footer_->stripes_size()) * sizeof(uint64_t);

    // Decompressors need buffers for each stream
    uint64_t decompressorMemory = 0;
    if (contents_->compression != CompressionKind_NONE) {
      for (int i = 0; i < footer_->types_size(); i++) {
        if (selectedColumns[static_cast<size_t>(i)]) {
          const proto::Type& type = footer_->types(i);
          decompressorMemory += maxStreamsForType(type) * contents_->blockSize;
        }
      }
      if (contents_->compression == CompressionKind_SNAPPY) {
        decompressorMemory *= 2;  // Snappy decompressor uses a second buffer
      }
    }

    return memory + decompressorMemory;
  }

  // Update fields to indicate we've reached the end of file
  void RowReaderImpl::markEndOfFile() {
    currentStripe_ = lastStripe_;
    currentRowInStripe_ = 0;
    rowsInCurrentStripe_ = 0;
    if (lastStripe_ == 0) {
      // Empty file
      previousRow_ = 0;
    } else {
      previousRow_ = firstRowOfStripe_[lastStripe_ - 1] +
                     footer_->stripes(static_cast<int>(lastStripe_ - 1)).number_of_rows();
    }
  }

  void RowReaderImpl::startNextStripe() {
    reader_.reset();  // ColumnReaders use lots of memory; free old memory first
    rowIndexes_.clear();
    bloomFilterIndex_.clear();

    // evaluate file statistics if it exists
    if (sargsApplier_ &&
        !sargsApplier_->evaluateFileStatistics(*footer_, numRowGroupsInStripeRange_)) {
      // skip the entire file
      markEndOfFile();
      return;
    }

    do {
      currentStripeInfo_ = footer_->stripes(static_cast<int>(currentStripe_));
      uint64_t fileLength = contents_->stream->getLength();
      if (currentStripeInfo_.offset() + currentStripeInfo_.index_length() +
              currentStripeInfo_.data_length() + currentStripeInfo_.footer_length() >=
          fileLength) {
        std::stringstream msg;
        msg << "Malformed StripeInformation at stripe index " << currentStripe_
            << ": fileLength=" << fileLength
            << ", StripeInfo=(offset=" << currentStripeInfo_.offset()
            << ", indexLength=" << currentStripeInfo_.index_length()
            << ", dataLength=" << currentStripeInfo_.data_length()
            << ", footerLength=" << currentStripeInfo_.footer_length() << ")";
        throw ParseError(msg.str());
      }
      rowsInCurrentStripe_ = currentStripeInfo_.number_of_rows();
      processingStripe_ = currentStripe_;

      bool isStripeNeeded = true;
      // If PPD enabled and stripe stats existed, evaulate it first
      if (sargsApplier_ && contents_->metadata) {
        const auto& currentStripeStats =
            contents_->metadata->stripe_stats(static_cast<int>(currentStripe_));
        // skip this stripe after stats fail to satisfy sargs
        uint64_t stripeRowGroupCount =
            (rowsInCurrentStripe_ + footer_->row_index_stride() - 1) / footer_->row_index_stride();
        isStripeNeeded =
            sargsApplier_->evaluateStripeStatistics(currentStripeStats, stripeRowGroupCount);
      }

      if (isStripeNeeded) {
        currentStripeFooter_ = getStripeFooter(currentStripeInfo_, *contents_.get());
        if (sargsApplier_) {
          // read row group statistics and bloom filters of current stripe
          loadStripeIndex();

          // select row groups to read in the current stripe
          sargsApplier_->pickRowGroups(rowsInCurrentStripe_, rowIndexes_, bloomFilterIndex_);
          if (sargsApplier_->hasSelectedFrom(currentRowInStripe_)) {
            // current stripe has at least one row group matching the predicate
            break;
          }
          isStripeNeeded = false;
        }
      }

      if (!isStripeNeeded) {
        // advance to next stripe when current stripe has no matching rows
        currentStripe_ += 1;
        currentRowInStripe_ = 0;
      }
    } while (sargsApplier_ && currentStripe_ < lastStripe_);

    if (currentStripe_ < lastStripe_) {
      // get writer timezone info from stripe footer to help understand timestamp values.
      const Timezone& writerTimezone =
          currentStripeFooter_.has_writer_timezone()
              ? getTimezoneByName(currentStripeFooter_.writer_timezone())
              : localTimezone_;
      StripeStreamsImpl stripeStreams(*this, currentStripe_, currentStripeInfo_,
                                      currentStripeFooter_, currentStripeInfo_.offset(),
                                      *contents_->stream, writerTimezone, readerTimezone_);
      reader_ = buildReader(*contents_->schema, stripeStreams, useTightNumericVector_,
                            throwOnSchemaEvolutionOverflow_, /*convertToReadType=*/true);

      if (sargsApplier_) {
        // move to the 1st selected row group when PPD is enabled.
        currentRowInStripe_ =
            advanceToNextRowGroup(currentRowInStripe_, rowsInCurrentStripe_,
                                  footer_->row_index_stride(), sargsApplier_->getNextSkippedRows());
        previousRow_ = firstRowOfStripe_[currentStripe_] + currentRowInStripe_ - 1;
        if (currentRowInStripe_ > 0) {
          seekToRowGroup(static_cast<uint32_t>(currentRowInStripe_ / footer_->row_index_stride()));
        }
      }
    } else {
      // All remaining stripes are skipped.
      markEndOfFile();
    }
  }

  bool RowReaderImpl::next(ColumnVectorBatch& data) {
    SCOPED_STOPWATCH(contents_->readerMetrics, ReaderInclusiveLatencyUs, ReaderCall);
    if (currentStripe_ >= lastStripe_) {
      data.numElements = 0;
      markEndOfFile();
      return false;
    }
    if (currentRowInStripe_ == 0) {
      startNextStripe();
    }
    uint64_t rowsToRead =
        std::min(static_cast<uint64_t>(data.capacity), rowsInCurrentStripe_ - currentRowInStripe_);
    if (sargsApplier_ && rowsToRead > 0) {
      rowsToRead =
          computeBatchSize(rowsToRead, currentRowInStripe_, rowsInCurrentStripe_,
                           footer_->row_index_stride(), sargsApplier_->getNextSkippedRows());
    }
    data.numElements = rowsToRead;
    if (rowsToRead == 0) {
      markEndOfFile();
      return false;
    }
    if (enableEncodedBlock_) {
      reader_->nextEncoded(data, rowsToRead, nullptr);
    } else {
      reader_->next(data, rowsToRead, nullptr);
    }
    // update row number
    previousRow_ = firstRowOfStripe_[currentStripe_] + currentRowInStripe_;
    currentRowInStripe_ += rowsToRead;

    // check if we need to advance to next selected row group
    if (sargsApplier_) {
      uint64_t nextRowToRead =
          advanceToNextRowGroup(currentRowInStripe_, rowsInCurrentStripe_,
                                footer_->row_index_stride(), sargsApplier_->getNextSkippedRows());
      if (currentRowInStripe_ != nextRowToRead) {
        // it is guaranteed to be at start of a row group
        currentRowInStripe_ = nextRowToRead;
        if (currentRowInStripe_ < rowsInCurrentStripe_) {
          seekToRowGroup(static_cast<uint32_t>(currentRowInStripe_ / footer_->row_index_stride()));
        }
      }
    }

    if (currentRowInStripe_ >= rowsInCurrentStripe_) {
      currentStripe_ += 1;
      currentRowInStripe_ = 0;
    }
    return rowsToRead != 0;
  }

  uint64_t RowReaderImpl::computeBatchSize(uint64_t requestedSize, uint64_t currentRowInStripe,
                                           uint64_t rowsInCurrentStripe, uint64_t rowIndexStride,
                                           const std::vector<uint64_t>& nextSkippedRows) {
    // In case of PPD, batch size should be aware of row group boundaries. If only a subset of row
    // groups are selected then marker position is set to the end of range (subset of row groups
    // within stripe).
    uint64_t endRowInStripe = rowsInCurrentStripe;
    uint64_t groupsInStripe = nextSkippedRows.size();
    if (groupsInStripe > 0) {
      auto rg = static_cast<uint32_t>(currentRowInStripe / rowIndexStride);
      if (rg >= groupsInStripe) return 0;
      uint64_t nextSkippedRow = nextSkippedRows[rg];
      if (nextSkippedRow == 0) return 0;
      endRowInStripe = nextSkippedRow;
    }
    return std::min(requestedSize, endRowInStripe - currentRowInStripe);
  }

  uint64_t RowReaderImpl::advanceToNextRowGroup(uint64_t currentRowInStripe,
                                                uint64_t rowsInCurrentStripe,
                                                uint64_t rowIndexStride,
                                                const std::vector<uint64_t>& nextSkippedRows) {
    auto groupsInStripe = nextSkippedRows.size();
    if (groupsInStripe == 0) {
      // No PPD, keeps using the current row in stripe
      return std::min(currentRowInStripe, rowsInCurrentStripe);
    }
    auto rg = static_cast<uint32_t>(currentRowInStripe / rowIndexStride);
    if (rg >= groupsInStripe) {
      // Points to the end of the stripe
      return rowsInCurrentStripe;
    }
    if (nextSkippedRows[rg] != 0) {
      // Current row group is selected
      return currentRowInStripe;
    }
    // Advance to the next selected row group
    while (rg < groupsInStripe && nextSkippedRows[rg] == 0) ++rg;
    if (rg < groupsInStripe) {
      return rg * rowIndexStride;
    }
    return rowsInCurrentStripe;
  }

  static void getColumnIds(const Type* type, std::set<uint64_t>& columnIds) {
    columnIds.insert(type->getColumnId());
    for (uint64_t i = 0; i < type->getSubtypeCount(); ++i) {
      getColumnIds(type->getSubtype(i), columnIds);
    }
  }

  std::unique_ptr<ColumnVectorBatch> RowReaderImpl::createRowBatch(uint64_t capacity) const {
    // If the read type is specified, then check that the selected schema matches the read type
    // on the first call to createRowBatch.
    if (schemaEvolution_.getReadType() && selectedSchema_.get() == nullptr) {
      auto fileSchema = &getSelectedType();
      auto readType = schemaEvolution_.getReadType();
      std::set<uint64_t> readColumns, fileColumns;
      getColumnIds(readType, readColumns);
      getColumnIds(fileSchema, fileColumns);
      if (readColumns != fileColumns) {
        std::ostringstream ss;
        ss << "The selected schema " << fileSchema->toString() << " doesn't match read type "
           << readType->toString();
        throw SchemaEvolutionError(ss.str());
      }
    }
    const Type& readType =
        schemaEvolution_.getReadType() ? *schemaEvolution_.getReadType() : getSelectedType();
    return readType.createRowBatch(capacity, *contents_->pool, enableEncodedBlock_,
                                   useTightNumericVector_);
  }

  void ensureOrcFooter(InputStream* stream, DataBuffer<char>* buffer, uint64_t postscriptLength) {
    const std::string MAGIC("ORC");
    const uint64_t magicLength = MAGIC.length();
    const char* const bufferStart = buffer->data();
    const uint64_t bufferLength = buffer->size();

    if (postscriptLength < magicLength || bufferLength < magicLength) {
      throw ParseError("Invalid ORC postscript length");
    }
    const char* magicStart = bufferStart + bufferLength - 1 - magicLength;

    // Look for the magic string at the end of the postscript.
    if (memcmp(magicStart, MAGIC.c_str(), magicLength) != 0) {
      // If there is no magic string at the end, check the beginning.
      // Only files written by Hive 0.11.0 don't have the tail ORC string.
      std::unique_ptr<char[]> frontBuffer(new char[magicLength]);
      stream->read(frontBuffer.get(), magicLength, 0);
      bool foundMatch = memcmp(frontBuffer.get(), MAGIC.c_str(), magicLength) == 0;

      if (!foundMatch) {
        throw ParseError("Not an ORC file");
      }
    }
  }

  /**
   * Read the file's postscript from the given buffer.
   * @param stream the file stream
   * @param buffer the buffer with the tail of the file.
   * @param postscriptSize the length of postscript in bytes
   */
  std::unique_ptr<proto::PostScript> readPostscript(InputStream* stream, DataBuffer<char>* buffer,
                                                    uint64_t postscriptSize) {
    char* ptr = buffer->data();
    uint64_t readSize = buffer->size();

    ensureOrcFooter(stream, buffer, postscriptSize);

    auto postscript = std::make_unique<proto::PostScript>();
    if (readSize < 1 + postscriptSize) {
      std::stringstream msg;
      msg << "Invalid ORC postscript length: " << postscriptSize
          << ", file length = " << stream->getLength();
      throw ParseError(msg.str());
    }
    if (!postscript->ParseFromArray(ptr + readSize - 1 - postscriptSize,
                                    static_cast<int>(postscriptSize))) {
      throw ParseError("Failed to parse the postscript from " + stream->getName());
    }
    return postscript;
  }

  /**
   * Check that proto Types are valid. Indices in the type tree should be valid,
   * so we won't crash when we convert the proto::Types to TypeImpls (ORC-317).
   * For STRUCT types, fieldName size should match subTypes size (ORC-581).
   */
  void checkProtoTypes(const proto::Footer& footer) {
    std::stringstream msg;
    int maxId = footer.types_size();
    if (maxId <= 0) {
      throw ParseError("Footer is corrupt: no types found");
    }
    for (int i = 0; i < maxId; ++i) {
      const proto::Type& type = footer.types(i);
      if (type.kind() == proto::Type_Kind_STRUCT &&
          type.subtypes_size() != type.field_names_size()) {
        msg << "Footer is corrupt: STRUCT type " << i << " has " << type.subtypes_size()
            << " subTypes, but has " << type.field_names_size() << " fieldNames";
        throw ParseError(msg.str());
      }
      for (int j = 0; j < type.subtypes_size(); ++j) {
        int subTypeId = static_cast<int>(type.subtypes(j));
        if (subTypeId <= i) {
          msg << "Footer is corrupt: malformed link from type " << i << " to " << subTypeId;
          throw ParseError(msg.str());
        }
        if (subTypeId >= maxId) {
          msg << "Footer is corrupt: types(" << subTypeId << ") not exists";
          throw ParseError(msg.str());
        }
        if (j > 0 && static_cast<int>(type.subtypes(j - 1)) >= subTypeId) {
          msg << "Footer is corrupt: subType(" << (j - 1) << ") >= subType(" << j << ") in types("
              << i << "). (" << type.subtypes(j - 1) << " >= " << subTypeId << ")";
          throw ParseError(msg.str());
        }
      }
    }
  }

  /**
   * Parse the footer from the given buffer.
   * @param stream the file's stream
   * @param buffer the buffer to parse the footer from
   * @param footerOffset the offset within the buffer that contains the footer
   * @param ps the file's postscript
   * @param memoryPool the memory pool to use
   */
  std::unique_ptr<proto::Footer> readFooter(InputStream* stream, const DataBuffer<char>* buffer,
                                            uint64_t footerOffset, const proto::PostScript& ps,
                                            MemoryPool& memoryPool, ReaderMetrics* readerMetrics) {
    const char* footerPtr = buffer->data() + footerOffset;

    std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
        convertCompressionKind(ps),
        std::make_unique<SeekableArrayInputStream>(footerPtr, ps.footer_length()),
        getCompressionBlockSize(ps), memoryPool, readerMetrics);

    auto footer = std::make_unique<proto::Footer>();
    if (!footer->ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("Failed to parse the footer from " + stream->getName());
    }

    checkProtoTypes(*footer);
    return footer;
  }

  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream,
                                       const ReaderOptions& options) {
    auto contents = std::make_shared<FileContents>();
    contents->pool = options.getMemoryPool();
    contents->errorStream = options.getErrorStream();
    contents->readerMetrics = options.getReaderMetrics();
    std::string serializedFooter = options.getSerializedFileTail();
    uint64_t fileLength;
    uint64_t postscriptLength;
    if (serializedFooter.length() != 0) {
      // Parse the file tail from the serialized one.
      proto::FileTail tail;
      if (!tail.ParseFromString(serializedFooter)) {
        throw ParseError("Failed to parse the file tail from string");
      }
      contents->postscript = std::make_unique<proto::PostScript>(tail.postscript());
      contents->footer = std::make_unique<proto::Footer>(tail.footer());
      fileLength = tail.file_length();
      postscriptLength = tail.postscript_length();
    } else {
      // figure out the size of the file using the option or filesystem
      fileLength = std::min(options.getTailLocation(), static_cast<uint64_t>(stream->getLength()));

      // read last bytes into buffer to get PostScript
      uint64_t readSize = std::min(fileLength, DIRECTORY_SIZE_GUESS);
      if (readSize < 4) {
        throw ParseError("File size too small");
      }
      auto buffer = std::make_unique<DataBuffer<char>>(*contents->pool, readSize);
      stream->read(buffer->data(), readSize, fileLength - readSize);

      postscriptLength = buffer->data()[readSize - 1] & 0xff;
      contents->postscript = readPostscript(stream.get(), buffer.get(), postscriptLength);
      uint64_t footerSize = contents->postscript->footer_length();
      uint64_t tailSize = 1 + postscriptLength + footerSize;
      if (tailSize >= fileLength) {
        std::stringstream msg;
        msg << "Invalid ORC tailSize=" << tailSize << ", fileLength=" << fileLength;
        throw ParseError(msg.str());
      }
      uint64_t footerOffset;

      if (tailSize > readSize) {
        buffer->resize(footerSize);
        stream->read(buffer->data(), footerSize, fileLength - tailSize);
        footerOffset = 0;
      } else {
        footerOffset = readSize - tailSize;
      }

      contents->footer = readFooter(stream.get(), buffer.get(), footerOffset, *contents->postscript,
                                    *contents->pool, contents->readerMetrics);
    }
    contents->isDecimalAsLong = false;
    if (contents->postscript->version_size() == 2) {
      FileVersion v(contents->postscript->version(0), contents->postscript->version(1));
      if (v == FileVersion::UNSTABLE_PRE_2_0()) {
        contents->isDecimalAsLong = true;
      }
    }
    contents->stream = std::move(stream);
    return std::make_unique<ReaderImpl>(std::move(contents), options, fileLength, postscriptLength);
  }

  std::map<uint32_t, BloomFilterIndex> ReaderImpl::getBloomFilters(
      uint32_t stripeIndex, const std::set<uint32_t>& included) const {
    std::map<uint32_t, BloomFilterIndex> ret;

    uint64_t offset;
    auto currentStripeFooter = loadCurrentStripeFooter(stripeIndex, offset);

    // iterate stripe footer to get stream of bloom_filter
    for (int i = 0; i < currentStripeFooter.streams_size(); i++) {
      const proto::Stream& stream = currentStripeFooter.streams(i);
      uint32_t column = static_cast<uint32_t>(stream.column());
      uint64_t length = static_cast<uint64_t>(stream.length());

      // a bloom filter stream from a selected column is found
      if (stream.kind() == proto::Stream_Kind_BLOOM_FILTER_UTF8 &&
          (included.empty() || included.find(column) != included.end())) {
        std::unique_ptr<SeekableInputStream> pbStream =
            createDecompressor(contents_->compression,
                               std::make_unique<SeekableFileInputStream>(
                                   contents_->stream.get(), offset, length, *contents_->pool),
                               contents_->blockSize, *(contents_->pool), contents_->readerMetrics);

        proto::BloomFilterIndex pbBFIndex;
        if (!pbBFIndex.ParseFromZeroCopyStream(pbStream.get())) {
          throw ParseError("Failed to parse BloomFilterIndex");
        }

        BloomFilterIndex bfIndex;
        for (int j = 0; j < pbBFIndex.bloom_filter_size(); j++) {
          std::unique_ptr<BloomFilter> entry = BloomFilterUTF8Utils::deserialize(
              stream.kind(), currentStripeFooter.columns(static_cast<int>(stream.column())),
              pbBFIndex.bloom_filter(j));
          bfIndex.entries.push_back(std::shared_ptr<BloomFilter>(std::move(entry)));
        }

        // add bloom filters to result for one column
        ret[column] = bfIndex;
      }

      offset += length;
    }

    return ret;
  }

  proto::StripeFooter ReaderImpl::loadCurrentStripeFooter(uint32_t stripeIndex,
                                                          uint64_t& offset) const {
    // find stripe info
    if (stripeIndex >= static_cast<uint32_t>(footer_->stripes_size())) {
      throw std::logic_error("Illegal stripe index: " +
                             to_string(static_cast<int64_t>(stripeIndex)));
    }
    const proto::StripeInformation currentStripeInfo =
        footer_->stripes(static_cast<int>(stripeIndex));
    offset = static_cast<uint64_t>(currentStripeInfo.offset());
    return getStripeFooter(currentStripeInfo, *contents_);
  }

  std::map<uint32_t, RowGroupIndex> ReaderImpl::getRowGroupIndex(
      uint32_t stripeIndex, const std::set<uint32_t>& included) const {
    std::map<uint32_t, RowGroupIndex> ret;
    uint64_t offset;
    auto currentStripeFooter = loadCurrentStripeFooter(stripeIndex, offset);

    // iterate stripe footer to get stream of row_index
    for (int i = 0; i < currentStripeFooter.streams_size(); i++) {
      const proto::Stream& stream = currentStripeFooter.streams(i);
      uint32_t column = static_cast<uint32_t>(stream.column());
      uint64_t length = static_cast<uint64_t>(stream.length());
      RowGroupIndex& rowGroupIndex = ret[column];

      if (stream.kind() == proto::Stream_Kind_ROW_INDEX &&
          (included.empty() || included.find(column) != included.end())) {
        std::unique_ptr<SeekableInputStream> pbStream =
            createDecompressor(contents_->compression,
                               std::make_unique<SeekableFileInputStream>(
                                   contents_->stream.get(), offset, length, *contents_->pool),
                               contents_->blockSize, *(contents_->pool), contents_->readerMetrics);

        proto::RowIndex pbRowIndex;
        if (!pbRowIndex.ParseFromZeroCopyStream(pbStream.get())) {
          std::stringstream errMsgBuffer;
          errMsgBuffer << "Failed to parse RowIndex at column " << column << " in stripe "
                       << stripeIndex;
          throw ParseError(errMsgBuffer.str());
        }

        // add rowGroupIndex to result for one column
        for (auto& rowIndexEntry : pbRowIndex.entry()) {
          std::vector<uint64_t> posVector;
          for (auto& position : rowIndexEntry.positions()) {
            posVector.push_back(position);
          }
          rowGroupIndex.positions.push_back(posVector);
        }
      }
      offset += length;
    }
    return ret;
  }

  void ReaderImpl::releaseBuffer(uint64_t boundary) {
    std::lock_guard<std::mutex> lock(contents_->readCacheMutex);

    if (contents_->readCache) {
      contents_->readCache->evictEntriesBefore(boundary);
    }
  }

  void ReaderImpl::preBuffer(const std::vector<uint32_t>& stripes,
                             const std::list<uint64_t>& includeTypes) {
    std::vector<uint32_t> newStripes;
    for (auto stripe : stripes) {
      if (stripe < static_cast<uint32_t>(footer_->stripes_size())) newStripes.push_back(stripe);
    }

    std::list<uint64_t> newIncludeTypes;
    for (auto type : includeTypes) {
      if (type < static_cast<uint64_t>(footer_->types_size())) newIncludeTypes.push_back(type);
    }

    if (newStripes.empty() || newIncludeTypes.empty()) {
      return;
    }

    orc::RowReaderOptions rowReaderOptions;
    rowReaderOptions.includeTypes(newIncludeTypes);
    ColumnSelector columnSelector(contents_.get());
    std::vector<bool> selectedColumns;
    columnSelector.updateSelected(selectedColumns, rowReaderOptions);

    std::vector<ReadRange> ranges;
    ranges.reserve(newIncludeTypes.size());
    for (auto stripe : newStripes) {
      // get stripe information
      const auto& stripeInfo = footer_->stripes(stripe);
      uint64_t stripeFooterStart =
          stripeInfo.offset() + stripeInfo.index_length() + stripeInfo.data_length();
      uint64_t stripeFooterLength = stripeInfo.footer_length();

      // get stripe footer
      std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
          contents_->compression,
          std::make_unique<SeekableFileInputStream>(contents_->stream.get(), stripeFooterStart,
                                                    stripeFooterLength, *contents_->pool),
          contents_->blockSize, *contents_->pool, contents_->readerMetrics);
      proto::StripeFooter stripeFooter;
      if (!stripeFooter.ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError(std::string("bad StripeFooter from ") + pbStream->getName());
      }

      // traverse all streams in stripe footer, choose selected streams to prebuffer
      uint64_t offset = stripeInfo.offset();
      for (int i = 0; i < stripeFooter.streams_size(); i++) {
        const proto::Stream& stream = stripeFooter.streams(i);
        if (offset + stream.length() > stripeFooterStart) {
          std::stringstream msg;
          msg << "Malformed stream meta at stream index " << i << " in stripe " << stripe
              << ": streamOffset=" << offset << ", streamLength=" << stream.length()
              << ", stripeOffset=" << stripeInfo.offset()
              << ", stripeIndexLength=" << stripeInfo.index_length()
              << ", stripeDataLength=" << stripeInfo.data_length();
          throw ParseError(msg.str());
        }

        if (stream.has_kind() && selectedColumns[stream.column()]) {
          const auto& kind = stream.kind();
          if (kind == proto::Stream_Kind_DATA || kind == proto::Stream_Kind_DICTIONARY_DATA ||
              kind == proto::Stream_Kind_PRESENT || kind == proto::Stream_Kind_LENGTH ||
              kind == proto::Stream_Kind_SECONDARY) {
            ranges.emplace_back(offset, stream.length());
          }
        }

        offset += stream.length();
      }

      {
        std::lock_guard<std::mutex> lock(contents_->readCacheMutex);

        if (!contents_->readCache) {
          contents_->readCache = std::make_shared<ReadRangeCache>(
              getStream(), options_.getCacheOptions(), contents_->pool, contents_->readerMetrics);
        }
        contents_->readCache->cache(std::move(ranges));
      }
    }
  }

  RowReader::~RowReader() {
    // PASS
  }

  Reader::~Reader() {
    // PASS
  }

  InputStream::~InputStream(){
      // PASS
  };

}  // namespace orc
