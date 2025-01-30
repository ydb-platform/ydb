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

#ifndef ORC_OPTIONS_HH
#define ORC_OPTIONS_HH

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "io/Cache.hh"

#include <limits>

namespace orc {

  enum ColumnSelection {
    ColumnSelection_NONE = 0,
    ColumnSelection_NAMES = 1,
    ColumnSelection_FIELD_IDS = 2,
    ColumnSelection_TYPE_IDS = 3,
  };

  /**
   * ReaderOptions Implementation
   */
  struct ReaderOptionsPrivate {
    uint64_t tailLocation;
    std::ostream* errorStream;
    MemoryPool* memoryPool;
    std::string serializedTail;
    ReaderMetrics* metrics;
    CacheOptions cacheOptions;

    ReaderOptionsPrivate() {
      tailLocation = std::numeric_limits<uint64_t>::max();
      errorStream = &std::cerr;
      memoryPool = getDefaultPool();
      metrics = nullptr;
    }
  };

  ReaderOptions::ReaderOptions() : privateBits_(std::make_unique<ReaderOptionsPrivate>()) {
    // PASS
  }

  ReaderOptions::ReaderOptions(const ReaderOptions& rhs)
      : privateBits_(std::make_unique<ReaderOptionsPrivate>(*(rhs.privateBits_.get()))) {
    // PASS
  }

  ReaderOptions::ReaderOptions(ReaderOptions& rhs) {
    // swap privateBits with rhs
    privateBits_.swap(rhs.privateBits_);
  }

  ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits_.reset(new ReaderOptionsPrivate(*(rhs.privateBits_.get())));
    }
    return *this;
  }

  ReaderOptions::~ReaderOptions() {
    // PASS
  }

  ReaderOptions& ReaderOptions::setMemoryPool(MemoryPool& pool) {
    privateBits_->memoryPool = &pool;
    return *this;
  }

  MemoryPool* ReaderOptions::getMemoryPool() const {
    return privateBits_->memoryPool;
  }

  ReaderOptions& ReaderOptions::setReaderMetrics(ReaderMetrics* metrics) {
    privateBits_->metrics = metrics;
    return *this;
  }

  ReaderMetrics* ReaderOptions::getReaderMetrics() const {
    return privateBits_->metrics;
  }

  ReaderOptions& ReaderOptions::setTailLocation(uint64_t offset) {
    privateBits_->tailLocation = offset;
    return *this;
  }

  uint64_t ReaderOptions::getTailLocation() const {
    return privateBits_->tailLocation;
  }

  ReaderOptions& ReaderOptions::setSerializedFileTail(const std::string& value) {
    privateBits_->serializedTail = value;
    return *this;
  }

  std::string ReaderOptions::getSerializedFileTail() const {
    return privateBits_->serializedTail;
  }

  ReaderOptions& ReaderOptions::setErrorStream(std::ostream& stream) {
    privateBits_->errorStream = &stream;
    return *this;
  }

  std::ostream* ReaderOptions::getErrorStream() const {
    return privateBits_->errorStream;
  }

  ReaderOptions& ReaderOptions::setCacheOptions(const CacheOptions& cacheOptions) {
    privateBits_->cacheOptions = cacheOptions;
    return *this;
  }

  const CacheOptions& ReaderOptions::getCacheOptions() const {
    return privateBits_->cacheOptions;
  }

  /**
   * RowReaderOptions Implementation
   */

  struct RowReaderOptionsPrivate {
    ColumnSelection selection;
    std::list<uint64_t> includedColumnIndexes;
    std::list<std::string> includedColumnNames;
    uint64_t dataStart;
    uint64_t dataLength;
    bool throwOnHive11DecimalOverflow;
    int32_t forcedScaleOnHive11Decimal;
    bool enableLazyDecoding;
    std::shared_ptr<SearchArgument> sargs;
    std::string readerTimezone;
    RowReaderOptions::IdReadIntentMap idReadIntentMap;
    bool useTightNumericVector;
    std::shared_ptr<Type> readType;
    bool throwOnSchemaEvolutionOverflow;

    RowReaderOptionsPrivate() {
      selection = ColumnSelection_NONE;
      dataStart = 0;
      dataLength = std::numeric_limits<uint64_t>::max();
      throwOnHive11DecimalOverflow = true;
      forcedScaleOnHive11Decimal = 6;
      enableLazyDecoding = false;
      readerTimezone = "GMT";
      useTightNumericVector = false;
      throwOnSchemaEvolutionOverflow = false;
    }
  };

  RowReaderOptions::RowReaderOptions() : privateBits_(std::make_unique<RowReaderOptionsPrivate>()) {
    // PASS
  }

  RowReaderOptions::RowReaderOptions(const RowReaderOptions& rhs)
      : privateBits_(std::make_unique<RowReaderOptionsPrivate>(*(rhs.privateBits_.get()))) {
    // PASS
  }

  RowReaderOptions::RowReaderOptions(RowReaderOptions& rhs) {
    // swap privateBits with rhs
    privateBits_.swap(rhs.privateBits_);
  }

  RowReaderOptions& RowReaderOptions::operator=(const RowReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits_.reset(new RowReaderOptionsPrivate(*(rhs.privateBits_.get())));
    }
    return *this;
  }

  RowReaderOptions::~RowReaderOptions() {
    // PASS
  }

  RowReaderOptions& RowReaderOptions::include(const std::list<uint64_t>& include) {
    privateBits_->selection = ColumnSelection_FIELD_IDS;
    privateBits_->includedColumnIndexes.assign(include.begin(), include.end());
    privateBits_->includedColumnNames.clear();
    privateBits_->idReadIntentMap.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::include(const std::list<std::string>& include) {
    privateBits_->selection = ColumnSelection_NAMES;
    privateBits_->includedColumnNames.assign(include.begin(), include.end());
    privateBits_->includedColumnIndexes.clear();
    privateBits_->idReadIntentMap.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::includeTypes(const std::list<uint64_t>& types) {
    privateBits_->selection = ColumnSelection_TYPE_IDS;
    privateBits_->includedColumnIndexes.assign(types.begin(), types.end());
    privateBits_->includedColumnNames.clear();
    privateBits_->idReadIntentMap.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::includeTypesWithIntents(
      const IdReadIntentMap& idReadIntentMap) {
    privateBits_->selection = ColumnSelection_TYPE_IDS;
    privateBits_->includedColumnIndexes.clear();
    privateBits_->idReadIntentMap.clear();
    for (const auto& typeIntentPair : idReadIntentMap) {
      privateBits_->idReadIntentMap[typeIntentPair.first] = typeIntentPair.second;
      privateBits_->includedColumnIndexes.push_back(typeIntentPair.first);
    }
    privateBits_->includedColumnNames.clear();
    return *this;
  }

  RowReaderOptions& RowReaderOptions::range(uint64_t offset, uint64_t length) {
    privateBits_->dataStart = offset;
    privateBits_->dataLength = length;
    return *this;
  }

  bool RowReaderOptions::getIndexesSet() const {
    return privateBits_->selection == ColumnSelection_FIELD_IDS;
  }

  bool RowReaderOptions::getTypeIdsSet() const {
    return privateBits_->selection == ColumnSelection_TYPE_IDS;
  }

  const std::list<uint64_t>& RowReaderOptions::getInclude() const {
    return privateBits_->includedColumnIndexes;
  }

  bool RowReaderOptions::getNamesSet() const {
    return privateBits_->selection == ColumnSelection_NAMES;
  }

  const std::list<std::string>& RowReaderOptions::getIncludeNames() const {
    return privateBits_->includedColumnNames;
  }

  uint64_t RowReaderOptions::getOffset() const {
    return privateBits_->dataStart;
  }

  uint64_t RowReaderOptions::getLength() const {
    return privateBits_->dataLength;
  }

  RowReaderOptions& RowReaderOptions::throwOnHive11DecimalOverflow(bool shouldThrow) {
    privateBits_->throwOnHive11DecimalOverflow = shouldThrow;
    return *this;
  }

  bool RowReaderOptions::getThrowOnHive11DecimalOverflow() const {
    return privateBits_->throwOnHive11DecimalOverflow;
  }

  RowReaderOptions& RowReaderOptions::throwOnSchemaEvolutionOverflow(bool shouldThrow) {
    privateBits_->throwOnSchemaEvolutionOverflow = shouldThrow;
    return *this;
  }

  bool RowReaderOptions::getThrowOnSchemaEvolutionOverflow() const {
    return privateBits_->throwOnSchemaEvolutionOverflow;
  }

  RowReaderOptions& RowReaderOptions::forcedScaleOnHive11Decimal(int32_t forcedScale) {
    privateBits_->forcedScaleOnHive11Decimal = forcedScale;
    return *this;
  }

  int32_t RowReaderOptions::getForcedScaleOnHive11Decimal() const {
    return privateBits_->forcedScaleOnHive11Decimal;
  }

  bool RowReaderOptions::getEnableLazyDecoding() const {
    return privateBits_->enableLazyDecoding;
  }

  RowReaderOptions& RowReaderOptions::setEnableLazyDecoding(bool enable) {
    privateBits_->enableLazyDecoding = enable;
    return *this;
  }

  RowReaderOptions& RowReaderOptions::searchArgument(std::unique_ptr<SearchArgument> sargs) {
    privateBits_->sargs = std::move(sargs);
    return *this;
  }

  std::shared_ptr<SearchArgument> RowReaderOptions::getSearchArgument() const {
    return privateBits_->sargs;
  }

  RowReaderOptions& RowReaderOptions::setTimezoneName(const std::string& zoneName) {
    privateBits_->readerTimezone = zoneName;
    return *this;
  }

  const std::string& RowReaderOptions::getTimezoneName() const {
    return privateBits_->readerTimezone;
  }

  const RowReaderOptions::IdReadIntentMap RowReaderOptions::getIdReadIntentMap() const {
    return privateBits_->idReadIntentMap;
  }

  RowReaderOptions& RowReaderOptions::setUseTightNumericVector(bool useTightNumericVector) {
    privateBits_->useTightNumericVector = useTightNumericVector;
    return *this;
  }

  bool RowReaderOptions::getUseTightNumericVector() const {
    return privateBits_->useTightNumericVector;
  }

  RowReaderOptions& RowReaderOptions::setReadType(std::shared_ptr<Type> type) {
    privateBits_->readType = std::move(type);
    return *this;
  }

  std::shared_ptr<Type>& RowReaderOptions::getReadType() const {
    return privateBits_->readType;
  }
}  // namespace orc

#endif
