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

#include "SargsApplier.hh"
#include "Dictionary.hh"
#include "sargs/PredicateLeaf.hh"

#include <numeric>
#include <set>

namespace orc {

  // find column id from column name
  uint64_t SargsApplier::findColumn(const Type& type, const std::string& colName) {
    for (uint64_t i = 0; i != type.getSubtypeCount(); ++i) {
      // Only STRUCT type has field names
      if (type.getKind() == STRUCT && type.getFieldName(i) == colName) {
        return type.getSubtype(i)->getColumnId();
      } else {
        uint64_t ret = findColumn(*type.getSubtype(i), colName);
        if (ret != INVALID_COLUMN_ID) {
          return ret;
        }
      }
    }
    return INVALID_COLUMN_ID;
  }

  SargsApplier::SargsApplier(const Type& type, const SearchArgument* searchArgument,
                             uint64_t rowIndexStride, WriterVersion writerVersion,
                             size_t dictionaryFilteringSizeThreshold, ReaderMetrics* metrics,
                             const SchemaEvolution* schemaEvolution)
      : type_(type),
        searchArgument_(searchArgument),
        schemaEvolution_(schemaEvolution),
        rowIndexStride_(rowIndexStride),
        writerVersion_(writerVersion),
        dictionaryFilteringSizeThreshold_(dictionaryFilteringSizeThreshold),
        hasEvaluatedFileStats_(false),
        fileStatsEvalResult_(true),
        metrics_(metrics) {
    const SearchArgumentImpl* sargs = dynamic_cast<const SearchArgumentImpl*>(searchArgument_);

    std::set<uint64_t> columnsWithInExpr;

    // find the mapping from predicate leaves to columns
    const std::vector<PredicateLeaf>& leaves = sargs->getLeaves();
    filterColumns_.resize(leaves.size(), INVALID_COLUMN_ID);
    for (size_t i = 0; i != filterColumns_.size(); ++i) {
      if (leaves[i].hasColumnName()) {
        filterColumns_[i] = findColumn(type, leaves[i].getColumnName());
      } else {
        filterColumns_[i] = leaves[i].getColumnId();
      }

      if (leaves[i].getOperator() == PredicateLeaf::Operator::IN) {
        uint64_t columnId = filterColumns_[i];
        if (columnId != INVALID_COLUMN_ID) {
          columnsWithInExpr.insert(columnId);
        }
      }
    }

    columnsWithInExpr_.assign(columnsWithInExpr.begin(), columnsWithInExpr.end());
  }

  bool SargsApplier::pickRowGroups(uint64_t rowsInStripe,
                                   const std::unordered_map<uint64_t, proto::RowIndex>& rowIndexes,
                                   const std::map<uint32_t, BloomFilterIndex>& bloomFilters) {
    // init state of each row group
    uint64_t groupsInStripe = (rowsInStripe + rowIndexStride_ - 1) / rowIndexStride_;
    nextSkippedRows_.resize(groupsInStripe);
    totalRowsInStripe_ = rowsInStripe;

    // row indexes do not exist, simply read all rows
    if (rowIndexes.empty()) {
      return true;
    }

    const auto& leaves = dynamic_cast<const SearchArgumentImpl*>(searchArgument_)->getLeaves();
    std::vector<TruthValue> leafValues(leaves.size(), TruthValue::YES_NO_NULL);
    hasSelected_ = false;
    hasSkipped_ = false;
    uint64_t nextSkippedRowGroup = groupsInStripe;
    size_t rowGroup = groupsInStripe;
    do {
      --rowGroup;
      for (size_t pred = 0; pred != leaves.size(); ++pred) {
        uint64_t columnIdx = filterColumns_[pred];
        auto rowIndexIter = rowIndexes.find(columnIdx);
        if (columnIdx == INVALID_COLUMN_ID || rowIndexIter == rowIndexes.cend()) {
          // this column does not exist in current file
          leafValues[pred] = TruthValue::YES_NO_NULL;
        } else if (schemaEvolution_ && !schemaEvolution_->isSafePPDConversion(columnIdx)) {
          // cannot evaluate predicate when ppd is not safe
          leafValues[pred] = TruthValue::YES_NO_NULL;
        } else {
          // get column statistics
          const proto::ColumnStatistics& statistics =
              rowIndexIter->second.entry(static_cast<int>(rowGroup)).statistics();

          // get bloom filter
          std::shared_ptr<BloomFilter> bloomFilter;
          auto iter = bloomFilters.find(static_cast<uint32_t>(columnIdx));
          if (iter != bloomFilters.cend()) {
            bloomFilter = iter->second.entries.at(rowGroup);
          }

          leafValues[pred] = leaves[pred].evaluate(writerVersion_, statistics, bloomFilter.get());
        }
      }

      bool needed = isNeeded(searchArgument_->evaluate(leafValues));
      if (!needed) {
        nextSkippedRows_[rowGroup] = 0;
        nextSkippedRowGroup = rowGroup;
      } else {
        nextSkippedRows_[rowGroup] = (nextSkippedRowGroup == groupsInStripe)
                                         ? rowsInStripe
                                         : (nextSkippedRowGroup * rowIndexStride_);
      }
      hasSelected_ |= needed;
      hasSkipped_ |= !needed;
    } while (rowGroup != 0);

    // update stats
    uint64_t selectedRGs = std::accumulate(
        nextSkippedRows_.cbegin(), nextSkippedRows_.cend(), 0UL,
        [](uint64_t initVal, uint64_t rg) { return rg > 0 ? initVal + 1 : initVal; });
    if (metrics_ != nullptr) {
      metrics_->SelectedRowGroupCount.fetch_add(selectedRGs);
      metrics_->EvaluatedRowGroupCount.fetch_add(groupsInStripe);
    }

    return hasSelected_;
  }

  bool SargsApplier::evaluateColumnStatistics(const PbColumnStatistics& colStats) const {
    const SearchArgumentImpl* sargs = dynamic_cast<const SearchArgumentImpl*>(searchArgument_);
    if (sargs == nullptr) {
      throw InvalidArgument("Failed to cast to SearchArgumentImpl");
    }

    const std::vector<PredicateLeaf>& leaves = sargs->getLeaves();
    std::vector<TruthValue> leafValues(leaves.size(), TruthValue::YES_NO_NULL);

    for (size_t pred = 0; pred != leaves.size(); ++pred) {
      uint64_t columnId = filterColumns_[pred];
      if (columnId != INVALID_COLUMN_ID && colStats.size() > static_cast<int>(columnId)) {
        leafValues[pred] = leaves[pred].evaluate(writerVersion_,
                                                 colStats.Get(static_cast<int>(columnId)), nullptr);
      }
    }

    return isNeeded(searchArgument_->evaluate(leafValues));
  }

  bool SargsApplier::evaluateStripeStatistics(const proto::StripeStatistics& stripeStats,
                                              uint64_t stripeRowGroupCount) {
    if (stripeStats.col_stats_size() == 0) {
      return true;
    }

    bool ret = evaluateColumnStatistics(stripeStats.col_stats());
    if (metrics_ != nullptr) {
      metrics_->EvaluatedRowGroupCount.fetch_add(stripeRowGroupCount);
    }
    if (!ret) {
      // reset mNextSkippedRows when the current stripe does not satisfy the PPD
      nextSkippedRows_.clear();
    }
    return ret;
  }

  bool SargsApplier::evaluateFileStatistics(const proto::Footer& footer,
                                            uint64_t numRowGroupsInStripeRange) {
    if (!hasEvaluatedFileStats_) {
      if (footer.statistics_size() == 0) {
        fileStatsEvalResult_ = true;
      } else {
        fileStatsEvalResult_ = evaluateColumnStatistics(footer.statistics());
        if (metrics_ != nullptr) {
          metrics_->EvaluatedRowGroupCount.fetch_add(numRowGroupsInStripeRange);
        }
      }
      hasEvaluatedFileStats_ = true;
    }
    return fileStatsEvalResult_;
  }

  TruthValue SargsApplier::evaluateDictionaryForColumn(const StringDictionary& dictionary,
                                                       const PredicateLeaf& leaf) const {
    // Only handle IN expressions for dictionary filtering
    if (leaf.getOperator() != PredicateLeaf::Operator::IN) {
      return TruthValue::YES_NO_NULL;
    }

    const std::vector<Literal>& literals = leaf.getLiteralList();
    if (literals.empty()) {
      return TruthValue::YES_NO_NULL;
    }

    // Pre-compute string views for literals to avoid repeated function calls
    std::vector<std::string_view> literalViews;
    literalViews.reserve(literals.size());
    for (const auto& literal : literals) {
      literalViews.emplace_back(literal.getStringView());
    }

    // Check if any dictionary entry matches any literal in the IN list
    const int64_t* offsets = dictionary.dictionaryOffset.data();
    const char* blob = dictionary.dictionaryBlob.data();
    size_t dictSize = dictionary.dictionaryOffset.size() - 1;

    // Use a set to store matching dictionary entries
    size_t matchedEntriesCount = 0;

    for (size_t i = 0; i < dictSize; ++i) {
      int64_t start = offsets[i];
      int64_t length = offsets[i + 1] - start;
      std::string_view dictEntry(blob + start, static_cast<size_t>(length));

      // Check if this dictionary entry matches any literal in the IN list
      for (const auto& literalView : literalViews) {
        if (dictEntry == literalView) {
          matchedEntriesCount++;
          break;
        }
      }
    }

    // If all dictionary entries match, return YES
    if (matchedEntriesCount == dictSize) {
      return TruthValue::YES;
    }
    // If any dictionary entry matches, stripe might contain matching rows
    else if (matchedEntriesCount != 0) {
      return TruthValue::YES_NO_NULL;
    }
    // No dictionary entry matches any literal in the IN list - skip stripe
    else {
      return TruthValue::NO;
    }
  }

  bool SargsApplier::evaluateColumnDictionaries(
      const std::unordered_map<uint64_t, std::shared_ptr<StringDictionary>>& dictionaries) {
    const SearchArgumentImpl* sargs = dynamic_cast<const SearchArgumentImpl*>(searchArgument_);
    if (sargs == nullptr) {
      return true;  // Cannot evaluate, assume stripe is needed
    }

    const std::vector<PredicateLeaf>& leaves = sargs->getLeaves();
    std::vector<TruthValue> leafValues(leaves.size(), TruthValue::YES_NO_NULL);

    // Evaluate each predicate leaf against dictionaries (only IN expressions)
    for (size_t pred = 0; pred != leaves.size(); ++pred) {
      uint64_t columnId = filterColumns_[pred];

      // Only evaluate IN expressions
      if (leaves[pred].getOperator() != PredicateLeaf::Operator::IN) {
        leafValues[pred] = TruthValue::YES_NO_NULL;
        continue;
      }

      // Check if this column has a dictionary
      auto dictIter = dictionaries.find(columnId);
      if (columnId == INVALID_COLUMN_ID || dictIter == dictionaries.cend() ||
          dictIter->second == nullptr) {
        // No dictionary for this column, cannot evaluate
        leafValues[pred] = TruthValue::YES_NO_NULL;
        continue;
      }

      // Check if schema evolution makes PPD unsafe
      if (schemaEvolution_ && !schemaEvolution_->isSafePPDConversion(columnId)) {
        leafValues[pred] = TruthValue::YES_NO_NULL;
        continue;
      }

      // Check if this is a STRING/VARCHAR/CHAR column
      PredicateDataType predType = leaves[pred].getType();
      if (predType != PredicateDataType::STRING) {
        // Only string types are supported for dictionary filtering
        leafValues[pred] = TruthValue::YES_NO_NULL;
        continue;
      }

      // Evaluate the IN expression against the dictionary
      leafValues[pred] = evaluateDictionaryForColumn(*dictIter->second, leaves[pred]);
    }

    return isNeeded(searchArgument_->evaluate(leafValues));
  }

}  // namespace orc
