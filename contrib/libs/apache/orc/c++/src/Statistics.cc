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

#include "Statistics.hh"
#include "RLE.hh"
#include "orc/Exceptions.hh"

#include "wrap/coded-stream-wrapper.h"

namespace orc {

  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s,
                                            const StatContext& statContext) {
    if (s.has_int_statistics()) {
      return new IntegerColumnStatisticsImpl(s);
    } else if (s.has_double_statistics()) {
      return new DoubleColumnStatisticsImpl(s);
    } else if (s.has_collection_statistics()) {
      return new CollectionColumnStatisticsImpl(s);
    } else if (s.has_string_statistics()) {
      return new StringColumnStatisticsImpl(s, statContext);
    } else if (s.has_bucket_statistics()) {
      return new BooleanColumnStatisticsImpl(s, statContext);
    } else if (s.has_decimal_statistics()) {
      return new DecimalColumnStatisticsImpl(s, statContext);
    } else if (s.has_timestamp_statistics()) {
      return new TimestampColumnStatisticsImpl(s, statContext);
    } else if (s.has_date_statistics()) {
      return new DateColumnStatisticsImpl(s, statContext);
    } else if (s.has_binary_statistics()) {
      return new BinaryColumnStatisticsImpl(s, statContext);
    } else {
      return new ColumnStatisticsImpl(s);
    }
  }

  StatisticsImpl::StatisticsImpl(const proto::StripeStatistics& stripeStats,
                                 const StatContext& statContext) {
    for (int i = 0; i < stripeStats.col_stats_size(); i++) {
      colStats.push_back(convertColumnStatistics(stripeStats.col_stats(i), statContext));
    }
  }

  StatisticsImpl::StatisticsImpl(const proto::Footer& footer, const StatContext& statContext) {
    for (int i = 0; i < footer.statistics_size(); i++) {
      colStats.push_back(convertColumnStatistics(footer.statistics(i), statContext));
    }
  }

  StatisticsImpl::~StatisticsImpl() {
    for (std::vector<ColumnStatistics*>::iterator ptr = colStats.begin(); ptr != colStats.end();
         ++ptr) {
      delete *ptr;
    }
  }

  Statistics::~Statistics() {
    // PASS
  }

  StripeStatistics::~StripeStatistics() {
    // PASS
  }

  StripeStatisticsImpl::~StripeStatisticsImpl() {
    // PASS
  }

  StripeStatisticsImpl::StripeStatisticsImpl(
      const proto::StripeStatistics& stripeStats,
      std::vector<std::vector<proto::ColumnStatistics> >& indexStats,
      const StatContext& statContext) {
    columnStats = std::make_unique<StatisticsImpl>(stripeStats, statContext);
    rowIndexStats.resize(indexStats.size());
    for (size_t i = 0; i < rowIndexStats.size(); i++) {
      for (size_t j = 0; j < indexStats[i].size(); j++) {
        rowIndexStats[i].push_back(std::shared_ptr<const ColumnStatistics>(
            convertColumnStatistics(indexStats[i][j], statContext)));
      }
    }
  }

  ColumnStatistics::~ColumnStatistics() {
    // PASS
  }

  BinaryColumnStatistics::~BinaryColumnStatistics() {
    // PASS
  }

  BooleanColumnStatistics::~BooleanColumnStatistics() {
    // PASS
  }

  DateColumnStatistics::~DateColumnStatistics() {
    // PASS
  }

  DecimalColumnStatistics::~DecimalColumnStatistics() {
    // PASS
  }

  DoubleColumnStatistics::~DoubleColumnStatistics() {
    // PASS
  }

  IntegerColumnStatistics::~IntegerColumnStatistics() {
    // PASS
  }

  StringColumnStatistics::~StringColumnStatistics() {
    // PASS
  }

  TimestampColumnStatistics::~TimestampColumnStatistics() {
    // PASS
  }

  CollectionColumnStatistics::~CollectionColumnStatistics() {
    // PASS
  }

  MutableColumnStatistics::~MutableColumnStatistics() {
    // PASS
  }

  ColumnStatisticsImpl::~ColumnStatisticsImpl() {
    // PASS
  }

  BinaryColumnStatisticsImpl::~BinaryColumnStatisticsImpl() {
    // PASS
  }

  BooleanColumnStatisticsImpl::~BooleanColumnStatisticsImpl() {
    // PASS
  }

  DateColumnStatisticsImpl::~DateColumnStatisticsImpl() {
    // PASS
  }

  DecimalColumnStatisticsImpl::~DecimalColumnStatisticsImpl() {
    // PASS
  }

  DoubleColumnStatisticsImpl::~DoubleColumnStatisticsImpl() {
    // PASS
  }

  IntegerColumnStatisticsImpl::~IntegerColumnStatisticsImpl() {
    // PASS
  }

  CollectionColumnStatisticsImpl::~CollectionColumnStatisticsImpl() {
    // PASS
  }

  StringColumnStatisticsImpl::~StringColumnStatisticsImpl() {
    // PASS
  }

  TimestampColumnStatisticsImpl::~TimestampColumnStatisticsImpl() {
    // PASS
  }

  ColumnStatisticsImpl::ColumnStatisticsImpl(const proto::ColumnStatistics& pb) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
  }

  BinaryColumnStatisticsImpl::BinaryColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                         const StatContext& statContext) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (pb.has_binary_statistics() && statContext.correctStats) {
      _stats.setHasTotalLength(pb.binary_statistics().has_sum());
      _stats.setTotalLength(static_cast<uint64_t>(pb.binary_statistics().sum()));
    }
  }

  BooleanColumnStatisticsImpl::BooleanColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                           const StatContext& statContext) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (pb.has_bucket_statistics() && statContext.correctStats) {
      _hasCount = true;
      _trueCount = pb.bucket_statistics().count(0);
    } else {
      _hasCount = false;
      _trueCount = 0;
    }
  }

  DateColumnStatisticsImpl::DateColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                     const StatContext& statContext) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (!pb.has_date_statistics() || !statContext.correctStats) {
      // hasMinimum_ is false by default;
      // hasMaximum_ is false by default;
      _stats.setMinimum(0);
      _stats.setMaximum(0);
    } else {
      _stats.setHasMinimum(pb.date_statistics().has_minimum());
      _stats.setHasMaximum(pb.date_statistics().has_maximum());
      _stats.setMinimum(pb.date_statistics().minimum());
      _stats.setMaximum(pb.date_statistics().maximum());
    }
  }

  DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                           const StatContext& statContext) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (pb.has_decimal_statistics() && statContext.correctStats) {
      const proto::DecimalStatistics& stats = pb.decimal_statistics();
      _stats.setHasMinimum(stats.has_minimum());
      _stats.setHasMaximum(stats.has_maximum());
      _stats.setHasSum(stats.has_sum());

      _stats.setMinimum(Decimal(stats.minimum()));
      _stats.setMaximum(Decimal(stats.maximum()));
      _stats.setSum(Decimal(stats.sum()));
    }
  }

  DoubleColumnStatisticsImpl::DoubleColumnStatisticsImpl(const proto::ColumnStatistics& pb) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (!pb.has_double_statistics()) {
      _stats.setMinimum(0);
      _stats.setMaximum(0);
      _stats.setSum(0);
    } else {
      const proto::DoubleStatistics& stats = pb.double_statistics();
      _stats.setHasMinimum(stats.has_minimum());
      _stats.setHasMaximum(stats.has_maximum());
      _stats.setHasSum(stats.has_sum());

      _stats.setMinimum(stats.minimum());
      _stats.setMaximum(stats.maximum());
      _stats.setSum(stats.sum());
    }
  }

  IntegerColumnStatisticsImpl::IntegerColumnStatisticsImpl(const proto::ColumnStatistics& pb) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (!pb.has_int_statistics()) {
      _stats.setMinimum(0);
      _stats.setMaximum(0);
      _stats.setSum(0);
    } else {
      const proto::IntegerStatistics& stats = pb.int_statistics();
      _stats.setHasMinimum(stats.has_minimum());
      _stats.setHasMaximum(stats.has_maximum());
      _stats.setHasSum(stats.has_sum());

      _stats.setMinimum(stats.minimum());
      _stats.setMaximum(stats.maximum());
      _stats.setSum(stats.sum());
    }
  }

  StringColumnStatisticsImpl::StringColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                         const StatContext& statContext) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (!pb.has_string_statistics() || !statContext.correctStats) {
      _stats.setTotalLength(0);
    } else {
      const proto::StringStatistics& stats = pb.string_statistics();
      _stats.setHasMinimum(stats.has_minimum());
      _stats.setHasMaximum(stats.has_maximum());
      _stats.setHasTotalLength(stats.has_sum());

      _stats.setMinimum(stats.minimum());
      _stats.setMaximum(stats.maximum());
      _stats.setTotalLength(static_cast<uint64_t>(stats.sum()));
    }
  }

  TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                               const StatContext& statContext) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (!pb.has_timestamp_statistics() || !statContext.correctStats) {
      _stats.setMinimum(0);
      _stats.setMaximum(0);
      _lowerBound = 0;
      _upperBound = 0;
      _minimumNanos = DEFAULT_MIN_NANOS;
      _maximumNanos = DEFAULT_MAX_NANOS;
    } else {
      const proto::TimestampStatistics& stats = pb.timestamp_statistics();
      _stats.setHasMinimum(stats.has_minimum_utc() ||
                           (stats.has_minimum() && (statContext.writerTimezone != nullptr)));
      _stats.setHasMaximum(stats.has_maximum_utc() ||
                           (stats.has_maximum() && (statContext.writerTimezone != nullptr)));
      _hasLowerBound = stats.has_minimum_utc() || stats.has_minimum();
      _hasUpperBound = stats.has_maximum_utc() || stats.has_maximum();
      // to be consistent with java side, non-default minimum_nanos and maximum_nanos
      // are added by one in their serialized form.
      _minimumNanos = stats.has_minimum_nanos() ? stats.minimum_nanos() - 1 : DEFAULT_MIN_NANOS;
      _maximumNanos = stats.has_maximum_nanos() ? stats.maximum_nanos() - 1 : DEFAULT_MAX_NANOS;

      // Timestamp stats are stored in milliseconds
      if (stats.has_minimum_utc()) {
        int64_t minimum = stats.minimum_utc();
        _stats.setMinimum(minimum);
        _lowerBound = minimum;
      } else if (statContext.writerTimezone) {
        int64_t writerTimeSec = stats.minimum() / 1000;
        // multiply the offset by 1000 to convert to millisecond
        int64_t minimum = stats.minimum() +
                          (statContext.writerTimezone->getVariant(writerTimeSec).gmtOffset) * 1000;
        _stats.setMinimum(minimum);
        _lowerBound = minimum;
      } else {
        _stats.setMinimum(0);
        // subtract 1 day 1 hour (25 hours) in milliseconds to handle unknown
        // TZ and daylight savings
        _lowerBound = stats.minimum() - (25 * SECONDS_PER_HOUR * 1000);
      }

      // Timestamp stats are stored in milliseconds
      if (stats.has_maximum_utc()) {
        int64_t maximum = stats.maximum_utc();
        _stats.setMaximum(maximum);
        _upperBound = maximum;
      } else if (statContext.writerTimezone) {
        int64_t writerTimeSec = stats.maximum() / 1000;
        // multiply the offset by 1000 to convert to millisecond
        int64_t maximum = stats.maximum() +
                          (statContext.writerTimezone->getVariant(writerTimeSec).gmtOffset) * 1000;
        _stats.setMaximum(maximum);
        _upperBound = maximum;
      } else {
        _stats.setMaximum(0);
        // add 1 day 1 hour (25 hours) in milliseconds to handle unknown
        // TZ and daylight savings
        _upperBound = stats.maximum() + (25 * SECONDS_PER_HOUR * 1000);
      }
      // Add 1 millisecond to account for microsecond precision of values
      _upperBound += 1;
    }
  }

  CollectionColumnStatisticsImpl::CollectionColumnStatisticsImpl(
      const proto::ColumnStatistics& pb) {
    _stats.setNumberOfValues(pb.number_of_values());
    _stats.setHasNull(pb.has_null());
    if (!pb.has_collection_statistics()) {
      _stats.setMinimum(0);
      _stats.setMaximum(0);
      _stats.setSum(0);
    } else {
      const proto::CollectionStatistics& stats = pb.collection_statistics();
      _stats.setHasMinimum(stats.has_min_children());
      _stats.setHasMaximum(stats.has_max_children());
      _stats.setHasSum(stats.has_total_children());

      _stats.setMinimum(stats.min_children());
      _stats.setMaximum(stats.max_children());
      _stats.setSum(stats.total_children());
    }
  }

  std::unique_ptr<MutableColumnStatistics> createColumnStatistics(const Type& type) {
    switch (static_cast<int64_t>(type.getKind())) {
      case BOOLEAN:
        return std::make_unique<BooleanColumnStatisticsImpl>();
      case BYTE:
      case INT:
      case LONG:
      case SHORT:
        return std::make_unique<IntegerColumnStatisticsImpl>();
      case MAP:
      case LIST:
        return std::make_unique<CollectionColumnStatisticsImpl>();
      case STRUCT:
      case UNION:
        return std::make_unique<ColumnStatisticsImpl>();
      case FLOAT:
      case DOUBLE:
        return std::make_unique<DoubleColumnStatisticsImpl>();
      case BINARY:
        return std::make_unique<BinaryColumnStatisticsImpl>();
      case STRING:
      case CHAR:
      case VARCHAR:
        return std::make_unique<StringColumnStatisticsImpl>();
      case DATE:
        return std::make_unique<DateColumnStatisticsImpl>();
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return std::make_unique<TimestampColumnStatisticsImpl>();
      case DECIMAL:
        return std::make_unique<DecimalColumnStatisticsImpl>();
      default:
        throw NotImplementedYet("Not supported type: " + type.toString());
    }
  }

}  // namespace orc
