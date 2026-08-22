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
    } else if (s.has_geospatial_statistics()) {
      return new GeospatialColumnStatisticsImpl(s);
    } else {
      return new ColumnStatisticsImpl(s);
    }
  }

  StatisticsImpl::StatisticsImpl(const proto::StripeStatistics& stripeStats,
                                 const StatContext& statContext) {
    for (int i = 0; i < stripeStats.col_stats_size(); i++) {
      colStats_.push_back(convertColumnStatistics(stripeStats.col_stats(i), statContext));
    }
  }

  StatisticsImpl::StatisticsImpl(const proto::Footer& footer, const StatContext& statContext) {
    for (int i = 0; i < footer.statistics_size(); i++) {
      colStats_.push_back(convertColumnStatistics(footer.statistics(i), statContext));
    }
  }

  StatisticsImpl::~StatisticsImpl() {
    for (std::vector<ColumnStatistics*>::iterator ptr = colStats_.begin(); ptr != colStats_.end();
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

  StripeStatisticsImpl::StripeStatisticsImpl(const proto::StripeStatistics& stripeStats,
                                             const StatContext& statContext) {
    columnStats_ = std::make_unique<StatisticsImpl>(stripeStats, statContext);
  }

  StripeStatisticsWithRowGroupIndexImpl::~StripeStatisticsWithRowGroupIndexImpl() {
    // PASS
  }

  StripeStatisticsWithRowGroupIndexImpl::StripeStatisticsWithRowGroupIndexImpl(
      const proto::StripeStatistics& stripeStats,
      std::vector<std::vector<proto::ColumnStatistics> >& indexStats,
      const StatContext& statContext)
      : StripeStatisticsImpl(stripeStats, statContext) {
    rowIndexStats_.resize(indexStats.size());
    for (size_t i = 0; i < rowIndexStats_.size(); i++) {
      for (size_t j = 0; j < indexStats[i].size(); j++) {
        rowIndexStats_[i].push_back(std::shared_ptr<const ColumnStatistics>(
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

  GeospatialColumnStatistics::~GeospatialColumnStatistics() {
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

  GeospatialColumnStatisticsImpl::~GeospatialColumnStatisticsImpl() {
    // PASS
  }

  ColumnStatisticsImpl::ColumnStatisticsImpl(const proto::ColumnStatistics& pb) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
  }

  BinaryColumnStatisticsImpl::BinaryColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                         const StatContext& statContext) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (pb.has_binary_statistics() && statContext.correctStats) {
      stats_.setHasTotalLength(pb.binary_statistics().has_sum());
      stats_.setTotalLength(static_cast<uint64_t>(pb.binary_statistics().sum()));
    }
  }

  BooleanColumnStatisticsImpl::BooleanColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                           const StatContext& statContext) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (pb.has_bucket_statistics() && statContext.correctStats) {
      hasCount_ = true;
      trueCount_ = pb.bucket_statistics().count(0);
    } else {
      hasCount_ = false;
      trueCount_ = 0;
    }
  }

  DateColumnStatisticsImpl::DateColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                     const StatContext& statContext) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (!pb.has_date_statistics() || !statContext.correctStats) {
      // hasMinimum_ is false by default;
      // hasMaximum_ is false by default;
      stats_.setMinimum(0);
      stats_.setMaximum(0);
    } else {
      stats_.setHasMinimum(pb.date_statistics().has_minimum());
      stats_.setHasMaximum(pb.date_statistics().has_maximum());
      stats_.setMinimum(pb.date_statistics().minimum());
      stats_.setMaximum(pb.date_statistics().maximum());
    }
  }

  DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                           const StatContext& statContext) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (pb.has_decimal_statistics() && statContext.correctStats) {
      const proto::DecimalStatistics& stats = pb.decimal_statistics();
      stats_.setHasMinimum(stats.has_minimum());
      stats_.setHasMaximum(stats.has_maximum());
      stats_.setHasSum(stats.has_sum());

      stats_.setMinimum(Decimal(stats.minimum()));
      stats_.setMaximum(Decimal(stats.maximum()));
      stats_.setSum(Decimal(stats.sum()));
    }
  }

  DoubleColumnStatisticsImpl::DoubleColumnStatisticsImpl(const proto::ColumnStatistics& pb) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (!pb.has_double_statistics()) {
      stats_.setMinimum(0);
      stats_.setMaximum(0);
      stats_.setSum(0);
    } else {
      const proto::DoubleStatistics& stats = pb.double_statistics();
      stats_.setHasMinimum(stats.has_minimum());
      stats_.setHasMaximum(stats.has_maximum());
      stats_.setHasSum(stats.has_sum());

      stats_.setMinimum(stats.minimum());
      stats_.setMaximum(stats.maximum());
      stats_.setSum(stats.sum());
    }
  }

  IntegerColumnStatisticsImpl::IntegerColumnStatisticsImpl(const proto::ColumnStatistics& pb) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (!pb.has_int_statistics()) {
      stats_.setMinimum(0);
      stats_.setMaximum(0);
      stats_.setSum(0);
    } else {
      const proto::IntegerStatistics& stats = pb.int_statistics();
      stats_.setHasMinimum(stats.has_minimum());
      stats_.setHasMaximum(stats.has_maximum());
      stats_.setHasSum(stats.has_sum());

      stats_.setMinimum(stats.minimum());
      stats_.setMaximum(stats.maximum());
      stats_.setSum(stats.sum());
    }
  }

  StringColumnStatisticsImpl::StringColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                         const StatContext& statContext) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (!pb.has_string_statistics() || !statContext.correctStats) {
      stats_.setTotalLength(0);
    } else {
      const proto::StringStatistics& stats = pb.string_statistics();
      stats_.setHasMinimum(stats.has_minimum());
      stats_.setHasMaximum(stats.has_maximum());
      stats_.setHasTotalLength(stats.has_sum());

      stats_.setMinimum(stats.minimum());
      stats_.setMaximum(stats.maximum());
      stats_.setTotalLength(static_cast<uint64_t>(stats.sum()));
    }
  }

  TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl(const proto::ColumnStatistics& pb,
                                                               const StatContext& statContext) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (!pb.has_timestamp_statistics() || !statContext.correctStats) {
      stats_.setMinimum(0);
      stats_.setMaximum(0);
      lowerBound_ = 0;
      upperBound_ = 0;
      minimumNanos_ = DEFAULT_MIN_NANOS;
      maximumNanos_ = DEFAULT_MAX_NANOS;
    } else {
      const proto::TimestampStatistics& stats = pb.timestamp_statistics();
      stats_.setHasMinimum(stats.has_minimum_utc() ||
                           (stats.has_minimum() && (statContext.writerTimezone != nullptr)));
      stats_.setHasMaximum(stats.has_maximum_utc() ||
                           (stats.has_maximum() && (statContext.writerTimezone != nullptr)));
      hasLowerBound_ = stats.has_minimum_utc() || stats.has_minimum();
      hasUpperBound_ = stats.has_maximum_utc() || stats.has_maximum();
      // to be consistent with java side, non-default minimum_nanos and maximum_nanos
      // are added by one in their serialized form.
      minimumNanos_ = stats.has_minimum_nanos() ? stats.minimum_nanos() - 1 : DEFAULT_MIN_NANOS;
      maximumNanos_ = stats.has_maximum_nanos() ? stats.maximum_nanos() - 1 : DEFAULT_MAX_NANOS;

      // Timestamp stats are stored in milliseconds
      if (stats.has_minimum_utc()) {
        int64_t minimum = stats.minimum_utc();
        stats_.setMinimum(minimum);
        lowerBound_ = minimum;
      } else if (statContext.writerTimezone) {
        int64_t writerTimeSec = stats.minimum() / 1000;
        // multiply the offset by 1000 to convert to millisecond
        int64_t minimum = stats.minimum() +
                          (statContext.writerTimezone->getVariant(writerTimeSec).gmtOffset) * 1000;
        stats_.setMinimum(minimum);
        lowerBound_ = minimum;
      } else {
        stats_.setMinimum(0);
        // subtract 1 day 1 hour (25 hours) in milliseconds to handle unknown
        // TZ and daylight savings
        lowerBound_ = stats.minimum() - (25 * SECONDS_PER_HOUR * 1000);
      }

      // Timestamp stats are stored in milliseconds
      if (stats.has_maximum_utc()) {
        int64_t maximum = stats.maximum_utc();
        stats_.setMaximum(maximum);
        upperBound_ = maximum;
      } else if (statContext.writerTimezone) {
        int64_t writerTimeSec = stats.maximum() / 1000;
        // multiply the offset by 1000 to convert to millisecond
        int64_t maximum = stats.maximum() +
                          (statContext.writerTimezone->getVariant(writerTimeSec).gmtOffset) * 1000;
        stats_.setMaximum(maximum);
        upperBound_ = maximum;
      } else {
        stats_.setMaximum(0);
        // add 1 day 1 hour (25 hours) in milliseconds to handle unknown
        // TZ and daylight savings
        upperBound_ = stats.maximum() + (25 * SECONDS_PER_HOUR * 1000);
      }
      // Add 1 millisecond to account for microsecond precision of values
      upperBound_ += 1;
    }
  }

  CollectionColumnStatisticsImpl::CollectionColumnStatisticsImpl(
      const proto::ColumnStatistics& pb) {
    stats_.setNumberOfValues(pb.number_of_values());
    stats_.setHasNull(pb.has_has_null() ? pb.has_null() : true);
    if (!pb.has_collection_statistics()) {
      stats_.setMinimum(0);
      stats_.setMaximum(0);
      stats_.setSum(0);
    } else {
      const proto::CollectionStatistics& stats = pb.collection_statistics();
      stats_.setHasMinimum(stats.has_min_children());
      stats_.setHasMaximum(stats.has_max_children());
      stats_.setHasSum(stats.has_total_children());

      stats_.setMinimum(stats.min_children());
      stats_.setMaximum(stats.max_children());
      stats_.setSum(stats.total_children());
    }
  }

  GeospatialColumnStatisticsImpl::GeospatialColumnStatisticsImpl(
      const proto::ColumnStatistics& pb) {
    reset();
    if (!pb.has_geospatial_statistics()) {
      bounder_.invalidate();
    } else {
      const proto::GeospatialStatistics& stats = pb.geospatial_statistics();
      geospatial::BoundingBox::XYZM min;
      geospatial::BoundingBox::XYZM max;
      for (int i = 0; i < geospatial::MAX_DIMENSIONS; i++) {
        min[i] = max[i] = std::numeric_limits<double>::quiet_NaN();
      }
      if (stats.has_bbox()) {
        const auto& protoBBox = stats.bbox();
        min[0] = protoBBox.xmin();
        min[1] = protoBBox.ymin();
        max[0] = protoBBox.xmax();
        max[1] = protoBBox.ymax();
        if (protoBBox.has_zmin() && protoBBox.has_zmax()) {
          min[2] = protoBBox.zmin();
          max[2] = protoBBox.zmax();
        }
        if (protoBBox.has_mmin() && protoBBox.has_mmax()) {
          min[3] = protoBBox.mmin();
          max[3] = protoBBox.mmax();
        }
      }
      bounder_.mergeBox(geospatial::BoundingBox(min, max));
      std::vector<int32_t> types = {stats.geospatial_types().begin(),
                                    stats.geospatial_types().end()};
      bounder_.mergeGeometryTypes(types);
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
      case GEOGRAPHY:
      case GEOMETRY:
        return std::make_unique<GeospatialColumnStatisticsImpl>();
      default:
        throw NotImplementedYet("Not supported type: " + type.toString());
    }
  }

}  // namespace orc
