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

#ifndef ORC_STATISTICS_IMPL_HH
#define ORC_STATISTICS_IMPL_HH

#include "orc/Common.hh"
#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "Geospatial.hh"
#include "Timezone.hh"
#include "TypeImpl.hh"

namespace orc {

  /**
   * StatContext contains fields required to compute statistics
   */

  struct StatContext {
    const bool correctStats;
    const Timezone* const writerTimezone;
    StatContext() : correctStats(false), writerTimezone(nullptr) {}
    StatContext(bool cStat, const Timezone* const timezone = nullptr)
        : correctStats(cStat), writerTimezone(timezone) {}
  };

  /**
   * Internal Statistics Implementation
   */

  template <typename T>
  class InternalStatisticsImpl {
   private:
    bool hasNull_;
    bool hasMinimum_;
    bool hasMaximum_;
    bool hasSum_;
    bool hasTotalLength_;
    uint64_t totalLength_;
    uint64_t valueCount_;
    T minimum_;
    T maximum_;
    T sum_;

   public:
    InternalStatisticsImpl() {
      hasNull_ = false;
      hasMinimum_ = false;
      hasMaximum_ = false;
      hasSum_ = false;
      hasTotalLength_ = false;
      totalLength_ = 0;
      valueCount_ = 0;
    }

    ~InternalStatisticsImpl() {}

    // GET / SET _totalLength
    bool hasTotalLength() const {
      return hasTotalLength_;
    }

    void setHasTotalLength(bool hasTotalLength) {
      hasTotalLength_ = hasTotalLength;
    }

    uint64_t getTotalLength() const {
      return totalLength_;
    }

    void setTotalLength(uint64_t totalLength) {
      totalLength_ = totalLength;
    }

    // GET / SET _sum
    bool hasSum() const {
      return hasSum_;
    }

    void setHasSum(bool hasSum) {
      hasSum_ = hasSum;
    }

    T getSum() const {
      return sum_;
    }

    void setSum(T sum) {
      sum_ = sum;
    }

    // GET / SET _maximum
    bool hasMaximum() const {
      return hasMaximum_;
    }

    const T& getMaximum() const {
      return maximum_;
    }

    void setHasMaximum(bool hasMax) {
      hasMaximum_ = hasMax;
    }

    void setMaximum(T max) {
      maximum_ = max;
    }

    // GET / SET _minimum
    bool hasMinimum() const {
      return hasMinimum_;
    }

    void setHasMinimum(bool hasMin) {
      hasMinimum_ = hasMin;
    }

    const T& getMinimum() const {
      return minimum_;
    }

    void setMinimum(T min) {
      minimum_ = min;
    }

    // GET / SET _valueCount
    uint64_t getNumberOfValues() const {
      return valueCount_;
    }

    void setNumberOfValues(uint64_t numValues) {
      valueCount_ = numValues;
    }

    // GET / SET _hasNullValue
    bool hasNull() const {
      return hasNull_;
    }

    void setHasNull(bool hasNull) {
      hasNull_ = hasNull;
    }

    void reset() {
      hasNull_ = false;
      hasMinimum_ = false;
      hasMaximum_ = false;
      hasSum_ = false;
      hasTotalLength_ = false;
      totalLength_ = 0;
      valueCount_ = 0;
    }

    void updateMinMax(T value) {
      if (!hasMinimum_) {
        hasMinimum_ = hasMaximum_ = true;
        minimum_ = maximum_ = value;
      } else if (compare(value, minimum_)) {
        minimum_ = value;
      } else if (compare(maximum_, value)) {
        maximum_ = value;
      }
    }

    // sum is not merged here as we need to check overflow
    void merge(const InternalStatisticsImpl& other) {
      hasNull_ = hasNull_ || other.hasNull_;
      valueCount_ += other.valueCount_;

      if (other.hasMinimum_) {
        if (!hasMinimum_) {
          hasMinimum_ = hasMaximum_ = true;
          minimum_ = other.minimum_;
          maximum_ = other.maximum_;
        } else {
          // all template types should support operator<
          if (compare(maximum_, other.maximum_)) {
            maximum_ = other.maximum_;
          }
          if (compare(other.minimum_, minimum_)) {
            minimum_ = other.minimum_;
          }
        }
      }

      hasTotalLength_ = hasTotalLength_ && other.hasTotalLength_;
      totalLength_ += other.totalLength_;
    }
  };

  typedef InternalStatisticsImpl<char> InternalCharStatistics;
  typedef InternalStatisticsImpl<char> InternalBooleanStatistics;
  typedef InternalStatisticsImpl<int64_t> InternalIntegerStatistics;
  typedef InternalStatisticsImpl<int32_t> InternalDateStatistics;
  typedef InternalStatisticsImpl<double> InternalDoubleStatistics;
  typedef InternalStatisticsImpl<Decimal> InternalDecimalStatistics;
  typedef InternalStatisticsImpl<std::string> InternalStringStatistics;
  typedef InternalStatisticsImpl<uint64_t> InternalCollectionStatistics;

  /**
   * Mutable column statistics for use by the writer.
   */
  class MutableColumnStatistics {
   public:
    virtual ~MutableColumnStatistics();

    virtual void increase(uint64_t count) = 0;

    virtual void setNumberOfValues(uint64_t value) = 0;

    virtual void setHasNull(bool hasNull) = 0;

    virtual void merge(const MutableColumnStatistics& other) = 0;

    virtual void reset() = 0;

    virtual void toProtoBuf(proto::ColumnStatistics& pbStats) const = 0;
  };

  /**
   * ColumnStatistics Implementation
   */

  class ColumnStatisticsImpl : public ColumnStatistics, public MutableColumnStatistics {
   private:
    InternalCharStatistics stats_;

   public:
    ColumnStatisticsImpl() {
      reset();
    }
    ColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~ColumnStatisticsImpl() override;

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    void merge(const MutableColumnStatistics& other) override {
      stats_.merge(dynamic_cast<const ColumnStatisticsImpl&>(other).stats_);
    }

    void reset() override {
      stats_.reset();
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Column has " << getNumberOfValues() << " values"
             << " and has null value: " << (hasNull() ? "yes" : "no") << std::endl;
      return buffer.str();
    }
  };

  class BinaryColumnStatisticsImpl : public BinaryColumnStatistics, public MutableColumnStatistics {
   private:
    InternalCharStatistics stats_;

   public:
    BinaryColumnStatisticsImpl() {
      reset();
    }
    BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               const StatContext& statContext);
    virtual ~BinaryColumnStatisticsImpl() override;

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    bool hasTotalLength() const override {
      return stats_.hasTotalLength();
    }

    uint64_t getTotalLength() const override {
      if (hasTotalLength()) {
        return stats_.getTotalLength();
      } else {
        throw ParseError("Total length is not defined.");
      }
    }

    void setTotalLength(uint64_t length) {
      stats_.setHasTotalLength(true);
      stats_.setTotalLength(length);
    }

    void update(size_t length) {
      stats_.setTotalLength(stats_.getTotalLength() + length);
    }

    void merge(const MutableColumnStatistics& other) override {
      const BinaryColumnStatisticsImpl& binStats =
          dynamic_cast<const BinaryColumnStatisticsImpl&>(other);
      stats_.merge(binStats.stats_);
    }

    void reset() override {
      stats_.reset();
      setTotalLength(0);
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::BinaryStatistics* binStats = pbStats.mutable_binary_statistics();
      binStats->set_sum(static_cast<int64_t>(stats_.getTotalLength()));
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Binary" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasTotalLength()) {
        buffer << "Total length: " << getTotalLength() << std::endl;
      } else {
        buffer << "Total length: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class BooleanColumnStatisticsImpl : public BooleanColumnStatistics,
                                      public MutableColumnStatistics {
   private:
    InternalBooleanStatistics stats_;
    bool hasCount_;
    uint64_t trueCount_;

   public:
    BooleanColumnStatisticsImpl() {
      reset();
    }
    BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                const StatContext& statContext);
    virtual ~BooleanColumnStatisticsImpl() override;

    bool hasCount() const override {
      return hasCount_;
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
      hasCount_ = true;
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    uint64_t getFalseCount() const override {
      if (hasCount()) {
        return getNumberOfValues() - trueCount_;
      } else {
        throw ParseError("False count is not defined.");
      }
    }

    uint64_t getTrueCount() const override {
      if (hasCount()) {
        return trueCount_;
      } else {
        throw ParseError("True count is not defined.");
      }
    }

    void setTrueCount(uint64_t trueCount) {
      hasCount_ = true;
      trueCount_ = trueCount;
    }

    void update(bool value, size_t repetitions) {
      if (value) {
        trueCount_ += repetitions;
      }
    }

    void merge(const MutableColumnStatistics& other) override {
      const BooleanColumnStatisticsImpl& boolStats =
          dynamic_cast<const BooleanColumnStatisticsImpl&>(other);
      stats_.merge(boolStats.stats_);
      hasCount_ = hasCount_ && boolStats.hasCount_;
      trueCount_ += boolStats.trueCount_;
    }

    void reset() override {
      stats_.reset();
      setTrueCount(0);
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::BucketStatistics* bucketStats = pbStats.mutable_bucket_statistics();
      if (hasCount_) {
        bucketStats->add_count(trueCount_);
      } else {
        bucketStats->clear_count();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Boolean" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasCount()) {
        buffer << "(true: " << getTrueCount() << "; false: " << getFalseCount() << ")" << std::endl;
      } else {
        buffer << "(true: not defined; false: not defined)" << std::endl;
        buffer << "True and false counts are not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DateColumnStatisticsImpl : public DateColumnStatistics, public MutableColumnStatistics {
   private:
    InternalDateStatistics stats_;

   public:
    DateColumnStatisticsImpl() {
      reset();
    }
    DateColumnStatisticsImpl(const proto::ColumnStatistics& stats, const StatContext& statContext);
    virtual ~DateColumnStatisticsImpl() override;

    bool hasMinimum() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximum() const override {
      return stats_.hasMaximum();
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    int32_t getMinimum() const override {
      if (hasMinimum()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximum() const override {
      if (hasMaximum()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(int32_t minimum) {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximum(int32_t maximum) {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    void update(int32_t value) {
      stats_.updateMinMax(value);
    }

    void merge(const MutableColumnStatistics& other) override {
      const DateColumnStatisticsImpl& dateStats =
          dynamic_cast<const DateColumnStatisticsImpl&>(other);
      stats_.merge(dateStats.stats_);
    }

    void reset() override {
      stats_.reset();
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::DateStatistics* dateStatistics = pbStats.mutable_date_statistics();
      if (stats_.hasMinimum()) {
        dateStatistics->set_maximum(stats_.getMaximum());
        dateStatistics->set_minimum(stats_.getMinimum());
      } else {
        dateStatistics->clear_minimum();
        dateStatistics->clear_maximum();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Date" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimum()) {
        buffer << "Minimum: " << getMinimum() << std::endl;
      } else {
        buffer << "Minimum: not defined" << std::endl;
      }

      if (hasMaximum()) {
        buffer << "Maximum: " << getMaximum() << std::endl;
      } else {
        buffer << "Maximum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DecimalColumnStatisticsImpl : public DecimalColumnStatistics,
                                      public MutableColumnStatistics {
   private:
    InternalDecimalStatistics stats_;

   public:
    DecimalColumnStatisticsImpl() {
      reset();
    }
    DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                const StatContext& statContext);
    virtual ~DecimalColumnStatisticsImpl() override;

    bool hasMinimum() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximum() const override {
      return stats_.hasMaximum();
    }

    bool hasSum() const override {
      return stats_.hasSum();
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    Decimal getMinimum() const override {
      if (hasMinimum()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    Decimal getMaximum() const override {
      if (hasMaximum()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(Decimal minimum) {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximum(Decimal maximum) {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    Decimal getSum() const override {
      if (hasSum()) {
        return stats_.getSum();
      } else {
        throw ParseError("Sum is not defined.");
      }
    }

    void setSum(Decimal sum) {
      stats_.setHasSum(true);
      stats_.setSum(sum);
    }

    void update(const Decimal& value) {
      stats_.updateMinMax(value);

      if (stats_.hasSum()) {
        updateSum(value);
      }
    }

    void merge(const MutableColumnStatistics& other) override {
      const DecimalColumnStatisticsImpl& decStats =
          dynamic_cast<const DecimalColumnStatisticsImpl&>(other);

      stats_.merge(decStats.stats_);

      stats_.setHasSum(stats_.hasSum() && decStats.hasSum());
      if (stats_.hasSum()) {
        updateSum(decStats.getSum());
      }
    }

    void reset() override {
      stats_.reset();
      setSum(Decimal());
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::DecimalStatistics* decStats = pbStats.mutable_decimal_statistics();
      if (stats_.hasMinimum()) {
        decStats->set_minimum(stats_.getMinimum().toString(true));
        decStats->set_maximum(stats_.getMaximum().toString(true));
      } else {
        decStats->clear_minimum();
        decStats->clear_maximum();
      }
      if (stats_.hasSum()) {
        decStats->set_sum(stats_.getSum().toString(true));
      } else {
        decStats->clear_sum();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Decimal" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimum()) {
        buffer << "Minimum: " << getMinimum().toString() << std::endl;
      } else {
        buffer << "Minimum: not defined" << std::endl;
      }

      if (hasMaximum()) {
        buffer << "Maximum: " << getMaximum().toString() << std::endl;
      } else {
        buffer << "Maximum: not defined" << std::endl;
      }

      if (hasSum()) {
        buffer << "Sum: " << getSum().toString() << std::endl;
      } else {
        buffer << "Sum: not defined" << std::endl;
      }

      return buffer.str();
    }

   private:
    void updateSum(Decimal value) {
      if (stats_.hasSum()) {
        bool overflow = false;
        Decimal sum = stats_.getSum();
        if (sum.scale > value.scale) {
          value.value = scaleUpInt128ByPowerOfTen(value.value, sum.scale - value.scale, overflow);
        } else if (sum.scale < value.scale) {
          sum.value = scaleUpInt128ByPowerOfTen(sum.value, value.scale - sum.scale, overflow);
          sum.scale = value.scale;
        }

        if (!overflow) {
          bool wasPositive = sum.value >= 0;
          sum.value += value.value;
          if ((value.value >= 0) == wasPositive) {
            stats_.setHasSum((sum.value >= 0) == wasPositive);
          }
        } else {
          stats_.setHasSum(false);
        }

        if (stats_.hasSum()) {
          stats_.setSum(sum);
        }
      }
    }
  };

  class DoubleColumnStatisticsImpl : public DoubleColumnStatistics, public MutableColumnStatistics {
   private:
    InternalDoubleStatistics stats_;

   public:
    DoubleColumnStatisticsImpl() {
      reset();
    }
    DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~DoubleColumnStatisticsImpl() override;

    bool hasMinimum() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximum() const override {
      return stats_.hasMaximum();
    }

    bool hasSum() const override {
      return stats_.hasSum();
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    double getMinimum() const override {
      if (hasMinimum()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    double getMaximum() const override {
      if (hasMaximum()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(double minimum) {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximum(double maximum) {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    double getSum() const override {
      if (hasSum()) {
        return stats_.getSum();
      } else {
        throw ParseError("Sum is not defined.");
      }
    }

    void setSum(double sum) {
      stats_.setHasSum(true);
      stats_.setSum(sum);
    }

    void update(double value) {
      stats_.updateMinMax(value);
      stats_.setSum(stats_.getSum() + value);
    }

    void merge(const MutableColumnStatistics& other) override {
      const DoubleColumnStatisticsImpl& doubleStats =
          dynamic_cast<const DoubleColumnStatisticsImpl&>(other);
      stats_.merge(doubleStats.stats_);

      stats_.setHasSum(stats_.hasSum() && doubleStats.hasSum());
      if (stats_.hasSum()) {
        stats_.setSum(stats_.getSum() + doubleStats.getSum());
      }
    }

    void reset() override {
      stats_.reset();
      setSum(0.0);
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::DoubleStatistics* doubleStats = pbStats.mutable_double_statistics();
      if (stats_.hasMinimum()) {
        doubleStats->set_minimum(stats_.getMinimum());
        doubleStats->set_maximum(stats_.getMaximum());
      } else {
        doubleStats->clear_minimum();
        doubleStats->clear_maximum();
      }
      if (stats_.hasSum()) {
        doubleStats->set_sum(stats_.getSum());
      } else {
        doubleStats->clear_sum();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Double" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimum()) {
        buffer << "Minimum: " << getMinimum() << std::endl;
      } else {
        buffer << "Minimum: not defined" << std::endl;
      }

      if (hasMaximum()) {
        buffer << "Maximum: " << getMaximum() << std::endl;
      } else {
        buffer << "Maximum: not defined" << std::endl;
      }

      if (hasSum()) {
        buffer << "Sum: " << getSum() << std::endl;
      } else {
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class IntegerColumnStatisticsImpl : public IntegerColumnStatistics,
                                      public MutableColumnStatistics {
   private:
    InternalIntegerStatistics stats_;

   public:
    IntegerColumnStatisticsImpl() {
      reset();
    }
    IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~IntegerColumnStatisticsImpl() override;

    bool hasMinimum() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximum() const override {
      return stats_.hasMaximum();
    }

    bool hasSum() const override {
      return stats_.hasSum();
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    int64_t getMinimum() const override {
      if (hasMinimum()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if (hasMaximum()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(int64_t minimum) {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximum(int64_t maximum) {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    int64_t getSum() const override {
      if (hasSum()) {
        return stats_.getSum();
      } else {
        throw ParseError("Sum is not defined.");
      }
    }

    void setSum(int64_t sum) {
      stats_.setHasSum(true);
      stats_.setSum(sum);
    }

    void update(int64_t value, int repetitions) {
      stats_.updateMinMax(value);

      if (stats_.hasSum()) {
        if (repetitions > 1) {
          stats_.setHasSum(multiplyExact(value, repetitions, &value));
        }

        if (stats_.hasSum()) {
          stats_.setHasSum(addExact(stats_.getSum(), value, &value));

          if (stats_.hasSum()) {
            stats_.setSum(value);
          }
        }
      }
    }

    void merge(const MutableColumnStatistics& other) override {
      const IntegerColumnStatisticsImpl& intStats =
          dynamic_cast<const IntegerColumnStatisticsImpl&>(other);

      stats_.merge(intStats.stats_);

      // update sum and check overflow
      stats_.setHasSum(stats_.hasSum() && intStats.hasSum());
      if (stats_.hasSum()) {
        int64_t value;
        stats_.setHasSum(addExact(stats_.getSum(), intStats.getSum(), &value));
        if (stats_.hasSum()) {
          stats_.setSum(value);
        }
      }
    }

    void reset() override {
      stats_.reset();
      setSum(0);
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::IntegerStatistics* intStats = pbStats.mutable_int_statistics();
      if (stats_.hasMinimum()) {
        intStats->set_minimum(stats_.getMinimum());
        intStats->set_maximum(stats_.getMaximum());
      } else {
        intStats->clear_minimum();
        intStats->clear_maximum();
      }
      if (stats_.hasSum()) {
        intStats->set_sum(stats_.getSum());
      } else {
        intStats->clear_sum();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Integer" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimum()) {
        buffer << "Minimum: " << getMinimum() << std::endl;
      } else {
        buffer << "Minimum: not defined" << std::endl;
      }

      if (hasMaximum()) {
        buffer << "Maximum: " << getMaximum() << std::endl;
      } else {
        buffer << "Maximum: not defined" << std::endl;
      }

      if (hasSum()) {
        buffer << "Sum: " << getSum() << std::endl;
      } else {
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StringColumnStatisticsImpl : public StringColumnStatistics, public MutableColumnStatistics {
   private:
    InternalStringStatistics stats_;

   public:
    StringColumnStatisticsImpl() {
      reset();
    }
    StringColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               const StatContext& statContext);
    virtual ~StringColumnStatisticsImpl() override;

    bool hasMinimum() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximum() const override {
      return stats_.hasMaximum();
    }

    bool hasTotalLength() const override {
      return stats_.hasTotalLength();
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    const std::string& getMinimum() const override {
      if (hasMinimum()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    const std::string& getMaximum() const override {
      if (hasMaximum()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(std::string minimum) {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximum(std::string maximum) {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    uint64_t getTotalLength() const override {
      if (hasTotalLength()) {
        return stats_.getTotalLength();
      } else {
        throw ParseError("Total length is not defined.");
      }
    }

    void setTotalLength(uint64_t length) {
      stats_.setHasTotalLength(true);
      stats_.setTotalLength(length);
    }

    void update(const char* value, size_t length) {
      if (value != nullptr) {
        if (!stats_.hasMinimum()) {
          std::string tempStr(value, value + length);
          setMinimum(tempStr);
          setMaximum(tempStr);
        } else {
          // update min
          int minCmp = strncmp(stats_.getMinimum().c_str(), value,
                               std::min(stats_.getMinimum().length(), length));
          if (minCmp > 0 || (minCmp == 0 && length < stats_.getMinimum().length())) {
            setMinimum(std::string(value, value + length));
          }

          // update max
          int maxCmp = strncmp(stats_.getMaximum().c_str(), value,
                               std::min(stats_.getMaximum().length(), length));
          if (maxCmp < 0 || (maxCmp == 0 && length > stats_.getMaximum().length())) {
            setMaximum(std::string(value, value + length));
          }
        }
      }

      stats_.setTotalLength(stats_.getTotalLength() + length);
    }

    void update(std::string value) {
      update(value.c_str(), value.length());
    }

    void merge(const MutableColumnStatistics& other) override {
      const StringColumnStatisticsImpl& strStats =
          dynamic_cast<const StringColumnStatisticsImpl&>(other);
      stats_.merge(strStats.stats_);
    }

    void reset() override {
      stats_.reset();
      setTotalLength(0);
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::StringStatistics* strStats = pbStats.mutable_string_statistics();
      if (stats_.hasMinimum()) {
        strStats->set_minimum(stats_.getMinimum());
        strStats->set_maximum(stats_.getMaximum());
      } else {
        strStats->clear_minimum();
        strStats->clear_maximum();
      }
      if (stats_.hasTotalLength()) {
        strStats->set_sum(static_cast<int64_t>(stats_.getTotalLength()));
      } else {
        strStats->clear_sum();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: String" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimum()) {
        buffer << "Minimum: " << getMinimum() << std::endl;
      } else {
        buffer << "Minimum is not defined" << std::endl;
      }

      if (hasMaximum()) {
        buffer << "Maximum: " << getMaximum() << std::endl;
      } else {
        buffer << "Maximum is not defined" << std::endl;
      }

      if (hasTotalLength()) {
        buffer << "Total length: " << getTotalLength() << std::endl;
      } else {
        buffer << "Total length is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class TimestampColumnStatisticsImpl : public TimestampColumnStatistics,
                                        public MutableColumnStatistics {
   private:
    InternalIntegerStatistics stats_;
    bool hasLowerBound_;
    bool hasUpperBound_;
    int64_t lowerBound_;
    int64_t upperBound_;
    int32_t minimumNanos_;  // last 6 digits of nanosecond of minimum timestamp
    int32_t maximumNanos_;  // last 6 digits of nanosecond of maximum timestamp
    static constexpr int32_t DEFAULT_MIN_NANOS = 0;
    static constexpr int32_t DEFAULT_MAX_NANOS = 999999;

   public:
    TimestampColumnStatisticsImpl() {
      reset();
    }
    TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                  const StatContext& statContext);
    virtual ~TimestampColumnStatisticsImpl() override;

    bool hasMinimum() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximum() const override {
      return stats_.hasMaximum();
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    int64_t getMinimum() const override {
      if (hasMinimum()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if (hasMaximum()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(int64_t minimum) {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximum(int64_t maximum) {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    void update(int64_t value) {
      stats_.updateMinMax(value);
    }

    void update(int64_t milli, int32_t nano) {
      if (!stats_.hasMinimum()) {
        stats_.setHasMinimum(true);
        stats_.setHasMaximum(true);
        stats_.setMinimum(milli);
        stats_.setMaximum(milli);
        maximumNanos_ = minimumNanos_ = nano;
      } else {
        if (milli <= stats_.getMinimum()) {
          if (milli < stats_.getMinimum() || nano < minimumNanos_) {
            minimumNanos_ = nano;
          }
          stats_.setMinimum(milli);
        }

        if (milli >= stats_.getMaximum()) {
          if (milli > stats_.getMaximum() || nano > maximumNanos_) {
            maximumNanos_ = nano;
          }
          stats_.setMaximum(milli);
        }
      }
    }

    void merge(const MutableColumnStatistics& other) override {
      const TimestampColumnStatisticsImpl& tsStats =
          dynamic_cast<const TimestampColumnStatisticsImpl&>(other);

      stats_.setHasNull(stats_.hasNull() || tsStats.hasNull());
      stats_.setNumberOfValues(stats_.getNumberOfValues() + tsStats.getNumberOfValues());

      if (tsStats.hasMinimum()) {
        if (!stats_.hasMinimum()) {
          stats_.setHasMinimum(true);
          stats_.setHasMaximum(true);
          stats_.setMinimum(tsStats.getMinimum());
          stats_.setMaximum(tsStats.getMaximum());
          minimumNanos_ = tsStats.getMinimumNanos();
          maximumNanos_ = tsStats.getMaximumNanos();
        } else {
          if (tsStats.getMaximum() >= stats_.getMaximum()) {
            if (tsStats.getMaximum() > stats_.getMaximum() ||
                tsStats.getMaximumNanos() > maximumNanos_) {
              maximumNanos_ = tsStats.getMaximumNanos();
            }
            stats_.setMaximum(tsStats.getMaximum());
          }
          if (tsStats.getMinimum() <= stats_.getMinimum()) {
            if (tsStats.getMinimum() < stats_.getMinimum() ||
                tsStats.getMinimumNanos() < minimumNanos_) {
              minimumNanos_ = tsStats.getMinimumNanos();
            }
            stats_.setMinimum(tsStats.getMinimum());
          }
        }
      }
    }

    void reset() override {
      stats_.reset();
      minimumNanos_ = DEFAULT_MIN_NANOS;
      maximumNanos_ = DEFAULT_MAX_NANOS;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::TimestampStatistics* tsStats = pbStats.mutable_timestamp_statistics();
      if (stats_.hasMinimum()) {
        tsStats->set_minimum_utc(stats_.getMinimum());
        tsStats->set_maximum_utc(stats_.getMaximum());
        if (minimumNanos_ != DEFAULT_MIN_NANOS) {
          tsStats->set_minimum_nanos(minimumNanos_ + 1);
        }
        if (maximumNanos_ != DEFAULT_MAX_NANOS) {
          tsStats->set_maximum_nanos(maximumNanos_ + 1);
        }
      } else {
        tsStats->clear_minimum_utc();
        tsStats->clear_maximum_utc();
        tsStats->clear_minimum_nanos();
        tsStats->clear_maximum_nanos();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      struct tm tmValue;
      char timeBuffer[20];
      time_t secs = 0;

      buffer << "Data type: Timestamp" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimum()) {
        secs = static_cast<time_t>(getMinimum() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "Minimum: " << timeBuffer << "." << (getMinimum() % 1000) << std::endl;
      } else {
        buffer << "Minimum is not defined" << std::endl;
      }

      if (hasLowerBound()) {
        secs = static_cast<time_t>(getLowerBound() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "LowerBound: " << timeBuffer << "." << (getLowerBound() % 1000) << std::endl;
      } else {
        buffer << "LowerBound is not defined" << std::endl;
      }

      if (hasMaximum()) {
        secs = static_cast<time_t>(getMaximum() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "Maximum: " << timeBuffer << "." << (getMaximum() % 1000) << std::endl;
      } else {
        buffer << "Maximum is not defined" << std::endl;
      }

      if (hasUpperBound()) {
        secs = static_cast<time_t>(getUpperBound() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "UpperBound: " << timeBuffer << "." << (getUpperBound() % 1000) << std::endl;
      } else {
        buffer << "UpperBound is not defined" << std::endl;
      }

      return buffer.str();
    }

    bool hasLowerBound() const override {
      return hasLowerBound_;
    }

    bool hasUpperBound() const override {
      return hasUpperBound_;
    }

    int64_t getLowerBound() const override {
      if (hasLowerBound()) {
        return lowerBound_;
      } else {
        throw ParseError("LowerBound is not defined.");
      }
    }

    int64_t getUpperBound() const override {
      if (hasUpperBound()) {
        return upperBound_;
      } else {
        throw ParseError("UpperBound is not defined.");
      }
    }

    int32_t getMinimumNanos() const override {
      if (hasMinimum()) {
        return minimumNanos_;
      } else {
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximumNanos() const override {
      if (hasMaximum()) {
        return maximumNanos_;
      } else {
        throw ParseError("Maximum is not defined.");
      }
    }
  };

  class CollectionColumnStatisticsImpl : public CollectionColumnStatistics,
                                         public MutableColumnStatistics {
   private:
    InternalCollectionStatistics stats_;

   public:
    CollectionColumnStatisticsImpl() {
      reset();
    }
    CollectionColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~CollectionColumnStatisticsImpl() override;

    bool hasMinimumChildren() const override {
      return stats_.hasMinimum();
    }

    bool hasMaximumChildren() const override {
      return stats_.hasMaximum();
    }

    bool hasTotalChildren() const override {
      return stats_.hasSum();
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    uint64_t getMinimumChildren() const override {
      if (hasMinimumChildren()) {
        return stats_.getMinimum();
      } else {
        throw ParseError("MinimumChildren is not defined.");
      }
    }

    uint64_t getMaximumChildren() const override {
      if (hasMaximumChildren()) {
        return stats_.getMaximum();
      } else {
        throw ParseError("MaximumChildren is not defined.");
      }
    }

    uint64_t getTotalChildren() const override {
      if (hasTotalChildren()) {
        return stats_.getSum();
      } else {
        throw ParseError("TotalChildren is not defined.");
      }
    }

    void setMinimumChildren(uint64_t minimum) override {
      stats_.setHasMinimum(true);
      stats_.setMinimum(minimum);
    }

    void setMaximumChildren(uint64_t maximum) override {
      stats_.setHasMaximum(true);
      stats_.setMaximum(maximum);
    }

    void setTotalChildren(uint64_t sum) override {
      stats_.setHasSum(true);
      stats_.setSum(sum);
    }

    void setHasTotalChildren(bool hasSum) override {
      stats_.setHasSum(hasSum);
    }

    void merge(const MutableColumnStatistics& other) override {
      const CollectionColumnStatisticsImpl& collectionStats =
          dynamic_cast<const CollectionColumnStatisticsImpl&>(other);

      stats_.merge(collectionStats.stats_);

      // hasSumValue here means no overflow
      stats_.setHasSum(stats_.hasSum() && collectionStats.hasTotalChildren());
      if (stats_.hasSum()) {
        uint64_t oldSum = stats_.getSum();
        stats_.setSum(stats_.getSum() + collectionStats.getTotalChildren());
        if (oldSum > stats_.getSum()) {
          stats_.setHasSum(false);
        }
      }
    }

    void reset() override {
      stats_.reset();
      setTotalChildren(0);
    }

    void update(uint64_t value) {
      stats_.updateMinMax(value);
      if (stats_.hasSum()) {
        uint64_t oldSum = stats_.getSum();
        stats_.setSum(stats_.getSum() + value);
        if (oldSum > stats_.getSum()) {
          stats_.setHasSum(false);
        }
      }
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::CollectionStatistics* collectionStats = pbStats.mutable_collection_statistics();
      if (stats_.hasMinimum()) {
        collectionStats->set_min_children(stats_.getMinimum());
        collectionStats->set_max_children(stats_.getMaximum());
      } else {
        collectionStats->clear_min_children();
        collectionStats->clear_max_children();
      }
      if (stats_.hasSum()) {
        collectionStats->set_total_children(stats_.getSum());
      } else {
        collectionStats->clear_total_children();
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Collection(LIST|MAP)" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if (hasMinimumChildren()) {
        buffer << "MinChildren: " << getMinimumChildren() << std::endl;
      } else {
        buffer << "MinChildren is not defined" << std::endl;
      }

      if (hasMaximumChildren()) {
        buffer << "MaxChildren: " << getMaximumChildren() << std::endl;
      } else {
        buffer << "MaxChildren is not defined" << std::endl;
      }

      if (hasTotalChildren()) {
        buffer << "TotalChildren: " << getTotalChildren() << std::endl;
      } else {
        buffer << "TotalChildren is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class GeospatialColumnStatisticsImpl : public GeospatialColumnStatistics,
                                         public MutableColumnStatistics {
   private:
    geospatial::WKBGeometryBounder bounder_;
    InternalCharStatistics stats_;

   public:
    GeospatialColumnStatisticsImpl() {
      reset();
    }
    explicit GeospatialColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~GeospatialColumnStatisticsImpl();

    uint64_t getNumberOfValues() const override {
      return stats_.getNumberOfValues();
    }

    void setNumberOfValues(uint64_t value) override {
      stats_.setNumberOfValues(value);
    }

    void increase(uint64_t count) override {
      stats_.setNumberOfValues(stats_.getNumberOfValues() + count);
    }

    bool hasNull() const override {
      return stats_.hasNull();
    }

    void setHasNull(bool hasNull) override {
      stats_.setHasNull(hasNull);
    }

    void merge(const MutableColumnStatistics& other) override {
      const GeospatialColumnStatisticsImpl& geoStats =
          dynamic_cast<const GeospatialColumnStatisticsImpl&>(other);
      stats_.merge(geoStats.stats_);
      bounder_.merge(geoStats.bounder_);
    }

    void reset() override {
      stats_.reset();
      bounder_.reset();
    }

    void update(const char* value, size_t length) override {
      bounder_.mergeGeometry(std::string_view(value, length));
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_has_null(stats_.hasNull());
      pbStats.set_number_of_values(stats_.getNumberOfValues());

      proto::GeospatialStatistics* geoStats = pbStats.mutable_geospatial_statistics();
      const auto& bbox = bounder_.bounds();
      if (bbox.boundValid(0) && bbox.boundValid(1) && !bbox.boundEmpty(0) && !bbox.boundEmpty(1)) {
        geoStats->mutable_bbox()->set_xmin(bbox.min[0]);
        geoStats->mutable_bbox()->set_xmax(bbox.max[0]);
        geoStats->mutable_bbox()->set_ymin(bbox.min[1]);
        geoStats->mutable_bbox()->set_ymax(bbox.max[1]);
        if (bbox.boundValid(2) && !bbox.boundEmpty(2)) {
          geoStats->mutable_bbox()->set_zmin(bbox.min[2]);
          geoStats->mutable_bbox()->set_zmax(bbox.max[2]);
        }
        if (bbox.boundValid(3) && !bbox.boundEmpty(3)) {
          geoStats->mutable_bbox()->set_mmin(bbox.min[3]);
          geoStats->mutable_bbox()->set_mmax(bbox.max[3]);
        }
      }
      for (auto type : bounder_.geometryTypes()) {
        geoStats->add_geospatial_types(type);
      }
    }

    std::string toString() const override {
      if (!bounder_.isValid()) {
        return "<GeoStatistics> invalid";
      }

      std::stringstream ss;
      ss << "<GeoStatistics>";

      std::string dim_label("xyzm");
      const auto& bbox = bounder_.bounds();
      auto dim_valid = bbox.dimensionValid();
      auto dim_empty = bbox.dimensionEmpty();
      auto lower = bbox.lowerBound();
      auto upper = bbox.upperBound();

      for (int i = 0; i < 4; i++) {
        ss << " " << dim_label[i] << ": ";
        if (!dim_valid[i]) {
          ss << "invalid";
        } else if (dim_empty[i]) {
          ss << "empty";
        } else {
          ss << "[" << lower[i] << ", " << upper[i] << "]";
        }
      }

      std::vector<int32_t> maybe_geometry_types = bounder_.geometryTypes();
      ss << " geometry_types: [";
      std::string sep("");
      for (int32_t geometry_type : maybe_geometry_types) {
        ss << sep << geometry_type;
        sep = ", ";
      }
      ss << "]";

      return ss.str();
    }

    const geospatial::BoundingBox& getBoundingBox() const override {
      return bounder_.bounds();
    }

    std::vector<int32_t> getGeospatialTypes() const override {
      return bounder_.geometryTypes();
    }
  };

  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s,
                                            const StatContext& statContext);

  class StatisticsImpl : public Statistics {
   private:
    std::vector<ColumnStatistics*> colStats_;

    // DELIBERATELY NOT IMPLEMENTED
    StatisticsImpl(const StatisticsImpl&);
    StatisticsImpl& operator=(const StatisticsImpl&);

   public:
    StatisticsImpl(const proto::StripeStatistics& stripeStats, const StatContext& statContext);

    StatisticsImpl(const proto::Footer& footer, const StatContext& statContext);

    virtual const ColumnStatistics* getColumnStatistics(uint32_t columnId) const override {
      return colStats_[columnId];
    }

    virtual ~StatisticsImpl() override;

    uint32_t getNumberOfColumns() const override {
      return static_cast<uint32_t>(colStats_.size());
    }
  };

  class StripeStatisticsImpl : public StripeStatistics {
   private:
    std::unique_ptr<StatisticsImpl> columnStats_;

    // DELIBERATELY NOT IMPLEMENTED
    StripeStatisticsImpl(const StripeStatisticsImpl&);
    StripeStatisticsImpl& operator=(const StripeStatisticsImpl&);

   public:
    StripeStatisticsImpl(const proto::StripeStatistics& stripeStats,
                         const StatContext& statContext);

    virtual const ColumnStatistics* getColumnStatistics(uint32_t columnId) const override {
      return columnStats_->getColumnStatistics(columnId);
    }

    uint32_t getNumberOfColumns() const override {
      return columnStats_->getNumberOfColumns();
    }

    virtual const ColumnStatistics* getRowIndexStatistics(uint32_t, uint32_t) const override {
      throw NotImplementedYet("set includeRowIndex true to get row index stats");
    }

    virtual ~StripeStatisticsImpl() override;

    virtual uint32_t getNumberOfRowIndexStats(uint32_t) const override {
      throw NotImplementedYet("set includeRowIndex true to get row index stats");
    }
  };

  class StripeStatisticsWithRowGroupIndexImpl : public StripeStatisticsImpl {
   private:
    std::vector<std::vector<std::shared_ptr<const ColumnStatistics> > > rowIndexStats_;

    // DELIBERATELY NOT IMPLEMENTED
    StripeStatisticsWithRowGroupIndexImpl(const StripeStatisticsWithRowGroupIndexImpl&);
    StripeStatisticsWithRowGroupIndexImpl& operator=(const StripeStatisticsWithRowGroupIndexImpl&);

   public:
    StripeStatisticsWithRowGroupIndexImpl(
        const proto::StripeStatistics& stripeStats,
        std::vector<std::vector<proto::ColumnStatistics> >& indexStats,
        const StatContext& statContext);

    virtual const ColumnStatistics* getRowIndexStatistics(uint32_t columnId,
                                                          uint32_t rowIndex) const override {
      // check id indices are valid
      return rowIndexStats_[columnId][rowIndex].get();
    }

    virtual ~StripeStatisticsWithRowGroupIndexImpl() override;

    uint32_t getNumberOfRowIndexStats(uint32_t columnId) const override {
      return static_cast<uint32_t>(rowIndexStats_[columnId].size());
    }
  };

  /**
   * Create ColumnStatistics for writers
   * @param type of column
   * @return MutableColumnStatistics instances
   */
  std::unique_ptr<MutableColumnStatistics> createColumnStatistics(const Type& type);

}  // namespace orc

#endif
