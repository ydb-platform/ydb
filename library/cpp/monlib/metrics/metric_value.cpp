#include "metric_value.h"


namespace NMonitoring {
    void TMetricTimeSeries::SortByTs() {
        SortPointsByTs(ValueType_, Points_);
    }

    void TMetricTimeSeries::Clear() noexcept {
        if (ValueType_ == EMetricValueType::HISTOGRAM) {
            for (TPoint& p: Points_) {
                SnapshotUnRef<EMetricValueType::HISTOGRAM>(p);
            }
        } else if (ValueType_ == EMetricValueType::SUMMARY) {
            for (TPoint& p: Points_) {
                SnapshotUnRef<EMetricValueType::SUMMARY>(p);
            }
        } else if (ValueType_ == EMetricValueType::LOGHISTOGRAM) {
            for (TPoint& p: Points_) {
                SnapshotUnRef<EMetricValueType::LOGHISTOGRAM>(p);
            }
        }

        Points_.clear();
        ValueType_ = EMetricValueType::UNKNOWN;
    }
}
