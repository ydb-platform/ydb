#pragma once

#include "string_pool.h"

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/encoder_state.h>
#include <library/cpp/monlib/encode/format.h>
#include <library/cpp/monlib/metrics/metric_value.h>

#include <util/datetime/base.h>
#include <util/digest/numeric.h>


namespace NMonitoring {

class TBufferedEncoderBase : public IMetricEncoder {
public:
    void OnStreamBegin() override;
    void OnStreamEnd() override;

    void OnCommonTime(TInstant time) override;

    void OnMetricBegin(EMetricType type) override;
    void OnMetricEnd() override;

    void OnLabelsBegin() override;
    void OnLabelsEnd() override;
    void OnLabel(TStringBuf name, TStringBuf value) override;
    void OnLabel(ui32 name, ui32 value) override;
    std::pair<ui32, ui32> PrepareLabel(TStringBuf name, TStringBuf value) override;

    void OnDouble(TInstant time, double value) override;
    void OnInt64(TInstant time, i64 value) override;
    void OnUint64(TInstant time, ui64 value) override;

    void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override;
    void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override;
    void OnLogHistogram(TInstant, TLogHistogramSnapshotPtr) override;

protected:
    using TPooledStr = TStringPoolBuilder::TValue;

    struct TPooledLabel {
        TPooledLabel(const TPooledStr* key, const TPooledStr* value)
            : Key{key}
            , Value{value}
        {
        }

        bool operator==(const TPooledLabel& other) const {
            return std::tie(Key, Value) == std::tie(other.Key, other.Value);
        }

        bool operator!=(const TPooledLabel& other) const {
            return !(*this == other);
        }

        const TPooledStr* Key;
        const TPooledStr* Value;
    };

    using TPooledLabels = TVector<TPooledLabel>;

    struct TPooledLabelsHash {
        size_t operator()(const TPooledLabels& val) const {
            size_t hash{0};

            for (auto v : val) {
                hash = CombineHashes<size_t>(hash, reinterpret_cast<size_t>(v.Key));
                hash = CombineHashes<size_t>(hash, reinterpret_cast<size_t>(v.Value));
            }

            return hash;
        }
    };

    using TMetricMap = THashMap<TPooledLabels, size_t, TPooledLabelsHash>;

    struct TMetric {
        EMetricType MetricType = EMetricType::UNKNOWN;
        TPooledLabels Labels;
        TMetricTimeSeries TimeSeries;
    };

protected:
    TString FormatLabels(const TPooledLabels& labels) const;

protected:
    TEncoderState State_;

    TStringPoolBuilder LabelNamesPool_;
    TStringPoolBuilder LabelValuesPool_;
    TInstant CommonTime_ = TInstant::Zero();
    TPooledLabels CommonLabels_;
    TVector<TMetric> Metrics_;
    TMetricMap MetricMap_;
    EMetricsMergingMode MetricsMergingMode_ = EMetricsMergingMode::DEFAULT;
};

}
