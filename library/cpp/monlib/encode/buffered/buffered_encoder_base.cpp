#include "buffered_encoder_base.h"

#include <util/string/join.h>
#include <util/string/builder.h>

namespace NMonitoring {

void TBufferedEncoderBase::OnStreamBegin() {
    State_.Expect(TEncoderState::EState::ROOT);
}

void TBufferedEncoderBase::OnStreamEnd() {
    State_.Expect(TEncoderState::EState::ROOT);
}

void TBufferedEncoderBase::OnCommonTime(TInstant time) {
    State_.Expect(TEncoderState::EState::ROOT);
    CommonTime_ = time;
}

void TBufferedEncoderBase::OnMetricBegin(EMetricType type) {
    State_.Switch(TEncoderState::EState::ROOT, TEncoderState::EState::METRIC);
    Metrics_.emplace_back();
    Metrics_.back().MetricType = type;
}

void TBufferedEncoderBase::OnMetricEnd() {
    State_.Switch(TEncoderState::EState::METRIC, TEncoderState::EState::ROOT);

    switch (MetricsMergingMode_) {
        case EMetricsMergingMode::MERGE_METRICS: {
            auto& metric = Metrics_.back();
            Sort(metric.Labels, [] (const TPooledLabel& lhs, const TPooledLabel& rhs) {
                return std::tie(lhs.Key, lhs.Value) < std::tie(rhs.Key, rhs.Value);
            });

            auto it = MetricMap_.find(metric.Labels);
            if (it == std::end(MetricMap_)) {
                MetricMap_.emplace(metric.Labels, Metrics_.size() - 1);
            } else {
                auto& existing = Metrics_[it->second].TimeSeries;

                Y_ENSURE(existing.GetValueType() == metric.TimeSeries.GetValueType(),
                    "Time series point type mismatch: expected " << existing.GetValueType()
                    << " but found " << metric.TimeSeries.GetValueType()
                    << ", labels '" << FormatLabels(metric.Labels) << "'");

                existing.CopyFrom(metric.TimeSeries);
                Metrics_.pop_back();
            }

            break;
        }
        case EMetricsMergingMode::DEFAULT:
            break;
    }
}

void TBufferedEncoderBase::OnLabelsBegin() {
    if (State_ == TEncoderState::EState::METRIC) {
        State_ = TEncoderState::EState::METRIC_LABELS;
    } else if (State_ == TEncoderState::EState::ROOT) {
        State_ = TEncoderState::EState::COMMON_LABELS;
    } else {
        State_.ThrowInvalid("expected METRIC or ROOT");
    }
}

void TBufferedEncoderBase::OnLabelsEnd() {
    if (State_ == TEncoderState::EState::METRIC_LABELS) {
        State_ = TEncoderState::EState::METRIC;
    } else if (State_ == TEncoderState::EState::COMMON_LABELS) {
        State_ = TEncoderState::EState::ROOT;
    } else {
        State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
    }
}

void TBufferedEncoderBase::OnLabel(TStringBuf name, TStringBuf value) {
    TPooledLabels* labels;
    if (State_ == TEncoderState::EState::METRIC_LABELS) {
        labels = &Metrics_.back().Labels;
    } else if (State_ == TEncoderState::EState::COMMON_LABELS) {
        labels = &CommonLabels_;
    } else {
        State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
    }

    labels->emplace_back(LabelNamesPool_.PutIfAbsent(name), LabelValuesPool_.PutIfAbsent(value));
}

void TBufferedEncoderBase::OnLabel(ui32 name, ui32 value) {
    TPooledLabels* labels;
    if (State_ == TEncoderState::EState::METRIC_LABELS) {
        labels = &Metrics_.back().Labels;
    } else if (State_ == TEncoderState::EState::COMMON_LABELS) {
        labels = &CommonLabels_;
    } else {
        State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
    }

    labels->emplace_back(LabelNamesPool_.GetByIndex(name), LabelValuesPool_.GetByIndex(value));
}

std::pair<ui32, ui32> TBufferedEncoderBase::PrepareLabel(TStringBuf name, TStringBuf value) {
    auto nameLabel = LabelNamesPool_.PutIfAbsent(name);
    auto valueLabel = LabelValuesPool_.PutIfAbsent(value);
    return std::make_pair(nameLabel->Index, valueLabel->Index);
}

void TBufferedEncoderBase::OnDouble(TInstant time, double value) {
    State_.Expect(TEncoderState::EState::METRIC);
    TMetric& metric = Metrics_.back();
    metric.TimeSeries.Add(time, value);
}

void TBufferedEncoderBase::OnInt64(TInstant time, i64 value) {
    State_.Expect(TEncoderState::EState::METRIC);
    TMetric& metric = Metrics_.back();
    metric.TimeSeries.Add(time, value);
}

void TBufferedEncoderBase::OnUint64(TInstant time, ui64 value) {
    State_.Expect(TEncoderState::EState::METRIC);
    TMetric& metric = Metrics_.back();
    metric.TimeSeries.Add(time, value);
}

void TBufferedEncoderBase::OnHistogram(TInstant time, IHistogramSnapshotPtr s) {
    State_.Expect(TEncoderState::EState::METRIC);
    TMetric& metric = Metrics_.back();
    metric.TimeSeries.Add(time, s.Get());
}

void TBufferedEncoderBase::OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr s) {
    State_.Expect(TEncoderState::EState::METRIC);
    TMetric& metric = Metrics_.back();
    metric.TimeSeries.Add(time, s.Get());
}

void TBufferedEncoderBase::OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr s) {
    State_.Expect(TEncoderState::EState::METRIC);
    TMetric& metric = Metrics_.back();
    metric.TimeSeries.Add(time, s.Get());
}

TString TBufferedEncoderBase::FormatLabels(const TPooledLabels& labels) const {
    auto formattedLabels = TVector<TString>(Reserve(labels.size() + CommonLabels_.size()));
    auto addLabel = [&](const TPooledLabel& l) {
        auto formattedLabel = TStringBuilder() << LabelNamesPool_.Get(l.Key) << '=' << LabelValuesPool_.Get(l.Value);
        formattedLabels.push_back(std::move(formattedLabel));
    };

    for (const auto& l: labels) {
        addLabel(l);
    }
    for (const auto& l: CommonLabels_) {
        const auto it = FindIf(labels, [&](const TPooledLabel& label) {
            return label.Key == l.Key;
        });
        if (it == labels.end()) {
            addLabel(l);
        }
    }
    Sort(formattedLabels);

    return TStringBuilder() << "{" <<  JoinSeq(", ", formattedLabels) << "}";
}

} // namespace NMonitoring
