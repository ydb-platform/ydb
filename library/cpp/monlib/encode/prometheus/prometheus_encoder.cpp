#include "prometheus.h"
#include "prometheus_model.h"

#include <library/cpp/monlib/encode/encoder_state.h>
#include <library/cpp/monlib/encode/buffered/string_pool.h>
#include <library/cpp/monlib/metrics/labels.h>
#include <library/cpp/monlib/metrics/metric_value.h>

#include <util/string/cast.h>
#include <util/generic/hash_set.h>


namespace NMonitoring {
    namespace {
        ///////////////////////////////////////////////////////////////////////
        // TPrometheusWriter
        ///////////////////////////////////////////////////////////////////////
        class TPrometheusWriter {
        public:
            explicit TPrometheusWriter(IOutputStream* out)
                : Out_(out)
            {
            }

            void WriteType(EMetricType type, const TString& name) {
                auto r = WrittenTypes_.insert(name);
                if (!r.second) {
                    // type for this metric was already written
                    return;
                }

                Out_->Write("# TYPE ");
                WriteMetricName(name);
                Out_->Write(' ');

                switch (type) {
                case EMetricType::GAUGE:
                case EMetricType::IGAUGE:
                    Out_->Write("gauge");
                    break;
                case EMetricType::RATE:
                case EMetricType::COUNTER:
                    Out_->Write("counter");
                    break;
                case EMetricType::HIST:
                case EMetricType::HIST_RATE:
                    Out_->Write("histogram");
                    break;
                case EMetricType::LOGHIST:
                    // TODO(@kbalakirev): implement this case
                    break;
                case EMetricType::DSUMMARY:
                    ythrow yexception() << "writing summary type is forbiden";
                case EMetricType::UNKNOWN:
                    ythrow yexception() << "unknown metric type: " << MetricTypeToStr(type)
                                        << ", name: " << name;
                }
                Out_->Write('\n');
            }

            void WriteDouble(TStringBuf name, const TLabels& labels, TInstant time, double value) {
                WriteValue(name, "", labels, "", "", time, value);
            }

            void WriteHistogram(TStringBuf name, const TLabels& labels, TInstant time, IHistogramSnapshot* h) {
                Y_ENSURE(!labels.Has(NPrometheus::BUCKET_LABEL),
                         "histogram metric " << name << " has label '" <<
                         NPrometheus::BUCKET_LABEL << "' which is reserved in Prometheus");

                double totalCount = 0;
                for (ui32 i = 0, count = h->Count(); i < count; i++) {
                    TBucketBound bound = h->UpperBound(i);
                    TStringBuf boundStr;
                    if (bound == HISTOGRAM_INF_BOUND) {
                        boundStr = TStringBuf("+Inf");
                    } else {
                        size_t len = FloatToString(bound, TmpBuf_, Y_ARRAY_SIZE(TmpBuf_));
                        boundStr = TStringBuf(TmpBuf_, len);
                    }

                    TBucketValue value = h->Value(i);
                    totalCount += static_cast<double>(value);

                    WriteValue(
                        name, NPrometheus::BUCKET_SUFFIX,
                        labels, NPrometheus::BUCKET_LABEL, boundStr,
                        time,
                        totalCount);
                }

                WriteValue(name, NPrometheus::COUNT_SUFFIX, labels, "", "", time, totalCount);
            }

            void WriteSummaryDouble(TStringBuf name, const TLabels& labels, TInstant time, ISummaryDoubleSnapshot* s) {
                WriteValue(name, NPrometheus::SUM_SUFFIX, labels, "", "", time, s->GetSum());
                WriteValue(name, NPrometheus::MIN_SUFFIX, labels, "", "", time, s->GetMin());
                WriteValue(name, NPrometheus::MAX_SUFFIX, labels, "", "", time, s->GetMax());
                WriteValue(name, NPrometheus::LAST_SUFFIX, labels, "", "", time, s->GetLast());
                WriteValue(name, NPrometheus::COUNT_SUFFIX, labels, "", "", time, s->GetCount());
            }

            void WriteLn() {
                Out_->Write('\n');
            }

        private:
            // will replace invalid chars with '_'
            void WriteMetricName(TStringBuf name) {
                Y_ENSURE(!name.Empty(), "trying to write metric with empty name");

                char ch = name[0];
                if (NPrometheus::IsValidMetricNameStart(ch)) {
                    Out_->Write(ch);
                } else {
                    Out_->Write('_');
                }

                for (size_t i = 1, len = name.length(); i < len; i++) {
                    ch = name[i];
                    if (NPrometheus::IsValidMetricNameContinuation(ch)) {
                        Out_->Write(ch);
                    } else {
                        Out_->Write('_');
                    }
                }
            }

            void WriteLabels(const TLabels& labels, TStringBuf addLabelKey, TStringBuf addLabelValue) {
                Out_->Write('{');
                for (auto&& l: labels) {
                    Out_->Write(l.Name());
                    Out_->Write('=');
                    WriteLabelValue(l.Value());
                    Out_->Write(", "); // trailign comma is supported in parsers
                }
                if (!addLabelKey.Empty() && !addLabelValue.Empty()) {
                    Out_->Write(addLabelKey);
                    Out_->Write('=');
                    WriteLabelValue(addLabelValue);
                }
                Out_->Write('}');
            }

            void WriteLabelValue(TStringBuf value) {
                Out_->Write('"');
                for (char ch: value) {
                    if (ch == '"') {
                        Out_->Write("\\\"");
                    } else if (ch == '\\') {
                        Out_->Write("\\\\");
                    } else if (ch == '\n') {
                        Out_->Write("\\n");
                    } else {
                        Out_->Write(ch);
                    }
                }
                Out_->Write('"');
            }

            void WriteValue(
                    TStringBuf name, TStringBuf suffix,
                    const TLabels& labels, TStringBuf addLabelKey, TStringBuf addLabelValue,
                    TInstant time, double value)
            {
                // (1) name
                WriteMetricName(name);
                if (!suffix.Empty()) {
                    Out_->Write(suffix);
                }

                // (2) labels
                if (!labels.Empty() || !addLabelKey.Empty()) {
                    WriteLabels(labels, addLabelKey, addLabelValue);
                }
                Out_->Write(' ');

                // (3) value
                {
                    size_t len = FloatToString(value, TmpBuf_, Y_ARRAY_SIZE(TmpBuf_));
                    Out_->Write(TmpBuf_, len);
                }

                // (4) time
                if (ui64 timeMillis = time.MilliSeconds()) {
                    Out_->Write(' ');
                    size_t len = IntToString<10>(timeMillis, TmpBuf_, Y_ARRAY_SIZE(TmpBuf_));
                    Out_->Write(TmpBuf_, len);
                }
                Out_->Write('\n');
            }

        private:
            IOutputStream* Out_;
            THashSet<TString> WrittenTypes_;
            char TmpBuf_[512]; // used to convert doubles to strings
        };

        ///////////////////////////////////////////////////////////////////////
        // TMetricState
        ///////////////////////////////////////////////////////////////////////
        struct TMetricState {
            EMetricType Type = EMetricType::UNKNOWN;
            TLabels Labels;
            TInstant Time = TInstant::Zero();
            EMetricValueType ValueType = EMetricValueType::UNKNOWN;
            TMetricValue Value;

            ~TMetricState() {
                ClearValue();
            }

            void Clear() {
                Type = EMetricType::UNKNOWN;
                Labels.Clear();
                Time = TInstant::Zero();
                ClearValue();
            }

            void ClearValue() {
                // TMetricValue does not keep ownership of histogram
                if (ValueType == EMetricValueType::HISTOGRAM) {
                    Value.AsHistogram()->UnRef();
                } else if (ValueType == EMetricValueType::SUMMARY) {
                    Value.AsSummaryDouble()->UnRef();
                }
                ValueType = EMetricValueType::UNKNOWN;
                Value = {};
            }

            template <typename T>
            void SetValue(T value) {
                // TMetricValue does not keep ownership of histogram
                if (ValueType == EMetricValueType::HISTOGRAM) {
                    Value.AsHistogram()->UnRef();
                } else if (ValueType == EMetricValueType::SUMMARY) {
                    Value.AsSummaryDouble()->UnRef();
                }
                ValueType = TValueType<T>::Type;
                Value = TMetricValue(value);
                if (ValueType == EMetricValueType::HISTOGRAM) {
                    Value.AsHistogram()->Ref();
                } else if (ValueType == EMetricValueType::SUMMARY) {
                    Value.AsSummaryDouble()->Ref();
                }
            }
        };

        ///////////////////////////////////////////////////////////////////////
        // TPrometheusEncoder
        ///////////////////////////////////////////////////////////////////////
        class TPrometheusEncoder final: public IMetricEncoder {
        public:
            explicit TPrometheusEncoder(IOutputStream* out, TStringBuf metricNameLabel)
                : Writer_(out)
                , MetricNameLabel_(metricNameLabel)
            {
            }

        private:
            void OnStreamBegin() override {
                State_.Expect(TEncoderState::EState::ROOT);
            }

            void OnStreamEnd() override {
                State_.Expect(TEncoderState::EState::ROOT);
                Writer_.WriteLn();
            }

            void OnCommonTime(TInstant time) override {
                State_.Expect(TEncoderState::EState::ROOT);
                CommonTime_ = time;
            }

            void OnMetricBegin(EMetricType type) override {
                State_.Switch(TEncoderState::EState::ROOT, TEncoderState::EState::METRIC);
                MetricState_.Clear();
                MetricState_.Type = type;
            }

            void OnMetricEnd() override {
                State_.Switch(TEncoderState::EState::METRIC, TEncoderState::EState::ROOT);
                WriteMetric();
            }

            void OnLabelsBegin() override {
                if (State_ == TEncoderState::EState::METRIC) {
                    State_ = TEncoderState::EState::METRIC_LABELS;
                } else if (State_ == TEncoderState::EState::ROOT) {
                    State_ = TEncoderState::EState::COMMON_LABELS;
                } else {
                    State_.ThrowInvalid("expected METRIC or ROOT");
                }
            }

            void OnLabelsEnd() override {
                if (State_ == TEncoderState::EState::METRIC_LABELS) {
                    State_ = TEncoderState::EState::METRIC;
                } else if (State_ == TEncoderState::EState::COMMON_LABELS) {
                    State_ = TEncoderState::EState::ROOT;
                } else {
                    State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
                }
            }

            void OnLabel(TStringBuf name, TStringBuf value) override {
                if (State_ == TEncoderState::EState::METRIC_LABELS) {
                    MetricState_.Labels.Add(name, value);
                } else if (State_ == TEncoderState::EState::COMMON_LABELS) {
                    CommonLabels_.Add(name, value);
                } else {
                    State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
                }
            }

            void OnLabel(ui32 name, ui32 value) override {
                OnLabel(LabelNamesPool_.Get(name), LabelValuesPool_.Get(value));
            }

            std::pair<ui32, ui32> PrepareLabel(TStringBuf name, TStringBuf value) override {
                auto nameLabel = LabelNamesPool_.PutIfAbsent(name);
                auto valueLabel = LabelValuesPool_.PutIfAbsent(value);
                return std::make_pair(nameLabel->Index, valueLabel->Index);
            }

            void OnDouble(TInstant time, double value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                MetricState_.Time = time;
                MetricState_.SetValue(value);
            }

            void OnInt64(TInstant time, i64 value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                MetricState_.Time = time;
                MetricState_.SetValue(value);
            }

            void OnUint64(TInstant time, ui64 value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                MetricState_.Time = time;
                MetricState_.SetValue(value);
            }

            void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                MetricState_.Time = time;
                MetricState_.SetValue(snapshot.Get());
            }

            void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                MetricState_.Time = time;
                MetricState_.SetValue(snapshot.Get());
            }

            void OnLogHistogram(TInstant, TLogHistogramSnapshotPtr) override {
                // TODO(@kbalakirev): implement this function
            }

            void Close() override {
            }

            void WriteMetric() {
                if (MetricState_.ValueType == EMetricValueType::UNKNOWN) {
                    return;
                }

                // XXX: poor performace
                for (auto&& l: CommonLabels_) {
                    MetricState_.Labels.Add(l.Name(), l.Value());
                }

                TMaybe<TLabel> nameLabel = MetricState_.Labels.Extract(MetricNameLabel_);
                if (!nameLabel) {
                    return;
                }

                const TString& metricName = ToString(nameLabel->Value());
                if (MetricState_.Type != EMetricType::DSUMMARY) {
                    Writer_.WriteType(MetricState_.Type, metricName);
                }

                if (MetricState_.Time == TInstant::Zero()) {
                    MetricState_.Time = CommonTime_;
                }

                EMetricType type = MetricState_.Type;
                if (type == EMetricType::HIST || type == EMetricType::HIST_RATE) {
                    Y_ENSURE(MetricState_.ValueType == EMetricValueType::HISTOGRAM,
                             "invalid value type for histogram: " << int(MetricState_.ValueType)); // TODO: to string conversion
                    Writer_.WriteHistogram(
                        metricName,
                        MetricState_.Labels,
                        MetricState_.Time,
                        MetricState_.Value.AsHistogram());
                } else if (type == EMetricType::DSUMMARY) {
                    Writer_.WriteSummaryDouble(
                        metricName,
                        MetricState_.Labels,
                        MetricState_.Time,
                        MetricState_.Value.AsSummaryDouble());
                } else {
                    Writer_.WriteDouble(
                        metricName,
                        MetricState_.Labels,
                        MetricState_.Time,
                        MetricState_.Value.AsDouble(MetricState_.ValueType));
                }
            }

        private:
            TEncoderState State_;
            TPrometheusWriter Writer_;
            TString MetricNameLabel_;
            TInstant CommonTime_ = TInstant::Zero();
            TLabels CommonLabels_;
            TMetricState MetricState_;

            TStringPoolBuilder LabelNamesPool_;
            TStringPoolBuilder LabelValuesPool_;
        };
    }

    IMetricEncoderPtr EncoderPrometheus(IOutputStream* out, TStringBuf metricNameLabel) {
        return MakeHolder<TPrometheusEncoder>(out, metricNameLabel);
    }

} // namespace NMonitoring
