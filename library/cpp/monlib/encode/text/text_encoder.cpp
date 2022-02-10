#include "text.h"

#include <library/cpp/monlib/encode/encoder_state.h>
#include <library/cpp/monlib/metrics/labels.h>
#include <library/cpp/monlib/metrics/metric_value.h>

#include <util/datetime/base.h>
#include <util/stream/format.h>

namespace NMonitoring {
    namespace {
        class TEncoderText final: public IMetricEncoder {
        public:
            TEncoderText(IOutputStream* out, bool humanReadableTs)
                : Out_(out)
                , HumanReadableTs_(humanReadableTs)
            {
            }

        private:
            void OnStreamBegin() override {
                State_.Expect(TEncoderState::EState::ROOT);
            }

            void OnStreamEnd() override {
                State_.Expect(TEncoderState::EState::ROOT);
            }

            void OnCommonTime(TInstant time) override {
                State_.Expect(TEncoderState::EState::ROOT);
                CommonTime_ = time;
                if (time != TInstant::Zero()) {
                    Out_->Write(TStringBuf("common time: "));
                    WriteTime(time);
                    Out_->Write('\n');
                }
            }

            void OnMetricBegin(EMetricType type) override {
                State_.Switch(TEncoderState::EState::ROOT, TEncoderState::EState::METRIC);
                ClearLastMetricState();
                MetricType_ = type;
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
                    Out_->Write(TStringBuf("common labels: "));
                    WriteLabels();
                    Out_->Write('\n');
                } else {
                    State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
                }
            }

            void OnLabel(TStringBuf name, TStringBuf value) override {
                Labels_.Add(name, value);
            }

            void OnDouble(TInstant time, double value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                TimeSeries_.Add(time, value);
            }

            void OnInt64(TInstant time, i64 value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                TimeSeries_.Add(time, value);
            }

            void OnUint64(TInstant time, ui64 value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                TimeSeries_.Add(time, value);
            }

            void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                TimeSeries_.Add(time, snapshot.Get());
            }

            void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                TimeSeries_.Add(time, snapshot.Get());
            }

            void OnLogHistogram(TInstant ts, TLogHistogramSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                TimeSeries_.Add(ts, snapshot.Get());
            }

            void Close() override {
            }

            void WriteTime(TInstant time) {
                if (HumanReadableTs_) {
                    char buf[64];
                    auto len = FormatDate8601(buf, sizeof(buf), time.TimeT());
                    Out_->Write(buf, len);
                } else {
                    (*Out_) << time.Seconds();
                }
            }

            void WriteValue(EMetricValueType type, TMetricValue value) {
                switch (type) {
                case EMetricValueType::DOUBLE:
                    (*Out_) << value.AsDouble();
                    break;
                case EMetricValueType::INT64:
                    (*Out_) << value.AsInt64();
                    break;
                case EMetricValueType::UINT64:
                    (*Out_) << value.AsUint64();
                    break;
                case EMetricValueType::HISTOGRAM:
                    (*Out_) << *value.AsHistogram();
                    break;
                case EMetricValueType::SUMMARY:
                    (*Out_) << *value.AsSummaryDouble();
                    break;
                case EMetricValueType::LOGHISTOGRAM:
                    (*Out_) << *value.AsLogHistogram();
                    break;
                case EMetricValueType::UNKNOWN:
                    ythrow yexception() << "unknown metric value type";
                }
            }

            void WriteLabels() {
                auto& out = *Out_;
                const auto size = Labels_.Size();
                size_t i = 0;

                out << '{';
                for (auto&& l : Labels_) {
                    out << l.Name() << TStringBuf("='") << l.Value() << '\'';

                    ++i;
                    if (i < size) {
                        out << TStringBuf(", ");
                    }
                };

                out << '}';
            }

            void WriteMetric() {
                // (1) type
                TStringBuf typeStr = MetricTypeToStr(MetricType_);
                (*Out_) << LeftPad(typeStr, MaxMetricTypeNameLength) << ' ';

                // (2) name and labels
                auto name = Labels_.Extract(TStringBuf("sensor"));
                if (name) {
                    if (name->Value().find(' ') != TString::npos) {
                        (*Out_) << '"' << name->Value() << '"';
                    } else {
                        (*Out_) << name->Value();
                    }
                }
                WriteLabels();

                // (3) values
                if (!TimeSeries_.Empty()) {
                    TimeSeries_.SortByTs();
                    Out_->Write(TStringBuf(" ["));
                    for (size_t i = 0; i < TimeSeries_.Size(); i++) {
                        if (i > 0) {
                            Out_->Write(TStringBuf(", "));
                        }

                        const auto& point = TimeSeries_[i];
                        if (point.GetTime() == CommonTime_ || point.GetTime() == TInstant::Zero()) {
                            WriteValue(TimeSeries_.GetValueType(), point.GetValue());
                        } else {
                            Out_->Write('(');
                            WriteTime(point.GetTime());
                            Out_->Write(TStringBuf(", "));
                            WriteValue(TimeSeries_.GetValueType(), point.GetValue());
                            Out_->Write(')');
                        }
                    }
                    Out_->Write(']');
                }
                Out_->Write('\n');
            }

            void ClearLastMetricState() {
                MetricType_ = EMetricType::UNKNOWN;
                Labels_.Clear();
                TimeSeries_.Clear();
            }

        private:
            TEncoderState State_;
            IOutputStream* Out_;
            bool HumanReadableTs_;
            TInstant CommonTime_ = TInstant::Zero();
            EMetricType MetricType_ = EMetricType::UNKNOWN;
            TLabels Labels_;
            TMetricTimeSeries TimeSeries_;
        };

    }

    IMetricEncoderPtr EncoderText(IOutputStream* out, bool humanReadableTs) {
        return MakeHolder<TEncoderText>(out, humanReadableTs);
    }

}
