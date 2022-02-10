#include "json.h"
#include "typed_point.h"

#include <library/cpp/monlib/encode/buffered/buffered_encoder_base.h>
#include <library/cpp/monlib/encode/encoder_state.h>
#include <library/cpp/monlib/metrics/metric.h>
#include <library/cpp/monlib/metrics/metric_value.h>
#include <library/cpp/monlib/metrics/labels.h>

#include <library/cpp/json/writer/json.h>

#include <util/charset/utf8.h>
#include <util/generic/algorithm.h>

namespace NMonitoring {
    namespace {
        enum class EJsonStyle {
            Solomon,
            Cloud
        };

        ///////////////////////////////////////////////////////////////////////
        // TJsonWriter
        ///////////////////////////////////////////////////////////////////////
        class TJsonWriter {
        public:
            TJsonWriter(IOutputStream* out, int indentation, EJsonStyle style, TStringBuf metricNameLabel)
                : Buf_(NJsonWriter::HEM_UNSAFE, out)
                , Style_(style)
                , MetricNameLabel_(metricNameLabel)
                , CurrentMetricName_()
            {
                Buf_.SetIndentSpaces(indentation);
                Buf_.SetWriteNanAsString();
            }

            void WriteTime(TInstant time) {
                if (time != TInstant::Zero()) {
                    Buf_.WriteKey(TStringBuf("ts"));
                    if (Style_ == EJsonStyle::Solomon) {
                        Buf_.WriteULongLong(time.Seconds());
                    } else {
                        Buf_.WriteString(time.ToString());
                    }
                }
            }

            void WriteValue(double value) {
                Buf_.WriteKey(TStringBuf("value"));
                Buf_.WriteDouble(value);
            }

            void WriteValue(i64 value) {
                Buf_.WriteKey(TStringBuf("value"));
                Buf_.WriteLongLong(value);
            }

            void WriteValue(ui64 value) {
                Buf_.WriteKey(TStringBuf("value"));
                Buf_.WriteULongLong(value);
            }

            void WriteValue(IHistogramSnapshot* s) {
                Y_ENSURE(Style_ == EJsonStyle::Solomon);

                Buf_.WriteKey(TStringBuf("hist"));
                Buf_.BeginObject();
                if (ui32 count = s->Count()) {
                    bool hasInf = (s->UpperBound(count - 1) == Max<double>());
                    if (hasInf) {
                        count--;
                    }

                    Buf_.WriteKey(TStringBuf("bounds"));
                    Buf_.BeginList();
                    for (ui32 i = 0; i < count; i++) {
                        Buf_.WriteDouble(s->UpperBound(i));
                    }
                    Buf_.EndList();

                    Buf_.WriteKey(TStringBuf("buckets"));
                    Buf_.BeginList();
                    for (ui32 i = 0; i < count; i++) {
                        Buf_.WriteULongLong(s->Value(i));
                    }
                    Buf_.EndList();

                    if (hasInf) {
                        Buf_.WriteKey(TStringBuf("inf"));
                        Buf_.WriteULongLong(s->Value(count));
                    }
                }
                Buf_.EndObject();
            }

            void WriteValue(ISummaryDoubleSnapshot* s) {
                Y_ENSURE(Style_ == EJsonStyle::Solomon);

                Buf_.WriteKey(TStringBuf("summary"));
                Buf_.BeginObject();

                Buf_.WriteKey(TStringBuf("sum"));
                Buf_.WriteDouble(s->GetSum());

                Buf_.WriteKey(TStringBuf("min"));
                Buf_.WriteDouble(s->GetMin());

                Buf_.WriteKey(TStringBuf("max"));
                Buf_.WriteDouble(s->GetMax());

                Buf_.WriteKey(TStringBuf("last"));
                Buf_.WriteDouble(s->GetLast());

                Buf_.WriteKey(TStringBuf("count"));
                Buf_.WriteULongLong(s->GetCount());

                Buf_.EndObject();
            }

            void WriteValue(TLogHistogramSnapshot* s) {
                Y_ENSURE(Style_ == EJsonStyle::Solomon);

                Buf_.WriteKey(TStringBuf("log_hist"));
                Buf_.BeginObject();

                Buf_.WriteKey(TStringBuf("base"));
                Buf_.WriteDouble(s->Base());

                Buf_.WriteKey(TStringBuf("zeros_count"));
                Buf_.WriteULongLong(s->ZerosCount());

                Buf_.WriteKey(TStringBuf("start_power"));
                Buf_.WriteInt(s->StartPower());

                Buf_.WriteKey(TStringBuf("buckets"));
                Buf_.BeginList();
                for (size_t i = 0; i < s->Count(); ++i) {
                    Buf_.WriteDouble(s->Bucket(i));
                }
                Buf_.EndList();

                Buf_.EndObject();
            }

            void WriteValue(EMetricValueType type, TMetricValue value) {
                switch (type) {
                    case EMetricValueType::DOUBLE:
                        WriteValue(value.AsDouble());
                        break;

                    case EMetricValueType::INT64:
                        WriteValue(value.AsInt64());
                        break;

                    case EMetricValueType::UINT64:
                        WriteValue(value.AsUint64());
                        break;

                    case EMetricValueType::HISTOGRAM:
                        WriteValue(value.AsHistogram());
                        break;

                    case EMetricValueType::SUMMARY:
                        WriteValue(value.AsSummaryDouble());
                        break;

                    case EMetricValueType::LOGHISTOGRAM:
                        WriteValue(value.AsLogHistogram());
                        break;

                    case EMetricValueType::UNKNOWN:
                        ythrow yexception() << "unknown metric value type";
                }
            }

            void WriteLabel(TStringBuf name, TStringBuf value) {
                Y_ENSURE(IsUtf(name), "label name is not valid UTF-8 string");
                Y_ENSURE(IsUtf(value), "label value is not valid UTF-8 string");
                if (Style_ == EJsonStyle::Cloud && name == MetricNameLabel_) {
                    CurrentMetricName_ = value;
                } else {
                    Buf_.WriteKey(name);
                    Buf_.WriteString(value);
                }
            }

            void WriteMetricType(EMetricType type) {
                if (Style_ == EJsonStyle::Cloud) {
                    Buf_.WriteKey("type");
                    Buf_.WriteString(MetricTypeToCloudStr(type));
                } else {
                    Buf_.WriteKey("kind");
                    Buf_.WriteString(MetricTypeToStr(type));
                }
            }

            void WriteName() {
                if (Style_ != EJsonStyle::Cloud) {
                    return;
                }
                if (CurrentMetricName_.Empty()) {
                    ythrow yexception() << "label '" << MetricNameLabel_ << "' is not defined";
                }
                Buf_.WriteKey("name");
                Buf_.WriteString(CurrentMetricName_);
                CurrentMetricName_.clear();
            }

        private:
            static TStringBuf MetricTypeToCloudStr(EMetricType type) {
                switch (type) {
                    case EMetricType::GAUGE:
                        return TStringBuf("DGAUGE");
                    case EMetricType::COUNTER:
                        return TStringBuf("COUNTER");
                    case EMetricType::RATE:
                        return TStringBuf("RATE");
                    case EMetricType::IGAUGE:
                        return TStringBuf("IGAUGE");
                    default:
                        ythrow yexception() << "metric type '" << type << "' is not supported by cloud json format";
                }
            }

        protected:
            NJsonWriter::TBuf Buf_;
            EJsonStyle Style_;
            TString MetricNameLabel_;
            TString CurrentMetricName_;
        };

        ///////////////////////////////////////////////////////////////////////
        // TEncoderJson
        ///////////////////////////////////////////////////////////////////////
        class TEncoderJson final: public IMetricEncoder, public TJsonWriter {
        public:
            TEncoderJson(IOutputStream* out, int indentation, EJsonStyle style, TStringBuf metricNameLabel)
                : TJsonWriter{out, indentation, style, metricNameLabel}
            {
            }

            ~TEncoderJson() override {
                Close();
            }

        private:
            void OnStreamBegin() override {
                State_.Expect(TEncoderState::EState::ROOT);
                Buf_.BeginObject();
            }

            void OnStreamEnd() override {
                State_.Expect(TEncoderState::EState::ROOT);
                if (!Buf_.KeyExpected()) {
                    // not closed metrics array
                    Buf_.EndList();
                }
                Buf_.EndObject();
            }

            void OnCommonTime(TInstant time) override {
                State_.Expect(TEncoderState::EState::ROOT);
                WriteTime(time);
            }

            void OnMetricBegin(EMetricType type) override {
                State_.Switch(TEncoderState::EState::ROOT, TEncoderState::EState::METRIC);
                if (Buf_.KeyExpected()) {
                    // first metric, so open metrics array
                    Buf_.WriteKey(TStringBuf(Style_ == EJsonStyle::Solomon ? "sensors" : "metrics"));
                    Buf_.BeginList();
                }
                Buf_.BeginObject();
                WriteMetricType(type);
            }

            void OnMetricEnd() override {
                State_.Switch(TEncoderState::EState::METRIC, TEncoderState::EState::ROOT);
                if (!Buf_.KeyExpected()) {
                    // not closed timeseries array
                    Buf_.EndList();
                }

                if (!TimeSeries_ && LastPoint_.HasValue()) {
                    // we have seen only one point between OnMetricBegin() and
                    // OnMetricEnd() calls
                    WriteTime(LastPoint_.GetTime());
                    WriteValue(LastPoint_.GetValueType(), LastPoint_.GetValue());
                }
                Buf_.EndObject();

                LastPoint_ = {};
                TimeSeries_ = false;
            }

            void OnLabelsBegin() override {
                if (!Buf_.KeyExpected()) {
                    // not closed metrics or timeseries array if labels go after values
                    Buf_.EndList();
                }
                if (State_ == TEncoderState::EState::ROOT) {
                    State_ = TEncoderState::EState::COMMON_LABELS;
                    Buf_.WriteKey(TStringBuf(Style_ == EJsonStyle::Solomon ? "commonLabels" : "labels"));
                } else if (State_ == TEncoderState::EState::METRIC) {
                    State_ = TEncoderState::EState::METRIC_LABELS;
                    Buf_.WriteKey(TStringBuf("labels"));
                } else {
                    State_.ThrowInvalid("expected METRIC or ROOT");
                }
                Buf_.BeginObject();

                EmptyLabels_ = true;
            }

            void OnLabelsEnd() override {
                if (State_ == TEncoderState::EState::METRIC_LABELS) {
                    State_ = TEncoderState::EState::METRIC;
                } else if (State_ == TEncoderState::EState::COMMON_LABELS) {
                    State_ = TEncoderState::EState::ROOT;
                } else {
                    State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
                }

                Y_ENSURE(!EmptyLabels_, "Labels cannot be empty");
                Buf_.EndObject();
                if (State_ == TEncoderState::EState::METRIC) {
                    WriteName();
                }
            }

            void OnLabel(TStringBuf name, TStringBuf value) override {
                if (State_ == TEncoderState::EState::METRIC_LABELS || State_ == TEncoderState::EState::COMMON_LABELS) {
                    WriteLabel(name, value);
                } else {
                    State_.ThrowInvalid("expected LABELS or COMMON_LABELS");
                }

                EmptyLabels_ = false;
            }

            void OnDouble(TInstant time, double value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                Write<double>(time, value);
            }

            void OnInt64(TInstant time, i64 value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                Write<i64>(time, value);
            }

            void OnUint64(TInstant time, ui64 value) override {
                State_.Expect(TEncoderState::EState::METRIC);
                Write<ui64>(time, value);
            }

            void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                Write<IHistogramSnapshot*>(time, snapshot.Get());
            }

            void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                Write<ISummaryDoubleSnapshot*>(time, snapshot.Get());
            }

            void OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr snapshot) override {
                State_.Expect(TEncoderState::EState::METRIC);
                Write<TLogHistogramSnapshot*>(time, snapshot.Get());
            }

            template <typename T>
            void Write(TInstant time, T value) {
                State_.Expect(TEncoderState::EState::METRIC);

                if (!LastPoint_.HasValue()) {
                    LastPoint_ = {time, value};
                } else {
                    // second point
                    // TODO: output types
                    Y_ENSURE(LastPoint_.GetValueType() == TValueType<T>::Type,
                             "mixed metric value types in one metric");

                    if (!TimeSeries_) {
                        Buf_.WriteKey(TStringBuf("timeseries"));
                        Buf_.BeginList();
                        Buf_.BeginObject();
                        Y_ENSURE(LastPoint_.GetTime() != TInstant::Zero(),
                            "time cannot be empty or zero in a timeseries point");
                        WriteTime(LastPoint_.GetTime());
                        WriteValue(LastPoint_.GetValueType(), LastPoint_.GetValue());
                        Buf_.EndObject();
                        TimeSeries_ = true;
                    }

                    if (TimeSeries_) {
                        Buf_.BeginObject();
                        Y_ENSURE(time != TInstant::Zero(),
                            "time cannot be empty or zero in a timeseries point");

                        WriteTime(time);
                        WriteValue(value);
                        Buf_.EndObject();
                    }
                }
            }

            void Close() override {
                LastPoint_ = {};
            }

        private:
            TEncoderState State_;
            TTypedPoint LastPoint_;
            bool TimeSeries_ = false;
            bool EmptyLabels_ = false;
        };

        ///////////////////////////////////////////////////////////////////////
        // TBufferedJsonEncoder
        ///////////////////////////////////////////////////////////////////////
        class TBufferedJsonEncoder : public TBufferedEncoderBase, public TJsonWriter {
        public:
            TBufferedJsonEncoder(IOutputStream* out, int indentation, EJsonStyle style, TStringBuf metricNameLabel)
                : TJsonWriter{out, indentation, style, metricNameLabel}
            {
                MetricsMergingMode_ = EMetricsMergingMode::MERGE_METRICS;
            }

            ~TBufferedJsonEncoder() override {
                Close();
            }

            void OnLabelsBegin() override {
                TBufferedEncoderBase::OnLabelsBegin();
                EmptyLabels_ = true;
            }

            void OnLabel(TStringBuf name, TStringBuf value) override {
                TBufferedEncoderBase::OnLabel(name, value);
                EmptyLabels_ = false;
            }

            void OnLabel(ui32 name, ui32 value) override {
                TBufferedEncoderBase::OnLabel(name, value);
                EmptyLabels_ = false;
            }

            void OnLabelsEnd() override {
                TBufferedEncoderBase::OnLabelsEnd();
                Y_ENSURE(!EmptyLabels_, "Labels cannot be empty");
            }

            void Close() final {
                if (Closed_) {
                    return;
                }

                Closed_ = true;

                LabelValuesPool_.Build();
                LabelNamesPool_.Build();

                Buf_.BeginObject();

                WriteTime(CommonTime_);
                if (CommonLabels_.size() > 0) {
                    Buf_.WriteKey(TStringBuf(Style_ == EJsonStyle::Solomon ? "commonLabels": "labels"));
                    WriteLabels(CommonLabels_, true);
                }

                if (Metrics_.size() > 0) {
                    Buf_.WriteKey(TStringBuf(Style_ == EJsonStyle::Solomon ? "sensors" : "metrics"));
                    WriteMetrics();
                }

                Buf_.EndObject();
            }

        private:
            void WriteMetrics() {
                Buf_.BeginList();
                for (auto&& metric : Metrics_) {
                    WriteMetric(metric);
                }
                Buf_.EndList();
            }

            void WriteMetric(TMetric& metric) {
                Buf_.BeginObject();

                WriteMetricType(metric.MetricType);

                Buf_.WriteKey(TStringBuf("labels"));
                WriteLabels(metric.Labels, false);

                metric.TimeSeries.SortByTs();
                if (metric.TimeSeries.Size() == 1) {
                    const auto& point = metric.TimeSeries[0];
                    WriteTime(point.GetTime());
                    WriteValue(metric.TimeSeries.GetValueType(), point.GetValue());
                } else if (metric.TimeSeries.Size() > 1) {
                    Buf_.WriteKey(TStringBuf("timeseries"));
                    Buf_.BeginList();
                    metric.TimeSeries.ForEach([this](TInstant time, EMetricValueType type, TMetricValue value) {
                        Buf_.BeginObject();
                        // make gcc 6.1 happy https://gcc.gnu.org/bugzilla/show_bug.cgi?id=61636
                        this->WriteTime(time);
                        this->WriteValue(type, value);
                        Buf_.EndObject();
                    });

                    Buf_.EndList();
                }

                Buf_.EndObject();
            }

            void WriteLabels(const TPooledLabels& labels, bool isCommon) {
                Buf_.BeginObject();

                for (auto i = 0u; i < labels.size(); ++i) {
                    TStringBuf name = LabelNamesPool_.Get(labels[i].Key->Index);
                    TStringBuf value = LabelValuesPool_.Get(labels[i].Value->Index);

                    WriteLabel(name, value);
                }

                Buf_.EndObject();

                if (!isCommon) {
                    WriteName();
                }
            }

        private:
            bool Closed_{false};
            bool EmptyLabels_ = false;
        };
    }

    IMetricEncoderPtr EncoderJson(IOutputStream* out, int indentation) {
        return MakeHolder<TEncoderJson>(out, indentation, EJsonStyle::Solomon, "");
    }

    IMetricEncoderPtr BufferedEncoderJson(IOutputStream* out, int indentation) {
        return MakeHolder<TBufferedJsonEncoder>(out, indentation, EJsonStyle::Solomon, "");
    }

    IMetricEncoderPtr EncoderCloudJson(IOutputStream* out, int indentation, TStringBuf metricNameLabel) {
        return MakeHolder<TEncoderJson>(out, indentation, EJsonStyle::Cloud, metricNameLabel);
    }

    IMetricEncoderPtr BufferedEncoderCloudJson(IOutputStream* out, int indentation, TStringBuf metricNameLabel) {
        return MakeHolder<TBufferedJsonEncoder>(out, indentation, EJsonStyle::Cloud, metricNameLabel);
    }
}
