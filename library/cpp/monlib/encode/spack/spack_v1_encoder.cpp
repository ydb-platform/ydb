#include "spack_v1.h"
#include "compression.h"
#include "varint.h"

#include <library/cpp/monlib/encode/buffered/buffered_encoder_base.h>

#include <util/generic/cast.h>
#include <util/datetime/base.h>
#include <util/string/builder.h>

#ifndef _little_endian_
#error Unsupported platform
#endif

namespace NMonitoring {
    namespace {
        ///////////////////////////////////////////////////////////////////////
        // TEncoderSpackV1
        ///////////////////////////////////////////////////////////////////////
        class TEncoderSpackV1 final: public TBufferedEncoderBase {
        public:
            TEncoderSpackV1(
                IOutputStream* out,
                ETimePrecision timePrecision,
                ECompression compression,
                EMetricsMergingMode mergingMode,
                ESpackV1Version version,
                TStringBuf metricNameLabel
            )
                : Out_(out)
                , TimePrecision_(timePrecision)
                , Compression_(compression)
                , Version_(version)
                , MetricName_(Version_ >= SV1_02 ? LabelNamesPool_.PutIfAbsent(metricNameLabel) : nullptr)
            {
                MetricsMergingMode_ = mergingMode;

                LabelNamesPool_.SetSorted(true);
                LabelValuesPool_.SetSorted(true);
            }

            ~TEncoderSpackV1() override {
                Close();
            }

        private:
            void OnDouble(TInstant time, double value) override {
                TBufferedEncoderBase::OnDouble(time, value);
            }

            void OnInt64(TInstant time, i64 value) override {
                TBufferedEncoderBase::OnInt64(time, value);
            }

            void OnUint64(TInstant time, ui64 value) override {
                TBufferedEncoderBase::OnUint64(time, value);
            }

            void OnHistogram(TInstant time, IHistogramSnapshotPtr snapshot) override {
                TBufferedEncoderBase::OnHistogram(time, snapshot);
            }

            void OnSummaryDouble(TInstant time, ISummaryDoubleSnapshotPtr snapshot) override {
                TBufferedEncoderBase::OnSummaryDouble(time, snapshot);
            }

            void OnLogHistogram(TInstant time, TLogHistogramSnapshotPtr snapshot) override {
                TBufferedEncoderBase::OnLogHistogram(time, snapshot);
            }

            void Close() override {
                if (Closed_) {
                    return;
                }
                Closed_ = true;

                LabelNamesPool_.Build();
                LabelValuesPool_.Build();

                // Sort all points uniquely by ts -- the size can decrease
                ui64 pointsCount = 0;
                for (TMetric& metric : Metrics_) {
                    if (metric.TimeSeries.Size() > 1) {
                        metric.TimeSeries.SortByTs();
                    }

                    pointsCount += metric.TimeSeries.Size();
                }

                // (1) write header
                TSpackHeader header;
                header.Version = Version_;
                header.TimePrecision = EncodeTimePrecision(TimePrecision_);
                header.Compression = EncodeCompression(Compression_);
                header.LabelNamesSize = static_cast<ui32>(
                    LabelNamesPool_.BytesSize() + LabelNamesPool_.Count());
                header.LabelValuesSize = static_cast<ui32>(
                    LabelValuesPool_.BytesSize() + LabelValuesPool_.Count());
                header.MetricCount = Metrics_.size();
                header.PointsCount = pointsCount;
                Out_->Write(&header, sizeof(header));

                // if compression enabled all below writes must go throught compressor
                auto compressedOut = CompressedOutput(Out_, Compression_);
                if (compressedOut) {
                    Out_ = compressedOut.Get();
                }

                // (2) write string pools
                auto strPoolWrite = [this](TStringBuf str, ui32, ui32) {
                    Out_->Write(str);
                    Out_->Write('\0');
                };

                LabelNamesPool_.ForEach(strPoolWrite);
                LabelValuesPool_.ForEach(strPoolWrite);

                // (3) write common time
                WriteTime(CommonTime_);

                // (4) write common labels' indexes
                WriteLabels(CommonLabels_, nullptr);

                // (5) write metrics
                // metrics count already written in header
                for (TMetric& metric : Metrics_) {
                    // (5.1) types byte
                    ui8 typesByte = PackTypes(metric);
                    Out_->Write(&typesByte, sizeof(typesByte));

                    // TODO: implement
                    ui8 flagsByte = 0x00;
                    Out_->Write(&flagsByte, sizeof(flagsByte));

                    // v1.2 format addition — metric name
                    if (Version_ >= SV1_02) {
                        const auto it = FindIf(metric.Labels, [&](const auto& l) {
                            return l.Key == MetricName_;
                        });
                        Y_ENSURE(it != metric.Labels.end(),
                                 "metric name label '" << LabelNamesPool_.Get(MetricName_->Index) << "' not found, "
                                 << "all metric labels '" << FormatLabels(metric.Labels) << "'");
                        WriteVarUInt32(Out_, it->Value->Index);
                    }

                    // (5.2) labels
                    WriteLabels(metric.Labels, MetricName_);

                    // (5.3) values
                    switch (metric.TimeSeries.Size()) {
                        case 0:
                            break;
                        case 1: {
                            const auto& point = metric.TimeSeries[0];
                            if (point.GetTime() != TInstant::Zero()) {
                                WriteTime(point.GetTime());
                            }
                            EMetricValueType valueType = metric.TimeSeries.GetValueType();
                            WriteValue(metric.MetricType, valueType, point.GetValue());
                            break;
                        }
                        default:
                            WriteVarUInt32(Out_, static_cast<ui32>(metric.TimeSeries.Size()));
                            const TMetricTimeSeries& ts = metric.TimeSeries;
                            EMetricType metricType = metric.MetricType;
                            ts.ForEach([this, metricType](TInstant time, EMetricValueType valueType, TMetricValue value) {
                                // workaround for GCC bug
                                // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=61636
                                this->WriteTime(time);
                                this->WriteValue(metricType, valueType, value);
                            });
                            break;
                    }
                }
            }

            // store metric type and values type in one byte
            ui8 PackTypes(const TMetric& metric) {
                EValueType valueType;
                if (metric.TimeSeries.Empty()) {
                    valueType = EValueType::NONE;
                } else if (metric.TimeSeries.Size() == 1) {
                    TInstant time = metric.TimeSeries[0].GetTime();
                    valueType = (time == TInstant::Zero())
                                    ? EValueType::ONE_WITHOUT_TS
                                    : EValueType::ONE_WITH_TS;
                } else {
                    valueType = EValueType::MANY_WITH_TS;
                }
                return (static_cast<ui8>(metric.MetricType) << 2) | static_cast<ui8>(valueType);
            }

            void WriteLabels(const TPooledLabels& labels, const TPooledStr* skipKey) {
                WriteVarUInt32(Out_, static_cast<ui32>(skipKey ? labels.size() - 1 : labels.size()));
                for (auto&& label : labels) {
                    if (label.Key == skipKey) {
                        continue;
                    }
                    WriteVarUInt32(Out_, label.Key->Index);
                    WriteVarUInt32(Out_, label.Value->Index);
                }
            }

            void WriteValue(EMetricType metricType, EMetricValueType valueType, TMetricValue value) {
                switch (metricType) {
                    case EMetricType::GAUGE:
                        WriteFixed(value.AsDouble(valueType));
                        break;

                    case EMetricType::IGAUGE:
                        WriteFixed(value.AsInt64(valueType));
                        break;

                    case EMetricType::COUNTER:
                    case EMetricType::RATE:
                        WriteFixed(value.AsUint64(valueType));
                        break;

                    case EMetricType::HIST:
                    case EMetricType::HIST_RATE:
                        WriteHistogram(*value.AsHistogram());
                        break;

                    case EMetricType::DSUMMARY:
                        WriteSummaryDouble(*value.AsSummaryDouble());
                        break;

                    case EMetricType::LOGHIST:
                        WriteLogHistogram(*value.AsLogHistogram());
                        break;

                    default:
                        ythrow yexception() << "unsupported metric type: " << metricType;
                }
            }

            void WriteTime(TInstant instant) {
                switch (TimePrecision_) {
                    case ETimePrecision::SECONDS: {
                        ui32 time = static_cast<ui32>(instant.Seconds());
                        Out_->Write(&time, sizeof(time));
                        break;
                    }
                    case ETimePrecision::MILLIS: {
                        ui64 time = static_cast<ui64>(instant.MilliSeconds());
                        Out_->Write(&time, sizeof(time));
                    }
                }
            }

            template <typename T>
            void WriteFixed(T value) {
                Out_->Write(&value, sizeof(value));
            }

            void WriteHistogram(const IHistogramSnapshot& histogram) {
                ui32 count = histogram.Count();
                WriteVarUInt32(Out_, count);

                for (ui32 i = 0; i < count; i++) {
                    double bound = histogram.UpperBound(i);
                    Out_->Write(&bound, sizeof(bound));
                }
                for (ui32 i = 0; i < count; i++) {
                    ui64 value = histogram.Value(i);
                    Out_->Write(&value, sizeof(value));
                }
            }

            void WriteLogHistogram(const TLogHistogramSnapshot& logHist) {
                WriteFixed(logHist.Base());
                WriteFixed(logHist.ZerosCount());
                WriteVarUInt32(Out_, static_cast<ui32>(logHist.StartPower()));
                WriteVarUInt32(Out_, logHist.Count());
                for (ui32 i = 0; i < logHist.Count(); ++i) {
                    WriteFixed(logHist.Bucket(i));
                }
            }

            void WriteSummaryDouble(const ISummaryDoubleSnapshot& summary) {
                WriteFixed(summary.GetCount());
                WriteFixed(summary.GetSum());
                WriteFixed(summary.GetMin());
                WriteFixed(summary.GetMax());
                WriteFixed(summary.GetLast());
            }

        private:
            IOutputStream* Out_;
            ETimePrecision TimePrecision_;
            ECompression Compression_;
            ESpackV1Version Version_;
            const TPooledStr* MetricName_;
            bool Closed_ = false;
        };

    }

    IMetricEncoderPtr EncoderSpackV1(
        IOutputStream* out,
        ETimePrecision timePrecision,
        ECompression compression,
        EMetricsMergingMode mergingMode
    ) {
        return MakeHolder<TEncoderSpackV1>(out, timePrecision, compression, mergingMode, SV1_01, "");
    }

    IMetricEncoderPtr EncoderSpackV12(
        IOutputStream* out,
        ETimePrecision timePrecision,
        ECompression compression,
        EMetricsMergingMode mergingMode,
        TStringBuf metricNameLabel
    ) {
        Y_ENSURE(!metricNameLabel.Empty(), "metricNameLabel can't be empty");
        return MakeHolder<TEncoderSpackV1>(out, timePrecision, compression, mergingMode, SV1_02, metricNameLabel);
    }
}
