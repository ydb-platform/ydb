#include "encode.h"

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>

#include <util/stream/str.h>

namespace NMonitoring {
    namespace {
        constexpr TInstant ZERO_TIME = TInstant::Zero();

        class TConsumer final: public ICountableConsumer {
            using TLabel = std::pair<TString, TString>; // name, value

        public:
            explicit TConsumer(NMonitoring::IMetricEncoderPtr encoderImpl, TCountableBase::EVisibility vis)
                : EncoderImpl_(std::move(encoderImpl))
                , Visibility_{vis}
            {
            }

            void OnCounter(
                const TString& labelName, const TString& labelValue,
                const TCounterForPtr* counter) override {
                NMonitoring::EMetricType metricType = counter->ForDerivative()
                                                          ? NMonitoring::EMetricType::RATE
                                                          : NMonitoring::EMetricType::GAUGE;
                EncoderImpl_->OnMetricBegin(metricType);
                EncodeLabels(labelName, labelValue);

                if (metricType == NMonitoring::EMetricType::GAUGE) {
                    EncoderImpl_->OnDouble(ZERO_TIME, static_cast<double>(counter->Val()));
                } else {
                    EncoderImpl_->OnUint64(ZERO_TIME, counter->Val());
                }

                EncoderImpl_->OnMetricEnd();
            }

            void OnHistogram(
                const TString& labelName, const TString& labelValue,
                IHistogramSnapshotPtr snapshot, bool derivative) override {
                NMonitoring::EMetricType metricType = derivative ? EMetricType::HIST_RATE : EMetricType::HIST;

                EncoderImpl_->OnMetricBegin(metricType);
                EncodeLabels(labelName, labelValue);
                EncoderImpl_->OnHistogram(ZERO_TIME, snapshot);
                EncoderImpl_->OnMetricEnd();
            }

            void OnGroupBegin(
                const TString& labelName, const TString& labelValue,
                const TDynamicCounters*) override {
                if (labelName.empty() && labelValue.empty()) {
                    // root group has empty label name and value
                    EncoderImpl_->OnStreamBegin();
                } else {
                    ParentLabels_.emplace_back(labelName, labelValue);
                }
            }

            void OnGroupEnd(
                const TString& labelName, const TString& labelValue,
                const TDynamicCounters*) override {
                if (labelName.empty() && labelValue.empty()) {
                    // root group has empty label name and value
                    EncoderImpl_->OnStreamEnd();
                    EncoderImpl_->Close();
                } else {
                    ParentLabels_.pop_back();
                }
            }

            TCountableBase::EVisibility Visibility() const override {
                return Visibility_;
            }

        private:
            void EncodeLabels(const TString& labelName, const TString& labelValue) {
                EncoderImpl_->OnLabelsBegin();
                for (const auto& label : ParentLabels_) {
                    EncoderImpl_->OnLabel(label.first, label.second);
                }
                EncoderImpl_->OnLabel(labelName, labelValue);
                EncoderImpl_->OnLabelsEnd();
            }

        private:
            NMonitoring::IMetricEncoderPtr EncoderImpl_;
            TVector<TLabel> ParentLabels_;
            TCountableBase::EVisibility Visibility_;
        };

    }

    THolder<ICountableConsumer> CreateEncoder(IOutputStream* out, EFormat format,
        TStringBuf nameLabel, TCountableBase::EVisibility vis)
    {
        switch (format) {
            case EFormat::JSON:
                return MakeHolder<TConsumer>(NMonitoring::EncoderJson(out), vis);
            case EFormat::SPACK:
                return MakeHolder<TConsumer>(NMonitoring::EncoderSpackV1(
                    out,
                    NMonitoring::ETimePrecision::SECONDS,
                    NMonitoring::ECompression::ZSTD), vis);
            case EFormat::PROMETHEUS:
                return MakeHolder<TConsumer>(NMonitoring::EncoderPrometheus(
                    out, nameLabel), vis);
            default:
                ythrow yexception() << "unsupported metric encoding format: " << format;
                break;
        }
    }

    THolder<ICountableConsumer> AsCountableConsumer(IMetricEncoderPtr encoder, TCountableBase::EVisibility visibility) {
        return MakeHolder<TConsumer>(std::move(encoder), visibility);
    }

    void ToJson(const TDynamicCounters& counters, IOutputStream* out) {
        TConsumer consumer{EncoderJson(out), TCountableBase::EVisibility::Public};
        counters.Accept(TString{}, TString{}, consumer);
    }

    TString ToJson(const TDynamicCounters& counters) {
        TStringStream ss;
        ToJson(counters, &ss);
        return ss.Str();
    }

}
