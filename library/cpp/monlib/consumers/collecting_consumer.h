#pragma once

#include <library/cpp/monlib/metrics/labels.h>
#include <library/cpp/monlib/metrics/metric_value.h>
#include <library/cpp/monlib/metrics/metric_consumer.h>

#include <util/datetime/base.h>


namespace NMonitoring {
    // TODO(ivanzhukov@): very similar to https://nda.ya.ru/t/ST-KDJAH3W3cfn. Merge them later
    struct TMetricData {
        TMetricData()
            : Values{new NMonitoring::TMetricTimeSeries}
        {
        }

        TMetricData(NMonitoring::TLabels labels, NMonitoring::EMetricType type, THolder<NMonitoring::TMetricTimeSeries> s)
            : Labels{std::move(labels)}
            , Kind{type}
            , Values{std::move(s)}
        {
        }

        NMonitoring::TLabels Labels;
        // TODO(ivanzhukov@): rename to Type
        NMonitoring::EMetricType Kind{NMonitoring::EMetricType::UNKNOWN};
        THolder<NMonitoring::TMetricTimeSeries> Values;
    };

    template <typename TLabelsImpl>
    struct TCollectingConsumerImpl: NMonitoring::IMetricConsumer {
        TCollectingConsumerImpl() = default;
        explicit TCollectingConsumerImpl(bool doMergeCommonLabels)
            : DoMergeCommonLabels{doMergeCommonLabels}
        {}

        void OnStreamBegin() override {}
        void OnStreamEnd() override {}

        void OnCommonTime(TInstant time) override {
            CommonTime = time;
        }

        void OnMetricBegin(NMonitoring::EMetricType kind) override {
            auto& metric = Metrics.emplace_back();
            metric.Kind = kind;
            InsideSensor = true;
        }

        void OnMetricEnd() override {
            InsideSensor = false;
        }

        void OnLabelsBegin() override {}
        void OnLabelsEnd() override {
            if (DoMergeCommonLabels) {
                for (auto& cl : CommonLabels) {
                    Metrics.back().Labels.Add(cl);
                }
            }
        }

        void OnLabel(TStringBuf key, TStringBuf value) override {
            if (InsideSensor) {
                Metrics.back().Labels.Add(key, value);
            } else {
                CommonLabels.Add(key, value);
            }
        }

        void OnDouble(TInstant time, double value) override {
            Metrics.back().Values->Add(time, value);
        }

        void OnInt64(TInstant time, i64 value) override {
            Metrics.back().Values->Add(time, value);
        }

        void OnUint64(TInstant time, ui64 value) override {
            Metrics.back().Values->Add(time, value);
        }

        void OnHistogram(TInstant time, NMonitoring::IHistogramSnapshotPtr snapshot) override {
            auto& val = Metrics.back().Values;
            val->Add(time, snapshot.Get());
        }

        void OnSummaryDouble(TInstant time, NMonitoring::ISummaryDoubleSnapshotPtr snapshot) override {
            auto& val = Metrics.back().Values;
            val->Add(time, snapshot.Get());
        }

        void OnLogHistogram(TInstant time, NMonitoring::TLogHistogramSnapshotPtr snapshot) override {
            auto& val = Metrics.back().Values;
            val->Add(time, snapshot.Get());
        }

        bool DoMergeCommonLabels{false};
        TVector<TMetricData> Metrics;
        TLabelsImpl CommonLabels;
        TInstant CommonTime;
        bool InsideSensor{false};
    };

    using TCollectingConsumer = TCollectingConsumerImpl<NMonitoring::TLabels>;

} // namespace NMonitoring
