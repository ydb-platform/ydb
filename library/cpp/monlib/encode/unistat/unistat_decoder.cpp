#include "unistat.h"

#include <library/cpp/monlib/metrics/histogram_collector.h>
#include <library/cpp/monlib/metrics/labels.h>
#include <library/cpp/monlib/metrics/metric_type.h>
#include <library/cpp/monlib/metrics/metric_value.h>
#include <library/cpp/monlib/metrics/metric_consumer.h>

#include <library/cpp/json/json_reader.h>

#include <util/datetime/base.h>
#include <util/string/split.h>

#include <contrib/libs/re2/re2/re2.h>

using namespace NJson;

const re2::RE2 NAME_RE{R"((?:[a-zA-Z0-9\.\-/@_]+_)+(?:[ad][vehmntx]{3}|summ|hgram|max))"};

namespace NMonitoring {
    namespace {
        bool IsNumber(const NJson::TJsonValue& j) {
            switch (j.GetType()) {
                case EJsonValueType::JSON_INTEGER:
                case EJsonValueType::JSON_UINTEGER:
                case EJsonValueType::JSON_DOUBLE:
                    return true;

                default:
                    return false;
            }
        }

        template <typename T>
        T ExtractNumber(const TJsonValue& val) {
            switch (val.GetType()) {
                case EJsonValueType::JSON_INTEGER:
                    return static_cast<T>(val.GetInteger());
                case EJsonValueType::JSON_UINTEGER:
                    return static_cast<T>(val.GetUInteger());
                case EJsonValueType::JSON_DOUBLE:
                    return static_cast<T>(val.GetDouble());

                default:
                    ythrow yexception() << "Expected number, but found " << val.GetType();
            }
        }

        auto ExtractDouble = ExtractNumber<double>;
        auto ExtractUi64 = ExtractNumber<ui64>;

        class THistogramBuilder {
        public:
            void Add(TBucketBound bound, TBucketValue value) {
                /// XXX: yasm uses left-closed intervals, while in monlib we use right-closed ones,
                /// so (-inf; 0) [0, 100) [100; +inf)
                /// becomes (-inf; 0] (0, 100] (100; +inf)
                /// but since we've already lost some information these no way to avoid this kind of error here
                Bounds_.push_back(bound);

                /// this will always be 0 for the first bucket,
                /// since there's no way to make (-inf; N) bucket in yasm
                Values_.push_back(NextValue_);

                /// we will write this value into the next bucket so that [[0, 10], [100, 20], [200, 50]]
                /// becomes (-inf; 0] -> 0; (0; 100] -> 10; (100; 200] -> 20; (200; +inf) -> 50
                NextValue_ = value;
            }

            IHistogramSnapshotPtr Finalize() {
                Bounds_.push_back(std::numeric_limits<TBucketBound>::max());
                Values_.push_back(NextValue_);

                return ExplicitHistogramSnapshot(Bounds_, Values_, true);
            }

        public:
            TBucketValue NextValue_ {0};
            TBucketBounds Bounds_;
            TBucketValues Values_;
        };

        class LogHistogramBuilder {
        public:
            void Add(double value) {
                while (Values_.size() < HISTOGRAM_MAX_BUCKETS_COUNT && value > MaxValueWithBickets(Values_.size())) {
                    Values_.push_back(0);
                }

                ui32 index = 0;
                if (value > MaxValueWithBickets(HISTOGRAM_MAX_BUCKETS_COUNT)) {
                    index = Values_.size() - 1;
                } else if (value > MIN_VALUE) {
                    double logBase = std::log(value) / std::log(BASE);
                    index = static_cast<ui32>(std::ceil(logBase));
                }
                ++Values_[index];
            }

            IHistogramSnapshotPtr Finalize() && {
                return new TExponentialHistogramSnapshot(BASE, MIN_VALUE, std::move(Values_));
            }

        private:
            static constexpr double BASE = 2;
            static constexpr double MIN_VALUE = 1;

            static double MaxValueWithBickets(ui64 buckets) {
                return std::pow(BASE, buckets - 2);
            }

        private:
            TBucketValues Values_ = TBucketValues(2, 0);
        };

        class TDecoderUnistat {
        private:
        public:
            explicit TDecoderUnistat(IMetricConsumer* consumer, IInputStream* is, TStringBuf metricNameLabel, TInstant ts)
                : Consumer_{consumer},
                MetricNameLabel(metricNameLabel),
                Timestamp_{ts} {
                ReadJsonTree(is, &Json_, /* throw */ true);
            }

            void Decode() {
                Y_ENSURE(Json_.IsArray(), "Expected array at the top level, but found " << Json_.GetType());

                for (auto&& metric : Json_.GetArray()) {
                    Y_ENSURE(metric.IsArray(), "Metric must be an array");
                    auto&& arr = metric.GetArray();
                    Y_ENSURE(arr.size() == 2, "Metric must be an array of 2 elements");
                    auto&& name = arr[0];
                    auto&& value = arr[1];
                    MetricContext_ = {};

                    ParseName(name.GetString());

                    if (value.IsArray()) {
                        const auto& array = value.GetArray();
                        if (!array.empty() && IsNumber(array[0])) {
                            OnLogHistogram(value);
                        } else {
                            OnHistogram(value);
                        }
                    } else if (IsNumber(value)) {
                        if (MetricContext_.Name.EndsWith("_ahhh")) {
                            OnLogHistogram(value);
                        } else {
                            OnScalar(value);
                        }
                    } else {
                        ythrow yexception() << "Expected list or number, but found " << value.GetType();
                    }

                    WriteValue();
                }
            }

        private:
            void OnScalar(const TJsonValue& jsonValue) {
                if (MetricContext_.IsDeriv) {
                    MetricContext_.Type = EMetricType::RATE;
                    MetricContext_.Value = TMetricValue{ExtractUi64(jsonValue)};
                } else {
                    MetricContext_.Type = EMetricType::GAUGE;
                    MetricContext_.Value = TMetricValue{ExtractDouble(jsonValue)};
                }
            }

            void OnLogHistogram(const TJsonValue& value) {
                Y_ENSURE(MetricContext_.Name.EndsWith("_ahhh"), "Values list is supported only for _ahhh metrics");
                MetricContext_.Type = EMetricType::HIST;

                LogHistogramBuilder histogramBuilder;
                if (IsNumber(value)) {
                    histogramBuilder.Add(value.GetDouble());
                } else {
                    for (auto&& item: value.GetArray()) {
                        Y_ENSURE(IsNumber(item), "Expected a number, but found " << item.GetType());
                        histogramBuilder.Add(item.GetDouble());
                    }
                }

                MetricContext_.Histogram = std::move(histogramBuilder).Finalize();
                MetricContext_.Value = TMetricValue{MetricContext_.Histogram.Get()};
            }

            void OnHistogram(const TJsonValue& jsonHist) {
                if (MetricContext_.IsDeriv) {
                    MetricContext_.Type = EMetricType::HIST_RATE;
                } else {
                    MetricContext_.Type = EMetricType::HIST;
                }

                auto histogramBuilder = THistogramBuilder();

                for (auto&& bucket : jsonHist.GetArray()) {
                    Y_ENSURE(bucket.IsArray(), "Expected an array, but found " << bucket.GetType());
                    auto&& arr = bucket.GetArray();
                    Y_ENSURE(arr.size() == 2, "Histogram bucket must be an array of 2 elements");
                    const auto bound = ExtractDouble(arr[0]);
                    const auto weight = ExtractUi64(arr[1]);
                    histogramBuilder.Add(bound, weight);
                }

                MetricContext_.Histogram = histogramBuilder.Finalize();
                MetricContext_.Value = TMetricValue{MetricContext_.Histogram.Get()};
            }

            bool IsDeriv(TStringBuf name) {
                TStringBuf ignore, suffix;
                name.RSplit('_', ignore, suffix);

                Y_ENSURE(suffix.size() >= 3 && suffix.size() <= 5, "Disallowed suffix value: " << suffix);

                if (suffix == TStringBuf("summ") || suffix == TStringBuf("hgram")) {
                    return true;
                } else if (suffix == TStringBuf("max")) {
                    return false;
                }

                return suffix[0] == 'd';
            }

            void ParseName(TStringBuf value) {
                TVector<TStringBuf> parts;
                StringSplitter(value).Split(';').SkipEmpty().Collect(&parts);

                Y_ENSURE(parts.size() >= 1 && parts.size() <= 16);

                TStringBuf name = parts.back();
                parts.pop_back();

                Y_ENSURE(RE2::FullMatch(re2::StringPiece{name.data(), name.size()}, NAME_RE),
                         "Metric name " << name << " doesn't match regex " << NAME_RE.pattern());

                MetricContext_.Name = name;
                MetricContext_.IsDeriv = IsDeriv(MetricContext_.Name);

                for (auto tag : parts) {
                    TStringBuf n, v;
                    tag.Split('=', n, v);
                    Y_ENSURE(n && v, "Unexpected tag format in " << tag);
                    MetricContext_.Labels.Add(n, v);
                }
            }

        private:
            void WriteValue() {
                Consumer_->OnMetricBegin(MetricContext_.Type);

                Consumer_->OnLabelsBegin();
                Consumer_->OnLabel(MetricNameLabel, TString{MetricContext_.Name});
                for (auto&& l : MetricContext_.Labels) {
                    Consumer_->OnLabel(l.Name(), l.Value());
                }

                Consumer_->OnLabelsEnd();

                switch (MetricContext_.Type) {
                    case EMetricType::GAUGE:
                        Consumer_->OnDouble(Timestamp_, MetricContext_.Value.AsDouble());
                        break;
                    case EMetricType::RATE:
                        Consumer_->OnUint64(Timestamp_, MetricContext_.Value.AsUint64());
                        break;
                    case EMetricType::HIST:
                    case EMetricType::HIST_RATE:
                        Consumer_->OnHistogram(Timestamp_, MetricContext_.Value.AsHistogram());
                        break;
                    case EMetricType::LOGHIST:
                    case EMetricType::DSUMMARY:
                    case EMetricType::IGAUGE:
                    case EMetricType::COUNTER:
                    case EMetricType::UNKNOWN:
                        ythrow yexception() << "Unexpected metric type: " << MetricContext_.Type;
                }

                Consumer_->OnMetricEnd();
            }

        private:
            IMetricConsumer* Consumer_;
            NJson::TJsonValue Json_;
            TStringBuf MetricNameLabel;
            TInstant Timestamp_;

            struct {
                TStringBuf Name;
                EMetricType Type{EMetricType::UNKNOWN};
                TMetricValue Value;
                bool IsDeriv{false};
                TLabels Labels;
                IHistogramSnapshotPtr Histogram;
            } MetricContext_;
        };

    }

    void DecodeUnistat(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel, TInstant ts) {
        c->OnStreamBegin();
        DecodeUnistatToStream(data, c, metricNameLabel, ts);
        c->OnStreamEnd();
    }

    void DecodeUnistatToStream(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel, TInstant ts) {
        TMemoryInput in{data.data(), data.size()};
        TDecoderUnistat decoder(c, &in, metricNameLabel, ts);
        decoder.Decode();
    }
}
