#include "prometheus.h"
#include "prometheus_model.h"

#include <library/cpp/monlib/metrics/histogram_snapshot.h>
#include <library/cpp/monlib/metrics/metric.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/generic/maybe.h>
#include <util/string/ascii.h>

#include <cmath>

#define Y_PARSER_FAIL(message) \
    ythrow ::NMonitoring::TPrometheusDecodeException() << message << " at line #" << CurrentLine_

#define Y_PARSER_ENSURE(cond, message) \
    Y_ENSURE_EX(cond, ::NMonitoring::TPrometheusDecodeException() << message << " at line #" << CurrentLine_)


namespace NMonitoring {
    namespace {
        constexpr ui32 MAX_LABEL_VALUE_LEN = 256;

        using TLabelsMap = THashMap<TString, TString>;

        TString LabelsToStr(const TLabelsMap& labels) {
            TStringBuilder sb;
            auto it = labels.begin();
            auto end = labels.end();

            sb << '{';
            while (it != end) {
                sb << it->first;
                sb << '=';
                sb << '"' << it->second << '"';

                ++it;
                if (it != end) {
                    sb << ", ";
                }
            }
            sb << '}';
            return sb;
        }

        template <typename T, typename U>
        bool TryStaticCast(U val, T& out) {
            static_assert(std::is_arithmetic_v<U>);
            if constexpr (std::is_floating_point_v<T> || std::is_floating_point_v<U>) {
                if (val > MaxFloor<T>() || val < -MaxFloor<T>()) {
                    return false;
                }

            } else {
                if (val > Max<T>() || val < Min<T>()) {
                    return false;
                }
            }

            out = static_cast<T>(val);
            return true;
        }

        ///////////////////////////////////////////////////////////////////////
        // THistogramBuilder
        ///////////////////////////////////////////////////////////////////////
        class THistogramBuilder {
            using TBucketData = std::pair<TBucketBound, TBucketValue>;
            constexpr static TBucketData ZERO_BUCKET = { -std::numeric_limits<TBucketBound>::max(), 0 };
        public:
            THistogramBuilder(TPrometheusDecodeSettings settings)
                : Settings_(settings) {
            }
            TStringBuf GetName() const noexcept {
                return Name_;
            }

            void SetName(TStringBuf name) noexcept {
                Name_ = name;
            }

            const TLabelsMap& GetLabels() const noexcept {
                return *Labels_;
            }

            void SetLabels(TLabelsMap&& labels) {
                if (Labels_.Defined()) {
                    Y_ENSURE(Labels_ == labels,
                             "mixed labels in one histogram, prev: " << LabelsToStr(*Labels_) <<
                             ", current: " << LabelsToStr(labels));
                } else {
                    Labels_.ConstructInPlace(std::move(labels));
                }
            }

            TInstant GetTime() const noexcept {
                return Time_;
            }

            void SetTime(TInstant time) noexcept {
                Time_ = time;
            }

            bool Empty() const noexcept {
                return Bounds_.empty();
            }

            bool Same(TStringBuf name, const TLabelsMap& labels) const noexcept {
                return Name_ == name && Labels_ == labels;
            }

            void AddBucket(TBucketBound bound, TBucketValue value) {
                Y_ENSURE_EX(PrevBucket_.first < bound, TPrometheusDecodeException() <<
                        "invalid order of histogram bounds " << PrevBucket_.first <<
                        " >= " << bound);

                Y_ENSURE_EX(PrevBucket_.second <= value, TPrometheusDecodeException() <<
                        "invalid order of histogram bucket values " << PrevBucket_.second <<
                        " > " << value);

                // convert infinite bound value
                if (bound == std::numeric_limits<TBucketBound>::infinity()) {
                    bound = HISTOGRAM_INF_BOUND;
                }

                Bounds_.push_back(bound);
                Values_.push_back(value - PrevBucket_.second); // keep only delta between buckets
                PrevBucket_ = { bound, value };
            }

            // will clear builder state
            IHistogramSnapshotPtr ToSnapshot() {
                Y_ENSURE_EX(!Empty(), TPrometheusDecodeException() << "histogram cannot be empty");
                Time_ = TInstant::Zero();
                PrevBucket_ = ZERO_BUCKET;
                Labels_.Clear();
                auto snapshot = ExplicitHistogramSnapshot(Bounds_, Values_, true);

                Bounds_.clear();
                Values_.clear();

                return snapshot;
            }

        private:
            TStringBuf Name_;
            TMaybe<TLabelsMap> Labels_;
            TInstant Time_;
            TBucketBounds Bounds_;
            TBucketValues Values_;
            TBucketData PrevBucket_ = ZERO_BUCKET;
            TPrometheusDecodeSettings Settings_;
        };

        ///////////////////////////////////////////////////////////////////////
        // EPrometheusMetricType
        ///////////////////////////////////////////////////////////////////////
        enum class EPrometheusMetricType {
            GAUGE,
            COUNTER,
            SUMMARY,
            UNTYPED,
            HISTOGRAM,
        };

        ///////////////////////////////////////////////////////////////////////
        // TPrometheusReader
        ///////////////////////////////////////////////////////////////////////
        class TPrometheusReader {
        public:
            TPrometheusReader(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel, const TPrometheusDecodeSettings& settings = TPrometheusDecodeSettings{})
                : Data_(data)
                , Consumer_(c)
                , MetricNameLabel_(metricNameLabel)
                , Settings_(settings)
                , HistogramBuilder_(settings)
            {
            }

            void Read() {
                Consumer_->OnStreamBegin();

                if (HasRemaining()) {
                    ReadNextByte();
                    SkipSpaces();

                    try {
                        while (HasRemaining()) {
                            switch (CurrentByte_) {
                                case '\n':
                                    ReadNextByte(); // skip '\n'
                                    CurrentLine_++;
                                    SkipSpaces();
                                    break;
                                case '#':
                                    ParseComment();
                                    break;
                                default:
                                    ParseMetric();
                                    break;
                            }
                        }

                        if (!HistogramBuilder_.Empty()) {
                            ConsumeHistogram();
                        }
                    } catch (const TPrometheusDecodeException& e) {
                        throw e;
                    } catch (...) {
                        Y_PARSER_FAIL("unexpected error " << CurrentExceptionMessage());
                    }
                }

                Consumer_->OnStreamEnd();
            }

        private:
            bool HasRemaining() const noexcept {
                return CurrentPos_ < Data_.Size();
            }

            // # 'TYPE' metric_name {counter|gauge|histogram|summary|untyped}
            // # 'HELP' metric_name some help info
            // # general comment message
            void ParseComment() {
                SkipExpectedChar('#');
                SkipSpaces();

                TStringBuf keyword = ReadToken();
                if (keyword == TStringBuf("TYPE")) {
                    SkipSpaces();

                    TStringBuf nextName = ReadTokenAsMetricName();
                    Y_PARSER_ENSURE(!nextName.Empty(), "invalid metric name");

                    SkipSpaces();
                    EPrometheusMetricType nextType = ReadType();

                    auto emplaceResult = SeenTypes_.emplace(nextName, nextType);
                    if (!emplaceResult.second) {
                            Y_PARSER_ENSURE(emplaceResult.first->second == nextType, "second diferent TYPE for metric " << nextName);
                    }

                    if (nextType == EPrometheusMetricType::HISTOGRAM) {
                        if (!HistogramBuilder_.Empty()) {
                            ConsumeHistogram();
                        }
                        HistogramBuilder_.SetName(nextName);
                    }
                } else {
                    // skip HELP and general comments
                    SkipUntilEol();
                }

                Y_PARSER_ENSURE(CurrentByte_ == '\n', "expected '\\n', found '" << CurrentByte_ << '\'');
            }

            // metric_name [labels] value [timestamp]
            void ParseMetric() {
                TStringBuf name = ReadTokenAsMetricName();
                SkipSpaces();

                TLabelsMap labels = ReadLabels();
                SkipSpaces();

                double value = ParseGoDouble(ReadToken());
                SkipSpaces();

                TInstant time = TInstant::Zero();
                if (CurrentByte_ != '\n') {
                    time = TInstant::MilliSeconds(FromString<ui64>(ReadToken()));
                }

                TStringBuf baseName = name;
                EPrometheusMetricType type = EPrometheusMetricType::UNTYPED;

                if (Settings_.Mode != EPrometheusDecodeMode::RAW) {
                    if (auto* seenType = SeenTypes_.FindPtr(name)) {
                        type = *seenType;
                    } else {
                        baseName = NPrometheus::ToBaseName(name);
                        if (auto* baseType = SeenTypes_.FindPtr(baseName)) {
                            type = *baseType;
                        }
                    }
                }

                switch (type) {
                    case EPrometheusMetricType::HISTOGRAM:
                        if (NPrometheus::IsBucket(name)) {
                            double bound = 0.0;
                            auto it = labels.find(NPrometheus::BUCKET_LABEL);
                            if (it != labels.end()) {
                                bound = ParseGoDouble(it->second);
                                labels.erase(it);
                            } else {
                                Y_PARSER_FAIL(
                                    "metric " << name << "has no " << NPrometheus::BUCKET_LABEL <<
                                    " label at line #" << CurrentLine_);
                            }

                            if (!HistogramBuilder_.Empty() && !HistogramBuilder_.Same(baseName, labels)) {
                                ConsumeHistogram();
                                HistogramBuilder_.SetName(baseName);
                            }

                            TBucketValue bucketVal;
                            Y_PARSER_ENSURE(TryStaticCast(value, bucketVal), "Cannot convert " << value << " to bucket value type");
                            HistogramBuilder_.AddBucket(bound, bucketVal);
                            HistogramBuilder_.SetTime(time);
                            HistogramBuilder_.SetLabels(std::move(labels));
                        } else if (NPrometheus::IsCount(name)) {
                            // translate x_count metric as COUNTER metric
                            ConsumeCounter(name, labels, time, value);
                        } else if (NPrometheus::IsSum(name)) {
                            // translate x_sum metric as GAUGE metric
                            ConsumeGauge(name, labels, time, value);
                        } else {
                            Y_PARSER_FAIL(
                                "metric " << name <<
                                " should be part of HISTOGRAM " << baseName);
                        }
                        break;

                    case EPrometheusMetricType::SUMMARY:
                        if (NPrometheus::IsCount(name)) {
                            // translate x_count metric as COUNTER metric
                            ConsumeCounter(name, labels, time, value);
                        } else if (NPrometheus::IsSum(name)) {
                            // translate x_sum metric as GAUGE metric
                            ConsumeGauge(name, labels, time, value);
                        } else {
                            ConsumeGauge(name, labels, time, value);
                        }
                        break;

                    case EPrometheusMetricType::COUNTER:
                        ConsumeCounter(name, labels, time, value);
                        break;

                    case EPrometheusMetricType::GAUGE:
                        ConsumeGauge(name, labels, time, value);
                        break;

                    case EPrometheusMetricType::UNTYPED:
                        ConsumeGauge(name, labels, time, value);
                        break;
                }

                Y_PARSER_ENSURE(CurrentByte_ == '\n', "expected '\\n', found '" << CurrentByte_ << '\'');
            }

            // { name = "value", name2 = "value2", }
            TLabelsMap ReadLabels() {
                TLabelsMap labels;
                if (CurrentByte_ != '{') {
                    return labels;
                }

                SkipExpectedChar('{');
                SkipSpaces();

                while (CurrentByte_ != '}') {
                    TStringBuf name = ReadTokenAsLabelName();
                    SkipSpaces();

                    SkipExpectedChar('=');
                    SkipSpaces();

                    TString value = ReadTokenAsLabelValue();
                    SkipSpaces();
                    labels.emplace(name, value);

                    if (CurrentByte_ == ',') {
                        SkipExpectedChar(',');
                        SkipSpaces();
                    }
                }

                SkipExpectedChar('}');
                return labels;
            }

            EPrometheusMetricType ReadType() {
                TStringBuf keyword = ReadToken();
                if (AsciiEqualsIgnoreCase(keyword, "GAUGE")) {
                    return EPrometheusMetricType::GAUGE;
                } else if (AsciiEqualsIgnoreCase(keyword, "COUNTER")) {
                    return EPrometheusMetricType::COUNTER;
                } else if (AsciiEqualsIgnoreCase(keyword, "SUMMARY")) {
                    return EPrometheusMetricType::SUMMARY;
                } else if (AsciiEqualsIgnoreCase(keyword, "HISTOGRAM")) {
                    return EPrometheusMetricType::HISTOGRAM;
                } else if (AsciiEqualsIgnoreCase(keyword, "UNTYPED")) {
                    return EPrometheusMetricType::UNTYPED;
                }

                Y_PARSER_FAIL(
                    "unknown metric type: " << keyword <<
                    " at line #" << CurrentLine_);
            }

            Y_FORCE_INLINE void ReadNextByteUnsafe() {
                CurrentByte_ = Data_[CurrentPos_++];
            }

            Y_FORCE_INLINE bool IsSpace(char ch) {
                return ch == ' ' || ch == '\t';
            }

            void ReadNextByte() {
                Y_PARSER_ENSURE(HasRemaining(), "unexpected end of file");
                ReadNextByteUnsafe();
            }

            void SkipExpectedChar(char ch) {
                Y_PARSER_ENSURE(CurrentByte_ == ch,
                    "expected '" << CurrentByte_ << "', found '" << ch << '\'');
                ReadNextByte();
            }

            void SkipSpaces() {
                while (HasRemaining() && IsSpace(CurrentByte_)) {
                    ReadNextByteUnsafe();
                }
            }

            void SkipUntilEol() {
                while (HasRemaining() && CurrentByte_ != '\n') {
                    ReadNextByteUnsafe();
                }
            }

            TStringBuf ReadToken() {
                Y_DEBUG_ABORT_UNLESS(CurrentPos_ > 0);
                size_t begin = CurrentPos_ - 1; // read first byte again
                while (HasRemaining() && !IsSpace(CurrentByte_) && CurrentByte_ != '\n') {
                    ReadNextByteUnsafe();
                }
                return TokenFromPos(begin);
            }

            TStringBuf ReadTokenAsMetricName() {
                if (!NPrometheus::IsValidMetricNameStart(CurrentByte_)) {
                    return "";
                }

                Y_DEBUG_ABORT_UNLESS(CurrentPos_ > 0);
                size_t begin = CurrentPos_ - 1; // read first byte again
                while (HasRemaining()) {
                    ReadNextByteUnsafe();
                    if (!NPrometheus::IsValidMetricNameContinuation(CurrentByte_)) {
                        break;
                    }
                }
                return TokenFromPos(begin);
            }

            TStringBuf ReadTokenAsLabelName() {
                if (!NPrometheus::IsValidLabelNameStart(CurrentByte_)) {
                    return "";
                }

                Y_DEBUG_ABORT_UNLESS(CurrentPos_ > 0);
                size_t begin = CurrentPos_ - 1; // read first byte again
                while (HasRemaining()) {
                    ReadNextByteUnsafe();
                    if (!NPrometheus::IsValidLabelNameContinuation(CurrentByte_)) {
                        break;
                    }
                }
                return TokenFromPos(begin);
            }

            TString ReadTokenAsLabelValue() {
                TString labelValue;

                SkipExpectedChar('"');
                for (ui32 i = 0; i < MAX_LABEL_VALUE_LEN; i++) {
                    switch (CurrentByte_) {
                        case '"':
                            SkipExpectedChar('"');
                            return labelValue;

                        case '\n':
                            Y_PARSER_FAIL("label value contains unescaped new-line");

                        case '\\':
                            ReadNextByte();
                            switch (CurrentByte_) {
                                case '"':
                                case '\\':
                                    labelValue.append(CurrentByte_);
                                    break;
                                case 'n':
                                    labelValue.append('\n');
                                    break;
                                default:
                                    Y_PARSER_FAIL("invalid escape sequence '" << CurrentByte_ << '\'');
                            }
                            break;

                        default:
                            labelValue.append(CurrentByte_);
                            break;
                    }

                    ReadNextByte();
                }

                Y_PARSER_FAIL("trying to parse too long label value, size >= " << MAX_LABEL_VALUE_LEN);
            }

            TStringBuf TokenFromPos(size_t begin) {
                Y_DEBUG_ABORT_UNLESS(CurrentPos_ > begin);
                size_t len = CurrentPos_ - begin - 1;
                if (len == 0) {
                    return {};
                }

                return Data_.SubString(begin, len);
            }

            void ConsumeLabels(TStringBuf name, const TLabelsMap& labels) {
                Y_PARSER_ENSURE(labels.count(MetricNameLabel_) == 0,
                    "label name '" << MetricNameLabel_ <<
                    "' is reserved, but is used with metric: " << name << LabelsToStr(labels));

                Consumer_->OnLabelsBegin();
                Consumer_->OnLabel(MetricNameLabel_, TString(name)); // TODO: remove this string allocation
                for (const auto& it: labels) {
                    Consumer_->OnLabel(it.first, it.second);
                }
                Consumer_->OnLabelsEnd();
            }

            void ConsumeCounter(TStringBuf name, const TLabelsMap& labels, TInstant time, double value) {
                ui64 uintValue{0};
                // not nan
                if (value == value && value > 0) {
                    if (!TryStaticCast(value, uintValue)) {
                        uintValue = std::numeric_limits<ui64>::max();
                    }
                }

                // see https://st.yandex-team.ru/SOLOMON-4142 for more details
                // why we convert Prometheus COUNTER into Solomon RATE
                // TODO: need to fix after server-side aggregation become correct for COUNTERs
                Consumer_->OnMetricBegin(EMetricType::RATE);
                ConsumeLabels(name, labels);
                Consumer_->OnUint64(time, uintValue);
                Consumer_->OnMetricEnd();
            }

            void ConsumeGauge(TStringBuf name, const TLabelsMap& labels, TInstant time, double value) {
                Consumer_->OnMetricBegin(EMetricType::GAUGE);
                ConsumeLabels(name, labels);
                Consumer_->OnDouble(time, value);
                Consumer_->OnMetricEnd();
            }

            void ConsumeHistogram() {
                Consumer_->OnMetricBegin(EMetricType::HIST_RATE);
                ConsumeLabels(HistogramBuilder_.GetName(), HistogramBuilder_.GetLabels());
                auto time = HistogramBuilder_.GetTime();
                auto hist = HistogramBuilder_.ToSnapshot();
                Consumer_->OnHistogram(time, std::move(hist));
                Consumer_->OnMetricEnd();
            }

            double ParseGoDouble(TStringBuf str) {
                if (str == TStringBuf("+Inf")) {
                    return std::numeric_limits<double>::infinity();
                } else if (str == TStringBuf("-Inf")) {
                    return -std::numeric_limits<double>::infinity();
                } else if (str == TStringBuf("NaN")) {
                    return NAN;
                }

                double r = 0.0;
                if (TryFromString(str, r)) {
                    return r;
                }
                Y_PARSER_FAIL("cannot parse double value from '" << str << "\' at line #" << CurrentLine_);
            }

        private:
            TStringBuf Data_;
            IMetricConsumer* Consumer_;
            TStringBuf MetricNameLabel_;
            TPrometheusDecodeSettings Settings_;
            THashMap<TString, EPrometheusMetricType> SeenTypes_;
            THistogramBuilder HistogramBuilder_;

            ui32 CurrentLine_ = 1;
            ui32 CurrentPos_ = 0;
            char CurrentByte_ = 0;
        };
    } // namespace

void DecodePrometheus(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel, const TPrometheusDecodeSettings& settings) {
    TPrometheusReader reader(data, c, metricNameLabel, settings);
    reader.Read();
}

} // namespace NMonitoring
