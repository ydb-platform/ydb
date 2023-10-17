#include "spack_v1.h"
#include "varint.h"
#include "compression.h"

#include <library/cpp/monlib/encode/buffered/string_pool.h>
#include <library/cpp/monlib/exception/exception.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>
#include <library/cpp/monlib/metrics/metric.h>

#include <util/generic/yexception.h>
#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>
#include <util/stream/format.h>

#ifndef _little_endian_
#error Unsupported platform
#endif

namespace NMonitoring {
    namespace {
#define DECODE_ENSURE(COND, ...) MONLIB_ENSURE_EX(COND, TSpackDecodeError() << __VA_ARGS__)

        constexpr ui64 LABEL_SIZE_LIMIT = 128_MB;

        ///////////////////////////////////////////////////////////////////////
        // TDecoderSpackV1
        ///////////////////////////////////////////////////////////////////////
        class TDecoderSpackV1 {
        public:
            TDecoderSpackV1(IInputStream* in, TStringBuf metricNameLabel)
                : In_(in)
                , MetricNameLabel_(metricNameLabel)
            {
            }

            void Decode(IMetricConsumer* c) {
                c->OnStreamBegin();

                // (1) read header
                size_t readBytes = In_->Read(&Header_, sizeof(Header_));
                DECODE_ENSURE(readBytes == sizeof(Header_), "not enough data in input stream to read header");

                ui8 version = ((Header_.Version >> 8) & 0xff);
                DECODE_ENSURE(version == 1, "versions mismatch (expected: 1, got: " << +version << ')');

                DECODE_ENSURE(Header_.HeaderSize >= sizeof(Header_), "invalid header size");
                if (size_t skipBytes = Header_.HeaderSize - sizeof(Header_)) {
                    DECODE_ENSURE(In_->Skip(skipBytes) == skipBytes, "input stream unexpectedly ended");
                }

                if (Header_.MetricCount == 0) {
                    // emulate empty stream
                    c->OnStreamEnd();
                    return;
                }

                // if compression enabled all below reads must go throught decompressor
                auto compressedIn = CompressedInput(In_, DecodeCompression(Header_.Compression));
                if (compressedIn) {
                    In_ = compressedIn.Get();
                }

                TimePrecision_ = DecodeTimePrecision(Header_.TimePrecision);

                const ui64 labelSizeTotal = ui64(Header_.LabelNamesSize) + Header_.LabelValuesSize;

                DECODE_ENSURE(labelSizeTotal <= LABEL_SIZE_LIMIT, "Label names & values size of " << HumanReadableSize(labelSizeTotal, SF_BYTES)
                    << " exceeds the limit which is " << HumanReadableSize(LABEL_SIZE_LIMIT, SF_BYTES));

                // (2) read string pools
                TVector<char> namesBuf(Header_.LabelNamesSize);
                readBytes = In_->Load(namesBuf.data(), namesBuf.size());
                DECODE_ENSURE(readBytes == Header_.LabelNamesSize, "not enough data to read label names pool");
                TStringPool labelNames(namesBuf.data(), namesBuf.size());

                TVector<char> valuesBuf(Header_.LabelValuesSize);
                readBytes = In_->Load(valuesBuf.data(), valuesBuf.size());
                DECODE_ENSURE(readBytes == Header_.LabelValuesSize, "not enough data to read label values pool");
                TStringPool labelValues(valuesBuf.data(), valuesBuf.size());

                // (3) read common time
                c->OnCommonTime(ReadTime());

                // (4) read common labels
                if (ui32 commonLabelsCount = ReadVarint()) {
                    c->OnLabelsBegin();
                    ReadLabels(labelNames, labelValues, commonLabelsCount, c);
                    c->OnLabelsEnd();
                }

                // (5) read metrics
                ReadMetrics(labelNames, labelValues, c);
                c->OnStreamEnd();
            }

        private:
            void ReadMetrics(
                    const TStringPool& labelNames,
                    const TStringPool& labelValues,
                    IMetricConsumer* c)
            {
                for (ui32 i = 0; i < Header_.MetricCount; i++) {
                    // (5.1) types byte
                    ui8 typesByte = ReadFixed<ui8>();
                    EMetricType metricType = DecodeMetricType(typesByte >> 2);
                    EValueType valueType = DecodeValueType(typesByte & 0x03);

                    c->OnMetricBegin(metricType);

                    // TODO: use it
                    ReadFixed<ui8>(); // skip flags byte

                    auto metricNameValueIndex = std::numeric_limits<ui32>::max();
                    if (Header_.Version >= SV1_02) {
                        metricNameValueIndex = ReadVarint();
                    }

                    // (5.2) labels
                    ui32 labelsCount = ReadVarint();
                    DECODE_ENSURE(Header_.Version >= SV1_02 || labelsCount > 0, "metric #" << i << " has no labels");
                    c->OnLabelsBegin();
                    if (Header_.Version >= SV1_02) {
                        c->OnLabel(MetricNameLabel_, labelValues.Get(metricNameValueIndex));
                    }
                    ReadLabels(labelNames, labelValues, labelsCount, c);
                    c->OnLabelsEnd();

                    // (5.3) values
                    switch (valueType) {
                        case EValueType::NONE:
                            break;
                        case EValueType::ONE_WITHOUT_TS:
                            ReadValue(metricType, TInstant::Zero(), c);
                            break;
                        case EValueType::ONE_WITH_TS: {
                            TInstant time = ReadTime();
                            ReadValue(metricType, time, c);
                            break;
                        }
                        case EValueType::MANY_WITH_TS: {
                            ui32 pointsCount = ReadVarint();
                            for (ui32 i = 0; i < pointsCount; i++) {
                                TInstant time = ReadTime();
                                ReadValue(metricType, time, c);
                            }
                            break;
                        }
                    }

                    c->OnMetricEnd();
                }
            }

            void ReadValue(EMetricType metricType, TInstant time, IMetricConsumer* c) {
                switch (metricType) {
                case EMetricType::GAUGE:
                    c->OnDouble(time, ReadFixed<double>());
                    break;

                case EMetricType::IGAUGE:
                    c->OnInt64(time, ReadFixed<i64>());
                    break;

                case EMetricType::COUNTER:
                case EMetricType::RATE:
                    c->OnUint64(time, ReadFixed<ui64>());
                    break;

                case EMetricType::DSUMMARY:
                    c->OnSummaryDouble(time, ReadSummaryDouble());
                    break;

                case EMetricType::HIST:
                case EMetricType::HIST_RATE:
                    c->OnHistogram(time, ReadHistogram());
                    break;

                case EMetricType::LOGHIST:
                    c->OnLogHistogram(time, ReadLogHistogram());
                    break;

                default:
                    throw TSpackDecodeError() << "Unsupported metric type: " << metricType;
                }
            }

            ISummaryDoubleSnapshotPtr ReadSummaryDouble() {
                ui64 count = ReadFixed<ui64>();
                double sum = ReadFixed<double>();
                double min = ReadFixed<double>();
                double max = ReadFixed<double>();
                double last = ReadFixed<double>();
                return MakeIntrusive<TSummaryDoubleSnapshot>(sum, min, max, last, count);
            }

            TLogHistogramSnapshotPtr ReadLogHistogram() {
                double base = ReadFixed<double>();
                ui64 zerosCount = ReadFixed<ui64>();
                int startPower = static_cast<int>(ReadVarint());
                ui32 count = ReadVarint();
                // see https://a.yandex-team.ru/arc/trunk/arcadia/infra/yasm/stockpile_client/points.cpp?rev=r8593154#L31
                // and https://a.yandex-team.ru/arc/trunk/arcadia/infra/yasm/common/points/hgram/normal/normal.h?rev=r8268697#L9
                // TODO: share this constant value
                Y_ENSURE(count <= 100u, "more than 100 buckets in log histogram: " << count);
                TVector<double> buckets;
                buckets.reserve(count);
                for (ui32 i = 0; i < count; ++i) {
                    buckets.emplace_back(ReadFixed<double>());
                }
                return MakeIntrusive<TLogHistogramSnapshot>(base, zerosCount, startPower, std::move(buckets));
            }

            IHistogramSnapshotPtr ReadHistogram() {
                ui32 bucketsCount = ReadVarint();
                auto s = TExplicitHistogramSnapshot::New(bucketsCount);

                if (SV1_00 == Header_.Version) { // v1.0
                    for (ui32 i = 0; i < bucketsCount; i++) {
                        i64 bound = ReadFixed<i64>();
                        double doubleBound = (bound != Max<i64>())
                                ? static_cast<double>(bound)
                                : Max<double>();

                        (*s)[i].first = doubleBound;
                    }
                } else {
                    for (ui32 i = 0; i < bucketsCount; i++) {
                        double doubleBound = ReadFixed<double>();
                        (*s)[i].first = doubleBound;
                    }
                }


                // values
                for (ui32 i = 0; i < bucketsCount; i++) {
                    (*s)[i].second = ReadFixed<ui64>();
                }
                return s;
            }

            void ReadLabels(
                const TStringPool& labelNames,
                const TStringPool& labelValues,
                ui32 count,
                IMetricConsumer* c)
            {
                for (ui32 i = 0; i < count; i++) {
                    auto nameIdx = ReadVarint();
                    auto valueIdx = ReadVarint();
                    c->OnLabel(labelNames.Get(nameIdx), labelValues.Get(valueIdx));
                }
            }

            TInstant ReadTime() {
                switch (TimePrecision_) {
                    case ETimePrecision::SECONDS:
                        return TInstant::Seconds(ReadFixed<ui32>());
                    case ETimePrecision::MILLIS:
                        return TInstant::MilliSeconds(ReadFixed<ui64>());
                }
                Y_ABORT("invalid time precision");
            }

            template <typename T>
            inline T ReadFixed() {
                T value;
                size_t readBytes = In_->Load(&value, sizeof(T));
                DECODE_ENSURE(readBytes == sizeof(T), "no enough data to read " << TypeName<T>());
                return value;
            }

            inline ui32 ReadVarint() {
                return ReadVarUInt32(In_);
            }

        private:
            IInputStream* In_;
            TString MetricNameLabel_;
            ETimePrecision TimePrecision_;
            TSpackHeader Header_;
        }; // class TDecoderSpackV1

#undef DECODE_ENSURE
    } // namespace

    EValueType DecodeValueType(ui8 byte) {
        EValueType result;
        if (!TryDecodeValueType(byte, &result)) {
            throw TSpackDecodeError() << "unknown value type: " << byte;
        }
        return result;
    }

    bool TryDecodeValueType(ui8 byte, EValueType* result) {
        if (byte == EncodeValueType(EValueType::NONE)) {
            if (result) {
                *result = EValueType::NONE;
            }
            return true;
        } else if (byte == EncodeValueType(EValueType::ONE_WITHOUT_TS)) {
            if (result) {
                *result = EValueType::ONE_WITHOUT_TS;
            }
            return true;
        } else if (byte == EncodeValueType(EValueType::ONE_WITH_TS)) {
            if (result) {
                *result = EValueType::ONE_WITH_TS;
            }
            return true;
        } else if (byte == EncodeValueType(EValueType::MANY_WITH_TS)) {
            if (result) {
                *result = EValueType::MANY_WITH_TS;
            }
            return true;
        } else {
            return false;
        }
    }

    ETimePrecision DecodeTimePrecision(ui8 byte) {
        ETimePrecision result;
        if (!TryDecodeTimePrecision(byte, &result)) {
            throw TSpackDecodeError() << "unknown time precision: " << byte;
        }
        return result;
    }

    bool TryDecodeTimePrecision(ui8 byte, ETimePrecision* result) {
        if (byte == EncodeTimePrecision(ETimePrecision::SECONDS)) {
            if (result) {
                *result = ETimePrecision::SECONDS;
            }
            return true;
        } else if (byte == EncodeTimePrecision(ETimePrecision::MILLIS)) {
            if (result) {
                *result = ETimePrecision::MILLIS;
            }
            return true;
        } else {
            return false;
        }
    }

    EMetricType DecodeMetricType(ui8 byte) {
        EMetricType result;
        if (!TryDecodeMetricType(byte, &result)) {
            throw TSpackDecodeError() << "unknown metric type: " << byte;
        }
        return result;
    }

    bool TryDecodeMetricType(ui8 byte, EMetricType* result) {
        if (byte == EncodeMetricType(EMetricType::GAUGE)) {
            if (result) {
                *result = EMetricType::GAUGE;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::COUNTER)) {
            if (result) {
                *result = EMetricType::COUNTER;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::RATE)) {
            if (result) {
                *result = EMetricType::RATE;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::IGAUGE)) {
            if (result) {
                *result = EMetricType::IGAUGE;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::HIST)) {
            if (result) {
                *result = EMetricType::HIST;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::HIST_RATE)) {
            if (result) {
                *result = EMetricType::HIST_RATE;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::DSUMMARY)) {
            if (result) {
                *result = EMetricType::DSUMMARY;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::LOGHIST)) {
            if (result) {
                *result = EMetricType::LOGHIST;
            }
            return true;
        } else if (byte == EncodeMetricType(EMetricType::UNKNOWN)) {
            if (result) {
                *result = EMetricType::UNKNOWN;
            }
            return true;
        } else {
            return false;
        }
    }

    ui8 EncodeCompression(ECompression c) noexcept {
        switch (c) {
            case ECompression::IDENTITY:
                return 0x00;
            case ECompression::ZLIB:
                return 0x01;
            case ECompression::ZSTD:
                return 0x02;
            case ECompression::LZ4:
                return 0x03;
            case ECompression::UNKNOWN:
                return Max<ui8>();
        }
        Y_ABORT(); // for GCC
    }

    ECompression DecodeCompression(ui8 byte) {
        ECompression result;
        if (!TryDecodeCompression(byte, &result)) {
            throw TSpackDecodeError() << "unknown compression alg: " << byte;
        }
        return result;
    }

    bool TryDecodeCompression(ui8 byte, ECompression* result) {
        if (byte == EncodeCompression(ECompression::IDENTITY)) {
            if (result) {
                *result = ECompression::IDENTITY;
            }
            return true;
        } else if (byte == EncodeCompression(ECompression::ZLIB)) {
            if (result) {
                *result = ECompression::ZLIB;
            }
            return true;
        } else if (byte == EncodeCompression(ECompression::ZSTD)) {
            if (result) {
                *result = ECompression::ZSTD;
            }
            return true;
        } else if (byte == EncodeCompression(ECompression::LZ4)) {
            if (result) {
                *result = ECompression::LZ4;
            }
            return true;
        } else {
            return false;
        }
    }

    void DecodeSpackV1(IInputStream* in, IMetricConsumer* c, TStringBuf metricNameLabel) {
        TDecoderSpackV1 decoder(in, metricNameLabel);
        decoder.Decode(c);
    }

}
