#pragma once

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/format.h>
#include <library/cpp/monlib/metrics/metric.h>

#include <util/generic/yexception.h>

//
// format specification available here:
//     https://wiki.yandex-team.ru/solomon/api/dataformat/spackv1/
//

class IInputStream;
class IOutputStream;

namespace NMonitoring {
    class TSpackDecodeError: public yexception {
    };

    constexpr auto EncodeMetricType(EMetricType mt) noexcept {
        return static_cast<std::underlying_type_t<EMetricType>>(mt);
    }

    EMetricType DecodeMetricType(ui8 byte);

    [[nodiscard]]
    bool TryDecodeMetricType(ui8 byte, EMetricType* result);

    ///////////////////////////////////////////////////////////////////////////////
    // EValueType
    ///////////////////////////////////////////////////////////////////////////////
    enum class EValueType : ui8 {
        NONE = 0x00,
        ONE_WITHOUT_TS = 0x01,
        ONE_WITH_TS = 0x02,
        MANY_WITH_TS = 0x03,
    };

    constexpr auto EncodeValueType(EValueType vt) noexcept {
        return static_cast<std::underlying_type_t<EValueType>>(vt);
    }

    EValueType DecodeValueType(ui8 byte);

    [[nodiscard]]
    bool TryDecodeValueType(ui8 byte, EValueType* result);

    ///////////////////////////////////////////////////////////////////////////////
    // ETimePrecision
    ///////////////////////////////////////////////////////////////////////////////
    enum class ETimePrecision : ui8 {
        SECONDS = 0x00,
        MILLIS = 0x01,
    };

    constexpr auto EncodeTimePrecision(ETimePrecision tp) noexcept {
        return static_cast<std::underlying_type_t<ETimePrecision>>(tp);
    }

    ETimePrecision DecodeTimePrecision(ui8 byte);

    [[nodiscard]]
    bool TryDecodeTimePrecision(ui8 byte, ETimePrecision* result);

    ///////////////////////////////////////////////////////////////////////////////
    // ECompression
    ///////////////////////////////////////////////////////////////////////////////
    ui8 EncodeCompression(ECompression c) noexcept;

    ECompression DecodeCompression(ui8 byte);

    [[nodiscard]]
    bool TryDecodeCompression(ui8 byte, ECompression* result);

    ///////////////////////////////////////////////////////////////////////////////
    // TSpackHeader
    ///////////////////////////////////////////////////////////////////////////////
    struct Y_PACKED TSpackHeader {
        ui16 Magic = 0x5053;   // "SP"
        ui16 Version;          // MSB - major version, LSB - minor version
        ui16 HeaderSize = sizeof(TSpackHeader);
        ui8 TimePrecision;
        ui8 Compression;
        ui32 LabelNamesSize;
        ui32 LabelValuesSize;
        ui32 MetricCount;
        ui32 PointsCount;
        // add new fields here
    };

    enum ESpackV1Version: ui16 {
        SV1_00 = 0x0100,
        SV1_01 = 0x0101,
        SV1_02 = 0x0102
    };

    IMetricEncoderPtr EncoderSpackV1(
        IOutputStream* out,
        ETimePrecision timePrecision,
        ECompression compression,
        EMetricsMergingMode mergingMode = EMetricsMergingMode::DEFAULT
    );

    IMetricEncoderPtr EncoderSpackV12(
        IOutputStream* out,
        ETimePrecision timePrecision,
        ECompression compression,
        EMetricsMergingMode mergingMode = EMetricsMergingMode::DEFAULT,
        TStringBuf metricNameLabel = "name"
    );

    void DecodeSpackV1(IInputStream* in, IMetricConsumer* c, TStringBuf metricNameLabel = "name");

}
