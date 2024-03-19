#pragma once

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/format.h>

#include <util/generic/yexception.h>


namespace NMonitoring {

    class TPrometheusDecodeException: public yexception {
    };

    enum class EPrometheusDecodeMode {
        DEFAULT,
        RAW
    };

    struct TPrometheusDecodeSettings {
        EPrometheusDecodeMode Mode{EPrometheusDecodeMode::DEFAULT};
    };

    IMetricEncoderPtr EncoderPrometheus(IOutputStream* out, TStringBuf metricNameLabel = "sensor");

    void DecodePrometheus(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel = "sensor", const TPrometheusDecodeSettings& settings = TPrometheusDecodeSettings{});

}
