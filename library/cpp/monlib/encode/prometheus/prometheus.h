#pragma once

#include <functional>

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
        /*
         * Mangles the label that is equal to metricNameLabel. The
         * new name must be not equal to any already present in the
         * metric sample.
         */
        std::function<TString(TStringBuf)> NameMangler{};
    };

    IMetricEncoderPtr EncoderPrometheus(IOutputStream* out, TStringBuf metricNameLabel = "sensor");

    void DecodePrometheus(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel = "sensor", const TPrometheusDecodeSettings& settings = TPrometheusDecodeSettings{});

}
