#pragma once

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/format.h>

#include <util/generic/yexception.h>


namespace NMonitoring {

    class TPrometheusDecodeException: public yexception {
    };

    IMetricEncoderPtr EncoderPrometheus(IOutputStream* out, TStringBuf metricNameLabel = "sensor"); 

    void DecodePrometheus(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel = "sensor"); 

}
