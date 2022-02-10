#pragma once

#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/format.h>


class IOutputStream;

namespace NMonitoring {

    class TJsonDecodeError: public yexception {
    };

    IMetricEncoderPtr EncoderJson(IOutputStream* out, int indentation = 0);

    /// Buffered encoder will merge series with same labels into one.
    IMetricEncoderPtr BufferedEncoderJson(IOutputStream* out, int indentation = 0);

    IMetricEncoderPtr EncoderCloudJson(IOutputStream* out,
                                       int indentation = 0,
                                       TStringBuf metricNameLabel = "name");

    IMetricEncoderPtr BufferedEncoderCloudJson(IOutputStream* out,
                                               int indentation = 0,
                                               TStringBuf metricNameLabel = "name");

    void DecodeJson(TStringBuf data, IMetricConsumer* c, TStringBuf metricNameLabel = "name");

}
