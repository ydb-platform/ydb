#pragma once

#include <library/cpp/monlib/encode/encoder.h>

class IOutputStream;

namespace NMonitoring {
    IMetricEncoderPtr EncoderText(IOutputStream* out, bool humanReadableTs = true);
}
