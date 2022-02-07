#pragma once

#include <library/cpp/monlib/encode/encoder.h>

class IOutputStream;

namespace NMonitoring {
    // Does nothing: just implements IMetricEncoder interface with stubs
    IMetricEncoderPtr EncoderFake();
}
