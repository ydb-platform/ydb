#pragma once

#include <util/generic/fwd.h>

namespace NMonitoring {

    constexpr ui32 MaxMetricTypeNameLength = 9;

    enum class EMetricType {
        UNKNOWN = 0,
        GAUGE = 1,
        COUNTER = 2,
        RATE = 3,
        IGAUGE = 4,
        HIST = 5,
        HIST_RATE = 6,
        DSUMMARY = 7,
        // ISUMMARY = 8, reserved
        LOGHIST = 9,
    };

    TStringBuf MetricTypeToStr(EMetricType type);
    EMetricType MetricTypeFromStr(TStringBuf str);

}
