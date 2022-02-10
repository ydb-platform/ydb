#include "metric_type.h"

#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>

namespace NMonitoring {
    TStringBuf MetricTypeToStr(EMetricType type) {
        switch (type) {
            case EMetricType::GAUGE:
                return TStringBuf("GAUGE");
            case EMetricType::COUNTER:
                return TStringBuf("COUNTER");
            case EMetricType::RATE:
                return TStringBuf("RATE");
            case EMetricType::IGAUGE:
                return TStringBuf("IGAUGE");
            case EMetricType::HIST:
                return TStringBuf("HIST");
            case EMetricType::HIST_RATE:
                return TStringBuf("HIST_RATE");
            case EMetricType::DSUMMARY:
                return TStringBuf("DSUMMARY");
            case EMetricType::LOGHIST:
                return TStringBuf("LOGHIST");
            default:
                return TStringBuf("UNKNOWN");
        }
    }

    EMetricType MetricTypeFromStr(TStringBuf str) {
        if (str == TStringBuf("GAUGE") || str == TStringBuf("DGAUGE")) {
            return EMetricType::GAUGE;
        } else if (str == TStringBuf("COUNTER")) {
            return EMetricType::COUNTER;
        } else if (str == TStringBuf("RATE")) {
            return EMetricType::RATE;
        } else if (str == TStringBuf("IGAUGE")) {
            return EMetricType::IGAUGE;
        } else if (str == TStringBuf("HIST")) {
            return EMetricType::HIST;
        } else if (str == TStringBuf("HIST_RATE")) {
            return EMetricType::HIST_RATE;
        } else if (str == TStringBuf("DSUMMARY")) {
            return EMetricType::DSUMMARY;
        } else if (str == TStringBuf("LOGHIST")) {
            return EMetricType::LOGHIST;
        } else {
            ythrow yexception() << "unknown metric type: " << str;
        }
    }
}

template <>
void Out<NMonitoring::EMetricType>(IOutputStream& o, NMonitoring::EMetricType t) {
    o << NMonitoring::MetricTypeToStr(t);
}
