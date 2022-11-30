#pragma once

#include <util/generic/fwd.h>
#include <util/datetime/base.h>

namespace NMonitoring {
    /// Decodes unistat-style metrics
    /// https://wiki.yandex-team.ru/golovan/stat-handle
    void DecodeUnistat(TStringBuf data, class IMetricConsumer* c, TStringBuf metricNameLabel = "sensor", TInstant ts = TInstant::Zero());

    /// Assumes consumer's stream is open by the caller
    void DecodeUnistatToStream(TStringBuf data, class IMetricConsumer* c, TStringBuf metricNameLabel = "sensor", TInstant ts = TInstant::Zero());
}
