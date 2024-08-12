#pragma once
#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NMVP {
    // forward declaration from "mvp_tokens.h"
    class TMvpTokenator;
}

struct TMVPAppData {
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
    NMVP::TMvpTokenator* Tokenator = nullptr;
};

inline TMVPAppData* MVPAppData() {
    return NActors::TlsActivationContext->ExecutorThread.ActorSystem->template AppData<TMVPAppData>();
}
