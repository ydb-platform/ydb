#pragma once
#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NMVP {
    // forward declaration from "mvp_tokens.h"
    class TMvpTokenator;
}

namespace NYdbGrpc {
    inline namespace Dev {
        class TGRpcClientLow;
    }
}

struct TMVPAppData {
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
    NMVP::TMvpTokenator* Tokenator = nullptr;
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GRpcClientLow;

    TMVPAppData();
};

TMVPAppData* MVPAppData();
