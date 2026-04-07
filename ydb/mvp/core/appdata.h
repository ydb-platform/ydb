#pragma once
#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NYdbGrpc {
    inline namespace Dev {
        class TGRpcClientLow;
    }
}

namespace NMVP {
    // forward declaration from "mvp_tokens.h"
    class TMvpTokenator;

    struct TMVPAppData {
        std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
        TMvpTokenator* Tokenator = nullptr;
        std::shared_ptr<NYdbGrpc::TGRpcClientLow> GRpcClientLow;

        TMVPAppData();
    };

    TMVPAppData* MVPAppData();
}
