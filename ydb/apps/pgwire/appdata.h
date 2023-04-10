#pragma once
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/executor_thread.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NPGW {

struct TAppData {
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
};

inline TAppData* AppData() {
    return NActors::TlsActivationContext->ExecutorThread.ActorSystem->template AppData<TAppData>();
}

}
