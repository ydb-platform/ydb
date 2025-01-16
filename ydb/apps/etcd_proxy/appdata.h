#pragma once
#include <atomic>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

namespace NEtcd {

struct TAppData {
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
    std::shared_ptr<NYdb::NQuery::TQueryClient> Client;
    std::atomic_int64_t Revision;
};

inline TAppData* AppData() {
    return NActors::TlsActivationContext->ExecutorThread.ActorSystem->template AppData<TAppData>();
}

}
