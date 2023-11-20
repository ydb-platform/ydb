#pragma once
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

namespace NYql::NDqs {

struct TWorkerManagerBenchmarkOptions {
    int WorkerCount = 10;
    int Inflight = 10;
    int TotalRequests = 100000;
    TDuration MaxRunTimeMs = TDuration::Minutes(30);
};

NActors::IActor* CreateWorkerManagerBenchmark(NActors::TActorId workerManagerId, const TWorkerManagerBenchmarkOptions& options);

} // namespace NYql::NDqs
