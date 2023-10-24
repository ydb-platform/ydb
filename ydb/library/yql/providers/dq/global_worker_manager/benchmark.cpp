#include "benchmark.h"

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/actors/resource_allocator.h>

namespace NYql::NDqs {

using namespace NActors;

class TWorkerManagerBenchmark: public TRichActor<TWorkerManagerBenchmark> {
public:
    TWorkerManagerBenchmark(NActors::TActorId workerManagerId, const TWorkerManagerBenchmarkOptions& options)
        : TRichActor(&TWorkerManagerBenchmark::Handler)
        , WorkerManagerId(workerManagerId)
        , Options(options)
        , Settings(new TDqConfiguration)
        , Counters(new NMonitoring::TDynamicCounters)
    {
        Y_UNUSED(WorkerManagerId);
        Y_UNUSED(Options);
        Y_UNUSED(RunningRequests);
    }

private:
    STRICT_STFUNC(Handler, {
        cFunc(TEvents::TEvWakeup::EventType, Wakeup)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        cFunc(TEvents::TEvBootstrap::EventType, Bootstrap)
        HFunc(TEvAllocateWorkersResponse, OnAllocateWorkersResponse)
    });

    void Bootstrap() {
        Schedule(Options.MaxRunTimeMs, new TEvents::TEvPoison);
        Wakeup();
    }

    void Wakeup() {
        while ((int)RunningRequests.size() < Options.Inflight && Requests < Options.TotalRequests) {
            StartRequest();
        }

        if (RunningRequests.empty()) {
            PassAway();
        }
    }

    void StartRequest() {
        TString operationId = TStringBuilder() << "Benchmark-" << Requests;
        auto resourceAllocator = RegisterChild(
            CreateResourceAllocator(
                WorkerManagerId, SelfId(), SelfId(),
                Options.WorkerCount,
                operationId, Settings,
                Counters,
                {}));

        auto allocateRequest = MakeHolder<TEvAllocateWorkersRequest>(Options.WorkerCount, "TestUser");
        allocateRequest->Record.SetTraceId(operationId);

        TActivationContext::Send(
            new IEventHandle(
                WorkerManagerId, resourceAllocator, allocateRequest.Release()));

        RunningRequests.insert(std::make_pair(resourceAllocator, TRequestInfo{TInstant::Now()}));
        Requests ++;
    }

    void OnAllocateWorkersResponse(TEvAllocateWorkersResponse::TPtr& ev, const NActors::TActorContext&) {
        UnregisterChild(ev->Sender);
        RunningRequests.erase(ev->Sender);

        Wakeup();
    }


    const NActors::TActorId WorkerManagerId;
    const TWorkerManagerBenchmarkOptions Options;
    TDqConfiguration::TPtr Settings;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    struct TRequestInfo {
        TInstant StartTime;
    };

    THashMap<TActorId, TRequestInfo> RunningRequests;
    int Requests = 0;
};

NActors::IActor* CreateWorkerManagerBenchmark(NActors::TActorId workerManagerId, const TWorkerManagerBenchmarkOptions& options)
{
    return new TWorkerManagerBenchmark(workerManagerId, options);
}


} // namespace NYql::NDqs
