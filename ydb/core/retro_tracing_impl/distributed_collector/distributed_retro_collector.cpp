#include "distributed_retro_collector.h"

#include <ydb/library/actors/retro_tracing/retro_collector.h>
#include <ydb/library/actors/retro_tracing/span_buffer.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/monotonic.h>

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

namespace NKikimr {

namespace {

struct TEvPrivate {
    enum EEv {
        EvCollectRetroTrace = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCollectAllRetroTraces,
        EvFlushBatch,
    };

    struct TEvCollectRetroTrace : NActors::TEventLocal<TEvCollectRetroTrace, EvCollectRetroTrace> {
        NWilson::TTraceId TraceId;

        TEvCollectRetroTrace(const NWilson::TTraceId& traceId)
            : TraceId(traceId)
        {}
    };

    struct TEvCollectAllRetroTraces : NActors::TEventLocal<TEvCollectAllRetroTraces, EvCollectAllRetroTraces> {};
    struct TEvFlushBatch : NActors::TEventLocal<TEvFlushBatch, EvFlushBatch> {};
};

class TDistributedRetroCollector : public NActors::TActorBootstrapped<TDistributedRetroCollector> {
public:
    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvCollectRetroTrace, Handle);
        cFunc(TEvPrivate::TEvCollectAllRetroTraces::EventType, HandleCollectAll);
        cFunc(TEvPrivate::TEvFlushBatch::EventType, HandleFlushBatch);
        cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
    );

    void Handle(const TEvPrivate::TEvCollectRetroTrace::TPtr& ev) {
        const NWilson::TTraceId& traceId = ev->Get()->TraceId;
        if (!traceId) {
            return;
        }

        const TString hex = traceId.GetHexTraceId();
        if (RecentlyObserved.contains(hex)) {
            return;
        }

        if (PendingTraceIds.size() >= MaxBatchSize) {
            return;
        }

        PendingTraceIds.push_back(NWilson::TTraceId(traceId));
        RecentlyObserved[hex] = NActors::TMonotonic::Now();

        if (!std::exchange(FlushScheduled, true)) {
            Schedule(BatchFlushInterval, new TEvPrivate::TEvFlushBatch());
        }
    }

    void HandleCollectAll() {
        ConvertAndSend(NRetroTracing::GetAllSpans());
    }

    void HandleFlushBatch() {
        FlushScheduled = false;

        if (PendingTraceIds.empty()) {
            return;
        }

        SendToDistConf();
        ConvertAndSend(NRetroTracing::GetSpansOfTraces(std::exchange(PendingTraceIds, {})));
        EvictExpiredEntries(NActors::TMonotonic::Now());
    }

    void SendToDistConf() {
        auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto* req = ev->Record.MutableDemandRetroTrace();
        for (const NWilson::TTraceId& traceId : PendingTraceIds) {
            traceId.Serialize(req->AddTraceId());
        }
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
    }

    void EvictExpiredEntries(NActors::TMonotonic now) {
        auto it = RecentlyObserved.begin();
        while (it != RecentlyObserved.end()) {
            if (now - it->second > DeduplicationTTL) {
                it = RecentlyObserved.erase(it);
            } else {
                ++it;
            }
        }
    }

    void ConvertAndSend(std::vector<std::unique_ptr<NRetroTracing::TRetroSpan>>&& spans) {
        for (const std::unique_ptr<NRetroTracing::TRetroSpan>& span : spans) {
            std::unique_ptr<NWilson::TSpan> wilson = span->MakeWilsonSpan();
            wilson->Attribute("type", "RETRO");
            wilson->End();
        }
    }

private:
    // TODO: on-line configuration
    static constexpr ui32 MaxBatchSize = 64;
    static constexpr TDuration BatchFlushInterval = TDuration::MilliSeconds(500);
    static constexpr TDuration DeduplicationTTL = TDuration::Seconds(10);

    std::vector<NWilson::TTraceId> PendingTraceIds;
    bool FlushScheduled = false;

    std::unordered_map<TString, NActors::TMonotonic> RecentlyObserved;
};

} // anonymous namespace

NActors::IActor* CreateDistributedRetroCollector() {
    return new TDistributedRetroCollector;
}

} // namespace NKikimr
