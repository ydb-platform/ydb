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
private:
    // Compile-time constants (online configuration to be added separately)
    static constexpr ui32 MaxBatchSize = 64;
    static constexpr TDuration BatchFlushInterval = TDuration::MilliSeconds(500);
    static constexpr TDuration DeduplicationTTL = TDuration::Seconds(10);

    // Batch accumulation
    std::vector<NWilson::TTraceId> PendingTraceIds;
    bool FlushScheduled = false;

    // Deduplication — recently observed trace IDs
    std::unordered_map<TString, NActors::TMonotonic> RecentlyObserved;

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

        // Check deduplication — skip if recently flushed
        if (RecentlyObserved.contains(hex)) {
            return;
        }

        // Silently reject if batch is full — protects DistConf from overload
        if (PendingTraceIds.size() >= MaxBatchSize) {
            return;
        }

        PendingTraceIds.push_back(NWilson::TTraceId(traceId));
        RecentlyObserved[hex] = NActors::TMonotonic::Now();

        // Schedule flush timer if not already scheduled
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

        // Send batched request to DistConf via NodeWarden (before moving PendingTraceIds)
        SendToDistConf();

        // Single buffer scan for all pending trace IDs
        ConvertAndSend(NRetroTracing::GetSpansOfTraces(std::move(PendingTraceIds)));

        // Clear pending state (moved-from vector, but clear for safety)
        PendingTraceIds.clear();

        // Evict expired entries from RecentlyObserved
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
};

} // anonymous namespace

NActors::IActor* CreateDistributedRetroCollector() {
    return new TDistributedRetroCollector;
}

} // namespace NKikimr
