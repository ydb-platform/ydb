#include "distributed_retro_collector.h"

#include <ydb/library/actors/retro_tracing/collector/events.h>
#include <ydb/library/actors/retro_tracing/collector/retro_collector.h>
#include <ydb/library/actors/retro_tracing/collector/retro_span_deserialization.h>
#include <ydb/library/actors/retro_tracing/span/span_buffer.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/monotonic.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

namespace NKikimr {

namespace {

constexpr ui64 DefaultMaxBatchSize = 64;
constexpr ui64 MinMaxBatchSize = 1;
constexpr ui64 MaxMaxBatchSize = 100000;

constexpr TDuration DefaultBatchFlushInterval = TDuration::MilliSeconds(500);
constexpr ui64 MinBatchFlushIntervalMs = 1;
constexpr ui64 MaxBatchFlushIntervalMs = 3600000;

constexpr TDuration DefaultDeduplicationTTL = TDuration::Seconds(10);
constexpr ui64 MinDeduplicationTTLSec = 1;
constexpr ui64 MaxDeduplicationTTLSec = 3600;

struct TEvPrivate {
    enum EEv {
        EvFlushBatch = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    };

    struct TEvFlushBatch : NActors::TEventLocal<TEvFlushBatch, EvFlushBatch> {};
};

class TDistributedRetroCollector : public NActors::TActorBootstrapped<TDistributedRetroCollector> {
public:
    void Bootstrap() {
        if (AppData() && AppData()->Icb) {
            TIntrusivePtr<NKikimr::TControlBoard>& icb = AppData()->Icb;
            TControlBoard::RegisterSharedControl(MaxBatchSize, icb->RetroTracingControls.MaxBatchSize);
            TControlBoard::RegisterSharedControl(BatchFlushIntervalMs, icb->RetroTracingControls.BatchFlushIntervalMs);
            TControlBoard::RegisterSharedControl(DeduplicationTTLSec, icb->RetroTracingControls.DeduplicationTTLSec);
        }
        Become(&TThis::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NRetroTracing::TEvCollectRetroTrace, Handle);
        cFunc(NRetroTracing::TEvCollectAllRetroTraces::EventType, HandleCollectAll);
        cFunc(TEvPrivate::TEvFlushBatch::EventType, HandleFlushBatch);
        cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
    );

    void Handle(const NRetroTracing::TEvCollectRetroTrace::TPtr& ev) {
        const NWilson::TTraceId& traceId = ev->Get()->TraceId;
        if (!traceId) {
            return;
        }

        const TString hex = traceId.GetHexTraceId();
        if (RecentlyObserved.contains(hex)) {
            return;
        }

        if (PendingTraceIds.size() >= static_cast<size_t>(static_cast<i64>(MaxBatchSize))) {
            return;
        }

        PendingTraceIds.push_back(NWilson::TTraceId(traceId));
        RecentlyObserved[hex] = NActors::TMonotonic::Now();

        if (!std::exchange(FlushScheduled, true)) {
            Schedule(TDuration::MilliSeconds(static_cast<i64>(BatchFlushIntervalMs)),
                    new TEvPrivate::TEvFlushBatch());
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
        EvictExpiredEntries(NActors::TMonotonic::Now());
    }

    void SendToDistConf() {
        auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        auto* req = ev->Record.MutableDemandRetroTrace();
        for (const NWilson::TTraceId& traceId : std::exchange(PendingTraceIds, {})) {
            traceId.Serialize(req->AddTraceId());
        }
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
    }

    void EvictExpiredEntries(NActors::TMonotonic now) {
        const TDuration deduplicationTTL = TDuration::Seconds(static_cast<i64>(DeduplicationTTLSec));
        auto it = RecentlyObserved.begin();
        while (it != RecentlyObserved.end()) {
            if (now - it->second > deduplicationTTL) {
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
    TControlWrapper MaxBatchSize = TControlWrapper(DefaultMaxBatchSize, MinMaxBatchSize, MaxMaxBatchSize);
    TControlWrapper BatchFlushIntervalMs =
            TControlWrapper(DefaultBatchFlushInterval.MilliSeconds(), MinBatchFlushIntervalMs, MaxBatchFlushIntervalMs);
    TControlWrapper DeduplicationTTLSec =
            TControlWrapper(DefaultDeduplicationTTL.Seconds(), MinDeduplicationTTLSec, MaxDeduplicationTTLSec);

    std::vector<NWilson::TTraceId> PendingTraceIds;
    bool FlushScheduled = false;

    std::unordered_map<TString, NActors::TMonotonic> RecentlyObserved;
};

} // anonymous namespace

NActors::IActor* CreateDistributedRetroCollector() {
    return new TDistributedRetroCollector;
}

} // namespace NKikimr
