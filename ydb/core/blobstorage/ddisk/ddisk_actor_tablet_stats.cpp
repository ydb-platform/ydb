#include "ddisk_actor.h"

namespace NKikimr::NDDisk {

void TDDiskActor::ScheduleTabletStats() {
    if (Stopping || TabletStatsScheduled || TabletStatsAwaitingAck || !TabletStatsActor) {
        return;
    }
    if (const auto deadline = TabletStats.NextDeadline()) {
        const auto now = TActivationContext::Monotonic();
        TabletStatsScheduled = true;
        Schedule(*deadline > now ? *deadline - now : TDuration::MilliSeconds(1),
            new TEvents::TEvWakeup(EWakeupTag::WakeupCollectTabletStats));
    }
}

void TDDiskActor::CountTabletIo(ui64 tabletId, ETabletOperation operation, ui64 requests, ui64 bytes) {
    TabletStats.AddIo(tabletId, operation, requests, bytes, TActivationContext::Monotonic());
    ScheduleTabletStats();
}

void TDDiskActor::CountTabletChunks(ui64 tabletId, i64 delta) {
    TabletStats.AddChunks(tabletId, delta, TActivationContext::Monotonic());
    ScheduleTabletStats();
}

void TDDiskActor::CollectTabletStats() {
    TabletStatsScheduled = false;
    auto batch = std::make_unique<TEvTabletStatsBatch>();
    batch->Samples = TabletStats.Collect(TActivationContext::Monotonic());
    batch->SampledAt = TActivationContext::Now();
    if (!batch->Samples.empty()) {
        TabletStatsAwaitingAck = true;
        Send(TabletStatsActor, batch.release());
    } else {
        ScheduleTabletStats();
    }
}

void TDDiskActor::Handle(TEvTabletStatsAck::TPtr ev) {
    if (ev->Sender == TabletStatsActor && TabletStatsAwaitingAck) {
        TabletStatsAwaitingAck = false;
        ScheduleTabletStats();
    }
}

void TDDiskActor::SetDataChunkMapping(ui64 tabletId, TChunkRef* ref, TChunkIdx chunkIdx) {
    if (bool(ref->ChunkIdx) != bool(chunkIdx)) {
        CountTabletChunks(tabletId, chunkIdx ? 1 : -1);
    }
    ref->ChunkIdx = chunkIdx;
}

void TDDiskActor::Handle(TEvGetTabletStats::TPtr ev) {
    if (TabletStatsActor && !Stopping) {
        Forward(ev, TabletStatsActor);
    } else {
        auto result = std::make_unique<TEvTabletStats>();
        result->Available = false;
        Send(ev->Sender, result.release(), 0, ev->Cookie);
    }
}

}
