#include "ddisk_actor.h"

namespace NKikimr::NDDisk {

void TDDiskActor::NotifyTabletStats() {
    if (!Stopping && TabletStatsActor && !TabletStatsActive && TabletStats.NextDeadline()) {
        TabletStatsActive = true;
        Send(TabletStatsActor, new TEvTabletStatsChanged());
    }
}

void TDDiskActor::CountTabletIo(ui64 tabletId, ETabletOperation operation, ui64 requests, ui64 bytes) {
    TabletStats.AddIo(tabletId, operation, requests, bytes, TActivationContext::Monotonic());
    NotifyTabletStats();
}

void TDDiskActor::CountTabletIo(ui64 tabletId, TTabletStatsEntry* entry, ETabletOperation operation,
        ui64 requests, ui64 bytes) {
    TabletStats.AddIo(tabletId, entry, operation, requests, bytes, TActivationContext::Monotonic());
    NotifyTabletStats();
}

void TDDiskActor::CountTabletChunks(ui64 tabletId, i64 delta) {
    TabletStats.AddChunks(tabletId, delta, TActivationContext::Monotonic());
    NotifyTabletStats();
}

void TDDiskActor::Handle(TEvCollectTabletStats::TPtr ev) {
    Y_ABORT_UNLESS(ev->Sender == TabletStatsActor);
    if (Stopping) {
        return;
    }
    auto batch = std::make_unique<TEvTabletStatsBatch>();
    batch->Samples = TabletStats.Collect(TActivationContext::Monotonic());
    batch->SampledAt = TActivationContext::Now();
    batch->NextDeadline = TabletStats.NextDeadline();
    TabletStatsActive = batch->NextDeadline.has_value();
    Send(TabletStatsActor, batch.release());
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
