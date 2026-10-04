#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>

#include <util/generic/algorithm.h>

#include <iterator>
#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {
namespace {

// Fixed cadence until the configurable delay and jitter land with the portion walk.
constexpr TDuration ContinuationDelay = TDuration::MilliSeconds(100);

void ScheduleUnusedHistoryContinuation(const TActorContext& ctx) {
    ctx.Schedule(ContinuationDelay, new TEvPrivate::TEvContinueUnusedHistory());
}

// The executor may have rewritten the channel history since the scan started, so the interval is re-matched against it.
bool CanCutHistoryInterval(
    const TColumnShard& owner, const THistoryInterval& interval, const NOlap::TPendingGCBlobGenerations& pendingGenerations) {
    if (interval.HasBlobs || interval.Channel >= owner.Info()->Channels.size() || !owner.LauncherID()) {
        return false;
    }
    const auto& history = owner.Info()->Channels[interval.Channel].History;
    const auto entry = FindIf(history, [&](const auto& item) {
        return item.FromGeneration == interval.From;
    });
    if (entry == history.end() || entry->GroupID != interval.Group) {
        return false;
    }
    const auto next = std::next(entry);
    if (next == history.end() || next->FromGeneration != interval.To) {
        return false;
    }
    const auto storage =
        std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(owner.GetStoragesManager()->GetDefaultOperator());
    AFL_VERIFY(storage);
    return storage->CanCutHistory(pendingGenerations, interval.Channel, interval.From, interval.To);
}

}   // namespace

void TColumnShard::InitUnusedHistoryScan() {
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory()) {
        return;
    }
    TUnusedHistoryScan scan;
    for (ui32 channel = FirstDataChannel; channel < Info()->Channels.size(); ++channel) {
        const auto& history = Info()->Channels[channel].History;
        for (size_t i = 0; i + 1 < history.size(); ++i) {
            AFL_VERIFY(history[i].FromGeneration < history[i + 1].FromGeneration)("channel", channel);
            // The interval holding the current generation is still taking writes, and so is everything after it.
            if (history[i + 1].FromGeneration >= Generation()) {
                break;
            }
            scan.Intervals.push_back({ channel, history[i].FromGeneration, history[i + 1].FromGeneration, history[i].GroupID });
        }
    }
    if (scan.Intervals.empty()) {
        return;
    }
    UnusedHistoryScan = std::move(scan);
}

void TColumnShard::StartUnusedHistoryScan(const TActorContext& ctx) {
    if (!UnusedHistoryScan) {
        return;
    }
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory() ||
        !SharingSessionsManager->CanCutHistory()) {
        UnusedHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    AFL_VERIFY(storage);
    auto& scan = *UnusedHistoryScan;
    EraseIf(scan.Intervals, [&](const auto& interval) {
        return storage->GetSharedBlobs()->HasBlobsInRange(interval.Channel, interval.From, interval.To);
    });
    if (scan.Intervals.empty()) {
        UnusedHistoryScan.reset();
        return;
    }
    scan.Started = ctx.Now();
    // The preparation actor that collects the portion addresses off the tablet mailbox lands later.
    ScheduleUnusedHistoryContinuation(ctx);
}

void TColumnShard::AbortUnusedHistoryScan() {
    if (UnusedHistoryScan->PreparationActor) {
        Send(UnusedHistoryScan->PreparationActor, new TEvents::TEvPoisonPill());
    }
    UnusedHistoryScan.reset();
}

void TColumnShard::Handle(TEvPrivate::TEvUnusedHistoryPortionsReady::TPtr& ev, const TActorContext& ctx) {
    if (!UnusedHistoryScan || !UnusedHistoryScan->PreparationActor || ev->Sender != UnusedHistoryScan->PreparationActor) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortUnusedHistoryScan();
        return;
    }
    UnusedHistoryScan->PreparationActor = {};
    UnusedHistoryScan->Portions = std::move(ev->Get()->Portions);
    ScheduleUnusedHistoryContinuation(ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvContinueUnusedHistory::TPtr&, const TActorContext& ctx) {
    if (!UnusedHistoryScan || UnusedHistoryScan->Pending || UnusedHistoryScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortUnusedHistoryScan();
        return;
    }
    auto& scan = *UnusedHistoryScan;
    if (scan.PreparationActor) {
        return;
    }
    // Stub: the batched accessor walk that marks the intervals still holding blobs lands later.
    scan.Finished = ctx.Now();
    TryCutHistory(ctx);
}

void TColumnShard::ResumePostponedCutHistory(const TActorContext& ctx) {
    if (UnusedHistoryScan && UnusedHistoryScan->WaitingForGC) {
        TryCutHistory(ctx);
    }
}

void TColumnShard::TryCutHistory(const TActorContext& ctx) {
    if (!UnusedHistoryScan || !UnusedHistoryScan->Finished || UnusedHistoryScan->SavePending) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        UnusedHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    AFL_VERIFY(storage);
    // A running GC may still resurrect blobs in the interval, so back off and let the GC-finished transaction resume us.
    UnusedHistoryScan->WaitingForGC = storage->HasGCInFlight();
    if (UnusedHistoryScan->WaitingForGC) {
        return;
    }
    NOlap::TPendingGCBlobGenerations pendingGenerations;
    if (AnyOf(UnusedHistoryScan->Intervals, [](const auto& interval) {
            return !interval.Attempted && !interval.HasBlobs;
        })) {
        pendingGenerations = storage->GetPendingGCBlobGenerations();
    }
    for (size_t i = 0; i < UnusedHistoryScan->Intervals.size(); ++i) {
        auto& interval = UnusedHistoryScan->Intervals[i];
        // One request per interval per generation; TEvUndelivered clears this so the periodic wakeup retries.
        if (interval.Attempted) {
            continue;
        }
        interval.Attempted = true;
        if (!CanCutHistoryInterval(*this, interval, pendingGenerations)) {
            continue;
        }
        auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
        event->Record.SetTabletID(TabletID());
        event->Record.SetChannel(interval.Channel);
        event->Record.SetFromGeneration(interval.From);
        event->Record.SetGroupID(interval.Group);
        // The local-DB journal that records the request before it is sent lands later.
        ctx.Send(LauncherID(), event.release(), IEventHandle::FlagTrackDelivery, i + 1);
    }
}

}   // namespace NKikimr::NColumnShard
