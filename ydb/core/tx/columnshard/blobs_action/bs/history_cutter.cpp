#include "blob_manager.h"
#include "history_cutter.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/algorithm.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_BS

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

THistoryCutterWrapper::THistoryCutterWrapper(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo,
    const std::weak_ptr<NOlap::TBlobManager>& manager, const std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedBlobs,
    const TActorId& tabletActorId, const NColumnShard::THistoryCutterCounters& signals)
    : Signals(signals)
    , TabletInfo(tabletInfo)
    , Manager(manager)
    , SharedBlobs(sharedBlobs)
    , TabletActorId(tabletActorId)
{
    Enabled = ComputeEnabled();
}

bool THistoryCutterWrapper::ComputeEnabled() {
    if (NYDBTest::TControllers::GetColumnShardController()->IsCSCutHistoryEnabled()) {
        return true;
    }
    return HasAppData() && AppData()->FeatureFlags.GetEnableColumnshardGroupDecommission();
}

TDuration THistoryCutterWrapper::GetMinTriggeredNominateInterval() {
    return NYDBTest::TControllers::GetColumnShardController()->GetMinTriggeredNominateInterval(MinTriggeredNominateInterval);
}

void THistoryCutterWrapper::RequestNomination(bool triggered) {
    if (NominationPending) {
        // Upgrade to triggered if a more urgent caller arrives while the event is in flight.
        NominationTriggered = NominationTriggered || triggered;
        return;
    }
    NominationPending = true;
    NominationTriggered = triggered;
    if (triggered) {
        Signals.OnTriggeredNomination();
    }
    // Guard against callers that run outside an actor context (unit tests).
    if (!TabletActorId || !NActors::TlsActivationContext) {
        return;
    }
    NActors::TActivationContext::AsActorContext().Send(TabletActorId, new NColumnShard::TEvPrivate::TEvCutHistoryNominate());
}

void THistoryCutterWrapper::OnNominationEvent(const TActorContext& ctx) {
    const bool wasTriggered = NominationTriggered;
    NominationPending = false;
    NominationTriggered = false;
    TryNominate(ctx, wasTriggered);
}

TDuration THistoryCutterWrapper::GetNominateCadence() {
    if (!HasAppData()) {
        return DefaultNominateCadence;
    }
    const ui32 seconds = AppDataVerified().ColumnShardConfig.GetCutHistoryNominateCadenceSeconds();
    return seconds ? TDuration::Seconds(seconds) : DefaultNominateCadence;
}

ui32 THistoryCutterWrapper::GetMaxDrainChecksPerNomination() {
    if (!HasAppData()) {
        return DefaultMaxDrainChecksPerNomination;
    }
    const ui32 checks = AppDataVerified().ColumnShardConfig.GetCutHistoryMaxDrainChecksPerNomination();
    return checks ? checks : DefaultMaxDrainChecksPerNomination;
}

bool THistoryCutterWrapper::IsMeasureOnly() {
    return HasAppData() && AppDataVerified().ColumnShardConfig.GetCutHistoryMeasureOnly();
}

bool THistoryCutterWrapper::IsEnabled() const {
    return Enabled;
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(
    const std::vector<TTabletChannelInfo::THistoryEntry>& hist, const ui32 fromGeneration, const THashSet<ui32>& cutFromGenerations) {
    const auto target = FindIf(hist, [fromGeneration](const TTabletChannelInfo::THistoryEntry& entry) {
        return entry.FromGeneration == fromGeneration;
    });
    if (target == hist.end()) {
        return false;
    }
    return !AnyOf(hist.begin(), target, [&](const TTabletChannelInfo::THistoryEntry& entry) {
        return entry.GroupID == target->GroupID && !cutFromGenerations.contains(entry.FromGeneration);
    });
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return false;
    }
    THashSet<ui32> cutFromGenerations;
    for (const auto& [stateKey, state] : CutState) {
        if (stateKey.Channel == key.Channel && state == ECutState::Cut) {
            cutFromGenerations.insert(stateKey.FromGeneration);
        }
    }
    return SeenGroupsCheckPasses(TabletInfo->Channels[key.Channel].History, key.FromGeneration, cutFromGenerations);
}

ui32 THistoryCutterWrapper::GetNextFromGeneration(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return 0;
    }
    const auto& hist = TabletInfo->Channels[key.Channel].History;
    // History ascends by FromGeneration; end() means the active entry, never a cut candidate.
    const auto next = UpperBound(hist.begin(), hist.end(), key.FromGeneration, TTabletChannelInfo::THistoryEntry::TCmp());
    if (next == hist.begin() || next == hist.end() || (next - 1)->FromGeneration != key.FromGeneration) {
        return 0;
    }
    return next->FromGeneration;
}

bool THistoryCutterWrapper::IsDrained(const TEntryKey& key) const {
    const ui32 nextGen = GetNextFromGeneration(key);
    if (!nextGen) {
        return false;
    }
    const auto manager = Manager.lock();
    if (!manager) {
        return false;
    }
    if (!manager->HasNoBlobsInRange(key.Channel, key.FromGeneration, nextGen)) {
        return false;
    }
    // Shared-out blobs sit in no GC queue; the borrower's GC manages them, so they must drain here first.
    const auto sharedBlobs = SharedBlobs.lock();
    if (!sharedBlobs) {
        return false;
    }
    return !sharedBlobs->HasSharedBlobsInRange(TabletInfo->TabletID, key.Channel, key.FromGeneration, nextGen);
}

bool THistoryCutterWrapper::TryNominate(const TActorContext& ctx, bool triggered) {
    if (!Enabled) {
        return false;
    }
    // Triggered nominations bypass the full cadence but still respect a minimum interval to prevent spin.
    const TDuration rateLimit = triggered ? GetMinTriggeredNominateInterval() : GetNominateCadence();
    if (LastNominateAt && ctx.Now() - LastNominateAt < rateLimit) {
        return false;
    }
    LastNominateAt = ctx.Now();

    // The next round resumes from the first channel this one did not fully service.
    ui32 drainChecks = 0;
    const ui32 channelCount = static_cast<ui32>(TabletInfo->Channels.size());
    if (channelCount <= TGlobal::FirstDataChannel) {
        return false;
    }
    if (NextChannelToCheck < TGlobal::FirstDataChannel || NextChannelToCheck >= channelCount) {
        NextChannelToCheck = TGlobal::FirstDataChannel;
    }
    const ui32 dataChannels = channelCount - TGlobal::FirstDataChannel;
    const ui32 firstChannel = NextChannelToCheck;
    TVector<TEntryKey> batch;
    for (ui32 idx = 0; idx < dataChannels; ++idx) {
        const ui32 ch = TGlobal::FirstDataChannel + (firstChannel - TGlobal::FirstDataChannel + idx) % dataChannels;
        if (drainChecks >= GetMaxDrainChecksPerNomination()) {
            NextChannelToCheck = ch;
            break;
        }
        NextChannelToCheck = TGlobal::FirstDataChannel + (ch - TGlobal::FirstDataChannel + 1) % dataChannels;
        const auto& hist = TabletInfo->Channels[ch].History;
        for (int i = 0; i < static_cast<int>(hist.size()) - 1; ++i) {
            const TEntryKey key{ ch, hist[i].FromGeneration };
            if (const auto* state = CutState.FindPtr(key); state && *state != ECutState::None) {
                continue;
            }
            if (!SeenGroupsCheckPasses(key)) {
                continue;
            }
            if (drainChecks >= GetMaxDrainChecksPerNomination()) {
                break;
            }
            ++drainChecks;
            if (!IsDrained(key)) {
                continue;
            }
            batch.push_back(key);
            NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryNominated(key.Channel, key.FromGeneration);
        }
    }

    const bool nominated = !batch.empty();
    if (nominated) {
        Signals.OnNomination();
        DecideAndCut(batch, ctx);
    }
    return nominated;
}

void THistoryCutterWrapper::DecideAndCut(const TVector<TEntryKey>& candidates, const TActorContext& ctx) {
    // A refused cut is otherwise invisible: on a slice there are no hooks to ask why an entry stayed.
    const auto refuse = [this](const TEntryKey& key, const TStringBuf reason) {
        CutState[key] = ECutState::None;
        YDB_LOG_DEBUG("",
            {"event", "cut_refused"},
            {"channel", key.Channel},
            {"fromGeneration", key.FromGeneration},
            {"reason", reason});
    };
    for (const auto& key : candidates) {
        if (!IsDrained(key)) {
            refuse(key, "not_drained");
            continue;
        }
        // History may have changed since nomination: the same-group gate must hold at send time.
        if (!SeenGroupsCheckPasses(key)) {
            refuse(key, "seen_groups_check");
            continue;
        }
        const ui32 nextFromGen = GetNextFromGeneration(key);
        if (!nextFromGen) {
            refuse(key, "no_next_generation");
            continue;
        }
        std::optional<ui32> groupId;
        if (key.Channel < static_cast<ui32>(TabletInfo->Channels.size())) {
            // Exact match, not GroupForGeneration: once cut, that generation resolves to a different live group.
            if (const auto* entry = FindIfPtr(TabletInfo->Channels[key.Channel].History, [&key](const TTabletChannelInfo::THistoryEntry& e) {
                    return e.FromGeneration == key.FromGeneration;
                })) {
                groupId = entry->GroupID;
            }
        }
        if (!groupId) {
            refuse(key, "entry_not_in_history");
            continue;
        }
        Signals.OnEntryProven();
        if (IsMeasureOnly()) {
            // Nothing durable happens: reset to None so the next round re-measures the same entry.
            CutState[key] = ECutState::None;
            continue;
        }
        const auto mgr = Manager.lock();
        // The first GC round of this incarnation carries a soft barrier for every history group, so wait for it.
        if (!mgr || !mgr->HasCollectedBeforeCurrentGeneration()) {
            refuse(key, "no_gc_round_yet");
            continue;
        }
        // Drained queues alone prove nothing about live portions: only the row MoveData persisted does.
        if (!mgr->HasMoveDataRow(key.Channel, key.FromGeneration, nextFromGen, *groupId)) {
            refuse(key, "no_move_data_row");
            continue;
        }
        CutState[key] = ECutState::Cut;
        NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryCut(key.Channel, key.FromGeneration);
        Signals.OnEntryCut();
        auto req = MakeHolder<TEvTablet::TEvCutTabletHistory>();
        req->Record.SetTabletID(TabletInfo->TabletID);
        req->Record.SetChannel(key.Channel);
        req->Record.SetFromGeneration(key.FromGeneration);
        req->Record.SetGroupID(*groupId);
        ctx.Send(LauncherActorId, req.Release());
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
