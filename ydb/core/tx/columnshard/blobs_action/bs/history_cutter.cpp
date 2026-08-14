#include "blob_manager.h"
#include "history_cutter.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_BS

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

namespace {

class TCutHistoryBarrierActor: public TActorBootstrapped<TCutHistoryBarrierActor> {
public:
    TCutHistoryBarrierActor(const TActorId& tabletActorId, const TActorId& launcherActorId, ui64 tabletId, ui32 currentGen, ui32 channel,
        ui32 group, ui32 fromGen, ui32 nextFromGen)
        : TabletActorId(tabletActorId)
        , LauncherActorId(launcherActorId)
        , TabletId(tabletId)
        , CurrentGen(currentGen)
        , Channel(channel)
        , Group(group)
        , FromGen(fromGen)
        , NextFromGen(nextFromGen)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWait);
        SendBarrier(ctx);
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx) {
        const auto status = ev->Get()->Status;
        if (status == NKikimrProto::OK || status == NKikimrProto::ALREADY) {
            // ALREADY means the barrier is already at or beyond the requested level — safe to cut.
            // Send cut request to Hive.
            auto req = MakeHolder<TEvTablet::TEvCutTabletHistory>();
            req->Record.SetTabletID(TabletId);
            req->Record.SetChannel(Channel);
            req->Record.SetFromGeneration(FromGen);
            req->Record.SetGroupID(Group);
            ctx.Send(LauncherActorId, req.Release());
            // Notify tablet.
            ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone(Channel, FromGen, true));
            Die(ctx);
            return;
        }
        if (status == NKikimrProto::BLOCKED || ++Retries >= MaxRetries) {
            ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvCutHistoryBarrierDone(Channel, FromGen, false));
            Die(ctx);
            return;
        }
        SendBarrier(ctx);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) { HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle); }
    }

private:
    static constexpr int MaxRetries = 3;

    void SendBarrier(const TActorContext& ctx) {
        auto ev = MakeHolder<TEvBlobStorage::TEvCollectGarbage>(TabletId, CurrentGen, 0, Channel, /*collect=*/true,
            /*collectGeneration=*/NextFromGen - 1, /*collectStep=*/Max<ui32>(), /*keep=*/nullptr, /*doNotKeep=*/nullptr, TInstant::Max(),
            /*issueKeepFlag=*/false, TWriteSource::ColumnShardGC, /*hard=*/true);
        ev->PerGenerationCounter = TBlobManager::AllocateGCPerGenerationCounter(ev->PerGenerationCounterStepSize());
        SendToBSProxy(ctx, Group, ev.Release());
    }

    TActorId TabletActorId;
    TActorId LauncherActorId;
    ui64 TabletId = 0;
    ui32 CurrentGen = 0;
    ui32 Channel = 0;
    ui32 Group = 0;
    ui32 FromGen = 0;
    ui32 NextFromGen = 0;
    int Retries = 0;
};

}   // anonymous namespace

THistoryCutterWrapper::THistoryCutterWrapper(
    const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, const ui32 currentGen, NOlap::TBlobManager* manager, const TActorId& tabletActorId)
    : TabletInfo(tabletInfo)
    , CurrentGen(currentGen)
    , Manager(manager)
    , TabletActorId(tabletActorId)
{
}

bool THistoryCutterWrapper::IsEnabled() const {
    // Test hook takes priority over feature flags (allows enabling without AppData).
    if (NYDBTest::TControllers::GetColumnShardController()->IsCSCutHistoryEnabled()) {
        return true;
    }
    return HasAppData() && AppData()->FeatureFlags.GetEnableCutHistory() && AppData()->ColumnShardConfig.GetCutHistoryEnabled();
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(const std::vector<TTabletChannelInfo::THistoryEntry>& hist, const ui32 fromGeneration) {
    ui32 targetGroup = 0;
    bool found = false;
    std::unordered_set<ui32> seenGroups;
    for (const auto& e : hist) {
        if (e.FromGeneration == fromGeneration) {
            targetGroup = e.GroupID;
            found = true;
            break;
        }
        seenGroups.insert(e.GroupID);
    }
    return found && !seenGroups.contains(targetGroup);
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return false;
    }
    return SeenGroupsCheckPasses(TabletInfo->Channels[key.Channel].History, key.FromGeneration);
}

ui32 THistoryCutterWrapper::GetNextFromGeneration(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return 0;
    }
    const auto& hist = TabletInfo->Channels[key.Channel].History;
    for (int i = 0; i < static_cast<int>(hist.size()) - 1; ++i) {
        if (hist[i].FromGeneration == key.FromGeneration) {
            return hist[i + 1].FromGeneration;
        }
    }
    return 0;
}

bool THistoryCutterWrapper::IsDrained(const TEntryKey& key) const {
    const ui32 nextGen = GetNextFromGeneration(key);
    if (!nextGen) {
        return false;
    }
    return Manager->HasNoBlobsInRange(key.Channel, key.FromGeneration, nextGen);
}

bool THistoryCutterWrapper::GetEntryKey(const TLogoBlobID& blobId, TEntryKey& out) const {
    if (blobId.TabletID() != TabletInfo->TabletID) {
        return false;
    }
    const ui32 ch = blobId.Channel();
    if (ch < 2 || ch >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return false;
    }
    if (blobId.Generation() == CurrentGen) {
        return false;
    }
    const auto& hist = TabletInfo->Channels[ch].History;
    for (int i = static_cast<int>(hist.size()) - 2; i >= 0; --i) {
        if (hist[i].FromGeneration <= blobId.Generation()) {
            // Check it falls in [hist[i].FromGen, hist[i+1].FromGen).
            if (blobId.Generation() < hist[i + 1].FromGeneration) {
                out = TEntryKey{ ch, hist[i].FromGeneration };
                return true;
            }
            break;
        }
    }
    return false;
}

void THistoryCutterWrapper::IncrementCounter(const TEntryKey& key) {
    ++Counters[key];
}

void THistoryCutterWrapper::DecrementCounter(const TEntryKey& key) {
    auto it = Counters.find(key);
    if (it == Counters.end() || it->second == 0) {
        PoisonedChannels.insert(key.Channel);
        return;
    }
    if (--it->second == 0) {
        Counters.erase(it);
    }
}

void THistoryCutterWrapper::OnPortionAdded(const TPortionDataAccessor& accessor) {
    if (!IsEnabled()) {
        return;
    }
    const ui64 portionId = accessor.GetPortionInfo().GetPortionId();
    THashSet<TEntryKey>& portionKeySet = PortionKeys[portionId];
    for (const auto& blobId : accessor.GetBlobIds()) {
        TEntryKey key;
        if (!GetEntryKey(blobId.GetLogoBlobId(), key)) {
            continue;
        }
        if (portionKeySet.insert(key).second) {
            // First time this portion maps to this entry.
            IncrementCounter(key);
        }
    }
}

void THistoryCutterWrapper::OnPortionRemoved(const ui64 portionId) {
    if (!IsEnabled()) {
        return;
    }
    const THashSet<TEntryKey>* keys = PortionKeys.FindPtr(portionId);
    if (!keys) {
        return;
    }
    for (const auto& key : *keys) {
        DecrementCounter(key);
    }
    PortionKeys.erase(portionId);
}

void THistoryCutterWrapper::OnBootComplete(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds) {
    Counters.clear();
    CutState.clear();
    PoisonedChannels.clear();
    PortionKeys.clear();
    SweepInFlight = false;
    SweepCandidates.clear();
    SweepSurvivors.clear();
    SweepPortionIds.clear();
    SweepPortionOffset = 0;

    if (!IsEnabled()) {
        return;
    }
    for (const auto& [portionId, blobIds] : portionBlobIds) {
        THashSet<TEntryKey>& portionKeySet = PortionKeys[portionId];
        for (const auto& blobId : blobIds) {
            TEntryKey key;
            if (!GetEntryKey(blobId.GetLogoBlobId(), key)) {
                continue;
            }
            if (portionKeySet.insert(key).second) {
                IncrementCounter(key);
            }
        }
    }
}

bool THistoryCutterWrapper::TryNominate(const TActorContext& ctx) {
    if (!IsEnabled()) {
        return false;
    }
    if (SweepInFlight) {
        return false;
    }

    TVector<TEntryKey> batch;
    for (ui32 ch = 2; ch < static_cast<ui32>(TabletInfo->Channels.size()); ++ch) {
        if (PoisonedChannels.contains(ch)) {
            continue;
        }
        const auto& hist = TabletInfo->Channels[ch].History;
        // All entries except the last (active) are candidates.
        for (int i = 0; i < static_cast<int>(hist.size()) - 1; ++i) {
            const TEntryKey key{ ch, hist[i].FromGeneration };
            const auto stateIt = CutState.find(key);
            if (stateIt != CutState.end() && stateIt->second != ECutState::None) {
                continue;
            }
            const auto cntIt = Counters.find(key);
            if (cntIt != Counters.end() && cntIt->second != 0) {
                continue;
            }
            if (!IsDrained(key)) {
                continue;
            }
            if (!SeenGroupsCheckPasses(key)) {
                continue;
            }
            batch.push_back(key);
            NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryNominated(key.Channel, key.FromGeneration);
        }
    }

    if (batch.empty()) {
        return false;
    }

    for (const auto& key : batch) {
        CutState[key] = ECutState::Verifying;
    }
    SweepInFlight = true;
    SweepCandidates = batch;
    SweepSurvivors = std::move(batch);
    SweepPortionIds.clear();
    SweepPortionOffset = 0;

    ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
    return true;
}

void THistoryCutterWrapper::SetPortionSnapshot(TVector<std::pair<TInternalPathId, ui64>>&& ids) {
    SweepPortionIds = std::move(ids);
    SweepPortionOffset = 0;
}

TVector<std::pair<TInternalPathId, ui64>> THistoryCutterWrapper::GetNextBatch(size_t batchSize, bool& isLast) {
    TVector<std::pair<TInternalPathId, ui64>> batch;
    const size_t remaining = SweepPortionIds.size() - SweepPortionOffset;
    const size_t take = (remaining > batchSize) ? batchSize : remaining;
    for (size_t i = 0; i < take; ++i) {
        batch.push_back(SweepPortionIds[SweepPortionOffset + i]);
    }
    SweepPortionOffset += take;
    isLast = (SweepPortionOffset >= SweepPortionIds.size());
    return batch;
}

void THistoryCutterWrapper::OnBatchComplete(const THashSet<TEntryKey>& disproved, bool exhausted, const TActorContext& ctx) {
    // Remove disproved entries from in-progress survivors list.
    if (!disproved.empty()) {
        TVector<TEntryKey> kept;
        kept.reserve(SweepSurvivors.size());
        for (const auto& key : SweepSurvivors) {
            if (!disproved.contains(key)) {
                kept.push_back(key);
            }
        }
        SweepSurvivors = std::move(kept);
    }

    if (!exhausted) {
        // More portion batches to check — schedule next batch.
        ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
        return;
    }

    // Cursor exhausted: re-check each survivor and send hard barrier if still safe.
    SweepInFlight = false;
    SweepCandidates.clear();
    SweepPortionIds.clear();
    SweepPortionOffset = 0;

    for (const auto& key : SweepSurvivors) {
        // Re-check: counter must still be zero and no blobs in flight.
        const auto cntIt = Counters.find(key);
        if (cntIt != Counters.end() && cntIt->second != 0) {
            CutState[key] = ECutState::None;
            continue;
        }
        if (!IsDrained(key)) {
            CutState[key] = ECutState::None;
            continue;
        }

        const ui32 nextFromGen = GetNextFromGeneration(key);
        if (!nextFromGen) {
            CutState[key] = ECutState::None;
            continue;
        }

        ui32 groupId = 0;
        if (key.Channel < static_cast<ui32>(TabletInfo->Channels.size())) {
            for (const auto& e : TabletInfo->Channels[key.Channel].History) {
                if (e.FromGeneration == key.FromGeneration) {
                    groupId = e.GroupID;
                    break;
                }
            }
        }
        if (!groupId) {
            CutState[key] = ECutState::None;
            continue;
        }

        CutState[key] = ECutState::SentBarrier;
        ctx.Register(new TCutHistoryBarrierActor(
            TabletActorId, LauncherActorId, TabletInfo->TabletID, CurrentGen, key.Channel, groupId, key.FromGeneration, nextFromGen));
    }
    SweepSurvivors.clear();

    // Reset any remaining Verifying entries (disproved during scan).
    for (auto& [key, state] : CutState) {
        if (state == ECutState::Verifying) {
            state = ECutState::None;
        }
    }
}

void THistoryCutterWrapper::OnBarrierResult(const TEntryKey& key, bool ok) {
    auto it = CutState.find(key);
    if (it == CutState.end()) {
        return;
    }
    if (ok) {
        it->second = ECutState::Cut;
        NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryCut(key.Channel, key.FromGeneration);
    } else {
        it->second = ECutState::None;
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
