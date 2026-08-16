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

#include <util/generic/algorithm.h>

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
        // NextFromGen - 1 is the hard collect generation; 0 would underflow to a
        // collect-everything barrier. Callers must resolve a real next generation.
        AFL_VERIFY(NextFromGen > 0);
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWait);
        SendBarrier(ctx);
    }

    void HandleWakeup(const TActorContext& ctx) {
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
        // Linear backoff: an immediate retry against an overloaded group would only add load.
        ctx.Schedule(TDuration::Seconds(1) * Retries, new NActors::TEvents::TEvWakeup());
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeup);
        }
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

THistoryCutterWrapper::THistoryCutterWrapper(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, const ui32 currentGen,
    const std::weak_ptr<NOlap::TBlobManager>& manager, const TActorId& tabletActorId)
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

bool THistoryCutterWrapper::SeenGroupsCheckPasses(
    const std::vector<TTabletChannelInfo::THistoryEntry>& hist, const ui32 fromGeneration, const std::unordered_set<ui32>& cutFromGenerations) {
    ui32 targetGroup = 0;
    bool found = false;
    std::unordered_set<ui32> seenGroups;
    for (const auto& entry : hist) {
        if (entry.FromGeneration == fromGeneration) {
            targetGroup = entry.GroupID;
            found = true;
            break;
        }
        if (cutFromGenerations.contains(entry.FromGeneration)) {
            continue;
        }
        seenGroups.insert(entry.GroupID);
    }
    return found && !seenGroups.contains(targetGroup);
}

bool THistoryCutterWrapper::SeenGroupsCheckPasses(const TEntryKey& key) const {
    if (key.Channel >= static_cast<ui32>(TabletInfo->Channels.size())) {
        return false;
    }
    std::unordered_set<ui32> cutFromGenerations;
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
    const auto manager = Manager.lock();
    if (!manager) {
        return false;
    }
    return manager->HasNoBlobsInRange(key.Channel, key.FromGeneration, nextGen);
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
        if (PoisonedChannels.insert(key.Channel).second) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_channel_poisoned")("channel", key.Channel)(
                "from_generation", key.FromGeneration)("reason", "counter_underflow");
        }
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
    DisprovedAt.clear();
    LastNominateAt = TInstant::Zero();
    NextChannelToCheck = 2;
    SweepInFlight = false;
    SweepCandidates.reset();
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
    // Full candidate evaluation scans the GC queues (IsDrained); background activity
    // enqueues run every few seconds, so cap the evaluation cadence for this rare
    // maintenance operation instead of scanning per enqueue.
    if (LastNominateAt && ctx.Now() - LastNominateAt < NominateCadence) {
        return false;
    }
    LastNominateAt = ctx.Now();

    // Channel rotation + hard cap: at most MaxDrainChecksPerNomination queue scans per
    // round; the next round resumes from the first channel that was not fully serviced.
    ui32 drainChecks = 0;
    const ui32 channelCount = static_cast<ui32>(TabletInfo->Channels.size());
    if (channelCount <= 2) {
        return false;
    }
    if (NextChannelToCheck < 2 || NextChannelToCheck >= channelCount) {
        NextChannelToCheck = 2;
    }
    const ui32 firstChannel = NextChannelToCheck;
    TVector<TEntryKey> batch;
    for (ui32 idx = 0; idx < channelCount - 2; ++idx) {
        const ui32 ch = 2 + (firstChannel - 2 + idx) % (channelCount - 2);
        if (drainChecks >= MaxDrainChecksPerNomination) {
            NextChannelToCheck = ch;
            break;
        }
        NextChannelToCheck = 2 + (ch - 2 + 1) % (channelCount - 2);
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
            const auto disprovedIt = DisprovedAt.find(key);
            if (disprovedIt != DisprovedAt.end() && ctx.Now() - disprovedIt->second < DisprovedRetryCooldown) {
                continue;
            }
            // Cheap history walk first; the queue scans in IsDrained run only for
            // entries that already passed every in-memory gate.
            if (!SeenGroupsCheckPasses(key)) {
                continue;
            }
            if (drainChecks >= MaxDrainChecksPerNomination) {
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

    if (batch.empty()) {
        return false;
    }

    for (const auto& key : batch) {
        CutState[key] = ECutState::Verifying;
    }
    SweepInFlight = true;
    SweepSurvivors = batch;
    SweepCandidates = std::make_shared<const TVector<TEntryKey>>(std::move(batch));
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
    for (const auto& key : disproved) {
        DisprovedAt[key] = ctx.Now();
    }
    // Remove disproved entries from in-progress survivors list.
    if (!disproved.empty()) {
        EraseIf(SweepSurvivors, [&](const TEntryKey& key) {
            return disproved.contains(key);
        });
    }

    if (!exhausted) {
        // More portion batches to check — schedule next batch.
        ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
        return;
    }

    // Cursor exhausted: re-check each survivor and send hard barrier if still safe.
    SweepInFlight = false;
    SweepCandidates.reset();
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
        // The history may have changed between nomination and this point — the
        // same-group safety gate must hold at barrier-send time, not only at nomination.
        if (!SeenGroupsCheckPasses(key)) {
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
            for (const auto& entry : TabletInfo->Channels[key.Channel].History) {
                if (entry.FromGeneration == key.FromGeneration) {
                    groupId = entry.GroupID;
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
            DisprovedAt[key] = ctx.Now();
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
