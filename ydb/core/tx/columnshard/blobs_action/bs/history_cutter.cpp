#include "blob_manager.h"
#include "history_cutter.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
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
        AFL_VERIFY(NextFromGen > 0);   // NextFromGen - 1 below would underflow to collect-everything
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
            auto req = MakeHolder<TEvTablet::TEvCutTabletHistory>();
            req->Record.SetTabletID(TabletId);
            req->Record.SetChannel(Channel);
            req->Record.SetFromGeneration(FromGen);
            req->Record.SetGroupID(Group);
            ctx.Send(LauncherActorId, req.Release());
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
    const std::weak_ptr<NOlap::TBlobManager>& manager, const std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedBlobs,
    const TActorId& tabletActorId, const NColumnShard::THistoryCutterCounters& signals)
    : Signals(signals)
    , TabletInfo(tabletInfo)
    , CurrentGen(currentGen)
    , Manager(manager)
    , SharedBlobs(sharedBlobs)
    , TabletActorId(tabletActorId)
{
}

bool THistoryCutterWrapper::IsEnabled() const {
    if (NYDBTest::TControllers::GetColumnShardController()->IsCSCutHistoryEnabled()) {
        return true;
    }
    return HasAppData() && AppData()->FeatureFlags.GetEnableCutHistory();
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
    // History is ascending by FromGeneration, so the successor is one past the entry
    // itself; TCmp is the comparator TTabletChannelInfo::GroupForGeneration uses for
    // the same search. `next == end()` means the entry is the active one, which has no
    // successor and is therefore never a cut candidate.
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
    // Our blobs shared out to other tablets sit in no GC queue while shared; a hard
    // barrier would collect them under the borrower, so they pin the entry too.
    const auto sharedBlobs = SharedBlobs.lock();
    if (!sharedBlobs) {
        return false;
    }
    return !sharedBlobs->HasSharedBlobsInRange(TabletInfo->TabletID, key.Channel, key.FromGeneration, nextGen);
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
    // UpperBound lands one past the owning entry: begin() means no entry covers the
    // generation, end() means the owner is the active entry, never a cut candidate.
    const auto entry = UpperBound(hist.begin(), hist.end(), blobId.Generation(), TTabletChannelInfo::THistoryEntry::TCmp());
    if (entry == hist.begin() || entry == hist.end()) {
        return false;
    }
    out = TEntryKey{ ch, (entry - 1)->FromGeneration };
    return true;
}

void THistoryCutterWrapper::PublishLevels(const std::optional<ui64> sweepCandidates) {
    const ui64 candidates = sweepCandidates.value_or(Published.SweepCandidates);
    const ui64 poisoned = PoisonedChannels.size();
    const ui64 disproved = DisprovedAt.size();
    Signals.OnLevelsDelta((i64)candidates - (i64)Published.SweepCandidates, (i64)poisoned - (i64)Published.ChannelsPoisoned,
        (i64)disproved - (i64)Published.EntriesDisproved);
    Published.SweepCandidates = candidates;
    Published.ChannelsPoisoned = poisoned;
    Published.EntriesDisproved = disproved;
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
            PublishLevels();
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
    NextChannelToCheck = TGlobal::FirstDataChannel;
    SweepInFlight = false;
    SweepCandidates.reset();
    SweepSurvivors.clear();
    SweepPortionIds.clear();
    SweepPortionOffset = 0;
    PublishLevels(0);

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
    // Evaluating candidates scans the GC queues, and background enqueues fire every few
    // seconds: rate-limit rather than scan per enqueue.
    if (LastNominateAt && ctx.Now() - LastNominateAt < NominateCadence) {
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
        if (drainChecks >= MaxDrainChecksPerNomination) {
            NextChannelToCheck = ch;
            break;
        }
        NextChannelToCheck = TGlobal::FirstDataChannel + (ch - TGlobal::FirstDataChannel + 1) % dataChannels;
        if (PoisonedChannels.contains(ch)) {
            continue;
        }
        const auto& hist = TabletInfo->Channels[ch].History;
        for (int i = 0; i < static_cast<int>(hist.size()) - 1; ++i) {
            const TEntryKey key{ ch, hist[i].FromGeneration };
            if (const auto* state = CutState.FindPtr(key); state && *state != ECutState::None) {
                continue;
            }
            if (const auto* count = Counters.FindPtr(key); count && *count != 0) {
                continue;
            }
            if (const auto* disproval = DisprovedAt.FindPtr(key);
                disproval && ctx.Now() - disproval->At < GetDisprovedCooldown(disproval->Attempts)) {
                continue;
            }
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
    Signals.OnNomination();
    PublishLevels(batch.size());
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
        auto& state = DisprovedAt[key];
        state.At = ctx.Now();
        ++state.Attempts;
        CutState[key] = ECutState::None;
    }
    if (!disproved.empty()) {
        PublishLevels();
        EraseIf(SweepSurvivors, [&](const TEntryKey& key) {
            return disproved.contains(key);
        });
    }

    if (!exhausted) {
        ctx.Send(TabletActorId, new NColumnShard::TEvPrivate::TEvStartCutHistorySweep());
        return;
    }

    SweepInFlight = false;
    Signals.OnSweepCompleted();
    PublishLevels(0);
    SweepCandidates.reset();
    SweepPortionIds.clear();
    SweepPortionOffset = 0;

    for (const auto& key : SweepSurvivors) {
        if (const auto* count = Counters.FindPtr(key); count && *count != 0) {
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

        std::optional<ui32> groupId;
        if (key.Channel < static_cast<ui32>(TabletInfo->Channels.size())) {
            // Exact match rather than GroupForGeneration: once the entry has been cut
            // away, the entry that covers its generation belongs to a different, live
            // group, and barriering that one would collect blobs still in use.
            if (const auto* entry =
                    FindIfPtr(TabletInfo->Channels[key.Channel].History, [&key](const TTabletChannelInfo::THistoryEntry& historyEntry) {
                        return historyEntry.FromGeneration == key.FromGeneration;
                    })) {
                groupId = entry->GroupID;
            }
        }
        if (!groupId) {
            CutState[key] = ECutState::None;
            continue;
        }

        DisprovedAt.erase(key);
        PublishLevels();
        CutState[key] = ECutState::SentBarrier;
        ctx.Register(new TCutHistoryBarrierActor(
            TabletActorId, LauncherActorId, TabletInfo->TabletID, CurrentGen, key.Channel, *groupId, key.FromGeneration, nextFromGen));
    }
    SweepSurvivors.clear();

    // Safety net: the disproved loop settled those already, so anything still Verifying
    // is unexpected — reset it without counting an attempt.
    for (auto& [key, state] : CutState) {
        if (state == ECutState::Verifying) {
            state = ECutState::None;
        }
    }
}

void THistoryCutterWrapper::OnBarrierResult(const TEntryKey& key, bool ok) {
    auto* state = CutState.FindPtr(key);
    if (!state) {
        return;
    }
    Signals.OnBarrierResult(ok);
    if (ok) {
        *state = ECutState::Cut;
        NYDBTest::TControllers::GetColumnShardController()->OnHistoryEntryCut(key.Channel, key.FromGeneration);
    } else {
        *state = ECutState::None;
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
