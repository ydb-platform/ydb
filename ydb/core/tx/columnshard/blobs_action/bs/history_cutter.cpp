#include "blob_manager.h"
#include "history_cutter.h"

#include <ydb/core/base/appdata.h>
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
    Enabled = ComputeEnabled();
}

bool THistoryCutterWrapper::ComputeEnabled() {
    if (NYDBTest::TControllers::GetColumnShardController()->IsCSCutHistoryEnabled()) {
        return true;
    }
    return HasAppData() && AppData()->FeatureFlags.GetEnableColumnshardGroupDecommission();
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
    // UpperBound lands one past the owner: begin() means uncovered, end() means the active entry.
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

void THistoryCutterWrapper::PublishSeedLevels() {
    const ui64 state = static_cast<ui64>(SeedingState);
    const ui64 keys = PortionKeys.size();
    const ui64 tombs = SeedTombstones.size();
    Signals.OnSeedLevelsDelta(
        (i64)state - (i64)Published.SeedingStateVal, (i64)keys - (i64)Published.PortionKeysCountVal, (i64)tombs - (i64)Published.TombstonesVal);
    Published.SeedingStateVal = state;
    Published.PortionKeysCountVal = keys;
    Published.TombstonesVal = tombs;
}

void THistoryCutterWrapper::IncrementCounter(const TEntryKey& key) {
    ++Counters[key];
}

void THistoryCutterWrapper::DecrementCounter(const TEntryKey& key) {
    auto it = Counters.find(key);
    // Unreachable while PortionKeys fences OnPortionRemoved; poisoning guards a future divergence of the two maps.
    if (it == Counters.end() || it->second == 0) {
        if (PoisonedChannels.insert(key.Channel).second) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_channel_poisoned")("channel", key.Channel)(
                "from_generation", key.FromGeneration)("reason", "counter_underflow");
            PublishLevels();
            Signals.OnUnderflow();
        }
        return;
    }
    if (--it->second == 0) {
        Counters.erase(it);
    }
}

void THistoryCutterWrapper::OnPortionAdded(const TPortionDataAccessor& accessor) {
    if (!Enabled) {
        return;
    }
    const ui64 portionId = accessor.GetPortionInfo().GetPortionId();
    if (PortionKeys.contains(portionId)) {
        return;
    }
    TStackVec<TEntryKey, 2> keys;
    for (const auto& blobId : accessor.GetBlobIds()) {
        TEntryKey key;
        if (!GetEntryKey(blobId.GetLogoBlobId(), key)) {
            continue;
        }
        bool found = false;
        for (const auto& k : keys) {
            if (k == key) {
                found = true;
                break;
            }
        }
        if (!found) {
            keys.push_back(key);
            IncrementCounter(key);
        }
    }
    if (!keys.empty()) {
        PortionKeys.emplace(portionId, std::move(keys));
        PublishSeedLevels();
    }
}

void THistoryCutterWrapper::OnPortionRemoved(const ui64 portionId) {
    if (!Enabled) {
        return;
    }
    if (SeedingState == ESeedState::Seeding) {
        SeedTombstones.insert(portionId);
    }
    const TStackVec<TEntryKey, 2>* keys = PortionKeys.FindPtr(portionId);
    if (!keys) {
        PublishSeedLevels();
        return;
    }
    for (const auto& key : *keys) {
        DecrementCounter(key);
    }
    PortionKeys.erase(portionId);
    PublishSeedLevels();
}

void THistoryCutterWrapper::BeginSeeding() {
    ++ReseedEpoch;
    ++SeedRun;
    SeedTombstones.clear();
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
    SeedingState = ESeedState::Seeding;
    PublishLevels(0);
    PublishSeedLevels();
    Signals.OnSeedingStarted();
}

void THistoryCutterWrapper::ApplySeedBatch(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds) {
    if (SeedingState != ESeedState::Seeding || !Enabled) {
        return;
    }
    for (const auto& [portionId, blobIds] : portionBlobIds) {
        // Add if absent; a portion erased while seeding stays out of the counters.
        if (PortionKeys.contains(portionId) || SeedTombstones.contains(portionId)) {
            continue;
        }
        TStackVec<TEntryKey, 2> keys;
        for (const auto& blobId : blobIds) {
            TEntryKey key;
            if (!GetEntryKey(blobId.GetLogoBlobId(), key)) {
                continue;
            }
            bool found = false;
            for (const auto& k : keys) {
                if (k == key) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                keys.push_back(key);
                IncrementCounter(key);
            }
        }
        if (!keys.empty()) {
            PortionKeys.emplace(portionId, std::move(keys));
        }
    }
    PublishSeedLevels();
}

void THistoryCutterWrapper::FinishSeeding() {
    if (SeedingState != ESeedState::Seeding) {
        return;
    }
    SeedTombstones.clear();
    SeedingState = ESeedState::Seeded;
    PublishSeedLevels();
    Signals.OnSeedingCompleted();
    NYDBTest::TControllers::GetColumnShardController()->OnCutHistorySeedingCompleted(PortionKeys.size());
}

void THistoryCutterWrapper::FailSeeding(TInternalPathId pathId, ui64 portionId, const TString& reason) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_seeding_failed")("path_id", pathId)("portion_id", portionId)(
        "reason", reason);
    SeedingState = ESeedState::Failed;
    PublishSeedLevels();
    Signals.OnSeedingFailed();
    NYDBTest::TControllers::GetColumnShardController()->OnCutHistorySeedingFailed(reason);
}

void THistoryCutterWrapper::OnBootComplete(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds) {
    BeginSeeding();
    ApplySeedBatch(portionBlobIds);
    FinishSeeding();
}

bool THistoryCutterWrapper::TryNominate(const TActorContext& ctx) {
    if (!Enabled || SeedingState != ESeedState::Seeded) {
        return false;
    }
    if (SweepInFlight) {
        return false;
    }
    // Candidate evaluation scans the GC queues, so rate-limit rather than scan per enqueue.
    if (LastNominateAt && ctx.Now() - LastNominateAt < GetNominateCadence()) {
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

    if (batch.empty()) {
        return false;
    }

    for (const auto& key : batch) {
        CutState[key] = ECutState::Verifying;
    }
    SweepInFlight = true;
    ++SweepRound;
    NominateEpoch = ReseedEpoch;
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
        // History may have changed since nomination: the same-group gate must hold at barrier-send time.
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
            // Exact match, not GroupForGeneration: once cut, that generation resolves to a different live group.
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

        if (SeedingState != ESeedState::Seeded || ReseedEpoch != NominateEpoch) {
            CutState[key] = ECutState::None;
            continue;
        }
        Signals.OnEntryProven();
        if (IsMeasureOnly()) {
            // Nothing durable happens: reset to None so the next round re-measures the same entry.
            CutState[key] = ECutState::None;
            continue;
        }

        // The first GC round of this incarnation carries a soft barrier for every history group, so wait for it.
        if (const auto mgr = Manager.lock(); !mgr || !mgr->HasCollectedBeforeCurrentGeneration()) {
            CutState[key] = ECutState::None;
            continue;
        }

        DisprovedAt.erase(key);
        PublishLevels();
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
    SweepSurvivors.clear();

    // Safety net: the disproved loop settled these already, so reset without counting an attempt.
    for (auto& [key, state] : CutState) {
        if (state == ECutState::Verifying) {
            state = ECutState::None;
        }
    }
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
