#pragma once
#include "address.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/blobs_manager.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NOlap {
class TPortionDataAccessor;
class TBlobManager;

namespace NDataSharing {
class TStorageSharedBlobsManager;
}   // namespace NDataSharing
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

struct TEntryKey {
    ui32 Channel = 0;
    ui32 FromGeneration = 0;

    bool operator==(const TEntryKey& o) const noexcept {
        return Channel == o.Channel && FromGeneration == o.FromGeneration;
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage

template <>
struct THash<NKikimr::NOlap::NBlobOperations::NBlobStorage::TEntryKey> {
    size_t operator()(const NKikimr::NOlap::NBlobOperations::NBlobStorage::TEntryKey& k) const noexcept {
        return CombineHashes<ui32>(k.Channel, k.FromGeneration);
    }
};

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

enum class ECutState {
    None,
    Verifying,
    Cut,
};

enum class ESeedState {
    Unseeded,
    Seeding,
    Seeded,
    Failed,
};

// Two-tier engine for CutTabletHistory on the ColumnShard data channels.
class THistoryCutterWrapper {
public:
    THistoryCutterWrapper(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, ui32 currentGen,
        const std::weak_ptr<NOlap::TBlobManager>& manager, const std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedBlobs,
        const TActorId& tabletActorId, const NColumnShard::THistoryCutterCounters& signals);

    void SetLauncherActorId(const TActorId& id) {
        LauncherActorId = id;
    }

    // Minimum interval between consecutive triggered nominations (bypasses normal cadence).
    static constexpr TDuration MinTriggeredNominateInterval = TDuration::Seconds(1);
    static TDuration GetMinTriggeredNominateInterval();

    // Request a deferred nomination; if one is already pending this upgrades it to triggered if needed.
    void RequestNomination(bool triggered = false);
    // Clear the pending flag and evaluate TryNominate; call only from the TEvCutHistoryNominate handler.
    void OnNominationEvent(const TActorContext& ctx);

    void OnPortionAdded(const TPortionDataAccessor& accessor);

    void OnPortionRemoved(ui64 portionId);

    void OnBootComplete(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds);

    bool TryNominate(const TActorContext& ctx, bool triggered = false);

    std::shared_ptr<const TVector<TEntryKey>> GetSweepCandidates() const {
        static const auto empty = std::make_shared<const TVector<TEntryKey>>();
        return SweepCandidates ? SweepCandidates : empty;
    }

    // Excludes entries disproved by earlier batches, so later ones neither re-examine nor re-disprove them.
    std::shared_ptr<const TVector<TEntryKey>> GetActiveSweepCandidates() const {
        return std::make_shared<const TVector<TEntryKey>>(SweepSurvivors);
    }

    static constexpr TDuration DisprovedRetryCooldown = TDuration::Minutes(5);
    static constexpr TDuration DisprovedRetryMaxCooldown = TDuration::Hours(6);

    static TDuration GetDisprovedCooldown(const ui32 attempts) {
        const ui64 shift = Min<ui32>(attempts, 12);
        const TDuration cooldown = DisprovedRetryCooldown * (1ull << shift);
        return Min(cooldown, DisprovedRetryMaxCooldown);
    }

    // Defaults for TColumnShardConfig.CutHistory*: they bound IsDrained() queue scans per tablet.
    static constexpr TDuration DefaultNominateCadence = TDuration::Minutes(1);
    static constexpr ui32 DefaultMaxDrainChecksPerNomination = 8;

    static TDuration GetNominateCadence();
    static ui32 GetMaxDrainChecksPerNomination();

    // True while the build only measures: the proof runs to the end but stops short of the barrier.
    static bool IsMeasureOnly();

protected:
    void StartSweepForTest(TVector<TEntryKey>&& candidates) {
        SweepInFlight = true;
        NominateEpoch = ReseedEpoch;
        SweepSurvivors = candidates;
        for (const auto& key : SweepSurvivors) {
            CutState[key] = ECutState::Verifying;
        }
        SweepCandidates = std::make_shared<const TVector<TEntryKey>>(std::move(candidates));
    }

    ECutState GetCutStateForTest(const TEntryKey& key) const {
        const auto* state = CutState.FindPtr(key);
        return state ? *state : ECutState::None;
    }

    ui32 GetDisprovalAttemptsForTest(const TEntryKey& key) const {
        const auto* state = DisprovedAt.FindPtr(key);
        return state ? state->Attempts : 0;
    }

    bool IsChannelPoisonedForTest(const ui32 channel) const {
        return PoisonedChannels.contains(channel);
    }

    ui64 GetCounterForTest(const TEntryKey& key) const {
        const auto* count = Counters.FindPtr(key);
        return count ? *count : 0;
    }

    size_t GetPortionKeysCountForTest() const {
        return PortionKeys.size();
    }

    ESeedState GetSeedingStateForTest() const {
        return SeedingState;
    }

    ui64 GetReseedEpochForTest() const {
        return ReseedEpoch;
    }

    size_t GetTombstoneCountForTest() const {
        return SeedTombstones.size();
    }

    // protected for tests: no public call sequence reaches the underflow branch.
    void DecrementCounter(const TEntryKey& key);

    bool IsNominationPendingForTest() const {
        return NominationPending;
    }

    bool IsNominationTriggeredForTest() const {
        return NominationTriggered;
    }

public:
    void BeginSeeding();
    void ApplySeedBatch(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds);
    void FinishSeeding();
    void FailSeeding(TInternalPathId pathId, ui64 portionId, const TString& reason);

    void SetPortionSnapshot(TVector<std::pair<TInternalPathId, ui64>>&& ids);

    TVector<std::pair<TInternalPathId, ui64>> GetNextBatch(size_t batchSize, bool& isLast);

    void OnBatchComplete(const THashSet<TEntryKey>& disproved, bool exhausted, const TActorContext& ctx);

    ui64 GetSweepRound() const {
        return SweepRound;
    }

    ui64 GetSeedRun() const {
        return SeedRun;
    }

    const NColumnShard::THistoryCutterCounters& GetSignals() const {
        return Signals;
    }

    bool IsEnabled() const;

    bool IsSweepInFlight() const {
        return SweepInFlight;
    }

    bool HasPortionSnapshot() const {
        // Non-empty covers the not-yet-started case; a positive offset covers the fully handed-out tail.
        return SweepPortionOffset > 0 || !SweepPortionIds.empty();
    }

    // True when no earlier entry shares the target's GroupID; already-cut entries are transparent.
    static bool SeenGroupsCheckPasses(
        const std::vector<TTabletChannelInfo::THistoryEntry>& hist, ui32 fromGeneration, const THashSet<ui32>& cutFromGenerations = {});

    ui32 GetNextFromGeneration(const TEntryKey& key) const;

protected:
    bool IsDrained(const TEntryKey& key) const;

private:
    bool SeenGroupsCheckPasses(const TEntryKey& key) const;

    bool GetEntryKey(const TLogoBlobID& blobId, TEntryKey& out) const;

    void IncrementCounter(const TEntryKey& key);

    // Call after any change to PoisonedChannels or DisprovedAt; omitted sweepCandidates leaves it unchanged.
    void PublishLevels(std::optional<ui64> sweepCandidates = {});
    // Call after any change to SeedingState, PortionKeys, or SeedTombstones.
    void PublishSeedLevels();

    static bool ComputeEnabled();

    struct TPublishedLevels {
        ui64 SweepCandidates = 0;
        ui64 ChannelsPoisoned = 0;
        ui64 EntriesDisproved = 0;
        ui64 SeedingStateVal = 0;
        ui64 PortionKeysCountVal = 0;
        ui64 TombstonesVal = 0;
    };

    TPublishedLevels Published;

    const NColumnShard::THistoryCutterCounters Signals;

    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    ui32 CurrentGen;
    std::weak_ptr<NOlap::TBlobManager> Manager;
    // Shared-out blobs are in no GC queue; the drain gate consults this registry.
    std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> SharedBlobs;
    TActorId TabletActorId;
    TActorId LauncherActorId;

    // Computed once in the constructor; guards all tier-1 bookkeeping and sweep gating.
    bool Enabled = false;

    // Seeding state machine: Unseeded → Seeding → Seeded (or Failed).
    ESeedState SeedingState = ESeedState::Unseeded;
    ui64 ReseedEpoch = 0;
    ui64 SeedRun = 0;
    THashSet<ui64> SeedTombstones;
    // ReseedEpoch captured at nomination time; cut is aborted if it changed by OnBatchComplete.
    ui64 NominateEpoch = 0;

    // Tier-1 state (all ephemeral — reconstructed on restart).
    THashMap<TEntryKey, ui64> Counters;
    THashMap<TEntryKey, ECutState> CutState;
    THashSet<ui32> PoisonedChannels;

    THashMap<ui64, TStackVec<TEntryKey, 2>> PortionKeys;

    bool SweepInFlight = false;

    // Shared with per-batch sweep callbacks — one allocation per sweep, not per batch.
    std::shared_ptr<const TVector<TEntryKey>> SweepCandidates;

    // Sweep-disproved entries with exponential backoff, converging to one sweep per DisprovedRetryMaxCooldown.
    struct TDisprovalState {
        TInstant At;
        ui32 Attempts = 0;
    };

    THashMap<TEntryKey, TDisprovalState> DisprovedAt;

    // Deferred nomination: NominationPending prevents duplicate in-flight events.
    bool NominationPending = false;
    bool NominationTriggered = false;

    TInstant LastNominateAt;
    ui32 NextChannelToCheck = TGlobal::FirstDataChannel;
    TVector<TEntryKey> SweepSurvivors;

    TVector<std::pair<TInternalPathId, ui64>> SweepPortionIds;
    size_t SweepPortionOffset = 0;

    ui64 SweepRound = 0;
};

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
