#pragma once
#include "address.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <optional>
#include <unordered_set>

namespace NKikimr::NOlap {
class TPortionDataAccessor;
class TBlobManager;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

struct TEntryKey {
    ui32 Channel;
    ui32 FromGeneration;

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
    SentBarrier,
    Cut,
};

// Two-tier engine for CutTabletHistory on ColumnShard data channels (channels >= 2).
// Owned by TBlobManager; accessed from TColumnShard for nomination and sweep callbacks.
class THistoryCutterWrapper {
public:
    THistoryCutterWrapper(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, ui32 currentGen,
        const std::weak_ptr<NOlap::TBlobManager>& manager, const TActorId& tabletActorId);

    void SetLauncherActorId(const TActorId& id) {
        LauncherActorId = id;
    }

    // Called on write-index-complete path when a portion is added to the engine.
    void OnPortionAdded(const TPortionDataAccessor& accessor);

    // Called on cleanup-complete path when a portion is durably erased from the engine.
    void OnPortionRemoved(ui64 portionId);

    // One-shot rebuild after all granules finish loading at boot.
    // portionBlobIds: portionId -> blob ids for that portion.
    void OnBootComplete(const THashMap<ui64, std::vector<TUnifiedBlobId>>& portionBlobIds);

    // Nominates candidates for cutting. Starts tier-2 cursor scan by sending
    // TEvStartCutHistorySweep to the tablet actor and returning true.
    // Returns false when sweep already in flight or no candidates.
    bool TryNominate(const TActorContext& ctx);

    // Returns current sweep candidates (non-empty while sweep in flight).
    std::shared_ptr<const TVector<TEntryKey>> GetSweepCandidates() const {
        return SweepCandidates ? SweepCandidates : std::make_shared<const TVector<TEntryKey>>();
    }

    static constexpr TDuration DisprovedRetryCooldown = TDuration::Minutes(5);

protected:
    // Enters the sweeping state directly (unit tests subclass to reach this; TryNominate
    // needs a live actor context to send the sweep event, which unit tests do not have).
    void StartSweepForTest(TVector<TEntryKey>&& candidates) {
        SweepInFlight = true;
        SweepSurvivors = candidates;
        for (const auto& key : SweepSurvivors) {
            CutState[key] = ECutState::Verifying;
        }
        SweepCandidates = std::make_shared<const TVector<TEntryKey>>(std::move(candidates));
    }

    ECutState GetCutStateForTest(const TEntryKey& key) const {
        const auto it = CutState.find(key);
        return it == CutState.end() ? ECutState::None : it->second;
    }

public:
    // Sets the snapshot of all engine portion IDs that tier-2 will scan.
    // Called by TColumnShard::Handle(TEvStartCutHistorySweep) before the first accessor request.
    void SetPortionSnapshot(TVector<std::pair<TInternalPathId, ui64>>&& ids);

    // Returns the next batch of at most batchSize portion IDs, advancing the internal offset.
    // isLast is set to true if this is the last batch (offset reaches end).
    TVector<std::pair<TInternalPathId, ui64>> GetNextBatch(size_t batchSize, bool& isLast);

    // Called by Handle(TEvCutHistorySweepBatchDone) after each accessor batch.
    // disproved: {channel, fromGeneration} pairs whose blobs were found in this batch.
    // exhausted: true when the portion snapshot is fully consumed.
    void OnBatchComplete(const THashSet<TEntryKey>& disproved, bool exhausted, const TActorContext& ctx);

    // Called by barrier actor result handler in TColumnShard.
    void OnBarrierResult(const TEntryKey& key, bool ok);

    bool IsEnabled() const;

    bool IsSweepInFlight() const {
        return SweepInFlight;
    }

    bool HasPortionSnapshot() const {
        return SweepPortionOffset > 0 || !SweepPortionIds.empty();
    }

    // Public accessor used by Handle(TEvStartCutHistorySweep) to build nextGenMap for the callback.
    ui32 GetNextFromGenerationForSweep(const TEntryKey& key) const {
        return GetNextFromGeneration(key);
    }

    // Pure function: returns true if no earlier entry in `hist` (all entries before the one
    // with fromGeneration == key.FromGeneration) uses the same GroupID as that entry.
    // Testable without an actor context or THistoryCutterWrapper instance.
    // Entries whose FromGeneration is in cutFromGenerations are already barriered and
    // cut — they are transparent for the same-group safety walk (otherwise a cut entry
    // still visible in the boot-time TTabletStorageInfo would block a later same-group
    // entry until the next restart).
    static bool SeenGroupsCheckPasses(const std::vector<TTabletChannelInfo::THistoryEntry>& hist, ui32 fromGeneration,
        const std::unordered_set<ui32>& cutFromGenerations = {});

private:
    bool SeenGroupsCheckPasses(const TEntryKey& key) const;
    bool IsDrained(const TEntryKey& key) const;
    ui32 GetNextFromGeneration(const TEntryKey& key) const;

    // Computes the entry key (channel, fromGen) for a blob id.
    // Returns false if the blob belongs to the active entry or is foreign.
    bool GetEntryKey(const TLogoBlobID& blobId, TEntryKey& out) const;

    void IncrementCounter(const TEntryKey& key);
    void DecrementCounter(const TEntryKey& key);

    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    ui32 CurrentGen;
    std::weak_ptr<NOlap::TBlobManager> Manager;
    TActorId TabletActorId;
    TActorId LauncherActorId;

    // Tier-1 state (all ephemeral — reconstructed on restart).
    THashMap<TEntryKey, ui64> Counters;
    THashMap<TEntryKey, ECutState> CutState;
    THashSet<ui32> PoisonedChannels;

    // portionId -> set of TEntryKey the portion contributes to (for decrement at erase).
    THashMap<ui64, THashSet<TEntryKey>> PortionKeys;

    bool SweepInFlight = false;

    // Tier-2 sweep state (all in-memory; reset on restart/completion).
    // Shared with per-batch sweep callbacks — one allocation per sweep, not per batch.
    std::shared_ptr<const TVector<TEntryKey>> SweepCandidates;
    // Sweep-disproved entries: re-nomination is pointless until state changes, so it is
    // suppressed for DisprovedRetryCooldown to avoid a perpetual nominate/sweep cycle
    // (tier-1 counters start empty at boot and cannot veto for boot-loaded portions).
    THashMap<TEntryKey, TInstant> DisprovedAt;
    TVector<TEntryKey> SweepSurvivors;

    // Cursor over the engine's in-memory portion snapshot (snapshotted once per sweep).
    TVector<std::pair<TInternalPathId, ui64>> SweepPortionIds;
    size_t SweepPortionOffset = 0;
};

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
