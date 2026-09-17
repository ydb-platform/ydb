#pragma once
#include "address.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/blobs_manager.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NOlap {
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

// Cuts a channel history entry once MoveData has proven its generation range drained.
class THistoryCutterWrapper {
public:
    THistoryCutterWrapper(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo, const std::weak_ptr<NOlap::TBlobManager>& manager,
        const std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager>& sharedBlobs, const TActorId& tabletActorId,
        const NColumnShard::THistoryCutterCounters& signals);

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

    bool TryNominate(const TActorContext& ctx, bool triggered = false);

    // Defaults for TColumnShardConfig.CutHistory*: they bound IsDrained() queue scans per tablet.
    static constexpr TDuration DefaultNominateCadence = TDuration::Minutes(1);
    static constexpr ui32 DefaultMaxDrainChecksPerNomination = 8;

    static TDuration GetNominateCadence();
    static ui32 GetMaxDrainChecksPerNomination();

    // True while the build only measures: the proof runs to the end but stops short of the cut.
    static bool IsMeasureOnly();

protected:
    ECutState GetCutStateForTest(const TEntryKey& key) const {
        const auto* state = CutState.FindPtr(key);
        return state ? *state : ECutState::None;
    }

    bool IsNominationPendingForTest() const {
        return NominationPending;
    }

    bool IsNominationTriggeredForTest() const {
        return NominationTriggered;
    }

    bool IsDrained(const TEntryKey& key) const;

public:
    const NColumnShard::THistoryCutterCounters& GetSignals() const {
        return Signals;
    }

    bool IsEnabled() const;

    // True when no earlier entry shares the target's GroupID; already-cut entries are transparent.
    static bool SeenGroupsCheckPasses(
        const std::vector<TTabletChannelInfo::THistoryEntry>& hist, ui32 fromGeneration, const THashSet<ui32>& cutFromGenerations = {});

    ui32 GetNextFromGeneration(const TEntryKey& key) const;

private:
    bool SeenGroupsCheckPasses(const TEntryKey& key) const;

    // Final per-entry gates and the cut itself for a proven candidate list.
    void DecideAndCut(const TVector<TEntryKey>& candidates, const TActorContext& ctx);

    static bool ComputeEnabled();

    const NColumnShard::THistoryCutterCounters Signals;

    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    std::weak_ptr<NOlap::TBlobManager> Manager;
    // Shared-out blobs are in no GC queue; the drain gate consults this registry.
    std::weak_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> SharedBlobs;
    TActorId TabletActorId;
    TActorId LauncherActorId;

    // Computed once in the constructor; guards nomination and the cut.
    bool Enabled = false;

    THashMap<TEntryKey, ECutState> CutState;

    // Deferred nomination: NominationPending prevents duplicate in-flight events.
    bool NominationPending = false;
    bool NominationTriggered = false;

    TInstant LastNominateAt;
    ui32 NextChannelToCheck = TGlobal::FirstDataChannel;
};

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
