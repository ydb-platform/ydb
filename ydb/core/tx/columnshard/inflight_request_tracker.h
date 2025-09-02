#pragma once

#include "blob.h"

#include "counters/req_tracer.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap {
class TVersionedIndex;
}

namespace NKikimr::NColumnShard {
class TColumnShard;
using NOlap::IBlobInUseTracker;

class TSnapshotLiveInfo {
private:
    const NOlap::TSnapshot Snapshot;
    std::optional<TInstant> LastRequestFinishedInstant;
    THashSet<ui32> Requests;
    YDB_READONLY(bool, IsLock, false);

    TSnapshotLiveInfo(const NOlap::TSnapshot& snapshot)
        : Snapshot(snapshot) {
    }

public:
    void AddRequest(const ui32 cookie) {
        AFL_VERIFY(Requests.emplace(cookie).second);
    }

    [[nodiscard]] bool DelRequest(const ui32 cookie, const TInstant now) {
        AFL_VERIFY(Requests.erase(cookie));
        if (Requests.empty()) {
            LastRequestFinishedInstant = now;
        }
        if (!IsLock && Requests.empty()) {
            return true;
        }
        return false;
    }

    static TSnapshotLiveInfo BuildFromRequest(const NOlap::TSnapshot& reqSnapshot) {
        return TSnapshotLiveInfo(reqSnapshot);
    }

    static TSnapshotLiveInfo BuildFromDatabase(const NOlap::TSnapshot& reqSnapshot) {
        TSnapshotLiveInfo result(reqSnapshot);
        result.LastRequestFinishedInstant = TInstant::Now();
        result.IsLock = true;
        return result;
    }

    bool IsExpired(const TDuration critDuration, const TInstant now) const {
        if (Requests.size()) {
            return false;
        }
        AFL_VERIFY(LastRequestFinishedInstant);
        return critDuration < now - *LastRequestFinishedInstant;
    }

    bool CheckToLock(const TDuration snapshotLivetime, const TDuration usedSnapshotGuaranteeLivetime, const TInstant now) {
        if (IsLock) {
            return false;
        }

        if (Requests.size()) {
            if (now + usedSnapshotGuaranteeLivetime > Snapshot.GetPlanInstant() + snapshotLivetime) {
                IsLock = true;
                return true;
            }
        } else {
            AFL_VERIFY(LastRequestFinishedInstant);
            if (*LastRequestFinishedInstant + usedSnapshotGuaranteeLivetime > Snapshot.GetPlanInstant() + snapshotLivetime) {
                IsLock = true;
                return true;
            }
        }
        return false;
    }
};

class TInFlightReadsTracker {
private:
    std::map<NOlap::TSnapshot, TSnapshotLiveInfo> SnapshotsLive;
    std::shared_ptr<TRequestsTracerCounters> Counters;
    THashMap<ui64, NActors::TActorId> ActorIds;

    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    ui64 NextCookie = 1;
    THashMap<ui64, NOlap::NReader::TReadMetadataBase::TConstPtr> RequestsMeta;
    NOlap::TSelectInfo::TStats SelectStatsDelta;

public:
    std::optional<NOlap::TSnapshot> GetSnapshotToClean() const {
        if (SnapshotsLive.empty()) {
            return std::nullopt;
        } else {
            return SnapshotsLive.begin()->first;
        }
    }

    bool LoadFromDatabase(NTable::TDatabase& db);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> Ping(
        TColumnShard* self, const TDuration stalenessInMem, const TDuration usedSnapshotLivetime, const TInstant now);

    // Returns a unique cookie associated with this request
    [[nodiscard]] ui64 AddInFlightRequest(
        NOlap::NReader::TReadMetadataBase::TConstPtr readMeta, const NOlap::TVersionedIndex* index);
    void AddScanActorId(const ui64 cookie, const NActors::TActorId& actorId) {
        AFL_VERIFY(ActorIds.emplace(cookie, actorId).second);
    }

    [[nodiscard]] NOlap::NReader::TReadMetadataBase::TConstPtr ExtractInFlightRequest(ui64 cookie, const NOlap::TVersionedIndex* index, const TInstant now);

    NOlap::TSelectInfo::TStats GetSelectStatsDelta() {
        auto delta = SelectStatsDelta;
        SelectStatsDelta = NOlap::TSelectInfo::TStats();
        return delta;
    }

    void Stop(TColumnShard* /*self*/) {
        for (auto&& i : ActorIds) {
            NActors::TActivationContext::Send(i.second, std::make_unique<NActors::TEvents::TEvPoison>());
        }
    }

    TInFlightReadsTracker(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const std::shared_ptr<TRequestsTracerCounters>& counters)
        : Counters(counters)
        , StoragesManager(storagesManager) {
    }

private:
    void AddToInFlightRequest(
        const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* index);
};

}   // namespace NKikimr::NColumnShard
