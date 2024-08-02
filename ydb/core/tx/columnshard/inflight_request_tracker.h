#pragma once

#include "blob.h"
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
    std::optional<TInstant> LastPingInstant;
    std::optional<TInstant> LastRequestFinishedInstant;
    THashSet<ui32> Requests;
    YDB_READONLY(bool, IsLock, false);

    TSnapshotLiveInfo(const NOlap::TSnapshot& snapshot)
        : Snapshot(snapshot)
    {
    
    }

public:
    void AddRequest(const ui32 cookie) {
        AFL_VERIFY(Requests.emplace(cookie).second);
    }

    [[nodiscard]] bool DelRequest(const ui32 cookie) {
        AFL_VERIFY(Requests.erase(cookie));
        if (Requests.empty()) {
            LastRequestFinishedInstant = TInstant::Now();
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
        result.LastPingInstant = TInstant::Now();
        result.LastRequestFinishedInstant = result.LastPingInstant;
        result.IsLock = true;
        return result;
    }

    bool Ping(const TDuration critDuration) {
        LastPingInstant = TInstant::Now();
        if (Requests.empty()) {
            AFL_VERIFY(LastRequestFinishedInstant);
            if (critDuration < *LastPingInstant - *LastRequestFinishedInstant && IsLock) {
                IsLock = false;
                return true;
            }
        } else {
            if (critDuration < *LastPingInstant - Snapshot.GetPlanInstant() && !IsLock) {
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

public:
    std::optional<NOlap::TSnapshot> GetSnapshotToClean() const {
        if (SnapshotsLive.empty()) {
            return std::nullopt;
        } else {
            return SnapshotsLive.begin()->first;
        }
    }

    bool LoadFromDatabase(NTable::TDatabase& db);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> Ping(TColumnShard* self, const TDuration critDuration);

    // Returns a unique cookie associated with this request
    [[nodiscard]] TConclusion<ui64> AddInFlightRequest(NOlap::NReader::TReadMetadataBase::TConstPtr readMeta, const NOlap::TVersionedIndex* index) {
        const ui64 cookie = NextCookie++;
        auto it = SnapshotsLive.find(readMeta->GetRequestSnapshot());
        if (it == SnapshotsLive.end()) {
            it = SnapshotsLive.emplace(readMeta->GetRequestSnapshot(), TSnapshotLiveInfo::BuildFromRequest(readMeta->GetRequestSnapshot())).first;
        }
        it->second.AddRequest(cookie);
        auto status = AddToInFlightRequest(cookie, readMeta, index);
        if (!status) {
            return status;
        }
        return cookie;
    }

    void RemoveInFlightRequest(ui64 cookie, const NOlap::TVersionedIndex* index);

    // Checks if the portion is in use by any in-flight request
    bool IsPortionUsed(ui64 portionId) const {
        return PortionUseCount.contains(portionId);
    }

    NOlap::TSelectInfo::TStats GetSelectStatsDelta() {
        auto delta = SelectStatsDelta;
        SelectStatsDelta = NOlap::TSelectInfo::TStats();
        return delta;
    }

    TInFlightReadsTracker(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager)
        : StoragesManager(storagesManager)
    {

    }

private:
    [[nodiscard]] TConclusionStatus AddToInFlightRequest(const ui64 cookie, NOlap::NReader::TReadMetadataBase::TConstPtr readMetaBase, const NOlap::TVersionedIndex* index);

private:
    std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    ui64 NextCookie = 1;
    THashMap<ui64, TList<NOlap::NReader::TReadMetadataBase::TConstPtr>> RequestsMeta;
    THashMap<ui64, ui64> PortionUseCount;
    NOlap::TSelectInfo::TStats SelectStatsDelta;
};

}
