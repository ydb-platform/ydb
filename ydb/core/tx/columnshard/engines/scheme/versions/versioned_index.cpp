#include "versioned_index.h"
#include "snapshot_scheme.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap {

void TVersionedIndex::RemoveVersion(const ui64 version) {
    auto itVersion = SnapshotByVersion.find(version);
    AFL_VERIFY(itVersion != SnapshotByVersion.end());
    auto itSnap = Snapshots.find(itVersion->second->GetSnapshot());
    AFL_VERIFY(itSnap != Snapshots.end());
    SnapshotByVersion.erase(itVersion);
    Snapshots.erase(itSnap);
}

const TIndexInfo* TVersionedIndex::AddIndex(const TSnapshot& snapshot, TObjectCache<TSchemaVersionId, TIndexInfo>::TEntryGuard&& indexInfo) {
    if (Snapshots.empty()) {
        PrimaryKey = indexInfo->GetPrimaryKey();
    } else {
        Y_ABORT_UNLESS(PrimaryKey->Equals(indexInfo->GetPrimaryKey()));
    }

    const bool needActualization = indexInfo->GetSchemeNeedActualization();
    auto newVersion = indexInfo->GetVersion();
    auto itVersion = SnapshotByVersion.emplace(newVersion, std::make_shared<TSnapshotSchema>(std::move(indexInfo), snapshot));
    if (!itVersion.second) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("message", "Skip registered version")("version", LastSchemaVersion);
    } else if (needActualization) {
        if (!SchemeVersionForActualization || *SchemeVersionForActualization < newVersion) {
            SchemeVersionForActualization = newVersion;
            SchemeForActualization = itVersion.first->second;
        }
    }
    auto itSnap = Snapshots.emplace(snapshot, itVersion.first->second);
    Y_ABORT_UNLESS(itSnap.second);
    ui64 oldLastVersion = LastSchemaVersion;
    LastSchemaVersion = std::max(newVersion, LastSchemaVersion);
    if (LastSchemaVersion != oldLastVersion) {
        if (oldLastVersion != 0) {
            VersionCounters->VersionRemoveRef(oldLastVersion, "last");
        }
        VersionCounters->VersionAddRef(LastSchemaVersion, "last");
    }
    return &itSnap.first->second->GetIndexInfo();
}

bool TVersionedIndex::LoadShardingInfo(IDbWrapper& db) {
    TConclusion<THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>>> shardingLocal = db.LoadGranulesShardingInfo();
    if (shardingLocal.IsFail()) {
        return false;
    }
    ShardingInfo = std::move(shardingLocal.DetachResult());
    return true;
}

std::optional<NKikimr::NOlap::TGranuleShardingInfo> TVersionedIndex::GetShardingInfoActual(const ui64 pathId) const {
    auto it = ShardingInfo.find(pathId);
    if (it == ShardingInfo.end() || it->second.empty()) {
        return std::nullopt;
    } else {
        return it->second.rbegin()->second;
    }
}

}
