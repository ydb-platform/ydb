#include "versioned_index.h"
#include "snapshot_scheme.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap {

bool TVersionedIndex::RemoveVersion(ui64 version) {
    if (SnapshotByVersion.size() > 0) {
        ui64 lastVersion = SnapshotByVersion.rbegin()->first;
        if (lastVersion == version) { // keep last version until greater version is added
            if (LastNotDeletedVersion.has_value()) {
                RemoveVersionNoCheck(version);
            }
            LastNotDeletedVersion = version;
            return false;
        }
    }
    RemoveVersionNoCheck(version);
    return true;
}

void TVersionedIndex::RemoveVersionNoCheck(ui64 version) {
    auto itVersion = SnapshotByVersion.find(version);
    AFL_VERIFY(itVersion != SnapshotByVersion.end());
    auto itSnap = Snapshots.find(itVersion->second->GetSnapshot());
    AFL_VERIFY(itSnap != Snapshots.end());
    SnapshotByVersion.erase(itVersion);
    Snapshots.erase(itSnap);
}

const TIndexInfo* TVersionedIndex::AddIndex(const TSnapshot& snapshot, TIndexInfo&& indexInfo) {
    if (Snapshots.empty()) {
        PrimaryKey = indexInfo.GetPrimaryKey();
    } else {
        Y_ABORT_UNLESS(PrimaryKey->Equals(indexInfo.GetPrimaryKey()));
    }

    const bool needActualization = indexInfo.GetSchemeNeedActualization();
    auto newVersion = indexInfo.GetVersion();
    if (LastNotDeletedVersion.has_value() && (*LastNotDeletedVersion < newVersion)) {
        RemoveVersionNoCheck(*LastNotDeletedVersion);
        LastNotDeletedVersion.reset();
    }
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
    LastSchemaVersion = std::max(newVersion, LastSchemaVersion);
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
