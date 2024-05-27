#include "versioned_index.h"
#include "snapshot_scheme.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap {

void TVersionedIndex::AddIndex(const TSnapshot& snapshot, TIndexInfo&& indexInfo) {
    if (Snapshots.empty()) {
        PrimaryKey = indexInfo.GetPrimaryKey();
    } else {
        Y_ABORT_UNLESS(PrimaryKey->Equals(indexInfo.GetPrimaryKey()));
    }

    const bool needActualization = indexInfo.GetSchemeNeedActualization();
    auto newVersion = indexInfo.GetVersion();
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
