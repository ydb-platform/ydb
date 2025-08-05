#include "versioned_index.h"
#include "snapshot_scheme.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap {

const TIndexInfo* TVersionedIndex::AddIndex(const TSnapshot& snapshot, TObjectCache<TSchemaVersionId, TIndexInfo>::TEntryGuard&& indexInfo) {
    if (SnapshotByVersion.empty()) {
        PrimaryKey = indexInfo->GetPrimaryKey();
    } else {
        Y_ABORT_UNLESS(PrimaryKey->Equals(indexInfo->GetPrimaryKey()));
    }

    const bool needActualization = indexInfo->GetSchemeNeedActualization();
    auto newVersion = indexInfo->GetVersion();
    auto itVersion =
        SnapshotByVersion.emplace(newVersion, TSchemaInfoByVersion(std::make_shared<TSnapshotSchema>(std::move(indexInfo), snapshot)));
    AFL_VERIFY(itVersion.second)("message", "duplication for registered version")("version", LastSchemaVersion);
    if (needActualization) {
        if (!SchemeVersionForActualization || *SchemeVersionForActualization < newVersion) {
            SchemeVersionForActualization = newVersion;
            SchemeForActualization = itVersion.first->second.GetSchema();
        }
    }
    auto itSnap = Snapshots.emplace(snapshot, itVersion.first->second.GetSchema());
    Y_ABORT_UNLESS(itSnap.second);
    LastSchemaVersion = std::max(newVersion, LastSchemaVersion);
    return &itVersion.first->second->GetIndexInfo();
}

bool TVersionedIndex::LoadShardingInfo(IDbWrapper& db) {
    TConclusion<THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>>> shardingLocal = db.LoadGranulesShardingInfo();
    if (shardingLocal.IsFail()) {
        return false;
    }
    ShardingInfo = std::move(shardingLocal.DetachResult());
    return true;
}

std::optional<NKikimr::NOlap::TGranuleShardingInfo> TVersionedIndex::GetShardingInfoActual(const TInternalPathId pathId) const {
    auto it = ShardingInfo.find(pathId);
    if (it == ShardingInfo.end() || it->second.empty()) {
        return std::nullopt;
    } else {
        return it->second.rbegin()->second;
    }
}

}
