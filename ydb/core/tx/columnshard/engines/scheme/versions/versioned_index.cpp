#include "snapshot_scheme.h"
#include "versioned_index.h"

#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

const TIndexInfo* TVersionedIndex::AddIndex(const TSnapshot& snapshot, TObjectCache<TSchemaVersionId, TIndexInfo>::TEntryGuard&& indexInfo) {
    if (SnapshotByVersion.empty()) {
        PrimaryKey = indexInfo->GetPrimaryKey();
    } else {
        Y_ABORT_UNLESS(PrimaryKey->Equals(indexInfo->GetPrimaryKey()));
    }

    const bool needActualization = indexInfo->GetSchemeNeedActualization();
    const bool previousNeedActualization = !SnapshotByVersion.empty() && GetLastSchema()->GetIndexInfo().GetSchemeNeedActualization();
    auto newVersion = indexInfo->GetVersion();
    auto itVersion =
        SnapshotByVersion.emplace(newVersion, TSchemaInfoByVersion(std::make_shared<TSnapshotSchema>(std::move(indexInfo), snapshot)));
    AFL_VERIFY(itVersion.second)("message", "duplication for registered version")("version", LastSchemaVersion);
    auto itSnap = Snapshots.emplace(snapshot, itVersion.first->second.GetSchema());
    Y_ABORT_UNLESS(itSnap.second);
    if (newVersion >= LastSchemaVersion) {
        LastSchemaVersion = newVersion;
        // SchemeNeedActualization: on makes the current schema the target, switching it off stops the actualization;
        // off in both versions leaves alone a target set by ALTER TABLE ... COMPACT WITH (CASCADE = true)
        if (needActualization) {
            SetSchemeNeedActualization(true);
        } else if (previousNeedActualization) {
            SetSchemeNeedActualization(false);
        }
    }
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

}   // namespace NKikimr::NOlap
