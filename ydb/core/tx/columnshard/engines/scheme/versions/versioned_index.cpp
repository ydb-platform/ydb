#include "versioned_index.h"
#include "snapshot_scheme.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

void TVersionedIndex::AddIndex(const TSnapshot& snapshot, TIndexInfo&& indexInfo) {
    if (Snapshots.empty()) {
        PrimaryKey = indexInfo.GetPrimaryKey();
    } else {
        Y_ABORT_UNLESS(PrimaryKey->Equals(indexInfo.GetPrimaryKey()));
    }

    auto newVersion = indexInfo.GetVersion();
    auto itVersion = SnapshotByVersion.emplace(newVersion, std::make_shared<TSnapshotSchema>(std::move(indexInfo), snapshot));
    if (!itVersion.second) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("message", "Skip registered version")("version", LastSchemaVersion);
    }
    auto itSnap = Snapshots.emplace(snapshot, itVersion.first->second);
    Y_ABORT_UNLESS(itSnap.second);
    LastSchemaVersion = std::max(newVersion, LastSchemaVersion);
}

}
