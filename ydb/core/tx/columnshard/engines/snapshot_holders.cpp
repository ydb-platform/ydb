#include "snapshot_holders.h"

#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {

bool TSnapshotHoldersPerTable::CouldUseTable(const NColumnShard::TTableInfo& table) const {
    const TSnapshot dropSnapshot = table.GetDropVersionVerified();
    return CouldUse(
        [&dropSnapshot](const TSnapshot& heldSnapshot) { return dropSnapshot <= heldSnapshot; },
        [&table](const TSnapshot& heldSnapshot) { return table.CanBeUsedAt(heldSnapshot); });
}

}   // namespace NKikimr::NOlap
