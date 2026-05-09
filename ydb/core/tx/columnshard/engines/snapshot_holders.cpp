#include "snapshot_holders.h"

#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {

TRegistrySnapshotHolders::TRegistrySnapshotHolders(const TSnapshot minSnapshotForNewReads,
    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> registry, const ui64 schemeShardId, const IPathIdTranslator& pathIdTranslator)
    : MinSnapshotForNewReads(minSnapshotForNewReads)
    , Registry(std::move(registry))
    , SchemeShardId(schemeShardId)
    , PathIdTranslator(pathIdTranslator)
{
    AFL_VERIFY(Registry);
}

TSnapshotHoldersPerTable TRegistrySnapshotHolders::BuildHoldersForTable(
    const std::set<NColumnShard::TSchemeShardLocalPathId>& schemeShardLocalPathIds) const {
    std::set<TSnapshot> snapshots;
    for (const auto& schemeShardLocalPathId : schemeShardLocalPathIds) {
        const NKikimr::TTableId tableId(SchemeShardId, schemeShardLocalPathId.GetRawValue(), 0);
        for (const auto& rowVersion : Registry->GetActiveSnapshots(tableId)) {
            const TSnapshot snapshot(rowVersion.Step, rowVersion.TxId);
            if (snapshot < MinSnapshotForNewReads) {
                snapshots.emplace(snapshot);
            }
        }
    }
    std::vector<TSnapshot> txInFlight(snapshots.begin(), snapshots.end());
    return TSnapshotHoldersPerTable(MinSnapshotForNewReads, std::move(txInFlight));
}

const TSnapshotHoldersPerTable& TRegistrySnapshotHolders::GetHoldersByPathId(const TInternalPathId pathId) const {
    auto it = HoldersByPathId.find(pathId);
    if (it != HoldersByPathId.end()) {
        return it->second;
    }

    auto schemeShardLocalPathIds = PathIdTranslator.ResolveSchemeShardLocalPathIdsVerified(pathId);
    return HoldersByPathId.emplace(pathId, BuildHoldersForTable(schemeShardLocalPathIds)).first->second;
}

bool TRegistrySnapshotHolders::CouldUsePortion(const TPortionInfo::TConstPtr& portion) const {
    return GetHoldersByPathId(portion->GetPathId()).CouldUsePortion(portion);
}

}   // namespace NKikimr::NOlap
