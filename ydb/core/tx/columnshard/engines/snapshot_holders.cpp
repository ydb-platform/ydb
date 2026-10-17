#include "snapshot_holders.h"

#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {

namespace {
void VerifyBelow(const std::vector<TSnapshot>& snapshots, const TSnapshot& border) {
    AFL_VERIFY(std::is_sorted(snapshots.begin(), snapshots.end()));
    AFL_VERIFY(snapshots.empty() || snapshots.back() < border);
}
}   // namespace

TRegistrySnapshotHolders::TRegistrySnapshotHolders(const TSnapshot minSnapshotForNewReads,
    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> registry, const ui64 schemeShardId, const IPathIdTranslator& pathIdTranslator,
    TLocalActiveSnapshots localActiveSnapshots)
    : MinSnapshotForNewReads(minSnapshotForNewReads)
    , Registry(std::move(registry))
    , SchemeShardId(schemeShardId)
    , PathIdTranslator(pathIdTranslator)
    , LocalActiveSnapshots(std::move(localActiveSnapshots))
{
    AFL_VERIFY(Registry);
    VerifyBelow(LocalActiveSnapshots.ForAllTables, MinSnapshotForNewReads);
    for (const auto& [_, snapshots] : LocalActiveSnapshots.ByPathId) {
        VerifyBelow(snapshots, MinSnapshotForNewReads);
    }
}

TSnapshotHoldersPerTable TRegistrySnapshotHolders::BuildHoldersForTable(
    const TInternalPathId pathId, const std::set<NColumnShard::TSchemeShardLocalPathId>& schemeShardLocalPathIds) const {
    std::set<TSnapshot> snapshots(LocalActiveSnapshots.ForAllTables.begin(), LocalActiveSnapshots.ForAllTables.end());
    if (const auto* localSnapshots = LocalActiveSnapshots.ByPathId.FindPtr(pathId)) {
        snapshots.insert(localSnapshots->begin(), localSnapshots->end());
    }
    for (const auto& schemeShardLocalPathId : schemeShardLocalPathIds) {
        const NKikimr::TTableId tableId(SchemeShardId, schemeShardLocalPathId.GetRawValue(), 0);
        for (const auto& rowVersion : Registry->GetActiveSnapshots(tableId)) {
            const TSnapshot snapshot(rowVersion.Step, rowVersion.TxId);
            if (snapshot < MinSnapshotForNewReads) {
                snapshots.emplace(snapshot);
            }
        }
        if (const auto readOnlySnapshot = PathIdTranslator.GetCopyVersionOptional(schemeShardLocalPathId)) {
            if (*readOnlySnapshot < MinSnapshotForNewReads) {
                snapshots.emplace(*readOnlySnapshot);
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
    return HoldersByPathId.emplace(pathId, BuildHoldersForTable(pathId, schemeShardLocalPathIds)).first->second;
}

bool TRegistrySnapshotHolders::CouldUsePortion(const TPortionInfo::TConstPtr& portion) const {
    return GetHoldersByPathId(portion->GetPathId()).CouldUsePortion(portion);
}

bool TRegistrySnapshotHolders::CouldUseTable(const TInternalPathId& pathId, const TSnapshot& dropSnapshot) const {
    return GetHoldersByPathId(pathId).CouldUseTable(dropSnapshot);
}

}   // namespace NKikimr::NOlap
