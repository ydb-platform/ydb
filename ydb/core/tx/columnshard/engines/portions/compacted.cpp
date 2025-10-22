#include "compacted.h"
#include "constructor_portion.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

void TCompactedPortionInfo::DoSaveMetaToDatabase(const std::vector<TUnifiedBlobId>& blobIds, NIceDb::TNiceDb& db) const {
    auto metaProto = GetMeta().SerializeToProto(blobIds, NPortion::EProduced::SPLIT_COMPACTED);
    AFL_VERIFY(AppearanceSnapshot.Valid());
    auto* compactedProto = metaProto.MutableCompactedPortion();
    AppearanceSnapshot.SerializeToProto(*compactedProto->MutableAppearanceSnapshot());
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    const auto removeSnapshot = GetRemoveSnapshotOptional();
    db.Table<IndexPortions>()
        .Key(GetPathId().GetRawValue(), GetPortionId())
        .Update(NIceDb::TUpdate<IndexPortions::SchemaVersion>(GetSchemaVersionVerified()),
            NIceDb::TUpdate<IndexPortions::ShardingVersion>(GetShardingVersionDef(0)), NIceDb::TUpdate<IndexPortions::CommitPlanStep>(0),
            NIceDb::TUpdate<IndexPortions::CommitTxId>(0), NIceDb::TUpdate<IndexPortions::InsertWriteId>(0),
            NIceDb::TUpdate<IndexPortions::XPlanStep>(removeSnapshot ? removeSnapshot->GetPlanStep() : 0),
            NIceDb::TUpdate<IndexPortions::XTxId>(removeSnapshot ? removeSnapshot->GetTxId() : 0),
            NIceDb::TUpdate<IndexPortions::MinSnapshotPlanStep>(1),
            NIceDb::TUpdate<IndexPortions::MinSnapshotTxId>(1),
            NIceDb::TUpdate<IndexPortions::Metadata>(metaProto.SerializeAsString()));
}

const TSnapshot& TCompactedPortionInfo::RecordSnapshotMin(const std::optional<TSnapshot>& /*snapshotDefault*/) const {
    return GetMeta().RecordSnapshotMin;
}

const TSnapshot& TCompactedPortionInfo::RecordSnapshotMax(const std::optional<TSnapshot>& /*snapshotDefault*/) const {
    return GetMeta().RecordSnapshotMax;
}

std::unique_ptr<TPortionInfoConstructor> TCompactedPortionInfo::BuildConstructor(const bool withMetadata) const {
    return std::make_unique<TCompactedPortionInfoConstructor>(*this, withMetadata);
}

NSplitter::TEntityGroups TCompactedPortionInfo::GetEntityGroupsByStorageId(
    const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& indexInfo) const {
    return indexInfo.GetEntityGroupsByStorageId(specialTier, storages);
}

const TString& TCompactedPortionInfo::GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    return indexInfo.GetColumnStorageId(columnId, GetMeta().GetTierName());
}

const TString& TCompactedPortionInfo::GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    return indexInfo.GetEntityStorageId(columnId, GetMeta().GetTierName());
}

const TString& TCompactedPortionInfo::GetIndexStorageId(const ui32 indexId, const TIndexInfo& indexInfo) const {
    return indexInfo.GetIndexStorageId(indexId);
}

}   // namespace NKikimr::NOlap
