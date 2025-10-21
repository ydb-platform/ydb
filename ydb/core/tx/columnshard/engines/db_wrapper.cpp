#include "db_wrapper.h"
#include "defs.h"

#include "portions/constructor_portion.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/sharding/sharding.h>

namespace NKikimr::NOlap {

void TDbWrapper::WriteColumn(
    const TPortionDataAccessor& acc, const NOlap::TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) {
    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage() && !AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        return;
    }
    NIceDb::TNiceDb db(Database);
    using IndexColumnsV1 = NColumnShard::Schema::IndexColumnsV1;
    auto rowProto = row.GetMeta().SerializeToProto();
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage()) {
        db.Table<IndexColumnsV1>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.ColumnId, row.Chunk)
            .Update(NIceDb::TUpdate<IndexColumnsV1::BlobIdx>(row.GetBlobRange().GetBlobIdxVerified()),
                NIceDb::TUpdate<IndexColumnsV1::Metadata>(rowProto.SerializeAsString()),
                NIceDb::TUpdate<IndexColumnsV1::Offset>(row.BlobRange.Offset), NIceDb::TUpdate<IndexColumnsV1::Size>(row.BlobRange.Size));
    }
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        if (row.GetChunkIdx() == 0 && row.GetColumnId() == firstPKColumnId) {
            *rowProto.MutablePortionMeta() = portion.GetMeta().SerializeToProto(acc.GetBlobIds(),
                portion.GetPortionType() == EPortionType::Compacted ? NPortion::EProduced::SPLIT_COMPACTED : NPortion::EProduced::INSERTED);
        }
        using IndexColumns = NColumnShard::Schema::IndexColumns;
        auto removeSnapshot = portion.GetRemoveSnapshotOptional();
        db.Table<IndexColumns>()
            .Key(0, 0, row.ColumnId, 1, 1, portion.GetPortionId(), row.Chunk)
            .Update(NIceDb::TUpdate<IndexColumns::XPlanStep>(removeSnapshot ? removeSnapshot->GetPlanStep() : 0),
                NIceDb::TUpdate<IndexColumns::XTxId>(removeSnapshot ? removeSnapshot->GetTxId() : 0),
                NIceDb::TUpdate<IndexColumns::Blob>(acc.GetBlobId(row.GetBlobRange().GetBlobIdxVerified()).SerializeBinary()),
                NIceDb::TUpdate<IndexColumns::BlobIdx>(row.GetBlobRange().GetBlobIdxVerified()),
                NIceDb::TUpdate<IndexColumns::Metadata>(rowProto.SerializeAsString()),
                NIceDb::TUpdate<IndexColumns::Offset>(row.BlobRange.Offset), NIceDb::TUpdate<IndexColumns::Size>(row.BlobRange.Size),
                NIceDb::TUpdate<IndexColumns::PathId>(portion.GetPathId().GetRawValue()));
    }
}

void TDbWrapper::WritePortion(const std::vector<TUnifiedBlobId>& blobIds, const NOlap::TPortionInfo& portion) {
    NIceDb::TNiceDb db(Database);
    portion.SaveMetaToDatabase(blobIds, db);
}

void TDbWrapper::CommitPortion(const NOlap::TPortionInfo& portion, const TSnapshot& commitSnapshot) {
    NIceDb::TNiceDb db(Database);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    if (portion.HasRemoveSnapshot()) {
        db.Table<IndexPortions>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId())
            .Update(NIceDb::TUpdate<IndexPortions::CommitPlanStep>(commitSnapshot.GetPlanStep()),
                NIceDb::TUpdate<IndexPortions::CommitTxId>(commitSnapshot.GetTxId()),
                NIceDb::TUpdate<IndexPortions::XPlanStep>(portion.GetRemoveSnapshotVerified().GetPlanStep()),
                NIceDb::TUpdate<IndexPortions::XTxId>(portion.GetRemoveSnapshotVerified().GetTxId()));
    } else {
        db.Table<IndexPortions>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId())
            .Update(NIceDb::TUpdate<IndexPortions::CommitPlanStep>(commitSnapshot.GetPlanStep()),
                NIceDb::TUpdate<IndexPortions::CommitTxId>(commitSnapshot.GetTxId()));
    }
}

void TDbWrapper::ErasePortion(const NOlap::TPortionInfo& portion) {
    NIceDb::TNiceDb db(Database);
    db.Table<NColumnShard::Schema::IndexPortions>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId()).Delete();
    db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId()).Delete();
}

void TDbWrapper::EraseColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) {
    NIceDb::TNiceDb db(Database);
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV1Usage()) {
        using IndexColumnsV1 = NColumnShard::Schema::IndexColumnsV1;
        db.Table<IndexColumnsV1>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.ColumnId, row.Chunk).Delete();
    }
    if (AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        using IndexColumns = NColumnShard::Schema::IndexColumns;
        db.Table<IndexColumns>().Key(0, 0, row.ColumnId, 1, 1, portion.GetPortionId(), row.Chunk).Delete();
    }
}

bool TDbWrapper::LoadColumns(const std::optional<TInternalPathId> pathId, const std::function<void(TColumnChunkLoadContextV2&&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
    const auto pred = [&](auto& rowset) {
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            AFL_VERIFY(DsGroupSelector);
            NOlap::TColumnChunkLoadContextV2 chunkLoadContext(rowset, *DsGroupSelector);
            callback(std::move(chunkLoadContext));

            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    };
    if (pathId) {
        auto rowset = db.Table<IndexColumnsV2>().Prefix(pathId->GetRawValue()).Select();
        return pred(rowset);
    } else {
        auto rowset = db.Table<IndexColumnsV2>().Select();
        return pred(rowset);
    }
}

bool TDbWrapper::LoadPortions(const std::optional<TInternalPathId> pathId,
    const std::function<void(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    const auto pred = [&](auto& rowset) {
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            std::unique_ptr<NOlap::TPortionInfoConstructor> portion;

            NKikimrTxColumnShard::TIndexPortionMeta metaProto;
            const TString metadata = rowset.template GetValue<NColumnShard::Schema::IndexPortions::Metadata>();
            AFL_VERIFY(metaProto.ParseFromArray(metadata.data(), metadata.size()))("event", "cannot parse metadata as protobuf");

            if (rowset.template GetValueOrDefault<IndexPortions::InsertWriteId>(0)) {
                auto portionImpl = std::make_unique<TWrittenPortionInfoConstructor>(
                    TInternalPathId::FromRawValue(rowset.template GetValue<IndexPortions::PathId>()),
                    rowset.template GetValue<IndexPortions::PortionId>());
                portionImpl->SetInsertWriteId((TInsertWriteId)rowset.template GetValue<IndexPortions::InsertWriteId>());
                if (rowset.template GetValueOrDefault<IndexPortions::CommitPlanStep>(0)) {
                    portionImpl->SetCommitSnapshot(TSnapshot(
                        rowset.template GetValue<IndexPortions::CommitPlanStep>(), rowset.template GetValue<IndexPortions::CommitTxId>()));
                } else {
                    AFL_VERIFY(!rowset.template GetValueOrDefault<IndexPortions::CommitTxId>(0));
                }
                portion.reset(portionImpl.release());
            } else {
                AFL_VERIFY(metaProto.HasCompactedPortion());
                AFL_VERIFY(metaProto.GetCompactedPortion().HasAppearanceSnapshot());
                auto cPortion = std::make_unique<TCompactedPortionInfoConstructor>(
                    TInternalPathId::FromRawValue(rowset.template GetValue<IndexPortions::PathId>()),
                    rowset.template GetValue<IndexPortions::PortionId>());
                TSnapshot snapshot = TSnapshot::Zero();
                snapshot.DeserializeFromProto(metaProto.GetCompactedPortion().GetAppearanceSnapshot()).Validate();
                cPortion->SetAppearanceSnapshot(snapshot);
                portion = std::move(cPortion);
            }
            portion->SetSchemaVersion(rowset.template GetValue<IndexPortions::SchemaVersion>());
            if (rowset.template HaveValue<IndexPortions::ShardingVersion>() && rowset.template GetValue<IndexPortions::ShardingVersion>()) {
                portion->SetShardingVersion(rowset.template GetValue<IndexPortions::ShardingVersion>());
            }
            portion->SetRemoveSnapshot(rowset.template GetValue<IndexPortions::XPlanStep>(), rowset.template GetValue<IndexPortions::XTxId>());
            callback(std::move(portion), metaProto);

            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    };
    if (pathId) {
        auto rowset = db.Table<IndexPortions>().Prefix(pathId->GetRawValue()).Select();
        return pred(rowset);
    } else {
        auto rowset = db.Table<IndexPortions>().Select();
        return pred(rowset);
    }
}

void TDbWrapper::WriteIndex(const TPortionDataAccessor& acc, const TPortionInfo& portion, const TIndexChunk& row) {
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    NIceDb::TNiceDb db(Database);
    if (auto bRange = row.GetBlobRangeOptional()) {
        AFL_VERIFY(bRange->IsValid());
        db.Table<IndexIndexes>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.GetIndexId(), row.GetChunkIdx())
            .Update(NIceDb::TUpdate<IndexIndexes::Blob>(acc.GetBlobId(bRange->GetBlobIdxVerified()).SerializeBinary()),
                NIceDb::TUpdate<IndexIndexes::BlobIdx>(bRange->GetBlobIdxVerified()), NIceDb::TUpdate<IndexIndexes::Offset>(bRange->Offset),
                NIceDb::TUpdate<IndexIndexes::Size>(row.GetDataSize()), NIceDb::TUpdate<IndexIndexes::RecordsCount>(row.GetRecordsCount()),
                NIceDb::TUpdate<IndexIndexes::RawBytes>(row.GetRawBytes()));
    } else if (auto bData = row.GetBlobDataOptional()) {
        db.Table<IndexIndexes>()
            .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.GetIndexId(), row.GetChunkIdx())
            .Update(NIceDb::TUpdate<IndexIndexes::BlobData>(*bData), NIceDb::TUpdate<IndexIndexes::RecordsCount>(row.GetRecordsCount()),
                NIceDb::TUpdate<IndexIndexes::RawBytes>(row.GetRawBytes()));
    } else {
        AFL_VERIFY(false);
    }
}

void TDbWrapper::EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) {
    NIceDb::TNiceDb db(Database);
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    db.Table<IndexIndexes>().Key(portion.GetPathId().GetRawValue(), portion.GetPortionId(), row.GetIndexId(), 0).Delete();
}

bool TDbWrapper::LoadIndexes(const std::optional<TInternalPathId> pathId,
    const std::function<void(const TInternalPathId pathId, const ui64 portionId, TIndexChunkLoadContext&&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    const auto pred = [&](auto& rowset) {
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            NOlap::TIndexChunkLoadContext chunkLoadContext(rowset, DsGroupSelector);
            callback(TInternalPathId::FromRawValue(rowset.template GetValue<IndexIndexes::PathId>()),
                rowset.template GetValue<IndexIndexes::PortionId>(), std::move(chunkLoadContext));

            if (!rowset.Next()) {
                return false;
            }
        }
        return true;
    };
    if (pathId) {
        auto rowset = db.Table<IndexIndexes>().Prefix(pathId->GetRawValue()).Select();
        return pred(rowset);
    } else {
        auto rowset = db.Table<IndexIndexes>().Select();
        return pred(rowset);
    }
}

void TDbWrapper::WriteCounter(ui32 counterId, ui64 value) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::IndexCounters_Write(db, counterId, value);
}

bool TDbWrapper::LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::IndexCounters_Load(db, callback);
}

TConclusion<THashMap<TInternalPathId, std::map<NOlap::TSnapshot, TGranuleShardingInfo>>> TDbWrapper::LoadGranulesShardingInfo() {
    using Schema = NColumnShard::Schema;
    NIceDb::TNiceDb db(Database);
    auto rowset = db.Table<Schema::ShardingInfo>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read rowset");
    }
    THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>> result;
    while (!rowset.EndOfSet()) {
        NOlap::TSnapshot snapshot = NOlap::TSnapshot::Zero();
        snapshot.DeserializeFromString(rowset.GetValue<Schema::ShardingInfo::Snapshot>()).Validate();
        NSharding::TGranuleShardingLogicContainer logic;
        logic.DeserializeFromString(rowset.GetValue<Schema::ShardingInfo::Logic>()).Validate();
        TGranuleShardingInfo gShardingInfo(logic, snapshot, rowset.GetValue<Schema::ShardingInfo::VersionId>(),
            TInternalPathId::FromRawValue(rowset.GetValue<Schema::ShardingInfo::PathId>()));
        AFL_VERIFY(result[gShardingInfo.GetPathId()].emplace(gShardingInfo.GetSinceSnapshot(), gShardingInfo).second);

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read rowset");
        }
    }
    return result;
}

void TDbWrapper::WriteColumns(const NOlap::TPortionInfo& portion, const NKikimrTxColumnShard::TIndexPortionAccessor& proto,
    const NKikimrTxColumnShard::TIndexPortionBlobsInfo& protoBlobs) {
    NIceDb::TNiceDb db(Database);
    using IndexColumnsV2 = NColumnShard::Schema::IndexColumnsV2;
    db.Table<IndexColumnsV2>()
        .Key(portion.GetPathId().GetRawValue(), portion.GetPortionId())
        .Update(NIceDb::TUpdate<IndexColumnsV2::Metadata>(proto.SerializeAsString()))
        .Update(NIceDb::TUpdate<IndexColumnsV2::BlobIds>(protoBlobs.SerializeAsString()));
}

}   // namespace NKikimr::NOlap
