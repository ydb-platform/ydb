#include "defs.h"
#include "db_wrapper.h"
#include "portions/constructor.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/sharding/sharding.h>

namespace NKikimr::NOlap {

void TDbWrapper::Insert(const TInsertedData& data) {
    NIceDb::TNiceDb db(Database);
    NColumnShard::Schema::InsertTable_Insert(db, data);
}

void TDbWrapper::Commit(const TInsertedData& data) {
    NIceDb::TNiceDb db(Database);
    NColumnShard::Schema::InsertTable_Commit(db, data);
}

void TDbWrapper::Abort(const TInsertedData& data) {
    NIceDb::TNiceDb db(Database);
    NColumnShard::Schema::InsertTable_Abort(db, data);
}

void TDbWrapper::EraseInserted(const TInsertedData& data) {
    NIceDb::TNiceDb db(Database);
    NColumnShard::Schema::InsertTable_EraseInserted(db, data);
}

void TDbWrapper::EraseCommitted(const TInsertedData& data) {
    NIceDb::TNiceDb db(Database);
    NColumnShard::Schema::InsertTable_EraseCommitted(db, data);
}

void TDbWrapper::EraseAborted(const TInsertedData& data) {
    NIceDb::TNiceDb db(Database);
    NColumnShard::Schema::InsertTable_EraseAborted(db, data);
}

bool TDbWrapper::Load(TInsertTableAccessor& insertTable,
                      const TInstant& loadTime) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::InsertTable_Load(db, DsGroupSelector, insertTable, loadTime);
}

void TDbWrapper::WriteColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) {
    NIceDb::TNiceDb db(Database);
    auto rowProto = row.GetMeta().SerializeToProto();
    if (row.GetChunkIdx() == 0 && row.GetColumnId() == firstPKColumnId) {
        *rowProto.MutablePortionMeta() = portion.GetMeta().SerializeToProto();
    }
    using IndexColumns = NColumnShard::Schema::IndexColumns;
    auto removeSnapshot = portion.GetRemoveSnapshotOptional();
    db.Table<IndexColumns>().Key(0, portion.GetPathId(), row.ColumnId,
        portion.GetMinSnapshotDeprecated().GetPlanStep(), portion.GetMinSnapshotDeprecated().GetTxId(), portion.GetPortion(), row.Chunk).Update(
            NIceDb::TUpdate<IndexColumns::XPlanStep>(removeSnapshot ? removeSnapshot->GetPlanStep() : 0),
            NIceDb::TUpdate<IndexColumns::XTxId>(removeSnapshot ? removeSnapshot->GetTxId() : 0),
            NIceDb::TUpdate<IndexColumns::Blob>(portion.GetBlobId(row.GetBlobRange().GetBlobIdxVerified()).SerializeBinary()),
            NIceDb::TUpdate<IndexColumns::Metadata>(rowProto.SerializeAsString()),
            NIceDb::TUpdate<IndexColumns::Offset>(row.BlobRange.Offset),
            NIceDb::TUpdate<IndexColumns::Size>(row.BlobRange.Size),
            NIceDb::TUpdate<IndexColumns::PathId>(portion.GetPathId())
        );
}

void TDbWrapper::WritePortion(const NOlap::TPortionInfo& portion) {
    NIceDb::TNiceDb db(Database);
    auto metaProto = portion.GetMeta().SerializeToProto();
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    auto removeSnapshot = portion.GetRemoveSnapshotOptional();
    db.Table<IndexPortions>().Key(portion.GetPathId(), portion.GetPortion()).Update(
        NIceDb::TUpdate<IndexPortions::SchemaVersion>(portion.GetSchemaVersionVerified()),
        NIceDb::TUpdate<IndexPortions::ShardingVersion>(portion.GetShardingVersionDef(0)),
        NIceDb::TUpdate<IndexPortions::XPlanStep>(removeSnapshot ? removeSnapshot->GetPlanStep() : 0),
        NIceDb::TUpdate<IndexPortions::XTxId>(removeSnapshot ? removeSnapshot->GetTxId() : 0),
        NIceDb::TUpdate<IndexPortions::Metadata>(metaProto.SerializeAsString()));
}

void TDbWrapper::ErasePortion(const NOlap::TPortionInfo& portion) {
    NIceDb::TNiceDb db(Database);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    db.Table<IndexPortions>().Key(portion.GetPathId(), portion.GetPortion()).Delete();
}

void TDbWrapper::EraseColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) {
    NIceDb::TNiceDb db(Database);
    using IndexColumns = NColumnShard::Schema::IndexColumns;
    db.Table<IndexColumns>().Key(0, portion.GetPathId(), row.ColumnId,
        portion.GetMinSnapshotDeprecated().GetPlanStep(), portion.GetMinSnapshotDeprecated().GetTxId(), portion.GetPortion(), row.Chunk).Delete();
}

bool TDbWrapper::LoadColumns(const std::function<void(NOlap::TPortionInfoConstructor&&, const TColumnChunkLoadContext&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexColumns = NColumnShard::Schema::IndexColumns;
    auto rowset = db.Table<IndexColumns>().Prefix(0).Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        NOlap::TSnapshot minSnapshot(rowset.GetValue<IndexColumns::PlanStep>(), rowset.GetValue<IndexColumns::TxId>());
        NOlap::TSnapshot removeSnapshot(rowset.GetValue<IndexColumns::XPlanStep>(), rowset.GetValue<IndexColumns::XTxId>());

        NOlap::TPortionInfoConstructor constructor(rowset.GetValue<IndexColumns::PathId>(), rowset.GetValue<IndexColumns::Portion>());
        constructor.SetMinSnapshotDeprecated(minSnapshot);
        constructor.SetRemoveSnapshot(removeSnapshot);

        NOlap::TColumnChunkLoadContext chunkLoadContext(rowset, DsGroupSelector);
        callback(std::move(constructor), chunkLoadContext);

        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

bool TDbWrapper::LoadPortions(const std::function<void(NOlap::TPortionInfoConstructor&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexPortions = NColumnShard::Schema::IndexPortions;
    auto rowset = db.Table<IndexPortions>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        NOlap::TPortionInfoConstructor portion(rowset.GetValue<IndexPortions::PathId>(), rowset.GetValue<IndexPortions::PortionId>());
        portion.SetSchemaVersion(rowset.GetValue<IndexPortions::SchemaVersion>());
        if (rowset.HaveValue<IndexPortions::ShardingVersion>() && rowset.GetValue<IndexPortions::ShardingVersion>()) {
            portion.SetShardingVersion(rowset.GetValue<IndexPortions::ShardingVersion>());
        }
        portion.SetRemoveSnapshot(rowset.GetValue<IndexPortions::XPlanStep>(), rowset.GetValue<IndexPortions::XTxId>());

        NKikimrTxColumnShard::TIndexPortionMeta metaProto;
        const TString metadata = rowset.template GetValue<NColumnShard::Schema::IndexPortions::Metadata>();
        AFL_VERIFY(metaProto.ParseFromArray(metadata.data(), metadata.size()))("event", "cannot parse metadata as protobuf");
        callback(std::move(portion), metaProto);

        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

void TDbWrapper::WriteIndex(const TPortionInfo& portion, const TIndexChunk& row) {
    AFL_VERIFY(row.GetBlobRange().IsValid());
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    NIceDb::TNiceDb db(Database);
    db.Table<IndexIndexes>().Key(portion.GetPathId(), portion.GetPortionId(), row.GetIndexId(), row.GetChunkIdx()).Update(
            NIceDb::TUpdate<IndexIndexes::Blob>(portion.GetBlobId(row.GetBlobRange().GetBlobIdxVerified()).SerializeBinary()),
            NIceDb::TUpdate<IndexIndexes::Offset>(row.GetBlobRange().Offset),
            NIceDb::TUpdate<IndexIndexes::Size>(row.GetBlobRange().Size),
            NIceDb::TUpdate<IndexIndexes::RecordsCount>(row.GetRecordsCount()),
            NIceDb::TUpdate<IndexIndexes::RawBytes>(row.GetRawBytes())
        );
}

void TDbWrapper::EraseIndex(const TPortionInfo& portion, const TIndexChunk& row) {
    NIceDb::TNiceDb db(Database);
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    db.Table<IndexIndexes>().Key(portion.GetPathId(), portion.GetPortionId(), row.GetIndexId(), 0).Delete();
}

bool TDbWrapper::LoadIndexes(const std::function<void(const ui64 pathId, const ui64 portionId, const TIndexChunkLoadContext&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexIndexes = NColumnShard::Schema::IndexIndexes;
    auto rowset = db.Table<IndexIndexes>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        NOlap::TIndexChunkLoadContext chunkLoadContext(rowset, DsGroupSelector);
        callback(rowset.GetValue<IndexIndexes::PathId>(), rowset.GetValue<IndexIndexes::PortionId>(), chunkLoadContext);

        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

void TDbWrapper::WriteCounter(ui32 counterId, ui64 value) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::IndexCounters_Write(db, counterId, value);
}

bool TDbWrapper::LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) {
    NIceDb::TNiceDb db(Database);
    return NColumnShard::Schema::IndexCounters_Load(db, callback);
}

TConclusion<THashMap<ui64, std::map<NOlap::TSnapshot, TGranuleShardingInfo>>> TDbWrapper::LoadGranulesShardingInfo() {
    using Schema = NColumnShard::Schema;
    NIceDb::TNiceDb db(Database);
    auto rowset = db.Table<Schema::ShardingInfo>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read rowset");
    }
    THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>> result;
    while (!rowset.EndOfSet()) {
        NOlap::TSnapshot snapshot = NOlap::TSnapshot::Zero();
        snapshot.DeserializeFromString(rowset.GetValue<Schema::ShardingInfo::Snapshot>()).Validate();
        NSharding::TGranuleShardingLogicContainer logic;
        logic.DeserializeFromString(rowset.GetValue<Schema::ShardingInfo::Logic>()).Validate();
        TGranuleShardingInfo gShardingInfo(logic, snapshot, rowset.GetValue<Schema::ShardingInfo::VersionId>(), rowset.GetValue<Schema::ShardingInfo::PathId>());
        AFL_VERIFY(result[gShardingInfo.GetPathId()].emplace(gShardingInfo.GetSinceSnapshot(), gShardingInfo).second);

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read rowset");
        }
    }
    return result;
}

}
