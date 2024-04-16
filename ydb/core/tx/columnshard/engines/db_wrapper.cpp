#include "defs.h"
#include "db_wrapper.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

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

void TDbWrapper::WriteColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) {
    NIceDb::TNiceDb db(Database);
    auto proto = portion.GetMeta().SerializeToProto(row.ColumnId, row.Chunk);
    auto rowProto = row.GetMeta().SerializeToProto();
    if (proto) {
        *rowProto.MutablePortionMeta() = std::move(*proto);
    }
    using IndexColumns = NColumnShard::Schema::IndexColumns;
    db.Table<IndexColumns>().Key(0, portion.GetDeprecatedGranuleId(), row.ColumnId,
        portion.GetMinSnapshot().GetPlanStep(), portion.GetMinSnapshot().GetTxId(), portion.GetPortion(), row.Chunk).Update(
            NIceDb::TUpdate<IndexColumns::XPlanStep>(portion.GetRemoveSnapshot().GetPlanStep()),
            NIceDb::TUpdate<IndexColumns::XTxId>(portion.GetRemoveSnapshot().GetTxId()),
            NIceDb::TUpdate<IndexColumns::Blob>(portion.GetBlobId(row.GetBlobRange().GetBlobIdxVerified()).SerializeBinary()),
            NIceDb::TUpdate<IndexColumns::Metadata>(rowProto.SerializeAsString()),
            NIceDb::TUpdate<IndexColumns::Offset>(row.BlobRange.Offset),
            NIceDb::TUpdate<IndexColumns::Size>(row.BlobRange.Size),
            NIceDb::TUpdate<IndexColumns::PathId>(portion.GetPathId())
        );
}

void TDbWrapper::EraseColumn(const NOlap::TPortionInfo& portion, const TColumnRecord& row) {
    NIceDb::TNiceDb db(Database);
    using IndexColumns = NColumnShard::Schema::IndexColumns;
    db.Table<IndexColumns>().Key(0, portion.GetDeprecatedGranuleId(), row.ColumnId,
        portion.GetMinSnapshot().GetPlanStep(), portion.GetMinSnapshot().GetTxId(), portion.GetPortion(), row.Chunk).Delete();
}

bool TDbWrapper::LoadColumns(const std::function<void(const NOlap::TPortionInfo&, const TColumnChunkLoadContext&)>& callback) {
    NIceDb::TNiceDb db(Database);
    using IndexColumns = NColumnShard::Schema::IndexColumns;
    auto rowset = db.Table<IndexColumns>().Prefix(0).Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        NOlap::TPortionInfo portion = NOlap::TPortionInfo::BuildEmpty();
        portion.SetPathId(rowset.GetValue<IndexColumns::PathId>());
        portion.SetMinSnapshot(rowset.GetValue<IndexColumns::PlanStep>(), rowset.GetValue<IndexColumns::TxId>());
        portion.SetPortion(rowset.GetValue<IndexColumns::Portion>());
        portion.SetDeprecatedGranuleId(rowset.GetValue<IndexColumns::Granule>());

        NOlap::TColumnChunkLoadContext chunkLoadContext(rowset, DsGroupSelector);

        portion.SetRemoveSnapshot(rowset.GetValue<IndexColumns::XPlanStep>(), rowset.GetValue<IndexColumns::XTxId>());

        callback(portion, chunkLoadContext);

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

}
