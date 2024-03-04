#include "portion_info.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <util/system/tls.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap {

const TColumnRecord& TPortionInfo::AppendOneChunkColumn(TColumnRecord&& record) {
    Y_ABORT_UNLESS(record.ColumnId);
    std::optional<ui32> maxChunk;
    for (auto&& i : Records) {
        if (i.ColumnId == record.ColumnId) {
            if (!maxChunk) {
                maxChunk = i.Chunk;
            } else {
                Y_ABORT_UNLESS(*maxChunk + 1 == i.Chunk);
                maxChunk = i.Chunk;
            }
        }
    }
    if (maxChunk) {
        AFL_VERIFY(*maxChunk + 1 == record.Chunk)("max", *maxChunk)("record", record.Chunk);
    } else {
        AFL_VERIFY(0 == record.Chunk)("record", record.Chunk);
    }
    Records.emplace_back(std::move(record));
    return Records.back();
}

void TPortionInfo::AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch, const TString& tierName) {
    Y_ABORT_UNLESS(batch->num_rows() == NumRows());
    AddMetadata(snapshotSchema, NArrow::TFirstLastSpecialKeys(NArrow::ExtractColumns(batch, snapshotSchema.GetIndexInfo().GetReplaceKey())),
        NArrow::TMinMaxSpecialKeys(batch, TIndexInfo::ArrowSchemaSnapshot()), tierName);
}

void TPortionInfo::AddMetadata(const ISnapshotSchema& snapshotSchema, const NArrow::TFirstLastSpecialKeys& primaryKeys, const NArrow::TMinMaxSpecialKeys& snapshotKeys, const TString& tierName) {
    const auto& indexInfo = snapshotSchema.GetIndexInfo();
    Meta.FirstPkColumn = indexInfo.GetPKFirstColumnId();
    Meta.FillBatchInfo(primaryKeys, snapshotKeys, indexInfo);
    Meta.SetTierName(tierName);
}

std::shared_ptr<arrow::Scalar> TPortionInfo::MinValue(ui32 columnId) const {
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& i : Records) {
        if (i.ColumnId == columnId) {
            if (!i.GetMeta().GetMin()) {
                return nullptr;
            }
            if (!result || NArrow::ScalarCompare(result, i.GetMeta().GetMin()) > 0) {
                result = i.GetMeta().GetMin();
            }
        }
    }
    return result;
}

std::shared_ptr<arrow::Scalar> TPortionInfo::MaxValue(ui32 columnId) const {
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& i : Records) {
        if (i.ColumnId == columnId) {
            if (!i.GetMeta().GetMax()) {
                return nullptr;
            }
            if (!result || NArrow::ScalarCompare(result, i.GetMeta().GetMax()) < 0) {
                result = i.GetMeta().GetMax();
            }
        }
    }
    return result;
}

TPortionInfo TPortionInfo::CopyWithFilteredColumns(const THashSet<ui32>& columnIds) const {
    TPortionInfo result(PathId, Portion, GetMinSnapshot(), BlobsOperator);
    result.Meta = Meta;
    result.Records.reserve(columnIds.size());

    for (auto& rec : Records) {
        Y_ABORT_UNLESS(rec.Valid());
        if (columnIds.contains(rec.ColumnId)) {
            result.Records.push_back(rec);
        }
    }
    return result;
}

ui64 TPortionInfo::GetRawBytes(const std::vector<ui32>& columnIds) const {
    ui64 sum = 0;
    const ui32 numRows = NumRows();
    for (auto&& i : columnIds) {
        if (TIndexInfo::IsSpecialColumn(i)) {
            sum += numRows * TIndexInfo::GetSpecialColumnByteWidth(i);
        } else {
            for (auto&& r : Records) {
                if (r.ColumnId == i) {
                    sum += r.GetMeta().GetRawBytesVerified();
                }
            }
        }
    }
    return sum;
}

ui64 TPortionInfo::GetRawBytes(const std::set<ui32>& columnIds) const {
    ui64 sum = 0;
    const ui32 numRows = NumRows();
    for (auto&& i : TIndexInfo::GetSpecialColumnIds()) {
        if (columnIds.contains(i)) {
            sum += numRows * TIndexInfo::GetSpecialColumnByteWidth(i);
        }
    }
    for (auto&& r : Records) {
        if (columnIds.contains(r.ColumnId)) {
            sum += r.GetMeta().GetRawBytesVerified();
        }
    }
    return sum;
}

int TPortionInfo::CompareSelfMaxItemMinByPk(const TPortionInfo& item, const TIndexInfo& info) const {
    return CompareByColumnIdsImpl<TMaxGetter, TMinGetter>(item, info.KeyColumns);
}

int TPortionInfo::CompareMinByPk(const TPortionInfo& item, const TIndexInfo& info) const {
    return CompareMinByColumnIds(item, info.KeyColumns);
}

TString TPortionInfo::DebugString(const bool withDetails) const {
    TStringBuilder sb;
    sb << "(portion_id:" << Portion << ";" <<
        "path_id:" << PathId << ";records_count:" << NumRows() << ";"
        "min_schema_snapshot:(" << MinSnapshot.DebugString() << ");";
    if (withDetails) {
        sb <<
            "records_snapshot_min:(" << RecordSnapshotMin().DebugString() << ");" <<
            "records_snapshot_max:(" << RecordSnapshotMax().DebugString() << ");" <<
            "from:" << IndexKeyStart().DebugString() << ";" <<
            "to:" << IndexKeyEnd().DebugString() << ";";
    }
    sb <<
        "size:" << BlobsBytes() << ";" <<
        "meta:(" << Meta.DebugString() << ");";
    if (RemoveSnapshot.Valid()) {
        sb << "remove_snapshot:(" << RemoveSnapshot.DebugString() << ");";
    }
    if (BlobsOperator) {
        sb << "blobs_operator:" << BlobsOperator->DebugString() << ";";
    }
    sb << "chunks:(" << Records.size() << ");";
    if (IS_TRACE_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        std::set<TString> blobIds;
        for (auto&& i : Records) {
            blobIds.emplace(::ToString(i.BlobRange.BlobId));
        }
        sb << "blobs:" << JoinSeq(",", blobIds) << ";blobs_count:" << blobIds.size() << ";";
    }
    return sb << ")";
}

void TPortionInfo::AddRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec, const NKikimrTxColumnShard::TIndexPortionMeta* portionMeta) {
    Records.push_back(rec);

    if (portionMeta) {
        Meta.FirstPkColumn = indexInfo.GetPKFirstColumnId();
        Y_ABORT_UNLESS(Meta.DeserializeFromProto(*portionMeta, indexInfo));
    }
}

bool TPortionInfo::HasPkMinMax() const {
    bool result = false;
    for (auto&& i : Records) {
        if (i.ColumnId == Meta.FirstPkColumn) {
            if (!i.GetMeta().HasMinMax()) {
                return false;
            }
            result = true;
        }
    }
    return result;
}

std::vector<const NKikimr::NOlap::TColumnRecord*> TPortionInfo::GetColumnChunksPointers(const ui32 columnId) const {
    std::vector<const TColumnRecord*> result;
    for (auto&& c : Records) {
        if (c.ColumnId == columnId) {
            Y_ABORT_UNLESS(c.Chunk == result.size());
            Y_ABORT_UNLESS(c.GetMeta().GetNumRowsVerified());
            result.emplace_back(&c);
        }
    }
    return result;
}

size_t TPortionInfo::NumBlobs() const {
    THashSet<TUnifiedBlobId> blobIds;
    for (auto&& i : Records) {
        blobIds.emplace(i.BlobRange.BlobId);
    }
    return blobIds.size();
}

bool TPortionInfo::IsEqualWithSnapshots(const TPortionInfo& item) const {
    return PathId == item.PathId && MinSnapshot == item.MinSnapshot
        && Portion == item.Portion && RemoveSnapshot == item.RemoveSnapshot;
}

std::shared_ptr<arrow::ChunkedArray> TPortionInfo::TPreparedColumn::Assemble() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(Blobs.size());
    for (auto& blob : Blobs) {
        batches.push_back(blob.BuildRecordBatch(*Loader));
        Y_ABORT_UNLESS(batches.back());
    }

    auto res = arrow::Table::FromRecordBatches(batches);
    Y_VERIFY_S(res.ok(), res.status().message());
    return (*res)->column(0);
}

std::shared_ptr<arrow::RecordBatch> TPortionInfo::TAssembleBlobInfo::BuildRecordBatch(const TColumnLoader& loader) const {
    if (NullRowsCount) {
        Y_ABORT_UNLESS(!Data);
        return NArrow::MakeEmptyBatch(loader.GetExpectedSchema(), NullRowsCount);
    } else {
        auto result = loader.Apply(Data);
        if (!result.ok()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot unpack batch")("error", result.status().ToString())("loader", loader.DebugString());
            return nullptr;
        }
        return *result;
    }
}

std::shared_ptr<arrow::Table> TPortionInfo::TPreparedBatchData::AssembleTable(const TAssembleOptions& options) const {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Columns) {
        if (!options.IsAcceptedColumn(i.GetColumnId())) {
            continue;
        }
        columns.emplace_back(i.Assemble());
        fields.emplace_back(i.GetField());
    }

    return arrow::Table::Make(std::make_shared<arrow::Schema>(fields), columns);
}

std::shared_ptr<arrow::RecordBatch> TPortionInfo::TPreparedBatchData::Assemble(const TAssembleOptions& options) const {
    auto table = AssembleTable(options);
    auto res = table->CombineChunks();
    Y_ABORT_UNLESS(res.ok());
    return NArrow::ToBatch(*res);
}

}
