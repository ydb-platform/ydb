#include "portion_info.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NOlap {

const TColumnRecord& TPortionInfo::AppendOneChunkColumn(TColumnRecord&& record) {
    Y_VERIFY(record.ColumnId);
    std::optional<ui32> maxChunk;
    for (auto&& i : Records) {
        if (i.ColumnId == record.ColumnId) {
            if (!maxChunk) {
                maxChunk = i.Chunk;
            } else {
                Y_VERIFY(*maxChunk + 1 == i.Chunk);
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

void TPortionInfo::AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch,
                               const TString& tierName) {
    const auto& indexInfo = snapshotSchema.GetIndexInfo();

    Meta = {};
    Meta.FirstPkColumn = indexInfo.GetPKFirstColumnId();
    Meta.FillBatchInfo(batch, indexInfo);
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
    TPortionInfo result(Granule, Portion, GetMinSnapshot());
    result.Meta = Meta;
    result.Records.reserve(columnIds.size());

    for (auto& rec : Records) {
        Y_VERIFY(rec.Valid());
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
                    sum += r.GetMeta().GetRawBytes();
                }
            }
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

TString TPortionInfo::DebugString() const {
    TStringBuilder sb;
    sb << "(portion_id:" << Portion << ";" <<
        "granule_id:" << Granule << ";records_count:" << NumRows() << ";"
        "min_snapshot:(" << MinSnapshot.DebugString() << ");" <<
        "size:" << BlobsBytes() << ";" <<
        "meta:(" << Meta.DebugString() << ");";
    if (RemoveSnapshot.Valid()) {
        sb << "remove_snapshot:(" << RemoveSnapshot.DebugString() << ");";
    }
    sb << "meta:(" << Meta.DebugString() << ");";
    sb << "chunks:(" << Records.size() << ");";
    return sb << ")";
}

void TPortionInfo::AddRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec, const NKikimrTxColumnShard::TIndexPortionMeta* portionMeta) {
    Records.push_back(rec);

    if (portionMeta) {
        Meta.FirstPkColumn = indexInfo.GetPKFirstColumnId();
        Y_VERIFY(Meta.DeserializeFromProto(*portionMeta, indexInfo));
    }
    if (!indexInfo.IsCompositeIndexKey() && indexInfo.GetPKFirstColumnId() == rec.ColumnId) {
        if (rec.GetMeta().GetMin()) {
            auto candidate = NArrow::TReplaceKey::FromScalar(rec.GetMeta().GetMin());
            if (!Meta.IndexKeyStart || candidate < *Meta.IndexKeyStart) {
                Meta.IndexKeyStart = candidate;
            }
        }
        if (rec.GetMeta().GetMax()) {
            auto candidate = NArrow::TReplaceKey::FromScalar(rec.GetMeta().GetMax());
            if (!Meta.IndexKeyEnd || *Meta.IndexKeyEnd < candidate) {
                Meta.IndexKeyEnd = candidate;
            }
        }
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

std::shared_ptr<arrow::ChunkedArray> TPortionInfo::TPreparedColumn::Assemble() const {
    Y_VERIFY(!Blobs.empty());

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(Blobs.size());
    for (auto& blob : Blobs) {
        batches.push_back(blob.BuildRecordBatch(*Loader));
        Y_VERIFY(batches.back());
    }

    auto res = arrow::Table::FromRecordBatches(batches);
    Y_VERIFY_S(res.ok(), res.status().message());
    return (*res)->column(0);
}

std::shared_ptr<arrow::RecordBatch> TPortionInfo::TPreparedBatchData::Assemble(const TAssembleOptions& options) const {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Columns) {
        if (!options.IsAcceptedColumn(i.GetColumnId())) {
            continue;
        }
        columns.emplace_back(i.Assemble());
        fields.emplace_back(i.GetField());
    }

    auto table = arrow::Table::Make(std::make_shared<arrow::Schema>(fields), columns);
    auto res = table->CombineChunks();
    Y_VERIFY(res.ok());
    return NArrow::ToBatch(*res);
}

}
