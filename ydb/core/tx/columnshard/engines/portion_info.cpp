#include "portion_info.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NOlap {

std::shared_ptr<arrow::RecordBatch> ISnapshotSchema::NormalizeBatch(const ISnapshotSchema& dataSchema, const std::shared_ptr<arrow::RecordBatch> batch) const {
    if (dataSchema.GetSnapshot() == GetSnapshot()) {
        return batch;
    }
    const std::shared_ptr<arrow::Schema>& resultArrowSchema = GetSchema();
    Y_VERIFY(dataSchema.GetSnapshot() < GetSnapshot());
    std::vector<std::shared_ptr<arrow::Array>> newColumns;
    newColumns.reserve(resultArrowSchema->num_fields());

    for (size_t i = 0; i < resultArrowSchema->fields().size(); ++i) {
        auto& resultField = resultArrowSchema->fields()[i];
        auto columnId = GetIndexInfo().GetColumnId(resultField->name());
        auto oldColumnIndex = dataSchema.GetFieldIndex(columnId);
        if (oldColumnIndex >= 0) { // ClumnExists
            auto oldColumnInfo = dataSchema.GetFieldByIndex(oldColumnIndex);
            Y_VERIFY(oldColumnInfo);
            auto columnData = batch->GetColumnByName(oldColumnInfo->name());
            Y_VERIFY(columnData);
            newColumns.push_back(columnData);
        } else { // AddNullColumn
            auto nullColumn = NArrow::MakeEmptyBatch(arrow::schema({resultField}), batch->num_rows());
            newColumns.push_back(nullColumn->column(0));
        }
    }
    return arrow::RecordBatch::Make(resultArrowSchema, batch->num_rows(), newColumns);
}

TString TPortionInfo::SerializeColumn(const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::Field>& field,
    const TColumnSaver saver) {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{ field });
    auto batch = arrow::RecordBatch::Make(schema, array->length(), { array });
    Y_VERIFY(batch);

    return saver.Apply(batch);
}

void TPortionInfo::AppendOneChunkColumn(TColumnRecord&& record) {
    record.Chunk = 0;
    Records.emplace_back(std::move(record));
}

void TPortionInfo::AddMinMax(ui32 columnId, const std::shared_ptr<arrow::Array>& column, bool sorted) {
    Y_VERIFY(column->length());

    std::pair<int, int> minMaxPos = {0, (column->length() - 1)};
    if (!sorted) {
        minMaxPos = NArrow::FindMinMaxPosition(column);
    }

    Y_VERIFY(minMaxPos.first >= 0);
    Y_VERIFY(minMaxPos.second >= 0);

    Meta.ColumnMeta[columnId].Min = NArrow::GetScalar(column, minMaxPos.first);
    Meta.ColumnMeta[columnId].Max = NArrow::GetScalar(column, minMaxPos.second);
}

void TPortionInfo::AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch,
                               const TString& tierName) {
    const auto& indexInfo = snapshotSchema.GetIndexInfo();
    const auto& minMaxColumns = indexInfo.GetMinMaxIdxColumns();

    TierName = tierName;
    Meta = {};
    Meta.FirstPkColumn = indexInfo.GetPKFirstColumnId();

    // Copy first and last key rows into new batch to free source batch's memory
    {
        auto keyBatch = NArrow::ExtractColumns(batch, indexInfo.GetReplaceKey());
        std::vector<bool> bits(batch->num_rows(), false);
        bits[0] = true;
        bits[batch->num_rows() - 1] = true; // it colud be 0 if batch has one row

        auto filter = NArrow::TColumnFilter(std::move(bits)).BuildArrowFilter(batch->num_rows());
        auto res = arrow::compute::Filter(keyBatch, filter);
        Y_VERIFY(res.ok());

        Meta.ReplaceKeyEdges = res->record_batch();
        Y_VERIFY(Meta.ReplaceKeyEdges->num_rows() == 1 || Meta.ReplaceKeyEdges->num_rows() == 2);
    }

    auto edgesBatch = NArrow::ExtractColumns(Meta.ReplaceKeyEdges, indexInfo.GetIndexKey());
    Meta.IndexKeyStart = NArrow::TReplaceKey::FromBatch(edgesBatch, 0);
    Meta.IndexKeyEnd = NArrow::TReplaceKey::FromBatch(edgesBatch, edgesBatch->num_rows() - 1);

    /// @note It does not add RawBytes info for snapshot columns, only for user ones.
    for (auto& [columnId, col] : indexInfo.Columns) {
        const auto& columnName = col.Name;
        auto column = batch->GetColumnByName(col.Name);
        Y_VERIFY(column);
        Meta.ColumnMeta[columnId].NumRows = column->length();
        Meta.ColumnMeta[columnId].RawBytes = NArrow::GetArrayDataSize(column);

        if (minMaxColumns.contains(columnId)) {
            auto column = batch->GetColumnByName(columnName);
            Y_VERIFY(column);

            bool isSorted = (columnId == Meta.FirstPkColumn);
            AddMinMax(columnId, column, isSorted);
            Y_VERIFY(Meta.HasMinMax(columnId));
        }
    }
}

TString TPortionInfo::GetMetadata(const TColumnRecord& rec) const {
    NKikimrTxColumnShard::TIndexColumnMeta meta; // TODO: move proto serialization out of engines folder
    if (Meta.ColumnMeta.contains(rec.ColumnId)) {
        const auto& columnMeta = Meta.ColumnMeta.find(rec.ColumnId)->second;
        if (auto numRows = columnMeta.NumRows) {
            meta.SetNumRows(numRows);
        }
        if (auto rawBytes = columnMeta.RawBytes) {
            meta.SetRawBytes(rawBytes);
        }
        if (columnMeta.HasMinMax()) {
            ScalarToConstant(*columnMeta.Min, *meta.MutableMinValue());
            ScalarToConstant(*columnMeta.Max, *meta.MutableMaxValue());
        }
    }

    if (rec.ColumnId == Meta.FirstPkColumn) {
        auto* portionMeta = meta.MutablePortionMeta();

        switch (Meta.Produced) {
            case TPortionMeta::UNSPECIFIED:
                Y_VERIFY(false);
            case TPortionMeta::INSERTED:
                portionMeta->SetIsInserted(true);
                break;
            case TPortionMeta::COMPACTED:
                portionMeta->SetIsCompacted(true);
                break;
            case TPortionMeta::SPLIT_COMPACTED:
                portionMeta->SetIsSplitCompacted(true);
                break;
            case TPortionMeta::EVICTED:
                portionMeta->SetIsEvicted(true);
                break;
            case TPortionMeta::INACTIVE:
                Y_FAIL("Unexpected inactive case");
                //portionMeta->SetInactive(true);
                break;
        }

        if (!TierName.empty()) {
            portionMeta->SetTierName(TierName);
        }

        if (const auto& keyEdgesBatch = Meta.ReplaceKeyEdges) {
            Y_VERIFY(keyEdgesBatch);
            Y_VERIFY_DEBUG(keyEdgesBatch->ValidateFull().ok());
            Y_VERIFY(keyEdgesBatch->num_rows() == 1 || keyEdgesBatch->num_rows() == 2);
            portionMeta->SetPrimaryKeyBorders(NArrow::SerializeBatchNoCompression(keyEdgesBatch));
        }
    }

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);
    return out;
}

void TPortionInfo::LoadMetadata(const TIndexInfo& indexInfo, const TColumnRecord& rec) {
    if (rec.Metadata.empty()) {
        return;
    }

    NKikimrTxColumnShard::TIndexColumnMeta meta;
    bool ok = meta.ParseFromString(rec.Metadata);
    Y_VERIFY(ok);

    Meta.FirstPkColumn = indexInfo.GetPKFirstColumnId();
    auto field = indexInfo.ArrowColumnField(rec.ColumnId);
    const bool compositeIndexKey = indexInfo.IsCompositeIndexKey();

    if (meta.HasPortionMeta()) {
        Y_VERIFY_DEBUG(rec.ColumnId == Meta.FirstPkColumn);

        auto& portionMeta = meta.GetPortionMeta();
        TierName = portionMeta.GetTierName();

        if (portionMeta.GetIsInserted()) {
            Meta.Produced = TPortionMeta::INSERTED;
        } else if (portionMeta.GetIsCompacted()) {
            Meta.Produced = TPortionMeta::COMPACTED;
        } else if (portionMeta.GetIsSplitCompacted()) {
            Meta.Produced = TPortionMeta::SPLIT_COMPACTED;
        } else if (portionMeta.GetIsEvicted()) {
            Meta.Produced = TPortionMeta::EVICTED;
        }

        if (portionMeta.HasPrimaryKeyBorders()) {
            Meta.ReplaceKeyEdges = NArrow::DeserializeBatch(portionMeta.GetPrimaryKeyBorders(), indexInfo.GetReplaceKey());
            Y_VERIFY(Meta.ReplaceKeyEdges);
            Y_VERIFY_DEBUG(Meta.ReplaceKeyEdges->ValidateFull().ok());
            Y_VERIFY(Meta.ReplaceKeyEdges->num_rows() == 1 || Meta.ReplaceKeyEdges->num_rows() == 2);

            if (compositeIndexKey) {
                auto edgesBatch = NArrow::ExtractColumns(Meta.ReplaceKeyEdges, indexInfo.GetIndexKey());
                Y_VERIFY(edgesBatch);
                Meta.IndexKeyStart = NArrow::TReplaceKey::FromBatch(edgesBatch, 0);
                Meta.IndexKeyEnd = NArrow::TReplaceKey::FromBatch(edgesBatch, edgesBatch->num_rows() - 1);
            }
        }
    }
    if (meta.HasNumRows()) {
        Meta.ColumnMeta[rec.ColumnId].NumRows = meta.GetNumRows();
    }
    if (meta.HasRawBytes()) {
        Meta.ColumnMeta[rec.ColumnId].RawBytes = meta.GetRawBytes();
    }
    if (meta.HasMinValue()) {
        auto scalar = ConstantToScalar(meta.GetMinValue(), field->type());
        Meta.ColumnMeta[rec.ColumnId].Min = scalar;

        // Restore Meta.IndexKeyStart for one column IndexKey
        if (!compositeIndexKey && rec.ColumnId == Meta.FirstPkColumn) {
            Meta.IndexKeyStart = NArrow::TReplaceKey::FromScalar(scalar);
        }
    }
    if (meta.HasMaxValue()) {
        auto scalar = ConstantToScalar(meta.GetMaxValue(), field->type());
        Meta.ColumnMeta[rec.ColumnId].Max = scalar;

        // Restore Meta.IndexKeyEnd for one column IndexKey
        if (!compositeIndexKey && rec.ColumnId == Meta.FirstPkColumn) {
            Meta.IndexKeyEnd = NArrow::TReplaceKey::FromScalar(scalar);
        }
    }

    // Portion genarated without PrimaryKeyBorders and loaded with indexInfo.IsCompositeIndexKey()
    // We should have no such portions for ForceColumnTablesCompositeMarks feature
    Y_VERIFY(Meta.IndexKeyStart && Meta.IndexKeyEnd);
}

std::tuple<std::shared_ptr<arrow::Scalar>, std::shared_ptr<arrow::Scalar>> TPortionInfo::MinMaxValue(const ui32 columnId) const {
    auto it = Meta.ColumnMeta.find(columnId);
    if (it == Meta.ColumnMeta.end()) {
        return std::make_tuple(std::shared_ptr<arrow::Scalar>(), std::shared_ptr<arrow::Scalar>());
    } else {
        return std::make_tuple(it->second.Min, it->second.Max);
    }
}

std::shared_ptr<arrow::Scalar> TPortionInfo::MinValue(ui32 columnId) const {
    if (!Meta.ColumnMeta.contains(columnId)) {
        return {};
    }
    return Meta.ColumnMeta.find(columnId)->second.Min;
}

std::shared_ptr<arrow::Scalar> TPortionInfo::MaxValue(ui32 columnId) const {
    if (!Meta.ColumnMeta.contains(columnId)) {
        return {};
    }
    return Meta.ColumnMeta.find(columnId)->second.Max;
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
    std::vector< std::shared_ptr<arrow::Field>> fields;
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
