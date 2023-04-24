#include "portion_info.h"
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NOlap {

TString TPortionInfo::SerializeColumn(const std::shared_ptr<arrow::Array>& array,
                                      const std::shared_ptr<arrow::Field>& field,
                                      const arrow::ipc::IpcWriteOptions& writeOptions)
{
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});
    Y_VERIFY(batch);

    return NArrow::SerializeBatch(batch, writeOptions);
}

TString TPortionInfo::AddOneChunkColumn(const std::shared_ptr<arrow::Array>& array,
                                        const std::shared_ptr<arrow::Field>& field,
                                        TColumnRecord&& record,
                                        const arrow::ipc::IpcWriteOptions& writeOptions,
                                        ui32 limitBytes) {
    auto blob = SerializeColumn(array, field, writeOptions);
    if (blob.size() >= limitBytes) {
        return {};
    }

    record.Chunk = 0;
    Records.emplace_back(std::move(record));
    return blob;
}

TPortionInfo::TPreparedBatchData TPortionInfo::PrepareForAssemble(const TIndexInfo& indexInfo,
                                                     const std::shared_ptr<arrow::Schema>& schema,
                                                     const THashMap<TBlobRange, TString>& blobsData) const {
    // Correct records order
    TMap<int, TMap<ui32, TBlobRange>> columnChunks; // position in schema -> ordered chunks

    for (auto& rec : Records) {
        ui32 columnId = rec.ColumnId;
        TString columnName = indexInfo.GetColumnName(columnId);
        std::string name(columnName.data(), columnName.size());
        int pos = schema->GetFieldIndex(name);
        if (pos < 0) {
            continue; // no such column in schema - do not need it
        }

        columnChunks[pos][rec.Chunk] = rec.BlobRange;
    }

    // Make chunked arrays for columns
    std::vector<TPreparedColumn> columns;
    columns.reserve(columnChunks.size());

    for (auto& [pos, orderedChunks] : columnChunks) {
        auto field = schema->field(pos);

        TVector<TString> blobs;
        blobs.reserve(orderedChunks.size());
        ui32 expected = 0;
        for (auto& [chunk, blobId] : orderedChunks) {
            Y_VERIFY(chunk == expected);
            ++expected;

            auto it = blobsData.find(blobId);
            Y_VERIFY(it != blobsData.end());
            TString data = it->second;
            blobs.push_back(data);
        }

        columns.emplace_back(TPreparedColumn(field, std::move(blobs)));
    }

    return TPreparedBatchData(std::move(columns), schema);
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

void TPortionInfo::AddMetadata(const TIndexInfo& indexInfo, const std::shared_ptr<arrow::RecordBatch>& batch,
                               const TString& tierName) {
    TierName = tierName;
    Meta = {};

    /// @note It does not add RawBytes info for snapshot columns, only for user ones.
    for (auto& [columnId, col] : indexInfo.Columns) {
        auto column = batch->GetColumnByName(col.Name);
        Y_VERIFY(column);
        Meta.ColumnMeta[columnId].NumRows = column->length();
        Meta.ColumnMeta[columnId].RawBytes = NArrow::GetArrayDataSize(column);
    }

    for (auto& mmxColumnId : indexInfo.GetMinMaxIdxColumns()) {
        auto columnName = indexInfo.GetColumnName(mmxColumnId, true);
        auto column = batch->GetColumnByName(columnName);
        Y_VERIFY(column);

        bool isSorted = false;
        if (mmxColumnId == indexInfo.GetPKFirstColumnId()) {
            FirstPkColumn = mmxColumnId;
            isSorted = true;
        }
        AddMinMax(mmxColumnId, column, isSorted);
        Y_VERIFY(HasMinMax(mmxColumnId));
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

    if (rec.ColumnId == FirstPkColumn) {
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

    FirstPkColumn = indexInfo.GetPKFirstColumnId();
    auto field = indexInfo.ArrowColumnField(rec.ColumnId);

    if (meta.HasPortionMeta()) {
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
    }
    if (meta.HasNumRows()) {
        Meta.ColumnMeta[rec.ColumnId].NumRows = meta.GetNumRows();
    }
    if (meta.HasRawBytes()) {
        Meta.ColumnMeta[rec.ColumnId].RawBytes = meta.GetRawBytes();
    }
    if (meta.HasMinValue()) {
        Meta.ColumnMeta[rec.ColumnId].Min = ConstantToScalar(meta.GetMinValue(), field->type());
    }
    if (meta.HasMaxValue()) {
        Meta.ColumnMeta[rec.ColumnId].Max = ConstantToScalar(meta.GetMaxValue(), field->type());
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

std::shared_ptr<arrow::ChunkedArray> TPortionInfo::TPreparedColumn::Assemble(const ui32 needCount, const bool reverse) const {
    Y_VERIFY(!Blobs.empty());
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{ Field });

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(Blobs.size());
    ui32 count = 0;
    if (!reverse) {
        for (auto& blob : Blobs) {
            batches.push_back(NArrow::DeserializeBatch(blob, schema));
            Y_VERIFY(batches.back());
            if (count + batches.back()->num_rows() >= needCount) {
                Y_VERIFY(count <= needCount);
                batches.back() = batches.back()->Slice(0, needCount - count);
            }
            count += batches.back()->num_rows();
            Y_VERIFY(count <= needCount);
            if (count == needCount) {
                break;
            }
        }
    } else {
        for (auto it = Blobs.rbegin(); it != Blobs.rend(); ++it) {
            batches.push_back(NArrow::DeserializeBatch(*it, schema));
            Y_VERIFY(batches.back());
            if (count + batches.back()->num_rows() >= needCount) {
                Y_VERIFY(count <= needCount);
                batches.back() = batches.back()->Slice(batches.back()->num_rows() - (needCount - count), needCount - count);
            }
            count += batches.back()->num_rows();
            Y_VERIFY(count <= needCount);
            if (count == needCount) {
                break;
            }
        }
        std::reverse(batches.begin(), batches.end());
    }

    auto res = arrow::Table::FromRecordBatches(batches);
    Y_VERIFY_S(res.ok(), res.status().message());
    return (*res)->column(0);
}

std::shared_ptr<arrow::RecordBatch> TPortionInfo::TPreparedBatchData::Assemble(const TAssembleOptions& options) const {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& f : Schema->fields()) {
        if (!options.CheckFieldAcceptance(f->name())) {
            continue;
        }
        fields.emplace_back(f);
    }
    if (fields.empty()) {
        return nullptr;
    }
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    for (auto&& i : Columns) {
        if (!options.CheckFieldAcceptance(i.GetName())) {
            continue;
        }
        columns.emplace_back(i.Assemble(options.GetRecordsCountLimitDef(Max<ui32>()), !options.IsForwardAssemble()));
    }

    auto table = arrow::Table::Make(std::make_shared<arrow::Schema>(fields), columns);
    auto res = table->CombineChunks();
    Y_VERIFY(res.ok());
    return NArrow::ToBatch(*res);
}

}
