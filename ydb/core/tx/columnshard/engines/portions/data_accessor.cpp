#include "data_accessor.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/composite/accessor.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap {

namespace {
template <class TExternalBlobInfo>
TPortionDataAccessor::TPreparedBatchData PrepareForAssembleImpl(const TPortionDataAccessor& portionData, const TPortionInfo& portionInfo,
    const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TExternalBlobInfo>& blobsData,
    const std::optional<TSnapshot>& defaultSnapshot) {
    std::vector<TPortionDataAccessor::TColumnAssemblingInfo> columns;
    columns.reserve(resultSchema.GetColumnIds().size());
    const ui32 rowsCount = portionInfo.GetRecordsCount();
    for (auto&& i : resultSchema.GetColumnIds()) {
        columns.emplace_back(rowsCount, dataSchema.GetColumnLoaderOptional(i), resultSchema.GetColumnLoaderVerified(i));
        if (portionInfo.HasInsertWriteId()) {
            if (portionInfo.HasCommitSnapshot()) {
                if (i == (ui32)IIndexInfo::ESpecialColumn::PLAN_STEP) {
                    columns.back().AddBlobInfo(0, portionInfo.GetRecordsCount(),
                        TPortionDataAccessor::TAssembleBlobInfo(portionInfo.GetRecordsCount(),
                            std::make_shared<arrow::UInt64Scalar>(portionInfo.GetCommitSnapshotVerified().GetPlanStep()), false));
                }
                if (i == (ui32)IIndexInfo::ESpecialColumn::TX_ID) {
                    columns.back().AddBlobInfo(0, portionInfo.GetRecordsCount(),
                        TPortionDataAccessor::TAssembleBlobInfo(portionInfo.GetRecordsCount(),
                            std::make_shared<arrow::UInt64Scalar>(portionInfo.GetCommitSnapshotVerified().GetPlanStep()), false));
                }
            } else {
                if (i == (ui32)IIndexInfo::ESpecialColumn::PLAN_STEP) {
                    columns.back().AddBlobInfo(0, portionInfo.GetRecordsCount(),
                        TPortionDataAccessor::TAssembleBlobInfo(portionInfo.GetRecordsCount(),
                            std::make_shared<arrow::UInt64Scalar>(defaultSnapshot ? defaultSnapshot->GetPlanStep() : 0)));
                }
                if (i == (ui32)IIndexInfo::ESpecialColumn::TX_ID) {
                    columns.back().AddBlobInfo(0, portionInfo.GetRecordsCount(),
                        TPortionDataAccessor::TAssembleBlobInfo(portionInfo.GetRecordsCount(),
                            std::make_shared<arrow::UInt64Scalar>(defaultSnapshot ? defaultSnapshot->GetTxId() : 0)));
                }
            }
            if (i == (ui32)IIndexInfo::ESpecialColumn::WRITE_ID) {
                columns.back().AddBlobInfo(0, portionInfo.GetRecordsCount(),
                    TPortionDataAccessor::TAssembleBlobInfo(portionInfo.GetRecordsCount(),
                        std::make_shared<arrow::UInt64Scalar>((ui64)portionInfo.GetInsertWriteIdVerified()), false));
            }
            if (i == (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG) {
                columns.back().AddBlobInfo(0, portionInfo.GetRecordsCount(),
                    TPortionDataAccessor::TAssembleBlobInfo(portionInfo.GetRecordsCount(),
                        std::make_shared<arrow::BooleanScalar>((bool)portionInfo.GetMeta().GetDeletionsCount()), true));
            }
        }
    }
    {
        int skipColumnId = -1;
        TPortionDataAccessor::TColumnAssemblingInfo* currentAssembler = nullptr;
        for (auto& rec : portionData.GetRecords()) {
            if (skipColumnId == (int)rec.ColumnId) {
                continue;
            }
            if (!currentAssembler || rec.ColumnId != currentAssembler->GetColumnId()) {
                const i32 resultPos = resultSchema.GetFieldIndex(rec.ColumnId);
                if (resultPos < 0) {
                    skipColumnId = rec.ColumnId;
                    continue;
                }
                AFL_VERIFY((ui32)resultPos < columns.size());
                currentAssembler = &columns[resultPos];
            }
            auto it = blobsData.find(rec.GetAddress());
            AFL_VERIFY(it != blobsData.end())("size", blobsData.size())("address", rec.GetAddress().DebugString());
            currentAssembler->AddBlobInfo(rec.Chunk, rec.GetMeta().GetNumRows(), std::move(it->second));
            blobsData.erase(it);
        }
    }

    // Make chunked arrays for columns
    std::vector<TPortionDataAccessor::TPreparedColumn> preparedColumns;
    preparedColumns.reserve(columns.size());
    for (auto& c : columns) {
        preparedColumns.emplace_back(c.Compile());
    }

    return TPortionDataAccessor::TPreparedBatchData(std::move(preparedColumns), rowsCount);
}

}   // namespace

TPortionDataAccessor::TPreparedBatchData TPortionDataAccessor::PrepareForAssemble(const ISnapshotSchema& dataSchema,
    const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TString>& blobsData, const std::optional<TSnapshot>& defaultSnapshot) const {
    return PrepareForAssembleImpl(*this, *PortionInfo, dataSchema, resultSchema, blobsData, defaultSnapshot);
}

TPortionDataAccessor::TPreparedBatchData TPortionDataAccessor::PrepareForAssemble(const ISnapshotSchema& dataSchema,
    const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData,
    const std::optional<TSnapshot>& defaultSnapshot) const {
    return PrepareForAssembleImpl(*this, *PortionInfo, dataSchema, resultSchema, blobsData, defaultSnapshot);
}

TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> TPortionDataAccessor::TPreparedColumn::AssembleAccessor() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    NArrow::NAccessor::TCompositeChunkedArray::TBuilder builder(GetField()->type());
    for (auto& blob : Blobs) {
        auto chunkedArray = blob.BuildRecordBatch(*Loader);
        if (chunkedArray.IsFail()) {
            return chunkedArray;
        }
        builder.AddChunk(chunkedArray.DetachResult());
    }
    return builder.Finish();
}

std::shared_ptr<NArrow::NAccessor::TDeserializeChunkedArray> TPortionDataAccessor::TPreparedColumn::AssembleForSeqAccess() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    std::vector<NArrow::NAccessor::TDeserializeChunkedArray::TChunk> chunks;
    chunks.reserve(Blobs.size());
    ui64 recordsCount = 0;
    for (auto& blob : Blobs) {
        chunks.push_back(blob.BuildDeserializeChunk(Loader));
        if (!!blob.GetData()) {
            recordsCount += blob.GetExpectedRowsCountVerified();
        } else {
            recordsCount += blob.GetDefaultRowsCount();
        }
    }

    return std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(recordsCount, Loader, std::move(chunks));
}

NArrow::NAccessor::TDeserializeChunkedArray::TChunk TPortionDataAccessor::TAssembleBlobInfo::BuildDeserializeChunk(
    const std::shared_ptr<TColumnLoader>& loader) const {
    if (DefaultRowsCount) {
        Y_ABORT_UNLESS(!Data);
        auto col = std::make_shared<NArrow::NAccessor::TTrivialArray>(
            NArrow::TThreadSimpleArraysCache::Get(loader->GetField()->type(), DefaultValue, DefaultRowsCount));
        return NArrow::NAccessor::TDeserializeChunkedArray::TChunk(col);
    } else {
        AFL_VERIFY(ExpectedRowsCount);
        return NArrow::NAccessor::TDeserializeChunkedArray::TChunk(*ExpectedRowsCount, Data);
    }
}

TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> TPortionDataAccessor::TAssembleBlobInfo::BuildRecordBatch(
    const TColumnLoader& loader) const {
    if (DefaultRowsCount) {
        Y_ABORT_UNLESS(!Data);
        if (NeedCache) {
            return std::make_shared<NArrow::NAccessor::TTrivialArray>(
                NArrow::TThreadSimpleArraysCache::Get(loader.GetField()->type(), DefaultValue, DefaultRowsCount));
        } else {
            return std::make_shared<NArrow::NAccessor::TTrivialArray>(
                NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(*DefaultValue, DefaultRowsCount)));
        }
    } else {
        AFL_VERIFY(ExpectedRowsCount);
        return loader.ApplyConclusion(Data, *ExpectedRowsCount);
    }
}

TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> TPortionDataAccessor::TPreparedBatchData::AssembleToGeneralContainer(
    const std::set<ui32>& sequentialColumnIds) const {
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Columns) {
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("column", i.GetField()->ToString())("id", i.GetColumnId());
        if (sequentialColumnIds.contains(i.GetColumnId())) {
            columns.emplace_back(i.AssembleForSeqAccess());
        } else {
            auto conclusion = i.AssembleAccessor();
            if (conclusion.IsFail()) {
                return conclusion;
            }
            columns.emplace_back(conclusion.DetachResult());
        }
        fields.emplace_back(i.GetField());
    }

    return std::make_shared<NArrow::TGeneralContainer>(fields, std::move(columns));
}

}   // namespace NKikimr::NOlap
