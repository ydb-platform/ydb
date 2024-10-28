#include "data_accessor.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

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

void TPortionDataAccessor::FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const {
    auto schema = PortionInfo->GetSchema(index);
    return FillBlobRangesByStorage(result, schema->GetIndexInfo());
}

void TPortionDataAccessor::FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo) const {
    for (auto&& i : PortionInfo->Records) {
        const TString& storageId = PortionInfo->GetColumnStorageId(i.GetColumnId(), indexInfo);
        AFL_VERIFY(result[storageId].emplace(PortionInfo->RestoreBlobRange(i.GetBlobRange())).second)(
            "blob_id", PortionInfo->RestoreBlobRange(i.GetBlobRange()).ToString());
    }
    for (auto&& i : PortionInfo->Indexes) {
        const TString& storageId = PortionInfo->GetIndexStorageId(i.GetIndexId(), indexInfo);
        if (auto bRange = i.GetBlobRangeOptional()) {
            AFL_VERIFY(result[storageId].emplace(PortionInfo->RestoreBlobRange(*bRange)).second)(
                "blob_id", PortionInfo->RestoreBlobRange(*bRange).ToString());
        }
    }
}

void TPortionDataAccessor::FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const {
    THashMap<TString, THashSet<TBlobRangeLink16::TLinkId>> local;
    THashSet<TBlobRangeLink16::TLinkId>* currentHashLocal = nullptr;
    THashSet<TUnifiedBlobId>* currentHashResult = nullptr;
    std::optional<ui32> lastEntityId;
    TString lastStorageId;
    ui32 lastBlobIdx = PortionInfo->BlobIds.size();
    for (auto&& i : PortionInfo->Records) {
        if (!lastEntityId || *lastEntityId != i.GetEntityId()) {
            const TString& storageId = PortionInfo->GetColumnStorageId(i.GetEntityId(), indexInfo);
            lastEntityId = i.GetEntityId();
            if (storageId != lastStorageId) {
                currentHashResult = &result[storageId];
                currentHashLocal = &local[storageId];
                lastStorageId = storageId;
                lastBlobIdx = PortionInfo->BlobIds.size();
            }
        }
        if (lastBlobIdx != i.GetBlobRange().GetBlobIdxVerified() && currentHashLocal->emplace(i.GetBlobRange().GetBlobIdxVerified()).second) {
            auto blobId = PortionInfo->GetBlobId(i.GetBlobRange().GetBlobIdxVerified());
            AFL_VERIFY(currentHashResult);
            AFL_VERIFY(currentHashResult->emplace(blobId).second)("blob_id", blobId.ToStringNew());
            lastBlobIdx = i.GetBlobRange().GetBlobIdxVerified();
        }
    }
    for (auto&& i : PortionInfo->Indexes) {
        if (!lastEntityId || *lastEntityId != i.GetEntityId()) {
            const TString& storageId = PortionInfo->GetIndexStorageId(i.GetEntityId(), indexInfo);
            lastEntityId = i.GetEntityId();
            if (storageId != lastStorageId) {
                currentHashResult = &result[storageId];
                currentHashLocal = &local[storageId];
                lastStorageId = storageId;
                lastBlobIdx = PortionInfo->BlobIds.size();
            }
        }
        if (auto bRange = i.GetBlobRangeOptional()) {
            if (lastBlobIdx != bRange->GetBlobIdxVerified() && currentHashLocal->emplace(bRange->GetBlobIdxVerified()).second) {
                auto blobId = PortionInfo->GetBlobId(bRange->GetBlobIdxVerified());
                AFL_VERIFY(currentHashResult);
                AFL_VERIFY(currentHashResult->emplace(blobId).second)("blob_id", blobId.ToStringNew());
                lastBlobIdx = bRange->GetBlobIdxVerified();
            }
        }
    }
}

void TPortionDataAccessor::FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const {
    auto schema = PortionInfo->GetSchema(index);
    return FillBlobIdsByStorage(result, schema->GetIndexInfo());
}

THashMap<TString, THashMap<NKikimr::NOlap::TChunkAddress, std::shared_ptr<NKikimr::NOlap::IPortionDataChunk>>>
TPortionDataAccessor::RestoreEntityChunks(NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> result;
    for (auto&& c : PortionInfo->Records) {
        const TString& storageId = PortionInfo->GetColumnStorageId(c.GetColumnId(), indexInfo);
        auto chunk = std::make_shared<NChunks::TChunkPreparation>(
            blobs.Extract(storageId, PortionInfo->RestoreBlobRange(c.GetBlobRange())), c, indexInfo.GetColumnFeaturesVerified(c.GetColumnId()));
        chunk->SetChunkIdx(c.GetChunkIdx());
        AFL_VERIFY(result[storageId].emplace(c.GetAddress(), chunk).second);
    }
    for (auto&& c : PortionInfo->Indexes) {
        const TString& storageId = indexInfo.GetIndexStorageId(c.GetIndexId());
        const TString blobData = [&]() -> TString {
            if (auto bRange = c.GetBlobRangeOptional()) {
                return blobs.Extract(storageId, PortionInfo->RestoreBlobRange(*bRange));
            } else if (auto data = c.GetBlobDataOptional()) {
                return *data;
            } else {
                AFL_VERIFY(false);
                Y_UNREACHABLE();
            }
        }();
        auto chunk = std::make_shared<NChunks::TPortionIndexChunk>(c.GetAddress(), c.GetRecordsCount(), c.GetRawBytes(), blobData);
        chunk->SetChunkIdx(c.GetChunkIdx());

        AFL_VERIFY(result[storageId].emplace(c.GetAddress(), chunk).second);
    }
    return result;
}

THashMap<TChunkAddress, TString> TPortionDataAccessor::DecodeBlobAddresses(
    NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TChunkAddress, TString> result;
    for (auto&& i : blobs) {
        for (auto&& b : i.second) {
            bool found = false;
            TString columnStorageId;
            ui32 columnId = 0;
            for (auto&& record : PortionInfo->Records) {
                if (PortionInfo->RestoreBlobRange(record.GetBlobRange()) == b.first) {
                    if (columnId != record.GetColumnId()) {
                        columnStorageId = PortionInfo->GetColumnStorageId(record.GetColumnId(), indexInfo);
                    }
                    if (columnStorageId != i.first) {
                        continue;
                    }
                    result.emplace(record.GetAddress(), std::move(b.second));
                    found = true;
                    break;
                }
            }
            if (found) {
                continue;
            }
            for (auto&& record : PortionInfo->Indexes) {
                if (!record.HasBlobRange()) {
                    continue;
                }
                if (PortionInfo->RestoreBlobRange(record.GetBlobRangeVerified()) == b.first) {
                    if (columnId != record.GetIndexId()) {
                        columnStorageId = indexInfo.GetIndexStorageId(record.GetIndexId());
                    }
                    if (columnStorageId != i.first) {
                        continue;
                    }
                    result.emplace(record.GetAddress(), std::move(b.second));
                    found = true;
                    break;
                }
            }
            AFL_VERIFY(found)("blobs", blobs.DebugString())("records", DebugString())("problem", b.first);
        }
    }
    return result;
}

bool TPortionDataAccessor::HasEntityAddress(const TChunkAddress& address) const {
    {
        auto it = std::lower_bound(
            PortionInfo->Records.begin(), PortionInfo->Records.end(), address, [](const TColumnRecord& item, const TChunkAddress& address) {
                return item.GetAddress() < address;
            });
        if (it != PortionInfo->Records.end() && it->GetAddress() == address) {
            return true;
        }
    }
    {
        auto it = std::lower_bound(
            PortionInfo->Indexes.begin(), PortionInfo->Indexes.end(), address, [](const TIndexChunk& item, const TChunkAddress& address) {
                return item.GetAddress() < address;
            });
        if (it != PortionInfo->Indexes.end() && it->GetAddress() == address) {
            return true;
        }
    }
    return false;
}

const NKikimr::NOlap::TColumnRecord* TPortionDataAccessor::GetRecordPointer(const TChunkAddress& address) const {
    auto it = std::lower_bound(
        PortionInfo->Records.begin(), PortionInfo->Records.end(), address, [](const TColumnRecord& item, const TChunkAddress& address) {
            return item.GetAddress() < address;
        });
    if (it != PortionInfo->Records.end() && it->GetAddress() == address) {
        return &*it;
    }
    return nullptr;
}

TString TPortionDataAccessor::DebugString() const {
    TStringBuilder sb;
    sb << "chunks:(" << PortionInfo->Records.size() << ");";
    if (IS_TRACE_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        std::vector<TBlobRange> blobRanges;
        for (auto&& i : PortionInfo->Records) {
            blobRanges.emplace_back(PortionInfo->RestoreBlobRange(i.BlobRange));
        }
        sb << "blobs:" << JoinSeq(",", blobRanges) << ";ranges_count:" << blobRanges.size() << ";";
    }
    return sb << ")";
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
