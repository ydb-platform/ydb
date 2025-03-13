#pragma once
#include "portion_info.h"

#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap {

namespace NBlobOperations::NRead {
class TCompositeReadBlobs;
}

class TPortionDataAccessor {
private:
    TPortionInfo::TConstPtr PortionInfo;
    std::optional<std::vector<TColumnRecord>> Records;
    std::optional<std::vector<TIndexChunk>> Indexes;

    template <class TChunkInfo>
    static void CheckChunksOrder(const std::vector<TChunkInfo>& chunks) {
        ui32 entityId = 0;
        ui32 chunkIdx = 0;
        for (auto&& i : chunks) {
            if (entityId != i.GetEntityId()) {
                AFL_VERIFY(entityId < i.GetEntityId());
                AFL_VERIFY(i.GetChunkIdx() == 0);
                entityId = i.GetEntityId();
                chunkIdx = 0;
            } else {
                AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1)("chunk", i.GetChunkIdx())("idx", chunkIdx);
                chunkIdx = i.GetChunkIdx();
            }
        }
    }

    void FullValidation() const;
    TPortionDataAccessor() = default;

public:
    ui64 GetMetadataSize() const {
        return (Records ? (Records->size() * sizeof(TColumnRecord)) : 0) + 
            (Indexes ? (Indexes->size() * sizeof(TIndexChunk)) : 0);
    }

    class TExtractContext {
    private:
        YDB_ACCESSOR_DEF(std::optional<std::set<ui32>>, ColumnIds);
        YDB_ACCESSOR_DEF(std::optional<std::set<ui32>>, IndexIds);

    public:
        TExtractContext() = default;
    };

    TPortionDataAccessor Extract(const std::optional<std::set<ui32>>& columnIds, const std::optional<std::set<ui32>>& indexIds) const {
        return Extract(TExtractContext().SetColumnIds(columnIds).SetIndexIds(indexIds));
    }

    TPortionDataAccessor Extract(const TExtractContext& context) const {
        AFL_VERIFY(Records);
        std::vector<TColumnRecord> extractedRecords;
        if (context.GetColumnIds()) {
            auto itRec = Records->begin();
            auto itExt = context.GetColumnIds()->begin();
            while (itRec != Records->end() && itExt != context.GetColumnIds()->end()) {
                if (itRec->GetEntityId() == *itExt) {
                    extractedRecords.emplace_back(*itRec);
                    ++itRec;
                } else if (itRec->GetEntityId() < *itExt) {
                    ++itRec;
                } else {
                    ++itExt;
                }
            }
        } else {
            extractedRecords = *Records;
        }

        AFL_VERIFY(Indexes);
        std::vector<TIndexChunk> extractedIndexes;
        if (context.GetIndexIds()) {
            auto itIdx = Indexes->begin();
            auto itExt = context.GetIndexIds()->begin();
            while (itIdx != Indexes->end() && itExt != context.GetIndexIds()->end()) {
                if (itIdx->GetEntityId() == *itExt) {
                    extractedIndexes.emplace_back(*itIdx);
                    ++itIdx;
                } else if (itIdx->GetEntityId() < *itExt) {
                    ++itIdx;
                } else {
                    ++itExt;
                }
            }
        } else {
            extractedIndexes = *Indexes;
        }

        return TPortionDataAccessor(PortionInfo, std::move(extractedRecords), std::move(extractedIndexes), false);
    }

    const std::vector<TColumnRecord>& TestGetRecords() const {
        AFL_VERIFY(Records);
        return std::move(*Records);
    }
    std::vector<TColumnRecord> ExtractRecords() {
        AFL_VERIFY(Records);
        return std::move(*Records);
    }
    std::vector<TIndexChunk> ExtractIndexes() {
        AFL_VERIFY(Indexes);
        return std::move(*Indexes);
    }

    TPortionDataAccessor SwitchPortionInfo(TPortionInfo&& newPortion) const {
        return TPortionDataAccessor(std::make_shared<TPortionInfo>(std::move(newPortion)), GetRecordsVerified(), GetIndexesVerified(), true);
    }

    template <class TAggregator, class TChunkInfo>
    static void AggregateIndexChunksData(
        const TAggregator& aggr, const std::vector<TChunkInfo>& chunks, const std::set<ui32>* columnIds, const bool validation) {
        if (columnIds) {
            auto itColumn = columnIds->begin();
            auto itRecord = chunks.begin();
            ui32 recordsInEntityCount = 0;
            while (itRecord != chunks.end() && itColumn != columnIds->end()) {
                if (itRecord->GetEntityId() < *itColumn) {
                    ++itRecord;
                } else if (*itColumn < itRecord->GetEntityId()) {
                    AFL_VERIFY(!validation || recordsInEntityCount)("problem", "validation")("reason", "no_chunks_for_column")(
                        "column_id", *itColumn);
                    ++itColumn;
                    recordsInEntityCount = 0;
                } else {
                    ++recordsInEntityCount;
                    aggr(*itRecord);
                    ++itRecord;
                }
            }
        } else {
            for (auto&& i : chunks) {
                aggr(i);
            }
        }
    }

    explicit TPortionDataAccessor(const TPortionInfo::TConstPtr& portionInfo, std::vector<TColumnRecord>&& records,
        std::vector<TIndexChunk>&& indexes, const bool validate)
        : PortionInfo(portionInfo)
        , Records(std::move(records))
        , Indexes(std::move(indexes)) {
        if (validate) {
            FullValidation();
        }
    }

    explicit TPortionDataAccessor(const TPortionInfo::TConstPtr& portionInfo, const std::vector<TColumnRecord>& records,
        const std::vector<TIndexChunk>& indexes, const bool validate)
        : PortionInfo(portionInfo)
        , Records(records)
        , Indexes(indexes) {
        if (validate) {
            FullValidation();
        }
    }

    static TConclusion<TPortionDataAccessor> BuildFromProto(
        const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector);

    std::vector<TString> GetIndexInplaceDataVerified(const ui32 indexId) const {
        if (!Indexes) {
            return {};
        }
        std::vector<TString> result;
        for (auto&& i : *Indexes) {
            if (i.GetEntityId() == indexId) {
                result.emplace_back(i.GetBlobDataVerified());
            }
        }
        return result;
    }

    std::set<ui32> GetColumnIds() const {
        std::set<ui32> result;
        for (auto&& i : GetRecordsVerified()) {
            result.emplace(i.GetColumnId());
        }
        return result;
    }

    const TPortionInfo& GetPortionInfo() const {
        return *PortionInfo;
    }

    TPortionInfo& MutablePortionInfo() const {
        return const_cast<TPortionInfo&>(*PortionInfo);
    }

    std::shared_ptr<TPortionInfo> MutablePortionInfoPtr() const {
        return std::const_pointer_cast<TPortionInfo>(PortionInfo);
    }

    const TPortionInfo::TConstPtr& GetPortionInfoPtr() const {
        return PortionInfo;
    }

    void RemoveFromDatabase(IDbWrapper& db) const;
    void SaveToDatabase(IDbWrapper& db, const ui32 firstPKColumnId, const bool saveOnlyMeta) const;

    NArrow::NSplitter::TSerializationStats GetSerializationStat(const ISnapshotSchema& schema) const {
        NArrow::NSplitter::TSerializationStats result;
        for (auto&& i : GetRecordsVerified()) {
            if (schema.GetFieldByColumnIdOptional(i.ColumnId)) {
                result.AddStat(i.GetSerializationStat(schema.GetFieldByColumnIdVerified(i.ColumnId)->name()));
            }
        }
        return result;
    }

    void SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto);

    ui64 GetColumnRawBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetColumnBlobBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetIndexRawBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetIndexRawBytes(const bool validation = true) const;

    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const;
    void FillBlobRangesByStorage(THashMap<ui32, THashMap<TString, THashSet<TBlobRange>>>& result, const TIndexInfo& indexInfo, const THashSet<ui32>& entityIds) const;
    void FillBlobRangesByStorage(
        THashMap<ui32, THashMap<TString, THashSet<TBlobRange>>>& result, const TVersionedIndex& index, const THashSet<ui32>& entityIds) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const;

    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> RestoreEntityChunks(
        NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const;

    std::vector<const TColumnRecord*> GetColumnChunksPointers(const ui32 columnId) const;
    std::vector<const TIndexChunk*> GetIndexChunksPointers(const ui32 indexId) const;

    THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const;

    THashMap<TString, THashSet<TUnifiedBlobId>> GetBlobIdsByStorage(const TIndexInfo& indexInfo) const {
        THashMap<TString, THashSet<TUnifiedBlobId>> result;
        FillBlobIdsByStorage(result, indexInfo);
        return result;
    }

    static TPortionDataAccessor BuildEmpty() {
        return TPortionDataAccessor();
    }

    const TColumnRecord* GetRecordPointer(const TChunkAddress& address) const;

    bool HasEntityAddress(const TChunkAddress& address) const;

    bool HasIndexes(const std::set<ui32>& ids) const {
        auto idsCopy = ids;
        for (auto&& i : GetIndexesVerified()) {
            idsCopy.erase(i.GetIndexId());
            if (idsCopy.empty()) {
                return true;
            }
        }
        return false;
    }

    TString DebugString() const;

    class TAssembleBlobInfo {
    private:
        YDB_READONLY_DEF(std::optional<ui32>, ExpectedRowsCount);
        ui32 DefaultRowsCount = 0;
        std::shared_ptr<arrow::Scalar> DefaultValue;
        TString Data;
        const bool NeedCache = true;

    public:
        ui32 GetExpectedRowsCountVerified() const {
            AFL_VERIFY(ExpectedRowsCount);
            return *ExpectedRowsCount;
        }

        void SetExpectedRecordsCount(const ui32 expectedRowsCount) {
            AFL_VERIFY(!ExpectedRowsCount || ExpectedRowsCount == expectedRowsCount);
            ExpectedRowsCount = expectedRowsCount;
            if (!Data) {
                AFL_VERIFY(*ExpectedRowsCount == DefaultRowsCount);
            }
        }

        TAssembleBlobInfo(const ui32 rowsCount, const std::shared_ptr<arrow::Scalar>& defValue, const bool needCache = true)
            : DefaultRowsCount(rowsCount)
            , DefaultValue(defValue)
            , NeedCache(needCache) {
            AFL_VERIFY(DefaultRowsCount);
        }

        TAssembleBlobInfo(const TString& data)
            : Data(data) {
            AFL_VERIFY(!!Data);
        }

        ui32 GetDefaultRowsCount() const noexcept {
            return DefaultRowsCount;
        }

        const TString& GetData() const noexcept {
            return Data;
        }

        bool IsBlob() const {
            return !DefaultRowsCount && !!Data;
        }

        bool IsDefault() const {
            return DefaultRowsCount && !Data;
        }

        TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> BuildRecordBatch(const TColumnLoader& loader) const;
        NArrow::NAccessor::TDeserializeChunkedArray::TChunk BuildDeserializeChunk(const std::shared_ptr<TColumnLoader>& loader) const;
    };

    class TPreparedColumn {
    private:
        std::shared_ptr<TColumnLoader> Loader;
        std::vector<TAssembleBlobInfo> Blobs;

    public:
        ui32 GetColumnId() const {
            return Loader->GetColumnId();
        }

        const std::string& GetName() const {
            return Loader->GetField()->name();
        }

        std::shared_ptr<arrow::Field> GetField() const {
            return Loader->GetField();
        }

        TPreparedColumn(std::vector<TAssembleBlobInfo>&& blobs, const std::shared_ptr<TColumnLoader>& loader)
            : Loader(loader)
            , Blobs(std::move(blobs)) {
            AFL_VERIFY(Loader);
        }

        std::shared_ptr<NArrow::NAccessor::TDeserializeChunkedArray> AssembleForSeqAccess() const;
        TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> AssembleAccessor() const;
    };

    class TPreparedBatchData {
    private:
        std::vector<TPreparedColumn> Columns;
        size_t RowsCount = 0;

    public:
        struct TAssembleOptions {
            std::optional<std::set<ui32>> IncludedColumnIds;
            std::optional<std::set<ui32>> ExcludedColumnIds;
            std::map<ui32, std::shared_ptr<arrow::Scalar>> ConstantColumnIds;

            bool IsConstantColumn(const ui32 columnId, std::shared_ptr<arrow::Scalar>& scalar) const {
                if (ConstantColumnIds.empty()) {
                    return false;
                }
                auto it = ConstantColumnIds.find(columnId);
                if (it == ConstantColumnIds.end()) {
                    return false;
                }
                scalar = it->second;
                return true;
            }

            bool IsAcceptedColumn(const ui32 columnId) const {
                if (IncludedColumnIds && !IncludedColumnIds->contains(columnId)) {
                    return false;
                }
                if (ExcludedColumnIds && ExcludedColumnIds->contains(columnId)) {
                    return false;
                }
                return true;
            }
        };

        std::shared_ptr<arrow::Field> GetFieldVerified(const ui32 columnId) const {
            for (auto&& i : Columns) {
                if (i.GetColumnId() == columnId) {
                    return i.GetField();
                }
            }
            AFL_VERIFY(false);
            return nullptr;
        }

        size_t GetColumnsCount() const {
            return Columns.size();
        }

        size_t GetRowsCount() const {
            return RowsCount;
        }

        TPreparedBatchData(std::vector<TPreparedColumn>&& columns, const size_t rowsCount)
            : Columns(std::move(columns))
            , RowsCount(rowsCount) {
        }

        TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> AssembleToGeneralContainer(const std::set<ui32>& sequentialColumnIds) const;
    };

    class TColumnAssemblingInfo {
    private:
        std::vector<TAssembleBlobInfo> BlobsInfo;
        YDB_READONLY(ui32, ColumnId, 0);
        const ui32 RecordsCount;
        ui32 RecordsCountByChunks = 0;
        const std::shared_ptr<TColumnLoader> DataLoader;
        const std::shared_ptr<TColumnLoader> ResultLoader;

    public:
        TColumnAssemblingInfo(
            const ui32 recordsCount, const std::shared_ptr<TColumnLoader>& dataLoader, const std::shared_ptr<TColumnLoader>& resultLoader)
            : ColumnId(resultLoader->GetColumnId())
            , RecordsCount(recordsCount)
            , DataLoader(dataLoader)
            , ResultLoader(resultLoader) {
            AFL_VERIFY(ResultLoader);
            if (DataLoader) {
                AFL_VERIFY(ResultLoader->GetColumnId() == DataLoader->GetColumnId());
                AFL_VERIFY(DataLoader->GetField()->IsCompatibleWith(ResultLoader->GetField()))("data", DataLoader->GetField()->ToString())(
                    "result", ResultLoader->GetField()->ToString());
            }
        }

        const std::shared_ptr<arrow::Field>& GetField() const {
            return ResultLoader->GetField();
        }

        void AddBlobInfo(const ui32 expectedChunkIdx, const ui32 expectedRecordsCount, TAssembleBlobInfo&& info) {
            AFL_VERIFY(expectedChunkIdx == BlobsInfo.size());
            info.SetExpectedRecordsCount(expectedRecordsCount);
            RecordsCountByChunks += expectedRecordsCount;
            BlobsInfo.emplace_back(std::move(info));
        }

        TPreparedColumn Compile() {
            if (BlobsInfo.empty()) {
                BlobsInfo.emplace_back(
                    TAssembleBlobInfo(RecordsCount, DataLoader ? DataLoader->GetDefaultValue() : ResultLoader->GetDefaultValue()));
                return TPreparedColumn(std::move(BlobsInfo), ResultLoader);
            } else {
                AFL_VERIFY(RecordsCountByChunks == RecordsCount)("by_chunks", RecordsCountByChunks)("expected", RecordsCount);
                AFL_VERIFY(DataLoader);
                return TPreparedColumn(std::move(BlobsInfo), DataLoader);
            }
        }
    };

    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TString>& blobsData, const std::optional<TSnapshot>& defaultSnapshot = std::nullopt,
        const bool restoreAbsent = true) const;
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData, const std::optional<TSnapshot>& defaultSnapshot = std::nullopt, const bool restoreAbsent = true) const;

    class TPage {
    private:
        YDB_READONLY_DEF(std::vector<const TColumnRecord*>, Records);
        YDB_READONLY_DEF(std::vector<const TIndexChunk*>, Indexes);
        YDB_READONLY(ui32, RecordsCount, 0);

    public:
        TPage(std::vector<const TColumnRecord*>&& records, std::vector<const TIndexChunk*>&& indexes, const ui32 recordsCount)
            : Records(std::move(records))
            , Indexes(std::move(indexes))
            , RecordsCount(recordsCount) {
        }
    };

    const std::vector<TColumnRecord>& GetRecordsVerified() const {
        AFL_VERIFY(Records);
        return *Records;
    }

    const std::vector<TIndexChunk>& GetIndexesVerified() const {
        AFL_VERIFY(Indexes);
        return *Indexes;
    }

    bool HasIndexes() const {
        return !!Indexes;
    }

    std::vector<TPage> BuildPages() const;
    ui64 GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const;

    class TReadPage {
    private:
        YDB_READONLY(ui32, IndexStart, 0);
        YDB_READONLY(ui32, RecordsCount, 0);
        YDB_READONLY(ui64, MemoryUsage, 0);

    public:
        TReadPage(const ui32 indexStart, const ui32 recordsCount, const ui64 memoryUsage)
            : IndexStart(indexStart)
            , RecordsCount(recordsCount)
            , MemoryUsage(memoryUsage) {
            AFL_VERIFY(RecordsCount);
        }
    };

    std::vector<TReadPage> BuildReadPages(const ui64 memoryLimit, const std::set<ui32>& entityIds) const;
};

}   // namespace NKikimr::NOlap
