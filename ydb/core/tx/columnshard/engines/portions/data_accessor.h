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
                AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1);
                chunkIdx = i.GetChunkIdx();
            }
        }
    }

    void FullValidation() const;

public:
    TPortionDataAccessor SwitchPortionInfo(TPortionInfo&& newPortion) const {
        return TPortionDataAccessor(std::make_shared<TPortionInfo>(std::move(newPortion)));
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

    explicit TPortionDataAccessor(const TPortionInfo::TConstPtr& portionInfo)
        : PortionInfo(portionInfo) {
    }

    static TConclusion<TPortionDataAccessor> BuildFromProto(
        const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector);

    std::set<ui32> GetColumnIds() const {
        std::set<ui32> result;
        for (auto&& i : PortionInfo->Records) {
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
        for (auto&& i : PortionInfo->Records) {
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
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const;

    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> RestoreEntityChunks(
        NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const;

    std::vector<const TColumnRecord*> GetColumnChunksPointers(const ui32 columnId) const;

    THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const;

    THashMap<TString, THashSet<TUnifiedBlobId>> GetBlobIdsByStorage(const TIndexInfo& indexInfo) const {
        THashMap<TString, THashSet<TUnifiedBlobId>> result;
        FillBlobIdsByStorage(result, indexInfo);
        return result;
    }

    const TColumnRecord* GetRecordPointer(const TChunkAddress& address) const;

    bool HasEntityAddress(const TChunkAddress& address) const;

    bool HasIndexes(const std::set<ui32>& ids) const {
        auto idsCopy = ids;
        for (auto&& i : PortionInfo->Indexes) {
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
            AFL_VERIFY(!ExpectedRowsCount);
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
        THashMap<TChunkAddress, TString>& blobsData, const std::optional<TSnapshot>& defaultSnapshot = std::nullopt) const;
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData, const std::optional<TSnapshot>& defaultSnapshot = std::nullopt) const;

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

    const std::vector<TColumnRecord>& GetRecords() const {
        return PortionInfo->Records;
    }

    const std::vector<TIndexChunk>& GetIndexes() const {
        return PortionInfo->Indexes;
    }

    std::vector<TPage> BuildPages() const;
    ui64 GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const;
};

}   // namespace NKikimr::NOlap
