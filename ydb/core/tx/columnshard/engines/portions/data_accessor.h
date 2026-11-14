#pragma once
#include "portion_chunks.h"
#include "portion_info.h"

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NOlap {

namespace NBlobOperations::NRead {
class TCompositeReadBlobs;
}

namespace NAssembling {

class TAssembleBlobInfo {
private:
    YDB_READONLY_DEF(std::optional<ui32>, ExpectedRowsCount);
    ui32 DefaultRowsCount = 0;
    std::shared_ptr<arrow::Scalar> DefaultValue;
    TString Data;
    std::shared_ptr<NArrow::NAccessor::IAdditionalAccessorData> AdditionalAccessorData;

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

    void SetAdditionalAccessorData(std::shared_ptr<NArrow::NAccessor::IAdditionalAccessorData> value) {
        AdditionalAccessorData = std::move(value);
    }

    const std::shared_ptr<NArrow::NAccessor::IAdditionalAccessorData>& GetAdditionalAccessorData() const {
        return AdditionalAccessorData;
    }

    TAssembleBlobInfo(const ui32 rowsCount, const std::shared_ptr<arrow::Scalar>& defValue)
        : DefaultRowsCount(rowsCount)
        , DefaultValue(defValue)
    {
        AFL_VERIFY(DefaultRowsCount);
    }

    TAssembleBlobInfo(const TString& data)
        : Data(data)
    {
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
    std::shared_ptr<NArrow::NAccessor::IChunkedArray> BuildDeserializeChunk(const std::shared_ptr<TColumnLoader>& loader) const;
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
        , Blobs(std::move(blobs))
    {
        AFL_VERIFY(Loader);
    }

    std::shared_ptr<NArrow::NAccessor::IChunkedArray> AssembleForSeqAccess() const;
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
        , RowsCount(rowsCount)
    {
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
        , ResultLoader(resultLoader)
    {
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

}   // namespace NAssembling

class TPortionDataAccessor {
private:
    TPortionInfo::TConstPtr PortionInfo;
    std::shared_ptr<const TPortionChunks> Chunks;

    TPortionDataAccessor() = default;

    THashMap<TChunkAddress, TString> DecodeBlobAddressesImpl(
        NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const;

public:
    using TColumnAssemblingInfo = NAssembling::TColumnAssemblingInfo;
    using TAssembleBlobInfo = NAssembling::TAssembleBlobInfo;
    using TPreparedColumn = NAssembling::TPreparedColumn;
    using TPreparedBatchData = NAssembling::TPreparedBatchData;
    using TExtractContext = TPortionChunks::TExtractContext;
    using TPage = TPortionChunks::TPage;

    const TPortionChunks& GetChunks() const {
        AFL_VERIFY(Chunks);
        return *Chunks;
    }

    const std::shared_ptr<const TPortionChunks>& GetChunksPtr() const {
        return Chunks;
    }

    ui64 GetMetadataSize() const {
        return (Chunks ? Chunks->GetMetadataSize() : 0) + sizeof(TPortionInfo::TConstPtr);
    }

    TPortionDataAccessor Extract(const std::optional<std::set<ui32>>& columnIds, const std::optional<std::set<ui32>>& indexIds) const {
        return Extract(TExtractContext().SetColumnIds(columnIds).SetIndexIds(indexIds));
    }

    TPortionDataAccessor Extract(const TExtractContext& context) const {
        AFL_VERIFY(Chunks);
        return TPortionDataAccessor(PortionInfo, GetChunks().Extract(context));
    }

    const std::vector<TColumnRecord>& TestGetRecords() const {
        return GetRecordsVerified();
    }

    std::vector<TColumnRecord> ExtractRecords() {
        AFL_VERIFY(Chunks);
        if (Chunks.use_count() == 1) {
            return std::const_pointer_cast<TPortionChunks>(Chunks)->ExtractRecords();
        }
        return GetRecordsVerified();
    }

    std::vector<TIndexChunk> ExtractIndexes() {
        AFL_VERIFY(Chunks);
        if (Chunks.use_count() == 1) {
            return std::const_pointer_cast<TPortionChunks>(Chunks)->ExtractIndexes();
        }
        return GetIndexesVerified();
    }

    std::vector<TUnifiedBlobId> ExtractBlobIds() {
        AFL_VERIFY(Chunks);
        if (Chunks.use_count() == 1) {
            return std::const_pointer_cast<TPortionChunks>(Chunks)->ExtractBlobIds();
        }
        return GetBlobIds();
    }

    TPortionDataAccessor SwitchPortionInfo(std::shared_ptr<TPortionInfo>&& newPortion) const {
        TPortionDataAccessor result(std::move(newPortion), Chunks);
        result.FullValidation();
        return result;
    }

    explicit TPortionDataAccessor(const TPortionInfo::TConstPtr& portionInfo, std::vector<TUnifiedBlobId>&& blobIds,
        std::vector<TColumnRecord>&& records, std::vector<TIndexChunk>&& indexes, const bool validate);
    explicit TPortionDataAccessor(const TPortionInfo::TConstPtr& portionInfo, const std::vector<TUnifiedBlobId>& blobIds,
        const std::vector<TColumnRecord>& records, const std::vector<TIndexChunk>& indexes, const bool validate);
    TPortionDataAccessor(TPortionInfo::TConstPtr portion, std::shared_ptr<const TPortionChunks> chunks);

    static TConclusion<std::shared_ptr<TPortionDataAccessor>> BuildFromProto(
        const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector);

    std::vector<TString> GetIndexInplaceDataOptional(const ui32 indexId) const {
        if (!Chunks) {
            return {};
        }
        return Chunks->GetIndexInplaceDataOptional(indexId);
    }

    std::set<ui32> GetColumnIds() const {
        return GetChunks().GetColumnIds();
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

    NArrow::NSplitter::TSerializationStats GetSerializationStat(const ISnapshotSchema& schema, const bool zeroIfAbsent = false) const {
        NArrow::NSplitter::TSerializationStats result;
        if (!zeroIfAbsent) {
            for (auto&& i : GetRecordsVerified()) {
                if (auto col = schema.GetFieldByColumnIdOptional(i.ColumnId)) {
                    result.AddStat(i.GetSerializationStat(col->name()));
                }
            }
        } else {
            auto itSchema = schema.GetColumnIds().begin();
            auto itRecords = GetRecordsVerified().begin();
            ui32 schemaIdx = 0;
            while (itSchema != schema.GetColumnIds().end() && itRecords != GetRecordsVerified().end()) {
                if (*itSchema < itRecords->ColumnId) {
                    NArrow::NSplitter::TColumnSerializationStat stat(*itSchema, schema.GetFieldByIndexVerified(schemaIdx)->name());
                    NArrow::NSplitter::TSimpleSerializationStat simpleStat(0, GetPortionInfo().GetRecordsCount(), 0);
                    stat.Merge(simpleStat);
                    result.AddStat(stat);
                    ++itSchema;
                    ++schemaIdx;
                } else if (itRecords->ColumnId < *itSchema) {
                    ++itRecords;
                } else {
                    while (itRecords != GetRecordsVerified().end() && itRecords->ColumnId == *itSchema) {
                        result.AddStat(itRecords->GetSerializationStat(schema.GetFieldByIndexVerified(schemaIdx)->name()));
                        ++itRecords;
                    }
                    ++itSchema;
                    ++schemaIdx;
                }
            }
        }
        return result;
    }

    void SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto);

    ui64 GetColumnRawBytes(const std::set<ui32>& entityIds, const bool validation = true) const {
        return GetChunks().GetColumnRawBytes(entityIds, validation);
    }

    ui64 GetColumnBlobBytes(const std::set<ui32>& entityIds, const bool validation = true) const {
        return GetChunks().GetColumnBlobBytes(entityIds, validation);
    }

    ui64 GetIndexRawBytes(const std::set<ui32>& entityIds, const bool validation = true) const {
        return GetChunks().GetIndexRawBytes(entityIds, validation);
    }

    ui64 GetIndexBlobBytes(const std::set<ui32>& entityIds, const bool validation = true) const {
        return GetChunks().GetIndexBlobBytes(entityIds, validation);
    }

    ui64 GetIndexBlobBytes(const bool validation = true) const {
        return GetChunks().GetIndexBlobBytes(validation);
    }

    ui64 GetIndexRawBytes(const bool validation = true) const {
        return GetChunks().GetIndexRawBytes(validation);
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        return GetChunks().GetBlobId(linkId);
    }

    const std::vector<TUnifiedBlobId>& GetBlobIds() const {
        return GetChunks().GetBlobIds();
    }

    ui32 GetBlobIdsCount() const {
        return GetChunks().GetBlobIdsCount();
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return GetChunks().RestoreBlobRange(linkRange);
    }

    ui64 GetMetadataMemorySize() const {
        return GetChunks().GetMetadataMemorySize();
    }

    ui64 GetMetadataDataSize() const {
        return GetChunks().GetMetadataDataSize();
    }

    void FillBlobRangesByStorage(
        THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo, const std::set<ui32>* entityIds = nullptr) const;
    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const;
    void FillBlobRangesByStorage(
        THashMap<ui32, THashMap<TString, THashSet<TBlobRange>>>& result, const TIndexInfo& indexInfo, const THashSet<ui32>& entityIds) const;
    void FillBlobRangesByStorage(
        THashMap<ui32, THashMap<TString, THashSet<TBlobRange>>>& result, const TVersionedIndex& index, const THashSet<ui32>& entityIds) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const;

    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> RestoreEntityChunks(
        NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const;

    std::vector<const TColumnRecord*> GetColumnChunksPointers(const ui32 columnId) const {
        return GetChunks().GetColumnChunksPointers(columnId);
    }

    std::vector<const TIndexChunk*> GetIndexChunksPointers(const ui32 indexId) const {
        return GetChunks().GetIndexChunksPointers(indexId);
    }

    THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const;
    static std::vector<THashMap<TChunkAddress, TString>> DecodeBlobAddresses(const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors,
        const std::vector<ISnapshotSchema::TPtr>& schemas, NBlobOperations::NRead::TCompositeReadBlobs&& blobs);

    THashMap<TString, THashSet<TUnifiedBlobId>> GetBlobIdsByStorage(const TIndexInfo& indexInfo) const {
        THashMap<TString, THashSet<TUnifiedBlobId>> result;
        FillBlobIdsByStorage(result, indexInfo);
        return result;
    }

    static TPortionDataAccessor BuildEmpty() {
        return TPortionDataAccessor();
    }

    const TColumnRecord* GetRecordPointer(const TChunkAddress& address) const {
        return GetChunks().GetRecordPointer(address);
    }

    bool HasEntityAddress(const TChunkAddress& address) const {
        return GetChunks().HasEntityAddress(address);
    }

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

    TString DebugString() const {
        return GetChunks().DebugString();
    }

    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TString>& blobsData, const std::optional<TSnapshot>& defaultSnapshot = std::nullopt,
        const bool restoreAbsent = true) const;
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData, const std::optional<TSnapshot>& defaultSnapshot = std::nullopt,
        const bool restoreAbsent = true) const;

    const std::vector<TColumnRecord>& GetRecordsVerified() const {
        return GetChunks().GetRecordsVerified();
    }

    const std::vector<TIndexChunk>& GetIndexesVerified() const {
        return GetChunks().GetIndexesVerified();
    }

    bool HasIndexes() const {
        return !!Chunks;
    }

    std::vector<TPage> BuildPages() const {
        return GetChunks().BuildPages();
    }

    ui64 GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const {
        return GetChunks().GetMinMemoryForReadColumns(columnIds);
    }

    class TReadPage {
    private:
        YDB_READONLY(ui32, IndexStart, 0);
        YDB_READONLY(ui32, RecordsCount, 0);
        YDB_READONLY(ui64, MemoryUsage, 0);

    public:
        TReadPage(const ui32 indexStart, const ui32 recordsCount, const ui64 memoryUsage)
            : IndexStart(indexStart)
            , RecordsCount(recordsCount)
            , MemoryUsage(memoryUsage)
        {
            AFL_VERIFY(RecordsCount);
        }
    };

    std::vector<TReadPage> BuildReadPages(const ui64 memoryLimit, const std::set<ui32>& entityIds) const;

    ui32 GetSliceOffsetRows(const ui32 sliceBorderIdx) const {
        return GetChunks().GetSliceOffsetRows(sliceBorderIdx);
    }

private:
    void FullValidation() const;
};

}   // namespace NKikimr::NOlap
