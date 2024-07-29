#pragma once
#include "column_record.h"
#include "index_chunk.h"
#include "meta.h"

#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/core/formats/arrow/common/accessor.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimrColumnShardDataSharingProto {
class TPortionInfo;
}

namespace NKikimr::NOlap {

namespace NBlobOperations::NRead {
class TCompositeReadBlobs;
}
class TPortionInfoConstructor;

struct TIndexInfo;
class TVersionedIndex;
class IDbWrapper;

class TDeserializeChunkedArray: public NArrow::NAccessor::IChunkedArray {
private:
    using TBase = NArrow::NAccessor::IChunkedArray;
public:
    class TChunk {
    private:
        YDB_READONLY(ui32, RecordsCount, 0);
        std::shared_ptr<arrow::Array> PredefinedArray;
        const TString Data;
    public:
        TChunk(const std::shared_ptr<arrow::Array>& predefinedArray)
            : PredefinedArray(predefinedArray) {
            AFL_VERIFY(PredefinedArray);
            RecordsCount = PredefinedArray->length();
        }

        TChunk(const ui32 recordsCount, const TString& data)
            : RecordsCount(recordsCount)
            , Data(data) {

        }

        std::shared_ptr<arrow::Array> GetArrayVerified(const std::shared_ptr<TColumnLoader>& loader) const {
            if (PredefinedArray) {
                return PredefinedArray;
            }
            auto result = loader->ApplyVerified(Data);
            AFL_VERIFY(result);
            AFL_VERIFY(result->num_columns() == 1);
            AFL_VERIFY(result->num_rows() == RecordsCount)("length", result->num_rows())("records_count", RecordsCount);
            return result->column(0);
        }
    };

    std::shared_ptr<TColumnLoader> Loader;
    std::vector<TChunk> Chunks;
protected:
    virtual std::optional<ui64> DoGetRawSize() const override {
        return {};
    }
    virtual TCurrentChunkAddress DoGetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const override;
    virtual std::shared_ptr<arrow::ChunkedArray> DoGetChunkedArray() const override {
        AFL_VERIFY(false);
        return nullptr;
    }
public:
    TDeserializeChunkedArray(const ui64 recordsCount, const std::shared_ptr<TColumnLoader>& loader, std::vector<TChunk>&& chunks)
        : TBase(recordsCount, NArrow::NAccessor::IChunkedArray::EType::SerializedChunkedArray, loader->GetField()->type())
        , Loader(loader)
        , Chunks(std::move(chunks)) {
        AFL_VERIFY(Loader);
    }
};

class TEntityChunk {
private:
    TChunkAddress Address;
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui64, RawBytes, 0);
    YDB_READONLY_DEF(TBlobRangeLink16, BlobRange);
public:
    const TChunkAddress& GetAddress() const {
        return Address;
    }

    TEntityChunk(const TChunkAddress& address, const ui32 recordsCount, const ui64 rawBytesSize, const TBlobRangeLink16& blobRange)
        : Address(address)
        , RecordsCount(recordsCount)
        , RawBytes(rawBytesSize)
        , BlobRange(blobRange)
    {

    }
};

class TPortionInfoConstructor;
class TGranuleShardingInfo;

class TPortionInfo {
public:
    using TRuntimeFeatures = ui8;
    enum class ERuntimeFeature: TRuntimeFeatures {
        Optimized = 1 /* "optimized" */
    };
private:
    friend class TPortionInfoConstructor;
    TPortionInfo(TPortionMeta&& meta)
        : Meta(std::move(meta))
    {

    }
    ui64 PathId = 0;
    ui64 Portion = 0;   // Id of independent (overlayed by PK) portion of data in pathId
    TSnapshot MinSnapshotDeprecated = TSnapshot::Zero();  // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    TSnapshot RemoveSnapshot = TSnapshot::Zero(); // {XPlanStep, XTxId} is snapshot where the blob has been removed (i.e. compacted into another one)
    std::optional<ui64> SchemaVersion;
    std::optional<ui64> ShardingVersion;

    TPortionMeta Meta;
    YDB_READONLY_DEF(std::vector<TIndexChunk>, Indexes);
    YDB_READONLY(TRuntimeFeatures, RuntimeFeatures, 0);
    std::vector<TUnifiedBlobId> BlobIds;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& info);

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

    template <class TAggregator, class TChunkInfo>
    static void AggregateIndexChunksData(const TAggregator& aggr, const std::vector<TChunkInfo>& chunks, const std::optional<std::set<ui32>>& columnIds, const bool validation) {
        if (columnIds) {
            auto itColumn = columnIds->begin();
            auto itRecord = chunks.begin();
            ui32 recordsInEntityCount = 0;
            while (itRecord != chunks.end() && itColumn != columnIds->end()) {
                if (itRecord->GetEntityId() < *itColumn) {
                    ++itRecord;
                } else if (*itColumn < itRecord->GetEntityId()) {
                    AFL_VERIFY(!validation || recordsInEntityCount)("problem", "validation")("reason", "no_chunks_for_column")("column_id", *itColumn);
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
public:
    ui64 GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const;

    bool NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const;

    const std::optional<ui64>& GetShardingVersionOptional() const {
        return ShardingVersion;
    }

    bool CrossSSWith(const TPortionInfo& p) const {
        return std::min(RecordSnapshotMax(), p.RecordSnapshotMax()) <= std::max(RecordSnapshotMin(), p.RecordSnapshotMin());
    }

    ui64 GetShardingVersionDef(const ui64 verDefault) const {
        return ShardingVersion.value_or(verDefault);
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!RemoveSnapshot.Valid());
        RemoveSnapshot = snap;
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        SetRemoveSnapshot(TSnapshot(planStep, txId));
    }

    std::vector<TString> GetIndexInplaceDataVerified(const ui32 indexId) const {
        std::vector<TString> result;
        for (auto&& i : Indexes) {
            if (i.GetEntityId() == indexId) {
                result.emplace_back(i.GetBlobDataVerified());
            }
        }
        return result;
    }

    void InitRuntimeFeature(const ERuntimeFeature feature, const bool activity) {
        if (activity) {
            AddRuntimeFeature(feature);
        } else {
            RemoveRuntimeFeature(feature);
        }
    }

    void AddRuntimeFeature(const ERuntimeFeature feature) {
        RuntimeFeatures |= (TRuntimeFeatures)feature;
    }

    void RemoveRuntimeFeature(const ERuntimeFeature feature) {
        RuntimeFeatures &= (Max<TRuntimeFeatures>() - (TRuntimeFeatures)feature);
    }

    bool HasRuntimeFeature(const ERuntimeFeature feature) const {
        if (feature == ERuntimeFeature::Optimized) {
            if ((RuntimeFeatures & (TRuntimeFeatures)feature)) {
                return true;
            } else {
                return GetTierNameDef(NOlap::NBlobOperations::TGlobal::DefaultStorageId) != NOlap::NBlobOperations::TGlobal::DefaultStorageId;
            }
        }
        return (RuntimeFeatures & (TRuntimeFeatures)feature);
    }

    void FullValidation() const;

    bool HasIndexes(const std::set<ui32>& ids) const {
        auto idsCopy = ids;
        for (auto&& i : Indexes) {
            idsCopy.erase(i.GetIndexId());
            if (idsCopy.empty()) {
                return true;
            }
        }
        return false;
    }

    void ReorderChunks();

    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> RestoreEntityChunks(NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const;

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < BlobIds.size());
        return BlobIds[linkId];
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const;

    const TString& GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const;
    const TString& GetEntityStorageId(const ui32 entityId, const TIndexInfo& indexInfo) const;

    ui64 GetTxVolume() const; // fake-correct method for determ volume on rewrite this portion in transaction progress
    ui64 GetMetadataMemorySize() const;

    class TPage {
    private:
        YDB_READONLY_DEF(std::vector<const TColumnRecord*>, Records);
        YDB_READONLY_DEF(std::vector<const TIndexChunk*>, Indexes);
        YDB_READONLY(ui32, RecordsCount, 0);
    public:
        TPage(std::vector<const TColumnRecord*>&& records, std::vector<const TIndexChunk*>&& indexes, const ui32 recordsCount)
            : Records(std::move(records))
            , Indexes(std::move(indexes))
            , RecordsCount(recordsCount)
        {

        }
    };

    TString GetTierNameDef(const TString& defaultTierName) const {
        if (GetMeta().GetTierName()) {
            return GetMeta().GetTierName();
        }
        return defaultTierName;
    }

    static TConclusion<TPortionInfo> BuildFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& info);
    void SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const;

    std::vector<TPage> BuildPages() const;

    std::vector<TColumnRecord> Records;

    const std::vector<TColumnRecord>& GetRecords() const {
        return Records;
    }

    ui64 GetPathId() const {
        return PathId;
    }

    void RemoveFromDatabase(IDbWrapper& db) const;

    void SaveToDatabase(IDbWrapper& db, const ui32 firstPKColumnId, const bool saveOnlyMeta) const;

    bool OlderThen(const TPortionInfo& info) const {
        return RecordSnapshotMin() < info.RecordSnapshotMin();
    }

    bool CrossPKWith(const TPortionInfo& info) const {
        return CrossPKWith(info.IndexKeyStart(), info.IndexKeyEnd());
    }

    bool CrossPKWith(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const {
        if (from < IndexKeyStart()) {
            if (to < IndexKeyEnd()) {
                return IndexKeyStart() <= to;
            } else {
                return true;
            }
        } else {
            if (to < IndexKeyEnd()) {
                return true;
            } else {
                return from <= IndexKeyEnd();
            }
        }
    }

    ui64 GetPortionId() const {
        return Portion;
    }

    NJson::TJsonValue SerializeToJsonVisual() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("id", Portion);
        result.InsertValue("s_max", RecordSnapshotMax().GetPlanStep() / 1000);
        /*
        result.InsertValue("s_min", RecordSnapshotMin().GetPlanStep());
        result.InsertValue("tx_min", RecordSnapshotMin().GetTxId());
        result.InsertValue("s_max", RecordSnapshotMax().GetPlanStep());
        result.InsertValue("tx_max", RecordSnapshotMax().GetTxId());
        result.InsertValue("pk_min", IndexKeyStart().DebugString());
        result.InsertValue("pk_max", IndexKeyEnd().DebugString());
        */
        return result;
    }

    static constexpr const ui32 BLOB_BYTES_LIMIT = 8 * 1024 * 1024;

    std::vector<const TColumnRecord*> GetColumnChunksPointers(const ui32 columnId) const;

    std::set<ui32> GetColumnIds() const {
        std::set<ui32> result;
        for (auto&& i : Records) {
            result.emplace(i.GetColumnId());
        }
        return result;
    }

    TSerializationStats GetSerializationStat(const ISnapshotSchema& schema) const {
        TSerializationStats result;
        for (auto&& i : Records) {
            if (schema.GetFieldByColumnIdOptional(i.ColumnId)) {
                result.AddStat(i.GetSerializationStat(schema.GetFieldByColumnIdVerified(i.ColumnId)->name()));
            }
        }
        return result;
    }

    const TPortionMeta& GetMeta() const {
        return Meta;
    }

    TPortionMeta& MutableMeta() {
        return Meta;
    }

    const TColumnRecord* GetRecordPointer(const TChunkAddress& address) const {
        for (auto&& i : Records) {
            if (i.GetAddress() == address) {
                return &i;
            }
        }
        return nullptr;
    }

    bool HasEntityAddress(const TChunkAddress& address) const {
        for (auto&& c : GetRecords()) {
            if (c.GetAddress() == address) {
                return true;
            }
        }
        for (auto&& c : GetIndexes()) {
            if (c.GetAddress() == address) {
                return true;
            }
        }
        return false;
    }

    bool Empty() const { return Records.empty(); }
    bool Produced() const { return Meta.GetProduced() != TPortionMeta::EProduced::UNSPECIFIED; }
    bool Valid() const { return ValidSnapshotInfo() && !Empty() && Produced(); }
    bool ValidSnapshotInfo() const { return MinSnapshotDeprecated.Valid() && PathId && Portion; }
    bool IsInserted() const { return Meta.GetProduced() == TPortionMeta::EProduced::INSERTED; }
    bool IsEvicted() const { return Meta.GetProduced() == TPortionMeta::EProduced::EVICTED; }
    bool CanHaveDups() const { return !Produced(); /* || IsInserted(); */ }
    bool CanIntersectOthers() const { return !Valid() || IsInserted() || IsEvicted(); }
    size_t NumChunks() const { return Records.size(); }

    TString DebugString(const bool withDetails = false) const;

    bool HasRemoveSnapshot() const {
        return RemoveSnapshot.Valid();
    }

    bool IsRemovedFor(const TSnapshot& snapshot) const {
        if (!HasRemoveSnapshot()) {
            return false;
        } else {
            return GetRemoveSnapshotVerified() <= snapshot;
        }
    }

    bool CheckForCleanup(const TSnapshot& snapshot) const {
        return IsRemovedFor(snapshot);
    }

    bool CheckForCleanup() const {
        return HasRemoveSnapshot();
    }

    ui64 GetPortion() const {
        return Portion;
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, Portion);
    }

    void ResetShardingVersion() {
        ShardingVersion.reset();
    }

    void SetPathId(const ui64 pathId) {
        PathId = pathId;
    }

    void SetPortion(const ui64 portion) {
        Portion = portion;
    }


    const TSnapshot& GetMinSnapshotDeprecated() const {
        return MinSnapshotDeprecated;
    }

    const TSnapshot& GetRemoveSnapshotVerified() const {
        AFL_VERIFY(HasRemoveSnapshot());
        return RemoveSnapshot;
    }

    std::optional<TSnapshot> GetRemoveSnapshotOptional() const {
        if (RemoveSnapshot.Valid()) {
            return RemoveSnapshot;
        } else {
            return {};
        }
    }

    ui64 GetSchemaVersionVerified() const {
        AFL_VERIFY(SchemaVersion);
        return SchemaVersion.value();
    }

    std::optional<ui64> GetSchemaVersionOptional() const {
        return SchemaVersion;
    }

    bool IsVisible(const TSnapshot& snapshot) const {
        if (Empty()) {
            return false;
        }

        bool visible = (Meta.RecordSnapshotMin <= snapshot);
        if (visible && RemoveSnapshot.Valid()) {
            visible = snapshot < RemoveSnapshot;
        }

        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "IsVisible")("analyze_portion", DebugString())("visible", visible)("snapshot", snapshot.DebugString());
        return visible;
    }

    std::shared_ptr<arrow::Scalar> MaxValue(ui32 columnId) const;

    const NArrow::TReplaceKey& IndexKeyStart() const {
        return Meta.IndexKeyStart;
    }

    const NArrow::TReplaceKey& IndexKeyEnd() const {
        return Meta.IndexKeyEnd;
    }

    const TSnapshot& RecordSnapshotMin() const {
        return Meta.RecordSnapshotMin;
    }

    const TSnapshot& RecordSnapshotMax() const {
        return Meta.RecordSnapshotMax;
    }


    THashMap<TString, THashSet<TUnifiedBlobId>> GetBlobIdsByStorage(const TIndexInfo& indexInfo) const {
        THashMap<TString, THashSet<TUnifiedBlobId>> result;
        FillBlobIdsByStorage(result, indexInfo);
        return result;
    }

    class TSchemaCursor {
        const NOlap::TVersionedIndex& VersionedIndex;
        ISnapshotSchema::TPtr CurrentSchema;
        TSnapshot LastSnapshot = TSnapshot::Zero();
    public:
        TSchemaCursor(const NOlap::TVersionedIndex& versionedIndex)
            : VersionedIndex(versionedIndex)
        {}

        ISnapshotSchema::TPtr GetSchema(const TPortionInfoConstructor& portion);

        ISnapshotSchema::TPtr GetSchema(const TPortionInfo& portion) {
            if (!CurrentSchema || portion.MinSnapshotDeprecated != LastSnapshot) {
                CurrentSchema = portion.GetSchema(VersionedIndex);
                LastSnapshot = portion.MinSnapshotDeprecated;
            }
            AFL_VERIFY(!!CurrentSchema)("portion", portion.DebugString());
            return CurrentSchema;
        }
    };

    ISnapshotSchema::TPtr GetSchema(const TVersionedIndex& index) const;

    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const;

    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const;

    ui32 GetRecordsCount() const {
        ui32 result = 0;
        std::optional<ui32> columnIdFirst;
        for (auto&& i : Records) {
            if (!columnIdFirst || *columnIdFirst == i.ColumnId) {
                result += i.GetMeta().GetNumRows();
                columnIdFirst = i.ColumnId;
            }
        }
        return result;
    }

    ui32 NumRows() const {
        return GetRecordsCount();
    }

    ui32 NumRows(const ui32 columnId) const {
        ui32 result = 0;
        for (auto&& i : Records) {
            if (columnId == i.ColumnId) {
                result += i.GetMeta().GetNumRows();
            }
        }
        return result;
    }

    ui64 GetIndexRawBytes(const std::optional<std::set<ui32>>& columnIds = {}, const bool validation = true) const;
    ui64 GetIndexBlobBytes() const noexcept {
        ui64 sum = 0;
        for (const auto& rec : Indexes) {
            sum += rec.GetDataSize();
        }
        return sum;
    }

    ui64 GetColumnRawBytes(const std::vector<ui32>& columnIds, const bool validation = true) const;
    ui64 GetColumnRawBytes(const std::optional<std::set<ui32>>& columnIds = {}, const bool validation = true) const;

    ui64 GetColumnBlobBytes(const std::vector<ui32>& columnIds, const bool validation = true) const;
    ui64 GetColumnBlobBytes(const std::optional<std::set<ui32>>& columnIds = {}, const bool validation = true) const;

    ui64 GetTotalBlobBytes() const noexcept {
        return GetIndexBlobBytes() + GetColumnBlobBytes();
    }

    ui64 GetTotalRawBytes() const {
        return GetColumnRawBytes() + GetIndexRawBytes();
    }
public:
    class TAssembleBlobInfo {
    private:
        YDB_READONLY_DEF(std::optional<ui32>, ExpectedRowsCount);
        ui32 DefaultRowsCount = 0;
        std::shared_ptr<arrow::Scalar> DefaultValue;
        TString Data;
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

        TAssembleBlobInfo(const ui32 rowsCount, const std::shared_ptr<arrow::Scalar>& defValue)
            : DefaultRowsCount(rowsCount)
            , DefaultValue(defValue)
        {
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

        std::shared_ptr<arrow::RecordBatch> BuildRecordBatch(const TColumnLoader& loader) const;
        TDeserializeChunkedArray::TChunk BuildDeserializeChunk(const std::shared_ptr<TColumnLoader>& loader) const;
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
            return Loader->GetExpectedSchema()->field(0)->name();
        }

        std::shared_ptr<arrow::Field> GetField() const {
            return Loader->GetExpectedSchema()->field(0);
        }

        TPreparedColumn(std::vector<TAssembleBlobInfo>&& blobs, const std::shared_ptr<TColumnLoader>& loader)
            : Loader(loader)
            , Blobs(std::move(blobs)) {
            Y_ABORT_UNLESS(Loader);
            Y_ABORT_UNLESS(Loader->GetExpectedSchema()->num_fields() == 1);
        }

        std::shared_ptr<arrow::ChunkedArray> Assemble() const;
        std::shared_ptr<TDeserializeChunkedArray> AssembleForSeqAccess() const;
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> AssembleAccessor() const;
    };

    class TPreparedBatchData {
    private:
        std::vector<TPreparedColumn> Columns;
        std::shared_ptr<arrow::Schema> Schema;
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

        std::vector<std::string> GetSchemaColumnNames() const {
            return Schema->field_names();
        }

        size_t GetColumnsCount() const {
            return Columns.size();
        }

        size_t GetRowsCount() const {
            return RowsCount;
        }

        TPreparedBatchData(std::vector<TPreparedColumn>&& columns, std::shared_ptr<arrow::Schema> schema, const size_t rowsCount)
            : Columns(std::move(columns))
            , Schema(schema)
            , RowsCount(rowsCount) {
        }

        std::shared_ptr<NArrow::TGeneralContainer> AssembleToGeneralContainer(const std::set<ui32>& sequentialColumnIds) const;
        std::shared_ptr<NArrow::TGeneralContainer> AssembleForSeqAccess() const;
    };

    class TColumnAssemblingInfo {
    private:
        std::vector<TAssembleBlobInfo> BlobsInfo;
        YDB_READONLY(ui32, ColumnId, 0);
        const ui32 NumRows;
        ui32 NumRowsByChunks = 0;
        const std::shared_ptr<TColumnLoader> DataLoader;
        const std::shared_ptr<TColumnLoader> ResultLoader;
    public:
        TColumnAssemblingInfo(const ui32 numRows, const std::shared_ptr<TColumnLoader>& dataLoader, const std::shared_ptr<TColumnLoader>& resultLoader)
            : ColumnId(resultLoader->GetColumnId())
            , NumRows(numRows)
            , DataLoader(dataLoader)
            , ResultLoader(resultLoader) {
            AFL_VERIFY(ResultLoader);
            if (DataLoader) {
                AFL_VERIFY(ResultLoader->GetColumnId() == DataLoader->GetColumnId());
                AFL_VERIFY(DataLoader->GetField()->IsCompatibleWith(ResultLoader->GetField()))("data", DataLoader->GetField()->ToString())("result", ResultLoader->GetField()->ToString());
            }
        }

        const std::shared_ptr<arrow::Field>& GetField() const {
            return ResultLoader->GetField();
        }

        void AddBlobInfo(const ui32 expectedChunkIdx, const ui32 expectedRecordsCount, TAssembleBlobInfo&& info) {
            AFL_VERIFY(expectedChunkIdx == BlobsInfo.size());
            info.SetExpectedRecordsCount(expectedRecordsCount);
            NumRowsByChunks += expectedRecordsCount;
            BlobsInfo.emplace_back(std::move(info));
        }

        TPreparedColumn Compile() {
            if (BlobsInfo.empty()) {
                BlobsInfo.emplace_back(TAssembleBlobInfo(NumRows, DataLoader ? DataLoader->GetDefaultValue() : ResultLoader->GetDefaultValue()));
                return TPreparedColumn(std::move(BlobsInfo), ResultLoader);
            } else {
                AFL_VERIFY(NumRowsByChunks == NumRows)("by_chunks", NumRowsByChunks)("expected", NumRows);
                AFL_VERIFY(DataLoader);
                return TPreparedColumn(std::move(BlobsInfo), DataLoader);
            }
        }
    };

    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TString>& blobsData) const;
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData) const;

    friend IOutputStream& operator << (IOutputStream& out, const TPortionInfo& info) {
        out << info.DebugString();
        return out;
    }
};

/// Ensure that TPortionInfo can be effectively assigned by moving the value.
static_assert(std::is_nothrow_move_assignable<TPortionInfo>::value);

/// Ensure that TPortionInfo can be effectively constructed by moving the value.
static_assert(std::is_nothrow_move_constructible<TPortionInfo>::value);

} // namespace NKikimr::NOlap
