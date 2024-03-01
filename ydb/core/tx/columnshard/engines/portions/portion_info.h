#pragma once
#include "column_record.h"
#include "index_chunk.h"
#include "meta.h"

#include <ydb/core/formats/arrow/special_keys.h>
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

struct TIndexInfo;
class TVersionedIndex;
class IDbWrapper;

class TPortionInfo {
private:
    TPortionInfo() = default;
    ui64 PathId = 0;
    ui64 Portion = 0;   // Id of independent (overlayed by PK) portion of data in pathId
    TSnapshot MinSnapshot = TSnapshot::Zero();  // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    TSnapshot RemoveSnapshot = TSnapshot::Zero(); // {XPlanStep, XTxId} is snapshot where the blob has been removed (i.e. compacted into another one)

    TPortionMeta Meta;
    ui64 DeprecatedGranuleId = 0;
    YDB_READONLY_DEF(std::vector<TIndexChunk>, Indexes);

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& info);
public:
    THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const;

    const TString& GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const;

    ui64 GetTxVolume() const; // fake-correct method for determ volume on rewrite this portion in transaction progress

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

    void RegisterBlobId(const TChunkAddress& address, const TUnifiedBlobId& blobId) {
        for (auto it = Records.begin(); it != Records.end(); ++it) {
            if (it->ColumnId == address.GetEntityId() && it->Chunk == address.GetChunkIdx()) {
                it->RegisterBlobId(blobId);
                return;
            }
        }
        for (auto it = Indexes.begin(); it != Indexes.end(); ++it) {
            if (it->GetIndexId() == address.GetEntityId() && it->GetChunkIdx() == address.GetChunkIdx()) {
                it->RegisterBlobId(blobId);
                return;
            }
        }
        AFL_VERIFY(false)("problem", "portion haven't address for blob registration")("address", address.DebugString());
    }

    void RemoveFromDatabase(IDbWrapper& db) const;

    void SaveToDatabase(IDbWrapper& db) const;

    void AddIndex(const TIndexChunk& chunk) {
        ui32 chunkIdx = 0;
        for (auto&& i : Indexes) {
            if (i.GetIndexId() == chunk.GetIndexId()) {
                AFL_VERIFY(chunkIdx == i.GetChunkIdx())("index_id", chunk.GetIndexId())("expected", chunkIdx)("real", i.GetChunkIdx());
                ++chunkIdx;
            }
        }
        AFL_VERIFY(chunkIdx == chunk.GetChunkIdx())("index_id", chunk.GetIndexId())("expected", chunkIdx)("real", chunk.GetChunkIdx());
        Indexes.emplace_back(chunk);
    }

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

    TSerializationStats GetSerializationStat(const ISnapshotSchema& schema) const {
        TSerializationStats result;
        for (auto&& i : Records) {
            if (schema.GetFieldByColumnIdOptional(i.ColumnId)) {
                result.AddStat(i.GetSerializationStat(schema.GetFieldByColumnIdVerified(i.ColumnId)->name()));
            }
        }
        return result;
    }

    void ResetMeta() {
        Meta = TPortionMeta();
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

    bool Empty() const { return Records.empty(); }
    bool Produced() const { return Meta.GetProduced() != TPortionMeta::EProduced::UNSPECIFIED; }
    bool Valid() const { return MinSnapshot.Valid() && PathId && Portion && !Empty() && Produced() && Meta.IndexKeyStart && Meta.IndexKeyEnd; }
    bool ValidSnapshotInfo() const { return MinSnapshot.Valid() && PathId && Portion; }
    bool IsInserted() const { return Meta.GetProduced() == TPortionMeta::EProduced::INSERTED; }
    bool IsEvicted() const { return Meta.GetProduced() == TPortionMeta::EProduced::EVICTED; }
    bool CanHaveDups() const { return !Produced(); /* || IsInserted(); */ }
    bool CanIntersectOthers() const { return !Valid() || IsInserted() || IsEvicted(); }
    size_t NumChunks() const { return Records.size(); }
    size_t NumBlobs() const;

    TPortionInfo CopyWithFilteredColumns(const THashSet<ui32>& columnIds) const;

    bool IsEqualWithSnapshots(const TPortionInfo& item) const;

    static TPortionInfo BuildEmpty() {
        return TPortionInfo();
    }

    TPortionInfo(const ui64 pathId, const ui64 portionId, const TSnapshot& minSnapshot)
        : PathId(pathId)
        , Portion(portionId)
        , MinSnapshot(minSnapshot) {
    }

    TString DebugString(const bool withDetails = false) const;

    bool HasRemoveSnapshot() const {
        return RemoveSnapshot.Valid();
    }

    bool CheckForCleanup(const TSnapshot& snapshot) const {
        if (!HasRemoveSnapshot()) {
            return false;
        }

        return GetRemoveSnapshot() < snapshot;
    }

    bool CheckForCleanup() const {
        return HasRemoveSnapshot();
    }

    bool AllowEarlyFilter() const {
        return Meta.GetProduced() == TPortionMeta::EProduced::COMPACTED
            || Meta.GetProduced() == TPortionMeta::EProduced::SPLIT_COMPACTED;
    }

    ui64 GetPortion() const {
        return Portion;
    }

    ui64 GetDeprecatedGranuleId() const {
        return DeprecatedGranuleId;
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, Portion);
    }

    void SetPathId(const ui64 pathId) {
        PathId = pathId;
    }

    void SetPortion(const ui64 portion) {
        Portion = portion;
    }

    void SetDeprecatedGranuleId(const ui64 granuleId) {
        DeprecatedGranuleId = granuleId;
    }

    const TSnapshot& GetMinSnapshot() const {
        return MinSnapshot;
    }

    const TSnapshot& GetRemoveSnapshot() const {
        return RemoveSnapshot;
    }

    void SetMinSnapshot(const TSnapshot& snap) {
        Y_ABORT_UNLESS(snap.Valid());
        MinSnapshot = snap;
    }

    void SetMinSnapshot(const ui64 planStep, const ui64 txId) {
        MinSnapshot = TSnapshot(planStep, txId);
        Y_ABORT_UNLESS(MinSnapshot.Valid());
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        const bool wasValid = RemoveSnapshot.Valid();
        Y_ABORT_UNLESS(!wasValid || snap.Valid());
        RemoveSnapshot = snap;
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        const bool wasValid = RemoveSnapshot.Valid();
        RemoveSnapshot = TSnapshot(planStep, txId);
        Y_ABORT_UNLESS(!wasValid || RemoveSnapshot.Valid());
    }

    std::pair<ui32, ui32> BlobsSizes() const {
        ui32 sum = 0;
        ui32 max = 0;
        for (const auto& rec : Records) {
            sum += rec.BlobRange.Size;
            max = Max(max, rec.BlobRange.Size);
        }
        return {sum, max};
    }

    ui64 GetBlobBytes() const noexcept {
        ui64 sum = 0;
        for (const auto& rec : Records) {
            sum += rec.BlobRange.Size;
        }
        return sum;
    }

    ui64 BlobsBytes() const noexcept {
        return GetBlobBytes();
    }

    bool IsVisible(const TSnapshot& snapshot) const {
        if (Empty()) {
            return false;
        }

        bool visible = (MinSnapshot <= snapshot);
        if (visible && RemoveSnapshot.Valid()) {
            visible = snapshot < RemoveSnapshot;
        }

        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "IsVisible")("analyze_portion", DebugString())("visible", visible)("snapshot", snapshot.DebugString());
        return visible;
    }

    void UpdateRecordsMeta(TPortionMeta::EProduced produced) {
        Meta.Produced = produced;
    }

    void AddRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec, const NKikimrTxColumnShard::TIndexPortionMeta* portionMeta);

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch,
        const TString& tierName);
    void AddMetadata(const ISnapshotSchema& snapshotSchema, const NArrow::TFirstLastSpecialKeys& primaryKeys, const NArrow::TMinMaxSpecialKeys& snapshotKeys,
        const TString& tierName);

    std::shared_ptr<arrow::Scalar> MaxValue(ui32 columnId) const;

    const NArrow::TReplaceKey& IndexKeyStart() const {
        Y_ABORT_UNLESS(Meta.IndexKeyStart);
        return *Meta.IndexKeyStart;
    }

    const NArrow::TReplaceKey& IndexKeyEnd() const {
        Y_ABORT_UNLESS(Meta.IndexKeyEnd);
        return *Meta.IndexKeyEnd;
    }

    const TSnapshot& RecordSnapshotMin() const {
        Y_ABORT_UNLESS(Meta.RecordSnapshotMin);
        return *Meta.RecordSnapshotMin;
    }

    const TSnapshot& RecordSnapshotMax() const {
        Y_ABORT_UNLESS(Meta.RecordSnapshotMax);
        return *Meta.RecordSnapshotMax;
    }


    THashMap<TString, THashSet<TUnifiedBlobId>> GetBlobIdsByStorage(const TIndexInfo& indexInfo) const {
        THashMap<TString, THashSet<TUnifiedBlobId>> result;
        FillBlobIdsByStorage(result, indexInfo);
        return result;
    }

    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const;

    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const;
    void FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const;

    ui32 GetRecordsCount() const {
        ui32 result = 0;
        std::optional<ui32> columnIdFirst;
        for (auto&& i : Records) {
            if (!columnIdFirst || *columnIdFirst == i.ColumnId) {
                result += i.GetMeta().GetNumRowsVerified();
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
                result += i.GetMeta().GetNumRowsVerified();
            }
        }
        return result;
    }

    ui64 GetIndexBytes(const std::set<ui32>& columnIds) const;

    ui64 GetRawBytes(const std::vector<ui32>& columnIds) const;
    ui64 GetRawBytes(const std::set<ui32>& columnIds) const;
    ui64 GetRawBytes() const {
        ui64 result = 0;
        for (auto&& i : Records) {
            result += i.GetMeta().GetRawBytesVerified();
        }
        return result;
    }

    ui64 RawBytesSum() const {
        return GetRawBytes();
    }

public:
    class TAssembleBlobInfo {
    private:
        ui32 NullRowsCount = 0;
        TString Data;
    public:
        TAssembleBlobInfo(const ui32 rowsCount)
            : NullRowsCount(rowsCount) {
            AFL_VERIFY(NullRowsCount);
        }

        TAssembleBlobInfo(const TString& data)
            : Data(data) {
            AFL_VERIFY(!!Data);
        }

        ui32 GetNullRowsCount() const noexcept {
            return NullRowsCount;
        }

        const TString& GetData() const noexcept {
            return Data;
        }

        bool IsBlob() const {
            return !NullRowsCount && !!Data;
        }

        bool IsNull() const {
            return NullRowsCount && !Data;
        }

        std::shared_ptr<arrow::RecordBatch> BuildRecordBatch(const TColumnLoader& loader) const;
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

        std::shared_ptr<arrow::RecordBatch> Assemble(const TAssembleOptions& options = {}) const;
        std::shared_ptr<arrow::Table> AssembleTable(const TAssembleOptions& options = {}) const;
    };

    class TColumnAssemblingInfo {
    private:
        std::vector<TAssembleBlobInfo> BlobsInfo;
        YDB_READONLY(ui32, ColumnId, 0);
        const ui32 NumRows;
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

        void AddBlobInfo(const ui32 expectedChunkIdx, TAssembleBlobInfo&& info) {
            AFL_VERIFY(expectedChunkIdx == BlobsInfo.size());
            BlobsInfo.emplace_back(std::move(info));
        }

        TPreparedColumn Compile() {
            if (BlobsInfo.empty()) {
                BlobsInfo.emplace_back(TAssembleBlobInfo(NumRows));
                return TPreparedColumn(std::move(BlobsInfo), ResultLoader);
            } else {
                AFL_VERIFY(DataLoader);
                return TPreparedColumn(std::move(BlobsInfo), DataLoader);
            }
        }
    };

    template <class TExternalBlobInfo>
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TExternalBlobInfo>& blobsData) const {
        std::vector<TColumnAssemblingInfo> columns;
        auto arrowResultSchema = resultSchema.GetSchema();
        columns.reserve(arrowResultSchema->num_fields());
        const ui32 rowsCount = NumRows();
        for (auto&& i : arrowResultSchema->fields()) {
            columns.emplace_back(rowsCount, dataSchema.GetColumnLoaderOptional(i->name()), resultSchema.GetColumnLoaderOptional(i->name()));
        }
        {
            int skipColumnId = -1;
            TColumnAssemblingInfo* currentAssembler = nullptr;
            for (auto& rec : Records) {
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
                Y_ABORT_UNLESS(it != blobsData.end());
                currentAssembler->AddBlobInfo(rec.Chunk, std::move(it->second));
                blobsData.erase(it);
            }
        }

        // Make chunked arrays for columns
        std::vector<TPreparedColumn> preparedColumns;
        preparedColumns.reserve(columns.size());
        for (auto& c : columns) {
            preparedColumns.emplace_back(c.Compile());
        }

        return TPreparedBatchData(std::move(preparedColumns), arrowResultSchema, rowsCount);
    }

    std::shared_ptr<arrow::RecordBatch> AssembleInBatch(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        THashMap<TChunkAddress, TString>& data) const {
        auto batch = PrepareForAssemble(dataSchema, resultSchema, data).Assemble();
        Y_ABORT_UNLESS(batch->Validate().ok());
        return batch;
    }

    const TColumnRecord& AppendOneChunkColumn(TColumnRecord&& record);

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
