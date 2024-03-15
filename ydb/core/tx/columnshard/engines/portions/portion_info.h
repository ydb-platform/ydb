#pragma once
#include "column_record.h"
#include "meta.h"
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/formats/arrow/special_keys.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

class TPortionInfo {
private:
    TPortionInfo() = default;
    ui64 PathId = 0;
    ui64 Portion = 0;   // Id of independent (overlayed by PK) portion of data in pathId
    TSnapshot MinSnapshot = TSnapshot::Zero();  // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    TSnapshot RemoveSnapshot = TSnapshot::Zero(); // {XPlanStep, XTxId} is snapshot where the blob has been removed (i.e. compacted into another one)

    TPortionMeta Meta;
    std::shared_ptr<NOlap::IBlobsStorageOperator> BlobsOperator;
    ui64 DeprecatedGranuleId = 0;
public:
    ui64 GetPathId() const {
        return PathId;
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

    bool HasStorageOperator() const {
        return !!BlobsOperator;
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

    void InitOperator(const std::shared_ptr<NOlap::IBlobsStorageOperator>& bOperator, const bool rewrite) {
        if (rewrite) {
            AFL_VERIFY(!!BlobsOperator);
        } else {
            AFL_VERIFY(!BlobsOperator);
        }
        AFL_VERIFY(!!bOperator);
        BlobsOperator = bOperator;
    }

    static constexpr const ui32 BLOB_BYTES_LIMIT = 8 * 1024 * 1024;

    const std::shared_ptr<NOlap::IBlobsStorageOperator>& GetBlobsStorage() const {
        Y_ABORT_UNLESS(BlobsOperator);
        return BlobsOperator;
    }
    std::vector<const TColumnRecord*> GetColumnChunksPointers(const ui32 columnId) const;

    TSerializationStats GetSerializationStat(const ISnapshotSchema& schema) const {
        TSerializationStats result;
        for (auto&& i : Records) {
            if (schema.GetFieldByColumnId(i.ColumnId)) {
                result.AddStat(i.GetSerializationStat(schema.GetFieldByColumnIdVerified(i.ColumnId)->name()));
            }
        }
        return result;
    }

    void ResetMeta() {
        Meta = TPortionMeta();
        BlobsOperator = nullptr;
    }

    const TPortionMeta& GetMeta() const {
        return Meta;
    }

    TPortionMeta& MutableMeta() {
        return Meta;
    }

    std::vector<TColumnRecord> Records;

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

    TPortionInfo(const ui64 pathId, const ui64 portionId, const TSnapshot& minSnapshot, const std::shared_ptr<NOlap::IBlobsStorageOperator>& blobsOperator)
        : PathId(pathId)
        , Portion(portionId)
        , MinSnapshot(minSnapshot)
        , BlobsOperator(blobsOperator)
    {
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

    THashSet<TUnifiedBlobId> GetBlobIds() const {
        THashSet<TUnifiedBlobId> result;
        for (auto&& i : Records) {
            result.emplace(i.BlobRange.BlobId);
        }
        return result;
    }

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

        }

        TAssembleBlobInfo(const TString& data)
            : Data(data) {

        }

        ui32 GetNullRowsCount() const noexcept {
             return NullRowsCount;
        }

        const TString& GetData() const noexcept {
            return Data;
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
            , Blobs(std::move(blobs))
        {
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
        mutable THashMap<TString, std::shared_ptr<arrow::Array>> NullColumns;
        mutable THashMap<TString, std::shared_ptr<arrow::Array>> ConstColumns;
    public:
        struct TAssembleOptions {
            std::optional<std::set<ui32>> IncludedColumnIds;
            std::optional<std::set<ui32>> ExcludedColumnIds;

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
            , RowsCount(rowsCount)
        {
        }

        std::shared_ptr<arrow::RecordBatch> Assemble(const TAssembleOptions& options = {}) const;
        std::shared_ptr<arrow::Table> AssembleTable(const TAssembleOptions& options = {}) const;
    };

    template <class TExternalBlobInfo>
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        const THashMap<TBlobRange, TExternalBlobInfo>& blobsData) const {
        std::vector<TPreparedColumn> columns;
        columns.reserve(resultSchema.GetSchema()->num_fields());

        const ui32 rowsCount = NumRows();
        for (auto&& field : resultSchema.GetSchema()->fields()) {
            columns.emplace_back(TPreparedColumn({ TAssembleBlobInfo(rowsCount) }, resultSchema.GetColumnLoaderOptional(field->name())));
        }

        TMap<size_t, TMap<ui32, TBlobRange>> columnChunks; // position in schema -> ordered chunks
        TMap<size_t, size_t> positionsMap;

        {
            int resulPos = -1;
            int dataSchemaPos = -1;
            std::optional<ui32> predColumnId;
            for (auto& rec : Records) {
                if (!predColumnId || rec.ColumnId != *predColumnId) {
                    resulPos = resultSchema.GetFieldIndex(rec.ColumnId);
                    dataSchemaPos = dataSchema.GetFieldIndex(rec.ColumnId);
                }
                predColumnId = rec.ColumnId;
                if (resulPos < 0) {
                    continue;
                }
                Y_ASSERT(dataSchemaPos >= 0);
                positionsMap[resulPos] = dataSchemaPos;
                AFL_VERIFY(columnChunks[resulPos].emplace(rec.Chunk, rec.BlobRange).second)("record", rec.DebugString());
                //            AFL_VERIFY(rowsCount == NumRows(rec.ColumnId))("error", "Inconsistent rows")("portion", DebugString())("record", rec.DebugString())("column_records", NumRows(rec.ColumnId));
            }
        }

        // Make chunked arrays for columns
        for (auto& [pos, orderedChunks] : columnChunks) {
            Y_ABORT_UNLESS(positionsMap.contains(pos));
            size_t dataPos = positionsMap[pos];
            auto portionField = dataSchema.GetFieldByIndex(dataPos);
            auto resultField = resultSchema.GetFieldByIndex(pos);

            Y_ABORT_UNLESS(portionField->IsCompatibleWith(*resultField));

            std::vector<TAssembleBlobInfo> blobs;
            blobs.reserve(orderedChunks.size());
            ui32 expected = 0;
            for (auto& [chunk, blobRange] : orderedChunks) {
                Y_ABORT_UNLESS(chunk == expected);
                ++expected;

                auto it = blobsData.find(blobRange);
                Y_ABORT_UNLESS(it != blobsData.end());
                blobs.emplace_back(it->second);
            }

            Y_ABORT_UNLESS(pos < columns.size());
            columns[pos] = TPreparedColumn(std::move(blobs), dataSchema.GetColumnLoaderOptional(resultField->name()));
        }

        return TPreparedBatchData(std::move(columns), resultSchema.GetSchema(), rowsCount);
    }

    std::shared_ptr<arrow::RecordBatch> AssembleInBatch(const ISnapshotSchema& dataSchema,
                                            const ISnapshotSchema& resultSchema,
                                            const THashMap<TBlobRange, TString>& data) const {
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
