#pragma once
#include "column_record.h"
#include "meta.h"
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

struct TPortionInfo {
private:
    TPortionInfo() = default;
    ui64 Granule = 0;
    ui64 Portion = 0;   // Id of independent (overlayed by PK) portion of data in granule
    TSnapshot MinSnapshot = TSnapshot::Zero();  // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    TSnapshot RemoveSnapshot = TSnapshot::Zero(); // {XPlanStep, XTxId} is snapshot where the blob has been removed (i.e. compacted into another one)
public:
    static constexpr const ui32 BLOB_BYTES_LIMIT = 8 * 1024 * 1024;

    std::vector<TColumnRecord> Records;
    TPortionMeta Meta;
    TString TierName;

    bool Empty() const { return Records.empty(); }
    bool Produced() const { return Meta.GetProduced() != TPortionMeta::EProduced::UNSPECIFIED; }
    bool Valid() const { return MinSnapshot.Valid() && Granule && Portion && !Empty() && Produced() && Meta.HasPkMinMax() && Meta.IndexKeyStart && Meta.IndexKeyEnd; }
    bool ValidSnapshotInfo() const { return MinSnapshot.Valid() && Granule && Portion; }
    bool IsInserted() const { return Meta.GetProduced() == TPortionMeta::EProduced::INSERTED; }
    bool IsEvicted() const { return Meta.GetProduced() == TPortionMeta::EProduced::EVICTED; }
    bool CanHaveDups() const { return !Produced(); /* || IsInserted(); */ }
    bool CanIntersectOthers() const { return !Valid() || IsInserted() || IsEvicted(); }
    size_t NumRecords() const { return Records.size(); }

    TPortionInfo CopyWithFilteredColumns(const THashSet<ui32>& columnIds) const;

    bool IsEqualWithSnapshots(const TPortionInfo& item) const {
        return Granule == item.Granule && MinSnapshot == item.MinSnapshot
            && Portion == item.Portion && RemoveSnapshot == item.RemoveSnapshot;
    }

    static TPortionInfo BuildEmpty() {
        return TPortionInfo();
    }

    TPortionInfo(const ui64 granuleId, const ui64 portionId, const TSnapshot& minSnapshot)
        : Granule(granuleId)
        , Portion(portionId)
        , MinSnapshot(minSnapshot)
    {

    }

    TString DebugString() const;

    bool CheckForCleanup(const TSnapshot& snapshot) const {
        if (!CheckForCleanup()) {
            return false;
        }

        return GetRemoveSnapshot() < snapshot;
    }

    bool CheckForCleanup() const {
        return !IsActive();
    }

    bool AllowEarlyFilter() const {
        return Meta.GetProduced() == TPortionMeta::EProduced::COMPACTED
            || Meta.GetProduced() == TPortionMeta::EProduced::SPLIT_COMPACTED;
    }

    bool EvictReady(size_t hotSize) const {
        return Meta.GetProduced() == TPortionMeta::EProduced::COMPACTED
            || Meta.GetProduced() == TPortionMeta::EProduced::SPLIT_COMPACTED
            || Meta.GetProduced() == TPortionMeta::EProduced::EVICTED
            || (Meta.GetProduced() == TPortionMeta::EProduced::INSERTED && BlobsSizes().first >= hotSize);
    }

    ui64 GetPortion() const {
        return Portion;
    }

    ui64 GetGranule() const {
        return Granule;
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(Granule, Portion);
    }

    void SetGranule(const ui64 granule) {
        Granule = granule;
    }

    void SetPortion(const ui64 portion) {
        Portion = portion;
    }

    const TSnapshot& GetMinSnapshot() const {
        return MinSnapshot;
    }

    const TSnapshot& GetRemoveSnapshot() const {
        return RemoveSnapshot;
    }

    bool IsActive() const {
        return GetRemoveSnapshot().IsZero();
    }

    void SetMinSnapshot(const TSnapshot& snap) {
        Y_VERIFY(snap.Valid());
        MinSnapshot = snap;
    }

    void SetMinSnapshot(const ui64 planStep, const ui64 txId) {
        MinSnapshot = TSnapshot(planStep, txId);
        Y_VERIFY(MinSnapshot.Valid());
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        const bool wasValid = RemoveSnapshot.Valid();
        Y_VERIFY(!wasValid || snap.Valid());
        RemoveSnapshot = snap;
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        const bool wasValid = RemoveSnapshot.Valid();
        RemoveSnapshot = TSnapshot(planStep, txId);
        Y_VERIFY(!wasValid || RemoveSnapshot.Valid());
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

    ui64 BlobsBytes() const noexcept {
        ui64 sum = 0;
        for (const auto& rec : Records) {
            sum += rec.BlobRange.Size;
        }
        return sum;
    }

    void UpdateRecordsMeta(TPortionMeta::EProduced produced) {
        Meta.Produced = produced;
        for (auto& record : Records) {
            record.Metadata = GetMetadata(record);
        }
    }

    void AddRecord(const TIndexInfo& indexInfo, const TColumnRecord& rec) {
        Records.push_back(rec);
        LoadMetadata(indexInfo, rec);
    }

    TString GetMetadata(const TColumnRecord& rec) const;
    void LoadMetadata(const TIndexInfo& indexInfo, const TColumnRecord& rec);
    void AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch,
                     const TString& tierName);
    void AddMinMax(ui32 columnId, const std::shared_ptr<arrow::Array>& column, bool sorted);

    std::shared_ptr<arrow::Scalar> MinValue(ui32 columnId) const;
    std::shared_ptr<arrow::Scalar> MaxValue(ui32 columnId) const;

    const NArrow::TReplaceKey& IndexKeyStart() const {
        Y_VERIFY(Meta.IndexKeyStart);
        return *Meta.IndexKeyStart;
    }

    const NArrow::TReplaceKey& IndexKeyEnd() const {
        Y_VERIFY(Meta.IndexKeyEnd);
        return *Meta.IndexKeyEnd;
    }

    ui32 NumRows() const {
        return Meta.NumRows();
    }

    ui64 GetRawBytes(const std::vector<ui32>& columnIds) const;

    ui64 RawBytesSum() const {
        ui64 sum = 0;
        for (auto& [columnId, colMeta] : Meta.ColumnMeta) {
            sum += colMeta.RawBytes;
        }
        return sum;
    }

private:
    class TMinGetter {
    public:
        static std::shared_ptr<arrow::Scalar> Get(const TPortionInfo& portionInfo, const ui32 columnId) {
            return portionInfo.MinValue(columnId);
        }
    };

    class TMaxGetter {
    public:
        static std::shared_ptr<arrow::Scalar> Get(const TPortionInfo& portionInfo, const ui32 columnId) {
            return portionInfo.MaxValue(columnId);
        }
    };

    template <class TSelfGetter, class TItemGetter = TSelfGetter>
    int CompareByColumnIdsImpl(const TPortionInfo& item, const std::vector<ui32>& columnIds) const {
        for (auto&& i : columnIds) {
            std::shared_ptr<arrow::Scalar> valueSelf = TSelfGetter::Get(*this, i);
            std::shared_ptr<arrow::Scalar> valueItem = TItemGetter::Get(item, i);
            if (!!valueSelf && !!valueItem) {
                const int cmpResult = NArrow::ScalarCompare(valueSelf, valueItem);
                if (cmpResult) {
                    return cmpResult;
                }
            } else if (!!valueSelf) {
                return 1;
            } else if (!!valueItem) {
                return -1;
            }
        }
        return 0;
    }
public:
    int CompareSelfMaxItemMinByPk(const TPortionInfo& item, const TIndexInfo& info) const;

    int CompareMinByPk(const TPortionInfo& item, const TIndexInfo& info) const;

    int CompareMinByColumnIds(const TPortionInfo& item, const std::vector<ui32>& columnIds) const {
        return CompareByColumnIdsImpl<TMinGetter>(item, columnIds);
    }

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

        std::shared_ptr<arrow::RecordBatch> BuildRecordBatch(const TColumnLoader& loader) const {
            if (NullRowsCount) {
                Y_VERIFY(!Data);
                return NArrow::MakeEmptyBatch(loader.GetExpectedSchema(), NullRowsCount);
            } else {
                auto result = loader.Apply(Data);
                if (!result.ok()) {
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot unpack batch")("error", result.status().ToString())("loader", loader.DebugString());
                    return nullptr;
                }
                return *result;
            }
        }
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
            Y_VERIFY(Loader);
            Y_VERIFY(Loader->GetExpectedSchema()->num_fields() == 1);
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
    };

    template <class TExternalBlobInfo>
    TPreparedBatchData PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
        const THashMap<TBlobRange, TExternalBlobInfo>& blobsData) const {
        std::vector<TPreparedColumn> columns;
        columns.reserve(resultSchema.GetSchema()->num_fields());

        Y_VERIFY(!Meta.ColumnMeta.empty());
        const ui32 rowsCount = Meta.ColumnMeta.begin()->second.NumRows;
        for (auto&& field : resultSchema.GetSchema()->fields()) {
            columns.emplace_back(TPreparedColumn({ TAssembleBlobInfo(rowsCount) }, resultSchema.GetColumnLoader(field->name())));
        }

        TMap<size_t, TMap<ui32, TBlobRange>> columnChunks; // position in schema -> ordered chunks
        TMap<size_t, size_t> positionsMap;

        for (auto& rec : Records) {
            auto resulPos = resultSchema.GetFieldIndex(rec.ColumnId);
            if (resulPos < 0) {
                continue;
            }
            auto pos = dataSchema.GetFieldIndex(rec.ColumnId);
            Y_ASSERT(pos >= 0);
            positionsMap[resulPos] = pos;
            Y_VERIFY(columnChunks[resulPos].emplace(rec.Chunk, rec.BlobRange).second);
            auto columnMeta = Meta.ColumnMeta.FindPtr(rec.ColumnId);
            if (columnMeta) {
                Y_VERIFY_S(rowsCount == columnMeta->NumRows, TStringBuilder() << "Inconsistent rows " << rowsCount << "/" << columnMeta->NumRows);
            }
        }

        // Make chunked arrays for columns
        for (auto& [pos, orderedChunks] : columnChunks) {
            Y_VERIFY(positionsMap.contains(pos));
            size_t dataPos = positionsMap[pos];
            auto portionField = dataSchema.GetFieldByIndex(dataPos);
            auto resultField = resultSchema.GetFieldByIndex(pos);

            Y_VERIFY(portionField->IsCompatibleWith(*resultField));

            std::vector<TAssembleBlobInfo> blobs;
            blobs.reserve(orderedChunks.size());
            ui32 expected = 0;
            for (auto& [chunk, blobRange] : orderedChunks) {
                Y_VERIFY(chunk == expected);
                ++expected;

                auto it = blobsData.find(blobRange);
                Y_VERIFY(it != blobsData.end());
                blobs.emplace_back(it->second);
            }

            Y_VERIFY(pos < columns.size());
            columns[pos] = TPreparedColumn(std::move(blobs), dataSchema.GetColumnLoader(resultField->name()));
        }

        return TPreparedBatchData(std::move(columns), resultSchema.GetSchema(), rowsCount);
    }

    std::shared_ptr<arrow::RecordBatch> AssembleInBatch(const ISnapshotSchema& dataSchema,
                                            const ISnapshotSchema& resultSchema,
                                            const THashMap<TBlobRange, TString>& data) const {
        auto batch = PrepareForAssemble(dataSchema, resultSchema, data).Assemble();
        Y_VERIFY(batch->Validate().ok());
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
