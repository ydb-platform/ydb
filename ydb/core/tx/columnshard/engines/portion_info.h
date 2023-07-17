#pragma once

#include "defs.h"
#include "columns_table.h"
#include "index_info.h"

#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>

namespace NKikimr::NOlap {

struct TPortionMeta {
    // NOTE: These values are persisted in LocalDB so they must be stable
    enum EProduced : ui32 {
        UNSPECIFIED = 0,
        INSERTED = 1,
        COMPACTED = 2,
        SPLIT_COMPACTED = 3,
        INACTIVE = 4,
        EVICTED = 5,
    };

    struct TColumnMeta {
        ui32 NumRows{0};
        ui32 RawBytes{0};
        std::shared_ptr<arrow::Scalar> Min;
        std::shared_ptr<arrow::Scalar> Max;

        bool HasMinMax() const noexcept {
            return Min.get() && Max.get();
        }
    };

    EProduced Produced{UNSPECIFIED};
    THashMap<ui32, TColumnMeta> ColumnMeta;
    ui32 FirstPkColumn = 0;
    std::shared_ptr<arrow::RecordBatch> ReplaceKeyEdges; // first and last PK rows
    std::optional<NArrow::TReplaceKey> IndexKeyStart;
    std::optional<NArrow::TReplaceKey> IndexKeyEnd;

    bool HasMinMax(ui32 columnId) const {
        if (!ColumnMeta.contains(columnId)) {
            return false;
        }
        return ColumnMeta.find(columnId)->second.HasMinMax();
    }

    bool HasPkMinMax() const {
        return HasMinMax(FirstPkColumn);
    }

    ui32 NumRows() const {
        if (FirstPkColumn) {
            Y_VERIFY(ColumnMeta.contains(FirstPkColumn));
            return ColumnMeta.find(FirstPkColumn)->second.NumRows;
        }
        return 0;
    }

    friend IOutputStream& operator << (IOutputStream& out, const TPortionMeta& info) {
        out << "reason" << (ui32)info.Produced;
        for (const auto& [_, meta] : info.ColumnMeta) {
            if (meta.NumRows) {
                out << " " << meta.NumRows << " rows";
                break;
            }
        }
        return out;
    }
};

class TPortionAddress {
private:
    YDB_READONLY(ui64, GranuleId, 0);
    YDB_READONLY(ui64, PortionId, 0);
public:
    TPortionAddress(const ui64 granuleId, const ui64 portionId)
        : GranuleId(granuleId)
        , PortionId(portionId)
    {

    }

    bool operator<(const TPortionAddress& item) const {
        return std::tie(GranuleId, PortionId) < std::tie(item.GranuleId, item.PortionId);
    }

    bool operator==(const TPortionAddress& item) const {
        return std::tie(GranuleId, PortionId) == std::tie(item.GranuleId, item.PortionId);
    }
};

struct TPortionInfo {
    static constexpr const ui32 BLOB_BYTES_LIMIT = 8 * 1024 * 1024;

    std::vector<TColumnRecord> Records;
    TPortionMeta Meta;
    TString TierName;

    bool Empty() const { return Records.empty(); }
    bool Produced() const { return Meta.Produced != TPortionMeta::UNSPECIFIED; }
    bool Valid() const { return !Empty() && Produced() && Meta.HasPkMinMax() && Meta.IndexKeyStart && Meta.IndexKeyEnd; }
    bool IsInserted() const { return Meta.Produced == TPortionMeta::INSERTED; }
    bool IsEvicted() const { return Meta.Produced == TPortionMeta::EVICTED; }
    bool CanHaveDups() const { return !Produced(); /* || IsInserted(); */ }
    bool CanIntersectOthers() const { return !Valid() || IsInserted() || IsEvicted(); }
    size_t NumRecords() const { return Records.size(); }

    bool CheckForCleanup(const TSnapshot& snapshot) const {
        if (!CheckForCleanup()) {
            return false;
        }

        return GetXSnapshot() < snapshot;
    }

    bool CheckForCleanup() const {
        return !IsActive();
    }

    bool AllowEarlyFilter() const {
        return Meta.Produced == TPortionMeta::COMPACTED
            || Meta.Produced == TPortionMeta::SPLIT_COMPACTED;
    }

    bool EvictReady(size_t hotSize) const {
        return Meta.Produced == TPortionMeta::COMPACTED
            || Meta.Produced == TPortionMeta::SPLIT_COMPACTED
            || Meta.Produced == TPortionMeta::EVICTED
            || (Meta.Produced == TPortionMeta::INSERTED && BlobsSizes().first >= hotSize);
    }

    ui64 Portion() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return rec.Portion;
    }

    ui64 Granule() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return rec.Granule;
    }

    TPortionAddress GetAddress() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return TPortionAddress(rec.Granule, rec.Portion);
    }

    void SetGranule(ui64 granule) {
        for (auto& rec : Records) {
            rec.Granule = granule;
        }
    }

    TSnapshot GetSnapshot() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return TSnapshot(rec.PlanStep, rec.TxId);
    }

    TSnapshot GetXSnapshot() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return TSnapshot(rec.XPlanStep, rec.XTxId);
    }

    bool IsActive() const {
        return GetXSnapshot().IsZero();
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

    void UpdateRecords(ui64 portion, const THashMap<ui64, ui64>& granuleRemap) {
        for (auto& rec : Records) {
            rec.Portion = portion;
        }
        if (!granuleRemap.empty()) {
            for (auto& rec : Records) {
                Y_VERIFY(granuleRemap.contains(rec.Granule));
                rec.Granule = granuleRemap.find(rec.Granule)->second;
            }
        }
    }

    void UpdateRecordsMeta(TPortionMeta::EProduced produced) {
        Meta.Produced = produced;
        for (auto& record : Records) {
            record.Metadata = GetMetadata(record);
        }
    }

    void SetStale(const TSnapshot& snapshot) {
        for (auto& rec : Records) {
            rec.SetXSnapshot(snapshot);
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

    ui64 GetRawBytes(const std::vector<ui32>& columnIds) const {
        ui64 sum = 0;
        const ui32 numRows = NumRows();
        for (auto&& i : columnIds) {
            if (TIndexInfo::IsSpecialColumn(i)) {
                sum += numRows * TIndexInfo::GetSpecialColumnByteWidth(i);
            } else {
                auto it = Meta.ColumnMeta.find(i);
                if (it != Meta.ColumnMeta.end()) {
                    sum += it->second.RawBytes;
                }
            }
        }
        return sum;
    }

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
    int CompareSelfMaxItemMinByPk(const TPortionInfo& item, const TIndexInfo& info) const {
        return CompareByColumnIdsImpl<TMaxGetter, TMinGetter>(item, info.KeyColumns);
    }

    int CompareMinByPk(const TPortionInfo& item, const TIndexInfo& info) const {
        return CompareMinByColumnIds(item, info.KeyColumns);
    }

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
            columnChunks[resulPos][rec.Chunk] = rec.BlobRange;
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

    static TString SerializeColumn(const std::shared_ptr<arrow::Array>& array,
        const std::shared_ptr<arrow::Field>& field,
        const TColumnSaver saver);

    void AppendOneChunkColumn(TColumnRecord&& record);

    friend IOutputStream& operator << (IOutputStream& out, const TPortionInfo& info) {
        for (auto& rec : info.Records) {
            out << " " << rec;
            out << " (1 of " << info.Records.size() << " blobs shown)";
            break;
        }
        out << ";activity=" << info.IsActive() << ";";
        if (!info.TierName.empty()) {
            out << " tier: " << info.TierName;
        }
        out << " " << info.Meta;
        return out;
    }
};

/// Ensure that TPortionInfo can be effectively assigned by moving the value.
static_assert(std::is_nothrow_move_assignable<TPortionInfo>::value);

/// Ensure that TPortionInfo can be effectively constructed by moving the value.
static_assert(std::is_nothrow_move_constructible<TPortionInfo>::value);

} // namespace NKikimr::NOlap
