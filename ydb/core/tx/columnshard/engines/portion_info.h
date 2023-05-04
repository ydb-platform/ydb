#pragma once

#include "defs.h"
#include "columns_table.h"
#include "index_info.h"

#include <ydb/core/formats/replace_key.h>


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

struct TPortionInfo {
    static constexpr const ui32 BLOB_BYTES_LIMIT = 8 * 1024 * 1024;

    TVector<TColumnRecord> Records;
    TPortionMeta Meta;
    ui32 FirstPkColumn = 0;
    TString TierName;

    bool Empty() const { return Records.empty(); }
    bool Produced() const { return Meta.Produced != TPortionMeta::UNSPECIFIED; }
    bool Valid() const { return !Empty() && Produced() && HasMinMax(FirstPkColumn); }
    bool IsInserted() const { return Meta.Produced == TPortionMeta::INSERTED; }
    bool CanHaveDups() const { return !Produced(); /* || IsInserted(); */ }
    bool CanIntersectOthers() const { return !Valid() || IsInserted(); }
    size_t NumRecords() const { return Records.size(); }

    bool IsSortableInGranule() const {
        return !CanIntersectOthers();
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

    void SetGranule(ui64 granule) {
        for (auto& rec : Records) {
            rec.Granule = granule;
        }
    }

    TSnapshot Snapshot() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return {rec.PlanStep, rec.TxId};
    }

    TSnapshot XSnapshot() const {
        Y_VERIFY(!Empty());
        auto& rec = Records[0];
        return {rec.XPlanStep, rec.XTxId};
    }

    bool IsActive() const {
        return XSnapshot().IsZero();
    }

    std::pair<ui32, ui32> BlobsSizes() const {
        ui32 sum = 0;
        ui32 max = 0;
        for (auto& rec : Records) {
            sum += rec.BlobRange.Size;
            max = Max(max, rec.BlobRange.Size);
        }
        return {sum, max};
    }

    ui64 BlobsBytes() const {
        ui64 sum = 0;
        for (auto& rec : Records) {
            sum += rec.BlobRange.Size;
        }
        return sum;
    }

    void UpdateRecords(ui64 portion, const THashMap<ui64, ui64>& granuleRemap, const TSnapshot& snapshot) {
        for (auto& rec : Records) {
            rec.Portion = portion;
            if (!rec.ValidSnapshot()) {
                rec.SetSnapshot(snapshot);
            }
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
    void AddMetadata(const TIndexInfo& indexInfo, const std::shared_ptr<arrow::RecordBatch>& batch,
                     const TString& tierName);
    void AddMinMax(ui32 columnId, const std::shared_ptr<arrow::Array>& column, bool sorted);
    void MinMaxValue(const ui32 columnId, std::shared_ptr<arrow::Scalar>& minValue, std::shared_ptr<arrow::Scalar>& maxValue) const;
    std::shared_ptr<arrow::Scalar> MinValue(ui32 columnId) const;
    std::shared_ptr<arrow::Scalar> MaxValue(ui32 columnId) const;

    NArrow::TReplaceKey EffKeyStart() const {
        Y_VERIFY(FirstPkColumn);
        Y_VERIFY(Meta.ColumnMeta.contains(FirstPkColumn));
        return NArrow::TReplaceKey::FromScalar(MinValue(FirstPkColumn));
    }

    NArrow::TReplaceKey EffKeyEnd() const {
        Y_VERIFY(FirstPkColumn);
        Y_VERIFY(Meta.ColumnMeta.contains(FirstPkColumn));
        return NArrow::TReplaceKey::FromScalar(MaxValue(FirstPkColumn));
    }

    ui32 NumRows() const {
        if (FirstPkColumn) {
            Y_VERIFY(Meta.ColumnMeta.contains(FirstPkColumn));
            return Meta.ColumnMeta.find(FirstPkColumn)->second.NumRows;
        }
        return 0;
    }

    ui64 RawBytesSum() const {
        ui64 sum = 0;
        for (auto& [columnId, colMeta] : Meta.ColumnMeta) {
            sum += colMeta.RawBytes;
        }
        return sum;
    }

    bool HasMinMax(ui32 columnId) const {
        if (!Meta.ColumnMeta.contains(columnId)) {
            return false;
        }
        return Meta.ColumnMeta.find(columnId)->second.HasMinMax();
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
    int CompareByColumnIdsImpl(const TPortionInfo& item, const TVector<ui32>& columnIds) const {
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

    int CompareMinByColumnIds(const TPortionInfo& item, const TVector<ui32>& columnIds) const {
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

        std::shared_ptr<arrow::RecordBatch> BuildRecordBatch(std::shared_ptr<arrow::Schema> schema) const {
            if (NullRowsCount) {
                Y_VERIFY(!Data);
                return NArrow::MakeEmptyBatch(schema, NullRowsCount);
            } else {
                Y_VERIFY(Data);
                return NArrow::DeserializeBatch(Data, schema);
            }
        }
    };

    class TPreparedColumn {
    private:
        ui32 ColumnId = 0;
        std::shared_ptr<arrow::Field> Field;
        std::vector<TAssembleBlobInfo> Blobs;
    public:
        ui32 GetColumnId() const noexcept {
            return ColumnId;
        }

        const std::string& GetName() const {
            return Field->name();
        }

        std::shared_ptr<arrow::Field> GetField() const {
            return Field;
        }

        TPreparedColumn(const std::shared_ptr<arrow::Field>& field, std::vector<TAssembleBlobInfo>&& blobs, const ui32 columnId)
            : ColumnId(columnId)
            , Field(field)
            , Blobs(std::move(blobs))
        {

        }

        std::shared_ptr<arrow::ChunkedArray> Assemble(const ui32 needCount, const bool reverse) const;
    };

    class TPreparedBatchData {
    private:
        std::vector<TPreparedColumn> Columns;
        std::shared_ptr<arrow::Schema> Schema;

    public:
        struct TAssembleOptions {
            const bool ForwardAssemble = true;
            std::optional<ui32> RecordsCountLimit;
            std::optional<std::set<ui32>> IncludedColumnIds;
            std::optional<std::set<ui32>> ExcludedColumnIds;

            TAssembleOptions() noexcept
                : TAssembleOptions(true)
            {}

            explicit TAssembleOptions(bool forward) noexcept
                : ForwardAssemble(forward)
            {}

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

        TPreparedBatchData(std::vector<TPreparedColumn>&& columns, std::shared_ptr<arrow::Schema> schema)
            : Columns(std::move(columns))
            , Schema(schema)
        {
        }

        std::shared_ptr<arrow::RecordBatch> Assemble(const TAssembleOptions& options = {}) const;
    };

    template <class TExternalBlobInfo>
    TPreparedBatchData PrepareForAssemble(const TIndexInfo& indexInfo,
        const std::shared_ptr<arrow::Schema>& schema,
        const THashMap<TBlobRange, TExternalBlobInfo>& blobsData, const std::optional<std::set<ui32>>& columnIds) const {
        // Correct records order
        TMap<int, TMap<ui32, TBlobRange>> columnChunks; // position in schema -> ordered chunks

        std::vector<std::shared_ptr<arrow::Field>> schemaFields;

        for (auto&& i : schema->fields()) {
            if (columnIds && !columnIds->contains(indexInfo.GetColumnId(i->name()))) {
                continue;
            }
            schemaFields.emplace_back(i);
        }

        for (auto& rec : Records) {
            if (columnIds && !columnIds->contains(rec.ColumnId)) {
                continue;
            }
            ui32 columnId = rec.ColumnId;
            TString columnName = indexInfo.GetColumnName(columnId);
            std::string name(columnName.data(), columnName.size());
            int pos = schema->GetFieldIndex(name);
            if (pos < 0) {
                continue; // no such column in schema - do not need it
            }

            columnChunks[pos][rec.Chunk] = rec.BlobRange;
        }

        // Make chunked arrays for columns
        std::vector<TPreparedColumn> columns;
        columns.reserve(columnChunks.size());

        for (auto& [pos, orderedChunks] : columnChunks) {
            auto field = schema->field(pos);
            TVector<TAssembleBlobInfo> blobs;
            blobs.reserve(orderedChunks.size());
            ui32 expected = 0;
            for (auto& [chunk, blobRange] : orderedChunks) {
                Y_VERIFY(chunk == expected);
                ++expected;

                auto it = blobsData.find(blobRange);
                Y_VERIFY(it != blobsData.end());
                blobs.emplace_back(it->second);
            }

            columns.emplace_back(TPreparedColumn(field, std::move(blobs), indexInfo.GetColumnId(field->name())));
        }

        return TPreparedBatchData(std::move(columns), std::make_shared<arrow::Schema>(schemaFields));
    }

    std::shared_ptr<arrow::RecordBatch> AssembleInBatch(const TIndexInfo& indexInfo,
                                           const std::shared_ptr<arrow::Schema>& schema,
                                           const THashMap<TBlobRange, TString>& data) const {
        return PrepareForAssemble(indexInfo, schema, data, {}).Assemble();
    }

    static TString SerializeColumn(const std::shared_ptr<arrow::Array>& array,
                                   const std::shared_ptr<arrow::Field>& field,
                                   const arrow::ipc::IpcWriteOptions& writeOptions);

    TString AddOneChunkColumn(const std::shared_ptr<arrow::Array>& array,
                              const std::shared_ptr<arrow::Field>& field,
                              TColumnRecord&& record,
                              const arrow::ipc::IpcWriteOptions& writeOptions,
                              ui32 limitBytes = BLOB_BYTES_LIMIT);

    friend IOutputStream& operator << (IOutputStream& out, const TPortionInfo& info) {
        for (auto& rec : info.Records) {
            out << " " << rec;
            out << " (1 of " << info.Records.size() << " blobs shown)";
            break;
        }
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
