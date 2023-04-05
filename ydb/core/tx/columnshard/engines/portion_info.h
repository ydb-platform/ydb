#pragma once
#include "defs.h"
#include "columns_table.h"
#include "index_info.h"

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

        bool HasMinMax() const {
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
    bool CanHaveDups() const { return !Valid(); /* || IsInserted(); */ }
    bool CanIntersectOthers() const { return !Valid() || IsInserted(); }
    size_t NumRecords() const { return Records.size(); }

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

    void UpdateRecords(ui64 portion, const THashMap<ui64, ui64>& granuleRemap, const TSnapshot& snapshot) {
        for (auto& rec : Records) {
            rec.Portion = portion;
            if (!rec.ValidSnapshot()) {
                rec.SetSnapshot(snapshot);
            }
        }
        if (!granuleRemap.empty()) {
            for (auto& rec : Records) {
                Y_VERIFY(granuleRemap.count(rec.Granule));
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
    std::shared_ptr<arrow::Scalar> MinValue(ui32 columnId) const;
    std::shared_ptr<arrow::Scalar> MaxValue(ui32 columnId) const;

    std::shared_ptr<arrow::Scalar> PkStart() const {
        if (FirstPkColumn) {
            Y_VERIFY(Meta.ColumnMeta.count(FirstPkColumn));
            return MinValue(FirstPkColumn);
        }
        return {};
    }

    std::shared_ptr<arrow::Scalar> PkEnd() const {
        if (FirstPkColumn) {
            Y_VERIFY(Meta.ColumnMeta.count(FirstPkColumn));
            return MaxValue(FirstPkColumn);
        }
        return {};
    }

    ui32 NumRows() const {
        if (FirstPkColumn) {
            Y_VERIFY(Meta.ColumnMeta.count(FirstPkColumn));
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
        if (!Meta.ColumnMeta.count(columnId)) {
            return false;
        }
        return Meta.ColumnMeta.find(columnId)->second.HasMinMax();
    }

    class TPreparedColumn {
    private:
        std::shared_ptr<arrow::Field> Field;
        std::vector<TString> Blobs;

    public:
        TPreparedColumn(const std::shared_ptr<arrow::Field>& field, std::vector<TString>&& blobs)
            : Field(field)
            , Blobs(std::move(blobs))
        {

        }

        std::shared_ptr<arrow::ChunkedArray> Assemble() const;
    };

    class TPreparedBatchData {
    private:
        std::vector<TPreparedColumn> Columns;
        std::shared_ptr<arrow::Schema> Schema;
    public:
        TPreparedBatchData(std::vector<TPreparedColumn>&& columns, std::shared_ptr<arrow::Schema> schema)
            : Columns(std::move(columns))
            , Schema(schema)
        {

        }

        std::shared_ptr<arrow::RecordBatch> Assemble() {
            std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
            for (auto&& i : Columns) {
                columns.emplace_back(i.Assemble());
            }

            auto table = arrow::Table::Make(Schema, columns);
            auto res = table->CombineChunks();
            Y_VERIFY(res.ok());
            return NArrow::ToBatch(*res);
        }
    };

    TPreparedBatchData PrepareForAssemble(const TIndexInfo& indexInfo,
                                           const std::shared_ptr<arrow::Schema>& schema,
                                           const THashMap<TBlobRange, TString>& data) const;
    std::shared_ptr<arrow::RecordBatch> AssembleInBatch(const TIndexInfo& indexInfo,
                                           const std::shared_ptr<arrow::Schema>& schema,
                                           const THashMap<TBlobRange, TString>& data) const {
        return PrepareForAssemble(indexInfo, schema, data).Assemble();
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

}
