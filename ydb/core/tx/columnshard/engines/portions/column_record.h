#pragma once

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NOlap {

struct TColumnRecord {
    ui64 Granule;
    ui64 PlanStep;  // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    ui64 TxId;
    ui64 Portion;   // Id of independent (overlayed by PK) portion of data in granule
    ui64 XPlanStep{0}; // {XPlanStep, XTxId} is snapshot where the blob has been removed (i.e. compacted into another one)
    ui64 XTxId{0};
    ui32 ColumnId{0};
    ui16 Chunk;     // Number of blob for column ColumnName in Portion
    TBlobRange BlobRange;
    TString Metadata;

    std::optional<ui32> GetChunkRowsCount() const {
        return {};
    }

    bool operator == (const TColumnRecord& rec) const {
        return (Granule == rec.Granule) && (ColumnId == rec.ColumnId) &&
            (PlanStep == rec.PlanStep) && (TxId == rec.TxId) && (Portion == rec.Portion) && (Chunk == rec.Chunk);
    }

    TString SerializedBlobId() const {
        return BlobRange.BlobId.SerializeBinary();
    }

    bool Valid() const {
        return ValidExceptSnapshot() && ValidSnapshot();
    }

    bool ValidSnapshot() const {
        return PlanStep && TxId;
    }

    bool ValidExceptSnapshot() const {
        return Granule && ColumnId && Portion && ValidBlob();
    }

    TString DebugString() const {
        return TStringBuilder()
            << "portion:" << Portion << ";"
            << "granule:" << Granule << ";"
            << "column_id:" << ColumnId << ";"
            << "blob_range:" << BlobRange << ";"
            << "plan_step:" << PlanStep << ";"
            << "tx_id:" << TxId << ";"
            ;
    }

    bool ValidBlob() const {
        return BlobRange.BlobId.IsValid() && BlobRange.Size;
    }

    void SetSnapshot(const TSnapshot& snap) {
        Y_VERIFY(snap.Valid());
        PlanStep = snap.GetPlanStep();
        TxId = snap.GetTxId();
    }

    void SetXSnapshot(const TSnapshot& snap) {
        Y_VERIFY(snap.Valid());
        XPlanStep = snap.GetPlanStep();
        XTxId = snap.GetTxId();
    }

    static TColumnRecord Make(ui64 granule, ui32 columnId, const TSnapshot& minSnapshot, ui64 portion, ui16 chunk = 0) {
        TColumnRecord row;
        row.Granule = granule;
        row.PlanStep = minSnapshot.GetPlanStep();
        row.TxId = minSnapshot.GetTxId();
        row.Portion = portion;
        row.ColumnId = columnId;
        row.Chunk = chunk;
        //row.BlobId
        //row.Metadata
        return row;
    }

    friend IOutputStream& operator << (IOutputStream& out, const TColumnRecord& rec) {
        out << '{';
        out << 'g' << rec.Granule << 'p' << rec.Portion;
        if (rec.Chunk) {
            out << 'n' << rec.Chunk;
        }
        out << ',' << (i32)rec.ColumnId;
        out << ',' << rec.PlanStep << ':' << (rec.TxId == Max<ui64>() ? "max" : ToString(rec.TxId));
        if (rec.XPlanStep) {
            out << '-' << rec.XPlanStep << ':' << (rec.XTxId == Max<ui64>() ? "max" : ToString(rec.XTxId));
        }
        out << ',' << rec.BlobRange.ToString();
        out << '}';
        return out;
    }
};

}
