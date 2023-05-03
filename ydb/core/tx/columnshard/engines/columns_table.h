#pragma once

#include "defs.h"
#include "db_wrapper.h"

#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

struct TColumnRecord {
    ui64 Granule;
    ui32 ColumnId{0};
    ui64 PlanStep;  // {PlanStep, TxId} is min snapshot for {Granule, Portion}
    ui64 TxId;
    ui64 Portion;   // Id of independent (overlayed by PK) portion of data in granule
    ui16 Chunk;     // Number of blob for column ColumnName in Portion
    ui64 XPlanStep{0}; // {XPlanStep, XTxId} is snapshot where the blob has been removed (i.e. compacted into another one)
    ui64 XTxId{0};
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

    bool ValidBlob() const {
        return BlobRange.BlobId.IsValid() && BlobRange.Size;
    }

    void SetSnapshot(const TSnapshot& snap) {
        Y_VERIFY(snap.Valid());
        PlanStep = snap.PlanStep;
        TxId = snap.TxId;
    }

    void SetXSnapshot(const TSnapshot& snap) {
        Y_VERIFY(snap.Valid());
        XPlanStep = snap.PlanStep;
        XTxId = snap.TxId;
    }

    static TColumnRecord Make(ui64 granule, ui32 columnId, const TSnapshot& minSnapshot, ui64 portion, ui16 chunk = 0) {
        TColumnRecord row;
        row.Granule = granule;
        row.ColumnId = columnId;
        row.PlanStep = minSnapshot.PlanStep;
        row.TxId = minSnapshot.TxId;
        row.Portion = portion;
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

class TColumnsTable {
public:
    TColumnsTable(ui32 indexId)
        : IndexId(indexId)
    {}

    void Write(IDbWrapper& db, const TColumnRecord& row) {
        db.WriteColumn(IndexId, row);
    }

    void Erase(IDbWrapper& db, const TColumnRecord& row) {
        db.EraseColumn(IndexId, row);
    }

    bool Load(IDbWrapper& db, std::function<void(TColumnRecord&&)> callback) {
        return db.LoadColumns(IndexId, callback);
    }

private:
    ui32 IndexId;
};

class TCountersTable {
public:
    TCountersTable(ui32 indexId)
        : IndexId(indexId)
    {}

    void Write(IDbWrapper& db, ui32 counterId, ui64 value) {
        db.WriteCounter(IndexId, counterId, value);
    }

    bool Load(IDbWrapper& db, std::function<void(ui32 id, ui64 value)> callback) {
        return db.LoadCounters(IndexId, callback);
    }

private:
    ui32 IndexId;
};

}
