#pragma once
#include "db_wrapper.h"

namespace NKikimr::NOlap {

struct TGranuleRecord {
    ui64 PathId;
    ui64 Granule;
    TSnapshot CreatedAt;
    std::shared_ptr<arrow::Scalar> Mark;

    TGranuleRecord(ui64 pathId, ui64 granule, const TSnapshot& createdAt, const std::shared_ptr<arrow::Scalar>& mark)
        : PathId(pathId)
        , Granule(granule)
        , CreatedAt(createdAt)
        , Mark(mark)
    {
        Y_VERIFY(Mark);
    }

    bool operator == (const TGranuleRecord& rec) const {
        return (PathId == rec.PathId) && (Mark->Equals(*rec.Mark));
    }

    friend IOutputStream& operator << (IOutputStream& out, const TGranuleRecord& rec) {
        out << '{';
        auto& snap = rec.CreatedAt;
        out << rec.PathId << '#' << rec.Granule << ' '
            << snap.PlanStep << ':' << (snap.TxId == Max<ui64>() ? "max" : ToString(snap.TxId));
        out << '}';
        return out;
    }
};

class TGranulesTable {
public:
    TGranulesTable(const IColumnEngine& engine, ui32 indexId)
        : Engine(engine)
        , IndexId(indexId)
    {}

    void Write(IDbWrapper& db, const TGranuleRecord& row) {
        db.WriteGranule(IndexId, Engine, row);
    }

    void Erase(IDbWrapper& db, const TGranuleRecord& row) {
        db.EraseGranule(IndexId, Engine, row);
    }

    bool Load(IDbWrapper& db, std::function<void(TGranuleRecord&&)> callback) {
        return db.LoadGranules(IndexId, Engine, callback);
    }

private:
    const IColumnEngine& Engine;
    ui32 IndexId;
};

}
