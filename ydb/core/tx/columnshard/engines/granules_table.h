#pragma once
#include "db_wrapper.h"

namespace NKikimr::NOlap {

struct TGranuleRecord {
    ui64 PathId;
    TString IndexKey;
    ui64 Granule;
    TSnapshot CreatedAt;

    bool operator == (const TGranuleRecord& rec) const {
        return (PathId == rec.PathId) && (IndexKey == rec.IndexKey);
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
    TGranulesTable(ui32 indexId)
        : IndexId(indexId)
    {}

    void Write(IDbWrapper& db, const TGranuleRecord& row) {
        db.WriteGranule(IndexId, row);
    }

    void Erase(IDbWrapper& db, const TGranuleRecord& row) {
        db.EraseGranule(IndexId, row);
    }

    bool Load(IDbWrapper& db, std::function<void(TGranuleRecord&&)> callback) {
        return db.LoadGranules(IndexId, callback);
    }

private:
    ui32 IndexId;
};

}
