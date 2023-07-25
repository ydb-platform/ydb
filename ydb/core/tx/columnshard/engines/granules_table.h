#pragma once
#include "db_wrapper.h"
#include <ydb/core/formats/arrow/replace_key.h>

namespace NKikimr::NOlap {

struct TGranuleRecord {
private:
    TSnapshot CreatedAt;
public:
    ui64 PathId;
    ui64 Granule;
    NArrow::TStoreReplaceKey Mark;

    TGranuleRecord(ui64 pathId, ui64 granule, const TSnapshot& createdAt, const NArrow::TReplaceKey& mark)
        : CreatedAt(createdAt)
        , PathId(pathId)
        , Granule(granule)
        , Mark(mark)
    {
        Y_VERIFY(Mark.Size());
    }

    const TSnapshot& GetCreatedAt() const {
        return CreatedAt;
    }

    bool operator == (const TGranuleRecord& rec) const {
        return (PathId == rec.PathId) && (Mark == rec.Mark);
    }

    friend IOutputStream& operator << (IOutputStream& out, const TGranuleRecord& rec) {
        out << '{';
        auto& snap = rec.CreatedAt;
        out << rec.PathId << '#' << rec.Granule << ' '
            << snap;
        out << '}';
        return out;
    }

    TString DebugString() const {
        return TStringBuilder() << *this;
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

    bool Load(IDbWrapper& db, const std::function<void(const TGranuleRecord&)>& callback) {
        return db.LoadGranules(IndexId, Engine, callback);
    }

private:
    const IColumnEngine& Engine;
    ui32 IndexId;
};

}
