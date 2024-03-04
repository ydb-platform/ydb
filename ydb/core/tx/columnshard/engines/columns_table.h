#pragma once

#include "db_wrapper.h"
#include "portions/column_record.h"

#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NOlap {

class TColumnsTable {
public:
    TColumnsTable(ui32 indexId)
        : IndexId(indexId)
    {}

    void Write(IDbWrapper& db, const TPortionInfo& portion, const TColumnRecord& row) {
        db.WriteColumn(IndexId, portion, row);
    }

    void Erase(IDbWrapper& db, const TPortionInfo& portion, const TColumnRecord& row) {
        db.EraseColumn(IndexId, portion, row);
    }

    bool Load(IDbWrapper& db, std::function<void(const TPortionInfo&, const TColumnChunkLoadContext&)> callback) {
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
