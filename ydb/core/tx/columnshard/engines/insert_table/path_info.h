#pragma once
#include <ydb/core/tx/columnshard/counters/insert_table.h>
#include <util/generic/noncopyable.h>
#include "data.h"

namespace NKikimr::NOlap {
class TInsertionSummary;
class TPathInfo: public TMoveOnly {
private:
    const ui64 PathId = 0;
    TSet<TInsertedData> Committed;
    i64 CommittedSize = 0;
    i64 InsertedSize = 0;
    bool CommittedOverload = false;
    bool InsertedOverload = false;
    TInsertionSummary* Summary = nullptr;
    const NColumnShard::TPathIdOwnedCounters PathIdCounters;

    bool SetCommittedOverload(const bool value);
    bool SetInsertedOverload(const bool value);

    void AddCommittedSize(const i64 size, const ui64 overloadLimit);

public:
    void AddInsertedSize(const i64 size, const ui64 overloadLimit);

    explicit TPathInfo(TInsertionSummary& summary, const ui64 pathId);

    ui64 GetPathId() const {
        return PathId;
    }

    ui64 GetIndexationPriority() const {
        return CommittedSize * Committed.size() * Committed.size();
    }

    bool EraseCommitted(const TInsertedData& data);

    const TSet<TInsertedData>& GetCommitted() const {
        return Committed;
    }

    bool AddCommitted(TInsertedData&& data);

    bool IsOverloaded() const {
        return CommittedOverload || InsertedOverload;
    }
};

}
