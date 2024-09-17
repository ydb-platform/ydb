#pragma once
#include "committed.h"
#include "inserted.h"

#include <ydb/core/tx/columnshard/counters/insert_table.h>

#include <util/generic/noncopyable.h>

namespace NKikimr::NOlap {
class TInsertionSummary;

class TPathInfoIndexPriority {
public:
    enum class EIndexationPriority {
        PreventOverload = 100,
        PreventManyPortions = 50,
        NoPriority = 0
    };

private:
    YDB_READONLY(EIndexationPriority, Category, EIndexationPriority::NoPriority);
    const ui32 Weight;

public:
    TPathInfoIndexPriority(const EIndexationPriority category, const ui32 weight)
        : Category(category)
        , Weight(weight) {
    }

    bool operator!() const {
        return !Weight;
    }

    bool operator<(const TPathInfoIndexPriority& item) const {
        return std::tie(Category, Weight) < std::tie(item.Category, item.Weight);
    }
};

class TPathInfo: public TMoveOnly {
private:
    const ui64 PathId = 0;
    TSet<TCommittedData> Committed;
    YDB_READONLY(i64, CommittedSize, 0);
    YDB_READONLY(i64, InsertedSize, 0);
    bool CommittedOverload = false;
    bool InsertedOverload = false;
    TInsertionSummary* Summary = nullptr;
    const NColumnShard::TPathIdOwnedCounters PathIdCounters;

    bool SetCommittedOverload(const bool value);
    bool SetInsertedOverload(const bool value);

    void AddCommittedSize(const i64 size, const ui64 overloadLimit);

public:
    bool IsEmpty() const {
        return Committed.empty() && !InsertedSize;
    }

    void AddInsertedSize(const i64 size, const ui64 overloadLimit);

    explicit TPathInfo(TInsertionSummary& summary, const ui64 pathId);

    ui64 GetPathId() const {
        return PathId;
    }

    TPathInfoIndexPriority GetIndexationPriority() const;

    bool EraseCommitted(const TCommittedData& data);
    bool HasCommitted(const TCommittedData& data);

    const TSet<TCommittedData>& GetCommitted() const {
        return Committed;
    }

    bool AddCommitted(TCommittedData&& data, const bool load = false);

    bool IsOverloaded() const {
        return CommittedOverload || InsertedOverload;
    }
};

}   // namespace NKikimr::NOlap
