#pragma once
#include <ydb/core/tx/columnshard/counters/insert_table.h>
#include <util/generic/noncopyable.h>
#include "data.h"

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
    const EIndexationPriority Category;
    const ui32 Weight;
public:
    TPathInfoIndexPriority(const EIndexationPriority category, const ui32 weight)
        : Category(category)
        , Weight(weight)
    {

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

    TPathInfoIndexPriority GetIndexationPriority() const;

    bool EraseCommitted(const TInsertedData& data);

    const TSet<TInsertedData>& GetCommitted() const {
        return Committed;
    }

    bool AddCommitted(TInsertedData&& data, const bool load = false);

    bool IsOverloaded() const {
        return CommittedOverload || InsertedOverload;
    }
};

}
