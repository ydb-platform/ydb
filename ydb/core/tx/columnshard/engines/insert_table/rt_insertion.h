#pragma once
#include <ydb/core/tx/columnshard/counters/insert_table.h>
#include <ydb/library/accessor/accessor.h>
#include "path_info.h"

namespace NKikimr::NOlap {

class TInsertionSummary {
private:
    const NColumnShard::TInsertTableCounters Counters;
    YDB_READONLY(i64, CommittedSize, 0);
    YDB_READONLY(i64, InsertedSize, 0);
    std::map<ui64, std::set<const TPathInfo*>> Priorities;
    THashMap<ui64, TPathInfo> PathInfo;
    friend class TPathInfo;
    void RemovePriority(const TPathInfo& pathInfo) noexcept;
    void AddPriority(const TPathInfo& pathInfo) noexcept;

public:
    const NColumnShard::TInsertTableCounters& GetCounters() const {
        return Counters;
    }
    NKikimr::NOlap::TPathInfo& GetPathInfo(const ui64 pathId);
    std::optional<TPathInfo> ExtractPathInfo(const ui64 pathId);
    TPathInfo* GetPathInfoOptional(const ui64 pathId);
    const TPathInfo* GetPathInfoOptional(const ui64 pathId) const;

    const THashMap<ui64, TPathInfo>& GetPathInfo() const {
        return PathInfo;
    }

    void Clear();

    bool IsOverloaded(const ui64 pathId) const;

    const std::map<ui64, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Priorities;
    }
};

}
