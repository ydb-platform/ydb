#pragma once
#include "inserted.h"
#include "path_info.h"

#include <ydb/core/tx/columnshard/counters/insert_table.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {
class IBlobsDeclareRemovingAction;
class TInsertionSummary {
public:
    struct TCounters {
        ui64 Rows{};
        ui64 Bytes{};
        ui64 RawBytes{};
    };

private:
    friend class TPathInfo;
    TCounters StatsPrepared;
    TCounters StatsCommitted;
    const NColumnShard::TInsertTableCounters Counters;

    THashMap<TInsertWriteId, TInsertedData> Inserted;
    THashMap<TInsertWriteId, TInsertedData> Aborted;
    mutable TInstant MinInsertedTs = TInstant::Zero();

    std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>> Priorities;
    THashMap<ui64, TPathInfo> PathInfo;
    void RemovePriority(const TPathInfo& pathInfo) noexcept;
    void AddPriority(const TPathInfo& pathInfo) noexcept;

    void OnNewCommitted(const ui64 dataSize, const bool load = false) noexcept;
    void OnEraseCommitted(TPathInfo& pathInfo, const ui64 dataSize) noexcept;
    void OnNewInserted(TPathInfo& pathInfo, const ui64 dataSize, const bool load) noexcept;
    void OnEraseInserted(TPathInfo& pathInfo, const ui64 dataSize) noexcept;
    static TAtomicCounter CriticalInserted;

public:
    bool HasPathIdData(const ui64 pathId) const {
        auto it = PathInfo.find(pathId);
        if (it == PathInfo.end()) {
            return false;
        }
        return !it->second.IsEmpty();
    }

    void ErasePath(const ui64 pathId) {
        auto it = PathInfo.find(pathId);
        if (it == PathInfo.end()) {
            return;
        }
        RemovePriority(it->second);
        AFL_VERIFY(it->second.IsEmpty());
        PathInfo.erase(it);
    }

    void MarkAsNotAbortable(const TInsertWriteId writeId) {
        auto it = Inserted.find(writeId);
        if (it == Inserted.end()) {
            return;
        }
        it->second.MarkAsNotAbortable();
    }

    THashSet<TInsertWriteId> GetInsertedByPathId(const ui64 pathId) const;

    THashSet<TInsertWriteId> GetExpiredInsertions(const TInstant timeBorder, const ui64 limit) const;

    const THashMap<TInsertWriteId, TInsertedData>& GetInserted() const {
        return Inserted;
    }
    const THashMap<TInsertWriteId, TInsertedData>& GetAborted() const {
        return Aborted;
    }

    const TInsertedData* AddAborted(TInsertedData&& data, const bool load = false);
    bool EraseAborted(const TInsertWriteId writeId);
    bool HasAborted(const TInsertWriteId writeId);

    bool EraseCommitted(const TCommittedData& data);
    bool HasCommitted(const TCommittedData& data);

    const TInsertedData* AddInserted(TInsertedData&& data, const bool load = false);
    std::optional<TInsertedData> ExtractInserted(const TInsertWriteId id);

    const TCounters& GetCountersPrepared() const {
        return StatsPrepared;
    }
    const TCounters& GetCountersCommitted() const {
        return StatsCommitted;
    }
    const NColumnShard::TInsertTableCounters& GetCounters() const {
        return Counters;
    }
    NKikimr::NOlap::TPathInfo& GetPathInfo(const ui64 pathId);
    TPathInfo* GetPathInfoOptional(const ui64 pathId);
    const TPathInfo* GetPathInfoOptional(const ui64 pathId) const;

    const THashMap<ui64, TPathInfo>& GetPathInfo() const {
        return PathInfo;
    }

    bool IsOverloaded(const ui64 pathId) const;

    const std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Priorities;
    }
};

}   // namespace NKikimr::NOlap
