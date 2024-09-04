#pragma once
#include <ydb/core/tx/columnshard/counters/insert_table.h>
#include <ydb/library/accessor/accessor.h>
#include "path_info.h"

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

    THashMap<TWriteId, TInsertedData> Inserted;
    THashMap<TWriteId, TInsertedData> Aborted;
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
    void MarkAsNotAbortable(const TWriteId writeId) {
        auto it = Inserted.find(writeId);
        if (it == Inserted.end()) {
            return;
        }
        it->second.MarkAsNotAbortable();
    }

    THashSet<TWriteId> GetInsertedByPathId(const ui64 pathId) const;

    THashSet<TWriteId> GetExpiredInsertions(const TInstant timeBorder, const ui64 limit) const;

    const THashMap<TWriteId, TInsertedData>& GetInserted() const {
        return Inserted;
    }
    const THashMap<TWriteId, TInsertedData>& GetAborted() const {
        return Aborted;
    }

    const TInsertedData* AddAborted(TInsertedData&& data, const bool load = false);
    bool EraseAborted(const TWriteId writeId);
    bool HasAborted(const TWriteId writeId);

    bool EraseCommitted(const TInsertedData& data);
    bool HasCommitted(const TInsertedData& data);

    const TInsertedData* AddInserted(TInsertedData&& data, const bool load = false);
    std::optional<TInsertedData> ExtractInserted(const TWriteId id);

    const TCounters& GetCountersPrepared() const { return StatsPrepared; }
    const TCounters& GetCountersCommitted() const { return StatsCommitted; }
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

    bool IsOverloaded(const ui64 pathId) const;

    const std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Priorities;
    }
};

}
