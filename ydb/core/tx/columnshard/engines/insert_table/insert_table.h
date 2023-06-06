#pragma once
#include "data.h"
#include "rt_insertion.h"
#include "path_info.h"
#include <ydb/core/tx/columnshard/counters/insert_table.h>

namespace NKikimr::NOlap {

class IDbWrapper;

/// Use one table for inserted and committed blobs:
/// !Commited => {ShardOrPlan, WriteTxId} are {MetaShard, WriteId}
///  Commited => {ShardOrPlan, WriteTxId} are {PlanStep, TxId}

class TInsertTableAccessor {
protected:
    THashMap<TWriteId, TInsertedData> Inserted;
    THashMap<TWriteId, TInsertedData> Aborted;
    TInsertionSummary Summary;

protected:
    void Clear() {
        Inserted.clear();
        Summary.Clear();
        Aborted.clear();
    }
public:
    const std::map<ui64, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Summary.GetPathPriorities();
    }

    bool AddInserted(const TWriteId& writeId, TInsertedData&& data) {
        return Inserted.emplace(writeId, std::move(data)).second;
    }
    bool AddAborted(const TWriteId& writeId, TInsertedData&& data) {
        return Aborted.emplace(writeId, std::move(data)).second;
    }
    bool AddCommitted(TInsertedData&& data) {
        const ui64 pathId = data.PathId;
        return Summary.GetPathInfo(pathId).AddCommitted(std::move(data));
    }
};

class TInsertTable: public TInsertTableAccessor {
private:
    void OnNewInserted(TPathInfo& pathInfo, const ui64 dataSize, const bool load = false) noexcept;
    void OnNewCommitted(const ui64 dataSize, const bool load = false) noexcept;
    void OnEraseInserted(TPathInfo& pathInfo, const ui64 dataSize) noexcept;
    void OnEraseCommitted(TPathInfo& pathInfo, const ui64 dataSize) noexcept;

public:
    static constexpr const TDuration WaitCommitDelay = TDuration::Hours(24);
    static constexpr const TDuration CleanDelay = TDuration::Minutes(10);

    struct TCounters {
        ui64 Rows{};
        ui64 Bytes{};
        ui64 RawBytes{};
    };

    bool Insert(IDbWrapper& dbTable, TInsertedData&& data);
    TCounters Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId, ui64 metaShard,
                     const THashSet<TWriteId>& writeIds, std::function<bool(ui64)> pathExists);
    void Abort(IDbWrapper& dbTable, ui64 metaShard, const THashSet<TWriteId>& writeIds);
    THashSet<TWriteId> OldWritesToAbort(const TInstant& now) const;
    THashSet<TWriteId> DropPath(IDbWrapper& dbTable, ui64 pathId);
    void EraseCommitted(IDbWrapper& dbTable, const TInsertedData& key);
    void EraseAborted(IDbWrapper& dbTable, const TInsertedData& key);
    std::vector<TCommittedBlob> Read(ui64 pathId, const TSnapshot& snapshot) const;
    bool Load(IDbWrapper& dbTable, const TInstant& loadTime);
    const TCounters& GetCountersPrepared() const { return StatsPrepared; }
    const TCounters& GetCountersCommitted() const { return StatsCommitted; }

    size_t InsertedSize() const { return Inserted.size(); }
    const THashMap<TWriteId, TInsertedData>& GetAborted() const { return Aborted; }
    bool IsOverloadedByCommitted(const ui64 pathId) const {
        return Summary.IsOverloaded(pathId);
    }
private:

    mutable TInstant LastCleanup;
    TCounters StatsPrepared;
    TCounters StatsCommitted;
    const NColumnShard::TInsertTableCounters Counters;
};

}
