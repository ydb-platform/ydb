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
    TInsertionSummary Summary;

protected:
    void Clear() {
        Summary.Clear();
    }
public:
    const std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Summary.GetPathPriorities();
    }

    bool AddInserted(TInsertedData&& data, const bool load) {
        return Summary.AddInserted(std::move(data), load);
    }
    bool AddAborted(TInsertedData&& data, const bool load) {
        return Summary.AddAborted(std::move(data), load);
    }
    bool AddCommitted(TInsertedData&& data, const bool load) {
        const ui64 pathId = data.PathId;
        return Summary.GetPathInfo(pathId).AddCommitted(std::move(data), load);
    }
    const THashMap<TWriteId, TInsertedData>& GetAborted() const { return Summary.GetAborted(); }
    const THashMap<TWriteId, TInsertedData>& GetInserted() const { return Summary.GetInserted(); }
    const TInsertionSummary::TCounters& GetCountersPrepared() const {
        return Summary.GetCountersPrepared();
    }
    const TInsertionSummary::TCounters& GetCountersCommitted() const {
        return Summary.GetCountersCommitted();
    }
    bool IsOverloadedByCommitted(const ui64 pathId) const {
        return Summary.IsOverloaded(pathId);
    }
};

class TInsertTable: public TInsertTableAccessor {
public:
    static constexpr const TDuration WaitCommitDelay = TDuration::Minutes(10);
    static constexpr const TDuration CleanDelay = TDuration::Minutes(10);

    bool Insert(IDbWrapper& dbTable, TInsertedData&& data);
    TInsertionSummary::TCounters Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId, ui64 metaShard,
                     const THashSet<TWriteId>& writeIds, std::function<bool(ui64)> pathExists);
    void Abort(IDbWrapper& dbTable, ui64 metaShard, const THashSet<TWriteId>& writeIds);
    THashSet<TWriteId> OldWritesToAbort(const TInstant& now) const;
    THashSet<TWriteId> DropPath(IDbWrapper& dbTable, ui64 pathId);
    void EraseCommitted(IDbWrapper& dbTable, const TInsertedData& key);
    void EraseAborted(IDbWrapper& dbTable, const TInsertedData& key);
    std::vector<TCommittedBlob> Read(ui64 pathId, const TSnapshot& snapshot) const;
    bool Load(IDbWrapper& dbTable, const TInstant loadTime);
private:

    mutable TInstant LastCleanup;
};

}
