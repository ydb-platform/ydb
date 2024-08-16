#pragma once
#include "data.h"
#include "rt_insertion.h"
#include "path_info.h"
#include <ydb/core/tx/columnshard/counters/insert_table.h>

namespace NKikimr::NOlap {

class IDbWrapper;

/// Use one table for inserted and committed blobs:
/// !Commited => {PlanStep, WriteTxId} are {0, WriteId}
///  Commited => {PlanStep, WriteTxId} are {PlanStep, TxId}

class TInsertTableAccessor {
protected:
    TInsertionSummary Summary;
    THashMap<TUnifiedBlobId, ui32> BlobLinks;

    void AddBlobLink(const TUnifiedBlobId& blobId) {
        ++BlobLinks[blobId];
    }

    bool RemoveBlobLinkOnExecute(const TUnifiedBlobId& blobId, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction);
    bool RemoveBlobLinkOnComplete(const TUnifiedBlobId& blobId);
public:
    const std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Summary.GetPathPriorities();
    }

    std::optional<TSnapshot> GetMinCommittedSnapshot(const ui64 pathId) const {
        auto* info = Summary.GetPathInfoOptional(pathId);
        if (!info) {
            return {};
        } else if (info->GetCommitted().empty()) {
            return {};
        } else {
            return info->GetCommitted().begin()->GetSnapshot();
        }
    }

    bool AddInserted(TInsertedData&& data, const bool load) {
        if (load) {
            AddBlobLink(data.GetBlobRange().BlobId);
        }
        return Summary.AddInserted(std::move(data), load);
    }
    bool AddAborted(TInsertedData&& data, const bool load) {
        if (load) {
            AddBlobLink(data.GetBlobRange().BlobId);
        }
        return Summary.AddAborted(std::move(data), load);
    }
    bool AddCommitted(TInsertedData&& data, const bool load) {
        if (load) {
            AddBlobLink(data.GetBlobRange().BlobId);
        }
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
private:
    bool Loaded = false;
public:
    static constexpr const TDuration WaitCommitDelay = TDuration::Minutes(10);
    static constexpr ui64 CleanupPackageSize = 10000;

    bool Insert(IDbWrapper& dbTable, TInsertedData&& data);
    TInsertionSummary::TCounters Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId,
                     const THashSet<TWriteId>& writeIds, std::function<bool(ui64)> pathExists);
    void Abort(IDbWrapper& dbTable, const THashSet<TWriteId>& writeIds);
    void MarkAsNotAbortable(const TWriteId writeId) {
        Summary.MarkAsNotAbortable(writeId);
    }
    THashSet<TWriteId> OldWritesToAbort(const TInstant& now) const;
    THashSet<TWriteId> DropPath(IDbWrapper& dbTable, ui64 pathId);

    void EraseCommittedOnExecute(IDbWrapper& dbTable, const TInsertedData& key, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction);
    void EraseCommittedOnComplete(const TInsertedData& key);

    void EraseAbortedOnExecute(IDbWrapper& dbTable, const TInsertedData& key, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction);
    void EraseAbortedOnComplete(const TInsertedData& key);

    std::vector<TCommittedBlob> Read(ui64 pathId, const TSnapshot& snapshot, const std::shared_ptr<arrow::Schema>& pkSchema) const;
    bool Load(IDbWrapper& dbTable, const TInstant loadTime);
};

}
