#pragma once
#include "committed.h"
#include "inserted.h"
#include "path_info.h"
#include "rt_insertion.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/counters/insert_table.h>

namespace NKikimr::NOlap {
class TPKRangesFilter;
class IDbWrapper;

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
    void ErasePath(const ui64 pathId) {
        Summary.ErasePath(pathId);
    }
    bool HasDataInPathId(const ui64 pathId) const {
        return Summary.HasPathIdData(pathId);
    }
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
        AFL_VERIFY_DEBUG(!Summary.ExtractInserted(data.GetInsertWriteId()));
        if (load) {
            AddBlobLink(data.GetBlobRange().BlobId);
        }
        return Summary.AddAborted(std::move(data), load);
    }
    bool AddCommitted(TCommittedData&& data, const bool load) {
        if (load) {
            AddBlobLink(data.GetBlobRange().BlobId);
        }
        const ui64 pathId = data.GetPathId();
        return Summary.GetPathInfo(pathId).AddCommitted(std::move(data), load);
    }
    bool HasPathIdData(const ui64 pathId) const {
        return Summary.HasPathIdData(pathId);
    }
    const THashMap<TInsertWriteId, TInsertedData>& GetAborted() const {
        return Summary.GetAborted();
    }
    const TInsertedContainer& GetInserted() const {
        return Summary.GetInserted();
    }
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
    TInsertWriteId LastWriteId = TInsertWriteId{ 0 };

public:
    static constexpr const TDuration WaitCommitDelay = TDuration::Minutes(10);
    static constexpr ui64 CleanupPackageSize = 10000;

    bool Insert(IDbWrapper& dbTable, TInsertedData&& data);
    TInsertionSummary::TCounters Commit(
        IDbWrapper& dbTable, ui64 planStep, ui64 txId, const THashSet<TInsertWriteId>& writeIds, std::function<bool(ui64)> pathExists);
    TInsertionSummary::TCounters CommitEphemeral(IDbWrapper& dbTable, TCommittedData&& data);
    void Abort(IDbWrapper& dbTable, const THashSet<TInsertWriteId>& writeIds);
    void MarkAsNotAbortable(const TInsertWriteId writeId) {
        Summary.MarkAsNotAbortable(writeId);
    }
    THashSet<TInsertWriteId> OldWritesToAbort(const TInstant& now) const;

    void EraseCommittedOnExecute(
        IDbWrapper& dbTable, const TCommittedData& key, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction);
    void EraseCommittedOnComplete(const TCommittedData& key);

    void EraseAbortedOnExecute(IDbWrapper& dbTable, const TInsertedData& key, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction);
    void EraseAbortedOnComplete(const TInsertedData& key);

    std::vector<TCommittedBlob> Read(ui64 pathId, const std::optional<ui64> lockId, const TSnapshot& reqSnapshot,
        const std::shared_ptr<arrow::Schema>& pkSchema, const TPKRangesFilter* pkRangesFilter) const;
    bool Load(NIceDb::TNiceDb& db, IDbWrapper& dbTable, const TInstant loadTime);

    TInsertWriteId BuildNextWriteId(NTabletFlatExecutor::TTransactionContext& txc);
    TInsertWriteId BuildNextWriteId(NIceDb::TNiceDb& db);
};

}   // namespace NKikimr::NOlap
