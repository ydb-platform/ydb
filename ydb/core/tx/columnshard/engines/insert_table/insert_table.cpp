#include "insert_table.h"

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>

namespace NKikimr::NOlap {

bool TInsertTable::Insert(IDbWrapper& dbTable, TInsertedData&& data) {
    if (auto* dataPtr = Summary.AddInserted(std::move(data))) {
        AddBlobLink(dataPtr->GetBlobRange().BlobId);
        dbTable.Insert(*dataPtr);
        return true;
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "skip_insertion");
        return false;
    }
}

TInsertionSummary::TCounters TInsertTable::Commit(
    IDbWrapper& dbTable, ui64 planStep, ui64 txId, const THashSet<TInsertWriteId>& writeIds, std::function<bool(ui64)> pathExists) {
    Y_ABORT_UNLESS(!writeIds.empty());

    TInsertionSummary::TCounters counters;
    for (auto writeId : writeIds) {
        std::optional<TInsertedData> data = Summary.ExtractInserted(writeId);
        if (!data) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("ps", planStep)("tx", txId)("write_id", (ui64)writeId)("event", "hasn't data for commit");
            continue;
        }

        counters.Rows += data->GetMeta().GetNumRows();
        counters.RawBytes += data->GetMeta().GetRawBytes();
        counters.Bytes += data->BlobSize();

        dbTable.EraseInserted(*data);

        const ui64 pathId = data->GetPathId();
        auto* pathInfo = Summary.GetPathInfoOptional(pathId);
        // There could be commit after drop: propose, drop, plan
        if (pathInfo && pathExists(pathId)) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "commit_insertion")("path_id", data->GetPathId())(
                "blob_range", data->GetBlobRange().ToString());
            auto committed = data->Commit(planStep, txId);
            dbTable.Commit(committed);

            pathInfo->AddCommitted(std::move(committed));
        } else {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "abort_insertion")("path_id", data->GetPathId())(
                "blob_range", data->GetBlobRange().ToString());
            dbTable.Abort(*data);
            Summary.AddAborted(std::move(*data));
        }
    }

    return counters;
}

TInsertionSummary::TCounters TInsertTable::CommitEphemeral(IDbWrapper& dbTable, TCommittedData&& data) {
    TInsertionSummary::TCounters counters;
    counters.Rows += data.GetMeta().GetNumRows();
    counters.RawBytes += data.GetMeta().GetRawBytes();
    counters.Bytes += data.BlobSize();

    AddBlobLink(data.GetBlobRange().BlobId);
    const ui64 pathId = data.GetPathId();
    auto& pathInfo = Summary.GetPathInfo(pathId);
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "commit_insertion")("path_id", pathId)("blob_range", data.GetBlobRange().ToString());
    dbTable.Commit(data);
    pathInfo.AddCommitted(std::move(data));

    return counters;
}

void TInsertTable::Abort(IDbWrapper& dbTable, const THashSet<TInsertWriteId>& writeIds) {
    Y_ABORT_UNLESS(!writeIds.empty());

    for (auto writeId : writeIds) {
        // There could be inconsistency with txs and writes in case of bugs. So we could find no record for writeId.
        if (std::optional<TInsertedData> data = Summary.ExtractInserted(writeId)) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "abort_insertion")("path_id", data->GetPathId())(
                "blob_range", data->GetBlobRange().ToString())("write_id", writeId);
            dbTable.EraseInserted(*data);
            dbTable.Abort(*data);
            Summary.AddAborted(std::move(*data));
        }
    }
}

THashSet<TInsertWriteId> TInsertTable::OldWritesToAbort(const TInstant& now) const {
    return Summary.GetExpiredInsertions(now - WaitCommitDelay, CleanupPackageSize);
}

void TInsertTable::EraseCommittedOnExecute(
    IDbWrapper& dbTable, const TCommittedData& data, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction) {
    if (Summary.HasCommitted(data)) {
        dbTable.EraseCommitted(data);
        RemoveBlobLinkOnExecute(data.GetBlobRange().BlobId, blobsAction);
    }
}

void TInsertTable::EraseCommittedOnComplete(const TCommittedData& data) {
    if (Summary.EraseCommitted(data)) {
        RemoveBlobLinkOnComplete(data.GetBlobRange().BlobId);
    }
}

void TInsertTable::EraseAbortedOnExecute(
    IDbWrapper& dbTable, const TInsertedData& data, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction) {
    if (Summary.HasAborted(data.GetInsertWriteId())) {
        dbTable.EraseAborted(data);
        RemoveBlobLinkOnExecute(data.GetBlobRange().BlobId, blobsAction);
    }
}

void TInsertTable::EraseAbortedOnComplete(const TInsertedData& data) {
    if (Summary.EraseAborted(data.GetInsertWriteId())) {
        RemoveBlobLinkOnComplete(data.GetBlobRange().BlobId);
    }
}

bool TInsertTable::Load(NIceDb::TNiceDb& db, IDbWrapper& dbTable, const TInstant loadTime) {
    Y_ABORT_UNLESS(!Loaded);
    Loaded = true;
    LastWriteId = (TInsertWriteId)0;
    if (!NColumnShard::Schema::GetSpecialValueOpt(db, NColumnShard::Schema::EValueIds::LastWriteId, LastWriteId)) {
        return false;
    }

    return dbTable.Load(*this, loadTime);
}

std::vector<TCommittedBlob> TInsertTable::Read(ui64 pathId, const std::optional<ui64> lockId, const TSnapshot& reqSnapshot,
    const std::shared_ptr<arrow::Schema>& pkSchema, const TPKRangesFilter* pkRangesFilter) const {
    const TPathInfo* pInfo = Summary.GetPathInfoOptional(pathId);
    if (!pInfo) {
        return {};
    }

    std::vector<TCommittedBlob> result;
    result.reserve(pInfo->GetCommitted().size() + Summary.GetInserted().size());

    for (const auto& data : pInfo->GetCommitted()) {
        if (lockId || data.GetSnapshot() <= reqSnapshot) {
            auto start = data.GetMeta().GetFirstPK(pkSchema);
            auto finish = data.GetMeta().GetLastPK(pkSchema);
            if (pkRangesFilter && pkRangesFilter->IsPortionInPartialUsage(start, finish) == TPKRangeFilter::EUsageClass::DontUsage) {
                continue;
            }
            result.emplace_back(TCommittedBlob(data.GetBlobRange(), data.GetSnapshot(), data.GetInsertWriteId(), data.GetSchemaVersion(), data.GetMeta().GetNumRows(),
                start, finish, data.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete, data.GetMeta().GetSchemaSubset()));
        }
    }
    if (lockId) {
        for (const auto& [writeId, data] : Summary.GetInserted()) {
            if (data.GetPathId() != pathId) {
                continue;
            }
            auto start = data.GetMeta().GetFirstPK(pkSchema);
            auto finish = data.GetMeta().GetLastPK(pkSchema);
            if (pkRangesFilter && pkRangesFilter->IsPortionInPartialUsage(start, finish) == TPKRangeFilter::EUsageClass::DontUsage) {
                continue;
            }
            result.emplace_back(TCommittedBlob(data.GetBlobRange(), writeId, data.GetSchemaVersion(), data.GetMeta().GetNumRows(), start, finish,
                data.GetMeta().GetModificationType() == NEvWrite::EModificationType::Delete, data.GetMeta().GetSchemaSubset()));
        }
    }
    return result;
}

TInsertWriteId TInsertTable::BuildNextWriteId(NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    return BuildNextWriteId(db);
}

TInsertWriteId TInsertTable::BuildNextWriteId(NIceDb::TNiceDb& db) {
    TInsertWriteId writeId = ++LastWriteId;
    NColumnShard::Schema::SaveSpecialValue(db, NColumnShard::Schema::EValueIds::LastWriteId, (ui64)writeId);
    return writeId;
}

bool TInsertTableAccessor::RemoveBlobLinkOnExecute(
    const TUnifiedBlobId& blobId, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction) {
    AFL_VERIFY(blobsAction);
    auto itBlob = BlobLinks.find(blobId);
    AFL_VERIFY(itBlob != BlobLinks.end());
    AFL_VERIFY(itBlob->second >= 1);
    if (itBlob->second == 1) {
        blobsAction->DeclareSelfRemove(itBlob->first);
        return true;
    } else {
        return false;
    }
}

bool TInsertTableAccessor::RemoveBlobLinkOnComplete(const TUnifiedBlobId& blobId) {
    auto itBlob = BlobLinks.find(blobId);
    AFL_VERIFY(itBlob != BlobLinks.end());
    AFL_VERIFY(itBlob->second >= 1);
    if (itBlob->second == 1) {
        BlobLinks.erase(itBlob);
        return true;
    } else {
        --itBlob->second;
        return false;
    }
}

}   // namespace NKikimr::NOlap
