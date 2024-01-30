#include "insert_table.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap {

bool TInsertTable::Insert(IDbWrapper& dbTable, TInsertedData&& data) {
    if (auto* dataPtr = Summary.AddInserted(std::move(data))) {
        AddBlobLink(dataPtr->GetBlobRange().BlobId);
        dbTable.Insert(*dataPtr);
        return true;
    } else {
        return false;
    }
}

TInsertionSummary::TCounters TInsertTable::Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId,
                                             const THashSet<TWriteId>& writeIds, std::function<bool(ui64)> pathExists) {
    Y_ABORT_UNLESS(!writeIds.empty());

    TInsertionSummary::TCounters counters;
    for (auto writeId : writeIds) {
        std::optional<TInsertedData> data = Summary.ExtractInserted(writeId);
        Y_ABORT_UNLESS(data, "Commit %" PRIu64 ":%" PRIu64 " : writeId %" PRIu64 " not found", planStep, txId, (ui64)writeId);

        counters.Rows += data->GetMeta().GetNumRows();
        counters.RawBytes += data->GetMeta().GetRawBytes();
        counters.Bytes += data->BlobSize();

        dbTable.EraseInserted(*data);

        const ui64 pathId = data->PathId;
        auto* pathInfo = Summary.GetPathInfoOptional(pathId);
        // There could be commit after drop: propose, drop, plan
        if (pathInfo && pathExists(pathId)) {
            data->Commit(planStep, txId);
            dbTable.Commit(*data);

            pathInfo->AddCommitted(std::move(*data));
        } else {
            dbTable.Abort(*data);
            Summary.AddAborted(std::move(*data));
        }
    }

    return counters;
}

void TInsertTable::Abort(IDbWrapper& dbTable, const THashSet<TWriteId>& writeIds) {
    Y_ABORT_UNLESS(!writeIds.empty());

    for (auto writeId : writeIds) {
        // There could be inconsistency with txs and writes in case of bugs. So we could find no record for writeId.
        if (std::optional<TInsertedData> data = Summary.ExtractInserted(writeId)) {
            dbTable.EraseInserted(*data);
            dbTable.Abort(*data);
            Summary.AddAborted(std::move(*data));
        }
    }
}

THashSet<TWriteId> TInsertTable::OldWritesToAbort(const TInstant& now) const {
    return Summary.GetExpiredInsertions(now - WaitCommitDelay, CleanupPackageSize);
}

THashSet<TWriteId> TInsertTable::DropPath(IDbWrapper& dbTable, ui64 pathId) {
    auto pathInfo = Summary.ExtractPathInfo(pathId);
    if (!!pathInfo) {
        for (auto& data : pathInfo->GetCommitted()) {
            dbTable.EraseCommitted(data);
            TInsertedData copy = data;
            copy.Undo();
            dbTable.Abort(copy);
            Summary.AddAborted(std::move(copy));
        }
    }

    return Summary.GetInsertedByPathId(pathId);
}

void TInsertTable::EraseCommitted(IDbWrapper& dbTable, const TInsertedData& data, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction) {
    if (Summary.EraseCommitted(data)) {
        RemoveBlobLink(data.GetBlobRange().BlobId, blobsAction);
        dbTable.EraseCommitted(data);
    }
}

void TInsertTable::EraseAborted(IDbWrapper& dbTable, const TInsertedData& data, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction) {
    if (Summary.EraseAborted((TWriteId)data.WriteTxId)) {
        RemoveBlobLink(data.GetBlobRange().BlobId, blobsAction);
        dbTable.EraseAborted(data);
    }
}

bool TInsertTable::Load(IDbWrapper& dbTable, const TInstant loadTime) {
    Y_ABORT_UNLESS(!Loaded);
    Loaded = true;
    return dbTable.Load(*this, loadTime);
}

std::vector<TCommittedBlob> TInsertTable::Read(ui64 pathId, const TSnapshot& snapshot, const std::shared_ptr<arrow::Schema>& pkSchema) const {
    const TPathInfo* pInfo = Summary.GetPathInfoOptional(pathId);
    if (!pInfo) {
        return {};
    }

    std::vector<const TInsertedData*> ret;
    ret.reserve(pInfo->GetCommitted().size());

    for (const auto& data : pInfo->GetCommitted()) {
        if (std::less_equal<TSnapshot>()(data.GetSnapshot(), snapshot)) {
            ret.emplace_back(&data);
        }
    }
    const auto pred = [pkSchema](const TInsertedData* l, const TInsertedData* r) {
        return l->GetMeta().GetFirstPK(pkSchema) < r->GetMeta().GetFirstPK(pkSchema);
    };
    std::sort(ret.begin(), ret.end(), pred);

    std::vector<TCommittedBlob> result;
    result.reserve(ret.size());
    for (auto&& i : ret) {
        result.emplace_back(TCommittedBlob(i->GetBlobRange(), i->GetSnapshot(), i->GetSchemaVersion(), i->GetMeta().GetFirstPK(pkSchema), i->GetMeta().GetLastPK(pkSchema)));
    }

    return result;
}

bool TInsertTableAccessor::RemoveBlobLink(const TUnifiedBlobId& blobId, const std::shared_ptr<IBlobsDeclareRemovingAction>& blobsAction) {
    AFL_VERIFY(blobsAction);
    auto itBlob = BlobLinks.find(blobId);
    AFL_VERIFY(itBlob != BlobLinks.end());
    AFL_VERIFY(itBlob->second >= 1);
    if (itBlob->second == 1) {
        blobsAction->DeclareRemove(itBlob->first);
        BlobLinks.erase(itBlob);
        return true;
    } else {
        --itBlob->second;
        return false;
    }
}

}
