#include "insert_table.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap {

bool TInsertTable::Insert(IDbWrapper& dbTable, TInsertedData&& data) {
    if (auto* dataPtr = Summary.AddInserted(std::move(data))) {
        dbTable.Insert(*dataPtr);
        return true;
    } else {
        return false;
    }
}

TInsertionSummary::TCounters TInsertTable::Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId, ui64 metaShard,
                                             const THashSet<TWriteId>& writeIds, std::function<bool(ui64)> pathExists) {
    Y_VERIFY(!writeIds.empty());
    Y_UNUSED(metaShard);

    TInsertionSummary::TCounters counters;
    for (auto writeId : writeIds) {
        std::optional<TInsertedData> data = Summary.ExtractInserted(writeId);
        Y_VERIFY(data, "Commit %" PRIu64 ":%" PRIu64 " : writeId %" PRIu64 " not found", planStep, txId, (ui64)writeId);

        NKikimrTxColumnShard::TLogicalMetadata meta;
        if (meta.ParseFromString(data->Metadata)) {
            counters.Rows += meta.GetNumRows();
            counters.RawBytes += meta.GetRawBytes();
        }
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

void TInsertTable::Abort(IDbWrapper& dbTable, ui64 metaShard, const THashSet<TWriteId>& writeIds) {
    Y_VERIFY(!writeIds.empty());
    Y_UNUSED(metaShard);

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
    // TODO: This protection does not save us from real flooder activity.
    // This cleanup is for seldom aborts caused by rare reasons. So there's a temporary simple O(N) here
    // keeping in mind we need a smarter cleanup logic here not a better algo.
    if (LastCleanup > now - CleanDelay) {
        return {};
    }
    LastCleanup = now;

    const TInstant timeBorder = now - WaitCommitDelay;
    return Summary.GetDeprecatedInsertions(timeBorder);
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

void TInsertTable::EraseCommitted(IDbWrapper& dbTable, const TInsertedData& data) {
    if (Summary.EraseCommitted(data)) {
        dbTable.EraseCommitted(data);
    }
}

void TInsertTable::EraseAborted(IDbWrapper& dbTable, const TInsertedData& data) {
    if (Summary.EraseAborted((TWriteId)data.WriteTxId)) {
        dbTable.EraseAborted(data);
    }
}

bool TInsertTable::Load(IDbWrapper& dbTable, const TInstant loadTime) {
    Clear();
    return dbTable.Load(*this, loadTime);
}

std::vector<TCommittedBlob> TInsertTable::Read(ui64 pathId, const TSnapshot& snapshot) const {
    const TPathInfo* pInfo = Summary.GetPathInfoOptional(pathId);
    if (!pInfo) {
        return {};
    }

    std::vector<TCommittedBlob> ret;
    ret.reserve(pInfo->GetCommitted().size());

    for (const auto& data : pInfo->GetCommitted()) {
        if (std::less_equal<TSnapshot>()(data.GetSnapshot(), snapshot)) {
            ret.emplace_back(TCommittedBlob(data.BlobId, data.GetSnapshot(), data.GetSchemaSnapshot()));
        }
    }

    return ret;
}

}
