#include "defs.h"
#include "insert_table.h"
#include "db_wrapper.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NOlap {

bool TInsertTable::Insert(IDbWrapper& dbTable, const TInsertedData& data) {
    TWriteId writeId{data.WriteTxId};
    if (Inserted.count(writeId)) {
        return false;
    }

    dbTable.Insert(data);
    Inserted[writeId] = data;
    return true;
}

TInsertTable::TCounters TInsertTable::Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId, ui64 metaShard,
                                             const THashSet<TWriteId>& writeIds) {
    Y_VERIFY(!writeIds.empty());
    Y_UNUSED(metaShard);

    TCounters counters;
    for (auto writeId : writeIds) {
        auto* data = Inserted.FindPtr(writeId);
        Y_VERIFY(data, "Commit %" PRIu64 ":%" PRIu64 " : writeId %" PRIu64 " not found", planStep, txId, (ui64)writeId);

        NKikimrTxColumnShard::TLogicalMetadata meta;
        if (meta.ParseFromString(data->Metadata)) {
            counters.Rows += meta.GetNumRows();
            counters.RawBytes += meta.GetRawBytes();
        }
        counters.Bytes += data->BlobSize();

        dbTable.EraseInserted(*data);

        data->Commit(planStep, txId);
        dbTable.Commit(*data);

        CommittedByPathId[data->PathId].emplace(std::move(*data));
        Inserted.erase(writeId);
    }

    return counters;
}

void TInsertTable::Abort(IDbWrapper& dbTable, ui64 metaShard, const THashSet<TWriteId>& writeIds) {
    Y_VERIFY(!writeIds.empty());
    Y_UNUSED(metaShard);

    for (auto writeId : writeIds) {
        auto* data = Inserted.FindPtr(writeId);
        Y_VERIFY(data, "Abort writeId %" PRIu64 " not found", (ui64)writeId);

        dbTable.EraseInserted(*data);
        dbTable.Abort(*data);

        Aborted[writeId] = std::move(Inserted[writeId]);
        Inserted.erase(writeId);
    }
}

THashSet<TWriteId> TInsertTable::AbortOld(IDbWrapper& dbTable, const TInstant& now) {
    // TODO: This protection does not save us from real flooder activity.
    // This cleanup is for seldom aborts caused by rare reasons. So there's a temporary simple O(N) here
    // keeping in mind we need a smarter cleanup logic here not a better algo.
    if (LastCleanup > now - CleanDelay) {
        return {};
    }
    LastCleanup = now;

    TInstant timeBorder = now - WaitCommitDelay;
    THashSet<TWriteId> toAbort;
    for (auto& [writeId, data] : Inserted) {
        if (data.DirtyTime < timeBorder) {
            toAbort.insert(writeId);
        }
    }

    if (!toAbort.empty()) {
        Abort(dbTable, 0, toAbort);
    }
    return toAbort;
}

THashSet<TWriteId> TInsertTable::DropPath(IDbWrapper& dbTable, ui64 pathId) {
    // Abort not committed

    THashSet<TWriteId> toAbort;
    for (auto& [writeId, data] : Inserted) {
        if (data.PathId == pathId) {
            toAbort.insert(writeId);
        }
    }

    if (!toAbort.empty()) {
        Abort(dbTable, 0, toAbort);
    }

    // Committed -> Aborted (for future cleanup)

    TSet<TInsertedData> committed = std::move(CommittedByPathId[pathId]);
    CommittedByPathId.erase(pathId);

    for (auto& data : committed) {
        dbTable.EraseCommitted(data);

        TInsertedData copy = data;
        copy.Undo();
        dbTable.Abort(copy);

        TWriteId writeId{copy.WriteTxId};
        Aborted.emplace(writeId, std::move(copy));
    }

    return toAbort;
}

void TInsertTable::EraseInserted(IDbWrapper& dbTable, const TInsertedData& data) {
    TWriteId writeId{data.WriteTxId};
    if (!Inserted.count(writeId)) {
        return;
    }

    dbTable.EraseInserted(data);
    Inserted.erase(writeId);
}

void TInsertTable::EraseCommitted(IDbWrapper& dbTable, const TInsertedData& data) {
    if (!CommittedByPathId.count(data.PathId)) {
        return;
    }

    dbTable.EraseCommitted(data);
    CommittedByPathId[data.PathId].erase(data);
}

void TInsertTable::EraseAborted(IDbWrapper& dbTable, const TInsertedData& data) {
    TWriteId writeId{data.WriteTxId};
    if (!Aborted.count(writeId)) {
        return;
    }

    dbTable.EraseAborted(data);
    Aborted.erase(writeId);
}

bool TInsertTable::Load(IDbWrapper& dbTable, const TInstant& loadTime) {
    Inserted.clear();
    CommittedByPathId.clear();
    Aborted.clear();

    return dbTable.Load(Inserted, CommittedByPathId, Aborted, loadTime);
}

std::vector<TCommittedBlob> TInsertTable::Read(ui64 pathId, ui64 plan, ui64 txId) const {
    const auto* committed = CommittedByPathId.FindPtr(pathId);
    if (!committed) {
        return {};
    }

    std::vector<TCommittedBlob> ret;
    ret.reserve(committed->size());

    for (auto& data : *committed) {
        if (snapLessOrEqual(data.ShardOrPlan, data.WriteTxId, plan, txId)) {
            ret.emplace_back(TCommittedBlob{data.BlobId, data.ShardOrPlan, data.WriteTxId});
        }
    }

    return ret;
}

void TInsertTable::SetOverloaded(ui64 pathId, bool overload) {
    if (overload) {
        PathsOverloaded.insert(pathId);
    } else {
        PathsOverloaded.erase(pathId);
    }
}

void TInsertTable::GetCounters(TCounters& prepared, TCounters& committed) const {
    prepared = TCounters();
    prepared.Rows = Inserted.size();
    for (auto& [_, data] : Inserted) {
        prepared.Bytes += data.BlobSize();
    }

    committed = TCounters();
    for (auto& [_, set] : CommittedByPathId) {
        committed.Rows += set.size();
        for (auto& data : set) {
            committed.Bytes += data.BlobSize();
        }
    }
}

}
