#include "defs.h"
#include "insert_table.h"
#include "db_wrapper.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

namespace NKikimr::NOlap {

bool TInsertTable::Insert(IDbWrapper& dbTable, TInsertedData&& data) {
    TWriteId writeId{data.WriteTxId};
    if (Inserted.contains(writeId)) {
        return false;
    }

    dbTable.Insert(data);
    ui32 dataSize = data.BlobSize();
    if (Inserted.emplace(writeId, std::move(data)).second) {
        StatsPrepared.Rows = Inserted.size();
        StatsPrepared.Bytes += dataSize;
    }
    return true;
}

TInsertTable::TCounters TInsertTable::Commit(IDbWrapper& dbTable, ui64 planStep, ui64 txId, ui64 metaShard,
                                             const THashSet<TWriteId>& writeIds, std::function<bool(ui64)> pathExists) {
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

        ui32 dataSize = data->BlobSize();

        // There could be commit after drop: propose, drop, plan
        if (pathExists(data->PathId)) {
            data->Commit(planStep, txId);
            dbTable.Commit(*data);

            if (CommittedByPathId[data->PathId].emplace(std::move(*data)).second) {
                ++StatsCommitted.Rows;
                StatsCommitted.Bytes += dataSize;
            }
        } else {
             dbTable.Abort(*data);
             Aborted.emplace(writeId, std::move(*data));
        }

        if (Inserted.erase(writeId)) {
            StatsPrepared.Rows = Inserted.size();
            StatsPrepared.Bytes -= dataSize;
        }
    }

    return counters;
}

void TInsertTable::Abort(IDbWrapper& dbTable, ui64 metaShard, const THashSet<TWriteId>& writeIds) {
    Y_VERIFY(!writeIds.empty());
    Y_UNUSED(metaShard);

    for (auto writeId : writeIds) {
        // There could be inconsistency with txs and writes in case of bugs. So we could find no record for writeId.
        if (auto* data = Inserted.FindPtr(writeId)) {
            dbTable.EraseInserted(*data);
            dbTable.Abort(*data);

            ui32 dataSize = data->BlobSize();
            Aborted.emplace(writeId, std::move(*data));
            if (Inserted.erase(writeId)) {
                StatsPrepared.Rows = Inserted.size();
                StatsPrepared.Bytes -= dataSize;
            }
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

    TInstant timeBorder = now - WaitCommitDelay;
    THashSet<TWriteId> toAbort;
    for (auto& [writeId, data] : Inserted) {
        if (data.DirtyTime && data.DirtyTime < timeBorder) {
            toAbort.insert(writeId);
        }
    }
    return toAbort;
}

THashSet<TWriteId> TInsertTable::DropPath(IDbWrapper& dbTable, ui64 pathId) {
    // Committed -> Aborted (for future cleanup)

    TSet<TInsertedData> committed = std::move(CommittedByPathId[pathId]);
    CommittedByPathId.erase(pathId);

    StatsCommitted.Rows -= committed.size();
    for (auto& data : committed) {
        StatsCommitted.Bytes -= data.BlobSize();

        dbTable.EraseCommitted(data);

        TInsertedData copy = data;
        copy.Undo();
        dbTable.Abort(copy);

        TWriteId writeId{copy.WriteTxId};
        Aborted.emplace(writeId, std::move(copy));
    }

    // Return not committed writes for abort. Tablet filter this list with proposed ones befor Abort().

    THashSet<TWriteId> toAbort;
    for (auto& [writeId, data] : Inserted) {
        if (data.PathId == pathId) {
            toAbort.insert(writeId);
        }
    }

    return toAbort;
}

void TInsertTable::EraseCommitted(IDbWrapper& dbTable, const TInsertedData& data) {
    if (!CommittedByPathId.contains(data.PathId)) {
        return;
    }

    dbTable.EraseCommitted(data);
    if (CommittedByPathId[data.PathId].erase(data)) {
        --StatsCommitted.Rows;
        StatsCommitted.Bytes -= data.BlobSize();
    }
}

void TInsertTable::EraseAborted(IDbWrapper& dbTable, const TInsertedData& data) {
    TWriteId writeId{data.WriteTxId};
    if (!Aborted.contains(writeId)) {
        return;
    }

    dbTable.EraseAborted(data);
    Aborted.erase(writeId);
}

bool TInsertTable::Load(IDbWrapper& dbTable, const TInstant& loadTime) {
    Inserted.clear();
    CommittedByPathId.clear();
    Aborted.clear();

    if (!dbTable.Load(Inserted, CommittedByPathId, Aborted, loadTime)) {
        return false;
    }

    // update stats

    StatsPrepared = {};
    StatsCommitted = {};

    StatsPrepared.Rows = Inserted.size();
    for (auto& [_, data] : Inserted) {
        StatsPrepared.Bytes += data.BlobSize();
    }

    for (auto& [_, set] : CommittedByPathId) {
        StatsCommitted.Rows += set.size();
        for (auto& data : set) {
            StatsCommitted.Bytes += data.BlobSize();
        }
    }

    return true;
}

std::vector<TCommittedBlob> TInsertTable::Read(ui64 pathId, const TSnapshot& snapshot) const {
    const auto* committed = CommittedByPathId.FindPtr(pathId);
    if (!committed) {
        return {};
    }

    std::vector<TCommittedBlob> ret;
    ret.reserve(committed->size());

    for (const auto& data : *committed) {
        if (std::less_equal<TSnapshot>()(data.GetSnapshot(), snapshot)) {
            ret.emplace_back(TCommittedBlob{data.BlobId, data.GetSnapshot()});
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

}
