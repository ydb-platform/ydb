#include "insert_table.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap {

void TInsertTable::OnNewInserted(TPathInfo& pathInfo, const ui64 dataSize, const bool load) noexcept {
    if (!load) {
        Counters.Inserted.Add(dataSize);
    }
    pathInfo.AddInsertedSize(dataSize, TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    ++StatsPrepared.Rows;
    StatsPrepared.Bytes += dataSize;
}

void TInsertTable::OnEraseInserted(TPathInfo& pathInfo, const ui64 dataSize) noexcept {
    Counters.Inserted.Erase(dataSize);
    pathInfo.AddInsertedSize(-1 * (i64)dataSize, TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    Y_VERIFY(--StatsPrepared.Rows >= 0);
    StatsPrepared.Bytes += dataSize;
}

void TInsertTable::OnNewCommitted(const ui64 dataSize, const bool load) noexcept {
    if (!load) {
        Counters.Committed.Add(dataSize);
    }
    ++StatsCommitted.Rows;
    StatsCommitted.Bytes += dataSize;
}

void TInsertTable::OnEraseCommitted(TPathInfo& /*pathInfo*/, const ui64 dataSize) noexcept {
    Counters.Committed.Erase(dataSize);
    Y_VERIFY(--StatsCommitted.Rows >= 0);
    StatsCommitted.Bytes -= dataSize;
}

bool TInsertTable::Insert(IDbWrapper& dbTable, TInsertedData&& data) {
    TWriteId writeId{data.WriteTxId};
    if (Inserted.contains(writeId)) {
        Counters.Inserted.SkipAdd(data.BlobSize());
        return false;
    }

    dbTable.Insert(data);
    const ui32 dataSize = data.BlobSize();
    const ui64 pathId = data.PathId;
    if (Inserted.emplace(writeId, std::move(data)).second) {
        OnNewInserted(Summary.GetPathInfo(pathId), dataSize);
    } else {
        Counters.Inserted.SkipAdd(dataSize);
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

        const ui64 dataSize = data->BlobSize();
        const ui64 pathId = data->PathId;
        auto* pathInfo = Summary.GetPathInfoOptional(pathId);
        // There could be commit after drop: propose, drop, plan
        if (pathInfo && pathExists(pathId)) {
            data->Commit(planStep, txId);
            dbTable.Commit(*data);

            if (pathInfo->AddCommitted(std::move(*data))) {
                OnNewCommitted(dataSize);
            }
        } else {
             dbTable.Abort(*data);
             Counters.Aborted.Add(data->BlobSize());
             Aborted.emplace(writeId, std::move(*data));
        }

        if (pathInfo && Inserted.erase(writeId)) {
            OnEraseInserted(*pathInfo, dataSize);
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
            Counters.Aborted.Add(data->BlobSize());
            dbTable.EraseInserted(*data);
            dbTable.Abort(*data);

            const ui64 pathId = data->PathId;
            const ui32 dataSize = data->BlobSize();
            Aborted.emplace(writeId, std::move(*data));
            if (Inserted.erase(writeId)) {
                OnEraseInserted(Summary.GetPathInfo(pathId), dataSize);
            } else {
                Counters.Inserted.SkipErase(dataSize);
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

    auto pathInfo = Summary.ExtractPathInfo(pathId);
    if (!pathInfo) {
        return {};
    }
    for (auto& data : pathInfo->GetCommitted()) {
        dbTable.EraseCommitted(data);
        OnEraseCommitted(*pathInfo, data.BlobSize());
        TInsertedData copy = data;
        copy.Undo();
        dbTable.Abort(copy);

        TWriteId writeId{copy.WriteTxId};
        Counters.Aborted.Add(copy.BlobSize());
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
    TPathInfo* pathInfo = Summary.GetPathInfoOptional(data.PathId);
    if (!pathInfo) {
        Counters.Committed.SkipErase(data.BlobSize());
        return;
    }

    dbTable.EraseCommitted(data);
    if (pathInfo->EraseCommitted(data)) {
        OnEraseCommitted(*pathInfo, data.BlobSize());
    } else {
        Counters.Committed.SkipErase(data.BlobSize());
    }
}

void TInsertTable::EraseAborted(IDbWrapper& dbTable, const TInsertedData& data) {
    TWriteId writeId{data.WriteTxId};
    if (!Aborted.contains(writeId)) {
        return;
    }

    dbTable.EraseAborted(data);
    Counters.Aborted.Erase(data.BlobSize());
    Aborted.erase(writeId);
}

bool TInsertTable::Load(IDbWrapper& dbTable, const TInstant& loadTime) {
    Clear();

    if (!dbTable.Load(*this, loadTime)) {
        return false;
    }

    // update stats

    StatsPrepared = {};
    StatsCommitted = {};

    for (auto& [_, data] : Inserted) {
        OnNewInserted(Summary.GetPathInfo(data.PathId), data.BlobSize());
    }

    for (auto& [pathId, pathInfo] : Summary.GetPathInfo()) {
        for (auto& data : pathInfo.GetCommitted()) {
            OnNewCommitted(data.BlobSize());
        }
    }

    return true;
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
