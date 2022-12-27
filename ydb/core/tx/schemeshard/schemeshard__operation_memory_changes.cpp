#include "schemeshard__operation_memory_changes.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_processing.h>

namespace NKikimr::NSchemeShard {

void TMemoryChanges::GrabNewTxState(TSchemeShard* ss, const TOperationId& op) {
    Y_VERIFY(!ss->TxInFlight.contains(op));

    TxStates.emplace(op, nullptr);
}

void TMemoryChanges::GrabNewPath(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(!ss->PathsById.contains(pathId));

    Paths.emplace(pathId, nullptr);
}

void TMemoryChanges::GrabPath(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(ss->PathsById.contains(pathId));

    TPathElement::TPtr copy = new TPathElement(*ss->PathsById.at(pathId));
    Paths.emplace(pathId, copy);
}

void TMemoryChanges::GrabNewTable(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(!ss->Tables.contains(pathId));

    Tables.emplace(pathId, nullptr);
}

void TMemoryChanges::GrabTable(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(ss->Tables.contains(pathId));

    TTableInfo::TPtr copy = new TTableInfo(*ss->Tables.at(pathId));
    Tables.emplace(pathId, copy);
}

void TMemoryChanges::GrabNewShard(TSchemeShard*, const TShardIdx& shardId) {
    Shards.emplace(shardId, nullptr);
}

void TMemoryChanges::GrabShard(TSchemeShard *ss, const TShardIdx &shardId) {
    Y_VERIFY(ss->ShardInfos.contains(shardId));

    const auto& shard = ss->ShardInfos.at(shardId);
    Shards.emplace(shardId, MakeHolder<TShardInfo>(shard));
}

void TMemoryChanges::GrabDomain(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(ss->SubDomains.contains(pathId));

    TSubDomainInfo::TPtr copy = new TSubDomainInfo(*ss->SubDomains.at(pathId));
    SubDomains.emplace(pathId, copy);
}

void TMemoryChanges::GrabNewIndex(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(!ss->Indexes.contains(pathId));

    Indexes.emplace(pathId, nullptr);
}

void TMemoryChanges::GrabIndex(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(ss->Indexes.contains(pathId));

    TTableIndexInfo::TPtr copy = new TTableIndexInfo(*ss->Indexes.at(pathId));
    Indexes.emplace(pathId, copy);
}

void TMemoryChanges::GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(!ss->CdcStreams.contains(pathId));

    CdcStreams.emplace(pathId, nullptr);
}

void TMemoryChanges::GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId) {
    Y_VERIFY(!ss->TablesWithSnapshots.contains(pathId));

    TablesWithSnapshots.emplace(pathId, snapshotTxId);
}

void TMemoryChanges::GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId) {
    Y_VERIFY(!ss->LockedPaths.contains(pathId));

    LockedPaths.emplace(pathId, lockTxId);
}

void TMemoryChanges::UnDo(TSchemeShard* ss) {
    // be aware of the order of grab & undo ops
    // stack is the best way to manage it right

    while (Paths) {
        const auto& [id, elem] = Paths.top();
        if (elem) {
            ss->PathsById[id] = elem;
        } else {
            ss->PathsById.erase(id);
        }
        Paths.pop();
    }

    while (Indexes) {
        const auto& [id, elem] = Indexes.top();
        if (elem) {
            ss->Indexes[id] = elem;
        } else {
            ss->Indexes.erase(id);
        }
        Indexes.pop();
    }

    while (CdcStreams) {
        const auto& [id, elem] = CdcStreams.top();
        if (elem) {
            ss->CdcStreams[id] = elem;
        } else {
            ss->CdcStreams.erase(id);
        }
        CdcStreams.pop();
    }

    while (TablesWithSnapshots) {
        const auto& [id, snapshotTxId] = TablesWithSnapshots.top();

        ss->TablesWithSnapshots.erase(id);
        auto it = ss->SnapshotTables.find(snapshotTxId);
        if (it != ss->SnapshotTables.end()) {
            it->second.erase(id);
            if (it->second.empty()) {
                ss->SnapshotTables.erase(it);
            }
        }

        TablesWithSnapshots.pop();
    }

    while (LockedPaths) {
        const auto& [id, _] = LockedPaths.top();
        ss->LockedPaths.erase(id);
        LockedPaths.pop();
    }

    while (Tables) {
        const auto& [id, elem] = Tables.top();
        if (elem) {
            ss->Tables[id] = elem;
        } else {
            ss->Tables.erase(id);
        }
        Tables.pop();
    }

    while (Shards) {
        const auto& [id, elem] = Shards.top();
        if (elem) {
            ss->ShardInfos[id] = *elem;
        } else {
            ss->ShardInfos.erase(id);
            ss->ShardRemoved(id);
        }
        Shards.pop();
    }

    while (SubDomains) {
        const auto& [id, elem] = SubDomains.top();
        if (elem) {
            ss->SubDomains[id] = elem;
        } else {
            ss->SubDomains.erase(id);
        }
        SubDomains.pop();
    }

    while (TxStates) {
        const auto& [id, elem] = TxStates.top();
        if (!elem) {
            ss->TxInFlight.erase(id);
        } else {
            Y_FAIL("No such cases are exist");
        }
        TxStates.pop();
    }
}

}
