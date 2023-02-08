#include "schemeshard__operation_memory_changes.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

template <typename I, typename C, typename H>
static void GrabNew(const I& id, const C& cont, H& holder) {
    Y_VERIFY(!cont.contains(id));
    holder.emplace(id, nullptr);
}

template <typename T, typename I, typename C, typename H>
static void Grab(const I& id, const C& cont, H& holder) {
    Y_VERIFY(cont.contains(id));
    holder.emplace(id, new T(*cont.at(id)));
}

void TMemoryChanges::GrabNewTxState(TSchemeShard* ss, const TOperationId& opId) {
    GrabNew(opId, ss->TxInFlight, TxStates);
}

void TMemoryChanges::GrabNewPath(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->PathsById, Paths);
}

void TMemoryChanges::GrabPath(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TPathElement>(pathId, ss->PathsById, Paths);
}

void TMemoryChanges::GrabNewTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Tables, Tables);
}

void TMemoryChanges::GrabTable(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TTableInfo>(pathId, ss->Tables, Tables);
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
    Grab<TSubDomainInfo>(pathId, ss->SubDomains, SubDomains);
}

void TMemoryChanges::GrabNewIndex(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Indexes, Indexes);
}

void TMemoryChanges::GrabIndex(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TTableIndexInfo>(pathId, ss->Indexes, Indexes);
}

void TMemoryChanges::GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->CdcStreams, CdcStreams);
}

void TMemoryChanges::GrabCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TCdcStreamInfo>(pathId, ss->CdcStreams, CdcStreams);
}

void TMemoryChanges::GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId) {
    Y_VERIFY(!ss->TablesWithSnapshots.contains(pathId));
    TablesWithSnapshots.emplace(pathId, snapshotTxId);
}

void TMemoryChanges::GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId) {
    Y_VERIFY(!ss->LockedPaths.contains(pathId));
    LockedPaths.emplace(pathId, InvalidTxId); // will be removed on UnDo()
}

void TMemoryChanges::GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId) {
    Y_VERIFY(ss->LockedPaths.contains(pathId));
    Y_VERIFY(ss->LockedPaths.at(pathId) == lockTxId);
    LockedPaths.emplace(pathId, lockTxId); // will be restored on UnDo()
}

void TMemoryChanges::GrabNewExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->ExternalTables, ExternalTables);
}

void TMemoryChanges::GrabExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TExternalTableInfo>(pathId, ss->ExternalTables, ExternalTables);
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
        const auto& [id, lockTxId] = LockedPaths.top();
        if (lockTxId != InvalidTxId) {
            ss->LockedPaths[id] = lockTxId;
        } else {
            ss->LockedPaths.erase(id);
        }
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

    while (ExternalTables) {
        const auto& [id, elem] = ExternalTables.top();
        if (elem) {
            ss->ExternalTables[id] = elem;
        } else {
            ss->ExternalTables.erase(id);
        }
        ExternalTables.pop();
    }
}

}
