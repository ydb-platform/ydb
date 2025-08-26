#include "schemeshard__operation_memory_changes.h"

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

template <typename I, typename C, typename H>
static void GrabNew(const I& id, const C& cont, H& holder) {
    Y_ABORT_UNLESS(!cont.contains(id));
    holder.emplace(id, nullptr);
}

template <typename T, typename I, typename C, typename H>
static void Grab(const I& id, const C& cont, H& holder) {
    Y_ABORT_UNLESS(cont.contains(id));
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
    Y_ABORT_UNLESS(ss->ShardInfos.contains(shardId));

    const auto& shard = ss->ShardInfos.at(shardId);
    Shards.emplace(shardId, MakeHolder<TShardInfo>(shard));
}

void TMemoryChanges::GrabDomain(TSchemeShard* ss, const TPathId& pathId) {
    // Copy TSubDomainInfo from ss->SubDomains to local SubDomains.
    // Make sure that copy will be made only when needed.
    const auto found = ss->SubDomains.find(pathId);
    Y_ABORT_UNLESS(found != ss->SubDomains.end());
    if (!SubDomains.contains(pathId)) {
        SubDomains.emplace(pathId, MakeIntrusive<TSubDomainInfo>(*found->second));
    }
}

void TMemoryChanges::GrabNewIndex(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Indexes, Indexes);
}

void TMemoryChanges::GrabIndex(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TTableIndexInfo>(pathId, ss->Indexes, Indexes);
}

void TMemoryChanges::GrabNewSequence(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Sequences, Sequences);
}

void TMemoryChanges::GrabSequence(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TSequenceInfo>(pathId, ss->Sequences, Sequences);
}

void TMemoryChanges::GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->CdcStreams, CdcStreams);
}

void TMemoryChanges::GrabCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TCdcStreamInfo>(pathId, ss->CdcStreams, CdcStreams);
}

void TMemoryChanges::GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId) {
    Y_ABORT_UNLESS(!ss->TablesWithSnapshots.contains(pathId));
    TablesWithSnapshots.emplace(pathId, snapshotTxId);
}

void TMemoryChanges::GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(!ss->LockedPaths.contains(pathId));
    LockedPaths.emplace(pathId, InvalidTxId); // will be removed on UnDo()
}

void TMemoryChanges::GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId) {
    Y_ABORT_UNLESS(ss->LockedPaths.contains(pathId));
    Y_ABORT_UNLESS(ss->LockedPaths.at(pathId) == lockTxId);
    LockedPaths.emplace(pathId, lockTxId); // will be restored on UnDo()
}

void TMemoryChanges::GrabExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TExternalTableInfo>(pathId, ss->ExternalTables, ExternalTables);
}

void TMemoryChanges::GrabExternalDataSource(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TExternalDataSourceInfo>(pathId, ss->ExternalDataSources, ExternalDataSources);
}

void TMemoryChanges::GrabNewView(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Views, Views);
}

void TMemoryChanges::GrabView(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TViewInfo>(pathId, ss->Views, Views);
}

void TMemoryChanges::GrabResourcePool(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TResourcePoolInfo>(pathId, ss->ResourcePools, ResourcePools);
}

void TMemoryChanges::GrabBackupCollection(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TBackupCollectionInfo>(pathId, ss->BackupCollections, BackupCollections);
}

void TMemoryChanges::GrabNewSysView(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->SysViews, SysViews);
}

void TMemoryChanges::GrabSysView(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TSysViewInfo>(pathId, ss->SysViews, SysViews);
}

void TMemoryChanges::GrabNewLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId) {
    Y_ABORT_UNLESS(!ss->LongIncrementalRestoreOps.contains(opId));
    LongIncrementalRestoreOps.emplace(opId, std::nullopt);
}

void TMemoryChanges::GrabLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId) {
    Y_ABORT_UNLESS(ss->LongIncrementalRestoreOps.contains(opId));
    LongIncrementalRestoreOps.emplace(opId, ss->LongIncrementalRestoreOps.at(opId));
}

void TMemoryChanges::GrabNewLongIncrementalBackupOp(TSchemeShard* ss, ui64 id) {
    Y_ABORT_UNLESS(!ss->IncrementalBackups.contains(id));
    IncrementalBackups.emplace(id, nullptr);
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

    while (Sequences) {
        const auto& [id, elem] = Sequences.top();
        if (elem) {
            ss->Sequences[id] = elem;
        } else {
            ss->Sequences.erase(id);
        }
        Sequences.pop();
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
            ss->OnShardRemoved(id);
        }
        Shards.pop();
    }

    // Restore ss->SubDomains entries to saved copies of TSubDomainInfo objects.
    // No copy, simple pointer replacement.
    for (const auto& [id, savedState] : SubDomains) {
        auto& subdomain = ss->SubDomains[id];
        subdomain = savedState;
        if (ss->GetCurrentSubDomainPathId() == id) {
            subdomain->UpdateCounters(ss);
        }
    }
    SubDomains.clear();

    while (TxStates) {
        const auto& [id, elem] = TxStates.top();
        if (!elem) {
            ss->TxInFlight.erase(id);
        } else {
            Y_ABORT("No such cases are exist");
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

    while (ExternalDataSources) {
        const auto& [id, elem] = ExternalDataSources.top();
        if (elem) {
            ss->ExternalDataSources[id] = elem;
        } else {
            ss->ExternalDataSources.erase(id);
        }
        ExternalDataSources.pop();
    }

    while (Views) {
        const auto& [id, elem] = Views.top();
        if (elem) {
            ss->Views[id] = elem;
        } else {
            ss->Views.erase(id);
        }
        Views.pop();
    }

    while (ResourcePools) {
        const auto& [id, elem] = ResourcePools.top();
        if (elem) {
            ss->ResourcePools[id] = elem;
        } else {
            ss->ResourcePools.erase(id);
        }
        ResourcePools.pop();
    }

    while (SysViews) {
        const auto& [id, elem] = SysViews.top();
        if (elem) {
            ss->SysViews[id] = elem;
        } else {
            ss->SysViews.erase(id);
        }
        SysViews.pop();
    }

    while (LongIncrementalRestoreOps) {
        const auto& [id, elem] = LongIncrementalRestoreOps.top();
        if (elem.has_value()) {
            ss->LongIncrementalRestoreOps[id] = elem.value();
        } else {
            ss->LongIncrementalRestoreOps.erase(id);
        }
        LongIncrementalRestoreOps.pop();
    }

    while (IncrementalBackups) {
        const auto& [id, elem] = IncrementalBackups.top();
        if (elem) {
            ss->IncrementalBackups[id] = elem;
        } else {
            ss->IncrementalBackups.erase(id);
        }
        IncrementalBackups.pop();
    }
}

}
