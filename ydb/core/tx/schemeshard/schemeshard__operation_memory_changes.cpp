#include "schemeshard_info_types_table.h"
#include "schemeshard_info_types_objects_storage.h"
#include "olap/manager/tables_storage.h"
#include "schemeshard_info_types_objects_misc.h"
#include "schemeshard_info_types_subdomain.h"
#include "schemeshard__operation_memory_changes.h"

#include "schemeshard_impl.h"

#include "olap/table/table.h"
#include <util/generic/stack.h>
#include <optional>

namespace NKikimr::NSchemeShard {

struct TMemoryChanges::TImpl {
    TStack<std::pair<TPathId, TPathElement::TPtr>> Paths;
    TStack<std::pair<TPathId, TIntrusivePtr<TTableIndexInfo>>> Indexes;
    TStack<std::pair<TPathId, TIntrusivePtr<TCdcStreamInfo>>> CdcStreams;
    TStack<std::pair<TPathId, TTxId>> TablesWithSnapshots;
    TStack<std::pair<TPathId, TTxId>> LockedPaths;
    TStack<std::pair<TPathId, TIntrusivePtr<TTableInfo>>> Tables;
    TStack<std::pair<TPathId, TColumnTableInfo::TPtr>> ColumnTables;
    TStack<std::pair<TPathId, TIntrusivePtr<TSequenceInfo>>> Sequences;
    TStack<std::pair<TShardIdx, THolder<TShardInfo>>> Shards;
    THashMap<TPathId, TIntrusivePtr<TSubDomainInfo>> SubDomains;
    TStack<std::pair<TOperationId, THolder<NSchemeShard::TTxState>>> TxStates;
    TStack<std::pair<TPathId, TIntrusivePtr<TExternalTableInfo>>> ExternalTables;
    TStack<std::pair<TPathId, TIntrusivePtr<TExternalDataSourceInfo>>> ExternalDataSources;
    TStack<std::pair<TPathId, TIntrusivePtr<TViewInfo>>> Views;
    TStack<std::pair<TPathId, TIntrusivePtr<TResourcePoolInfo>>> ResourcePools;
    TStack<std::pair<TPathId, TIntrusivePtr<TBackupCollectionInfo>>> BackupCollections;
    TStack<std::pair<TPathId, TIntrusivePtr<TSysViewInfo>>> SysViews;
    TStack<std::pair<TOperationId, std::optional<NKikimrSchemeOp::TLongIncrementalRestoreOp>>> LongIncrementalRestoreOps;
    TStack<std::pair<ui64, TIntrusivePtr<TIncrementalBackupInfo>>> IncrementalBackups;
    TStack<std::pair<ui64, TIntrusivePtr<TFullBackupInfo>>> FullBackups;
    TStack<std::pair<TPathId, std::optional<ui64>>> BCPathToFullBackup;
    TStack<std::pair<TPathId, TIntrusivePtr<TSecretInfo>>> Secrets;
    TStack<std::pair<TPathId, TIntrusivePtr<TStreamingQueryInfo>>> StreamingQueries;
    TStack<std::tuple<TShardIdx, TPathId, std::optional<TTxId>>> SharedShardEntries;
    TStack<std::pair<TPathId, TIntrusivePtr<TTestShardSetInfo>>> TestShardSets;
};

TMemoryChanges::TMemoryChanges()
    : Impl(std::make_unique<TImpl>())
{}
TMemoryChanges::~TMemoryChanges() = default;

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
    GrabNew(opId, ss->TxInFlight, Impl->TxStates);
}

void TMemoryChanges::GrabNewPath(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->PathsById, Impl->Paths);
}

void TMemoryChanges::GrabPath(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TPathElement>(pathId, ss->PathsById, Impl->Paths);
}

void TMemoryChanges::GrabNewTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Tables, Impl->Tables);
}

void TMemoryChanges::GrabTable(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TTableInfo>(pathId, ss->Tables, Impl->Tables);
}

void TMemoryChanges::GrabNewColumnTable(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(!ss->ColumnTables.contains(pathId));
    Impl->ColumnTables.emplace(pathId, nullptr);
}

void TMemoryChanges::GrabColumnTable(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(ss->ColumnTables.contains(pathId));
    Impl->ColumnTables.emplace(pathId, std::make_shared<TColumnTableInfo>(*ss->ColumnTables.GetVerified(pathId)));
}

void TMemoryChanges::GrabNewShard(TSchemeShard*, const TShardIdx& shardId) {
    Impl->Shards.emplace(shardId, nullptr);
}

void TMemoryChanges::GrabShard(TSchemeShard *ss, const TShardIdx &shardId) {
    Y_ABORT_UNLESS(ss->ShardInfos.contains(shardId));

    const auto& shard = ss->ShardInfos.at(shardId);
    Impl->Shards.emplace(shardId, MakeHolder<TShardInfo>(shard));
}

void TMemoryChanges::GrabDomain(TSchemeShard* ss, const TPathId& pathId) {
    // Copy TSubDomainInfo from ss->SubDomains to local Impl->SubDomains.
    // Make sure that copy will be made only when needed.
    const auto found = ss->SubDomains.find(pathId);
    Y_ABORT_UNLESS(found != ss->SubDomains.end());
    if (!Impl->SubDomains.contains(pathId)) {
        Impl->SubDomains.emplace(pathId, MakeIntrusive<TSubDomainInfo>(*found->second));
    }
}

void TMemoryChanges::GrabNewIndex(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Indexes, Impl->Indexes);
}

void TMemoryChanges::GrabIndex(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TTableIndexInfo>(pathId, ss->Indexes, Impl->Indexes);
}

void TMemoryChanges::GrabNewSequence(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Sequences, Impl->Sequences);
}

void TMemoryChanges::GrabSequence(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TSequenceInfo>(pathId, ss->Sequences, Impl->Sequences);
}

void TMemoryChanges::GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->CdcStreams, Impl->CdcStreams);
}

void TMemoryChanges::GrabCdcStream(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TCdcStreamInfo>(pathId, ss->CdcStreams, Impl->CdcStreams);
}

void TMemoryChanges::GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId) {
    Y_ABORT_UNLESS(!ss->TablesWithSnapshots.contains(pathId));
    Impl->TablesWithSnapshots.emplace(pathId, snapshotTxId);
}

void TMemoryChanges::GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId) {
    Y_ABORT_UNLESS(!ss->LockedPaths.contains(pathId));
    Impl->LockedPaths.emplace(pathId, InvalidTxId); // will be removed on UnDo()
}

void TMemoryChanges::GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId) {
    Y_ABORT_UNLESS(ss->LockedPaths.contains(pathId));
    Y_ABORT_UNLESS(ss->LockedPaths.at(pathId) == lockTxId);
    Impl->LockedPaths.emplace(pathId, lockTxId); // will be restored on UnDo()
}

void TMemoryChanges::GrabNewExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->ExternalTables, Impl->ExternalTables);
}

void TMemoryChanges::GrabExternalTable(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TExternalTableInfo>(pathId, ss->ExternalTables, Impl->ExternalTables);
}

void TMemoryChanges::GrabNewExternalDataSource(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->ExternalDataSources, Impl->ExternalDataSources);
}

void TMemoryChanges::GrabExternalDataSource(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TExternalDataSourceInfo>(pathId, ss->ExternalDataSources, Impl->ExternalDataSources);
}

void TMemoryChanges::GrabNewView(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Views, Impl->Views);
}

void TMemoryChanges::GrabView(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TViewInfo>(pathId, ss->Views, Impl->Views);
}

void TMemoryChanges::GrabNewResourcePool(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->ResourcePools, Impl->ResourcePools);
}

void TMemoryChanges::GrabResourcePool(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TResourcePoolInfo>(pathId, ss->ResourcePools, Impl->ResourcePools);
}

void TMemoryChanges::GrabNewBackupCollection(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->BackupCollections, Impl->BackupCollections);
}

void TMemoryChanges::GrabBackupCollection(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TBackupCollectionInfo>(pathId, ss->BackupCollections, Impl->BackupCollections);
}

void TMemoryChanges::GrabNewSysView(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->SysViews, Impl->SysViews);
}

void TMemoryChanges::GrabSysView(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TSysViewInfo>(pathId, ss->SysViews, Impl->SysViews);
}

void TMemoryChanges::GrabNewLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId) {
    Y_ABORT_UNLESS(!ss->LongIncrementalRestoreOps.contains(opId));
    Impl->LongIncrementalRestoreOps.emplace(opId, std::nullopt);
}

void TMemoryChanges::GrabLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId) {
    Y_ABORT_UNLESS(ss->LongIncrementalRestoreOps.contains(opId));
    Impl->LongIncrementalRestoreOps.emplace(opId, ss->LongIncrementalRestoreOps.at(opId));
}

void TMemoryChanges::GrabNewLongIncrementalBackupOp(TSchemeShard* ss, ui64 id) {
    Y_ABORT_UNLESS(!ss->IncrementalBackups.contains(id));
    Impl->IncrementalBackups.emplace(id, nullptr);
}

void TMemoryChanges::GrabNewFullBackupOp(TSchemeShard* ss, ui64 id) {
    Y_ABORT_UNLESS(!ss->FullBackups.contains(id));
    Impl->FullBackups.emplace(id, nullptr);
}

void TMemoryChanges::GrabNewBCPathToFullBackup(TSchemeShard* ss, const TPathId& bcPathId) {
    Y_ABORT_UNLESS(!ss->BCPathToFullBackup.contains(bcPathId));
    Impl->BCPathToFullBackup.emplace(bcPathId, std::nullopt);
}

void TMemoryChanges::GrabNewSecret(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->Secrets, Impl->Secrets);
}

void TMemoryChanges::GrabSecret(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TSecretInfo>(pathId, ss->Secrets, Impl->Secrets);
}

void TMemoryChanges::GrabNewStreamingQuery(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->StreamingQueries, Impl->StreamingQueries);
}

void TMemoryChanges::GrabStreamingQuery(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TStreamingQueryInfo>(pathId, ss->StreamingQueries, Impl->StreamingQueries);
}

void TMemoryChanges::GrabNewSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId) {
    auto shardIt = ss->SharedShards.find(shardIdx);
    if (shardIt != ss->SharedShards.end()) {
        Y_ABORT_UNLESS(!shardIt->second.contains(pathId));
    }
    Impl->SharedShardEntries.emplace(shardIdx, pathId, std::nullopt);
}

void TMemoryChanges::GrabSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId) {
    auto shardIt = ss->SharedShards.find(shardIdx);
    Y_ABORT_UNLESS(shardIt != ss->SharedShards.end());
    auto pathIt = shardIt->second.find(pathId);
    Y_ABORT_UNLESS(pathIt != shardIt->second.end());
    Impl->SharedShardEntries.emplace(shardIdx, pathId, pathIt->second);
}

void TMemoryChanges::GrabNewTestShardSet(TSchemeShard* ss, const TPathId& pathId) {
    GrabNew(pathId, ss->TestShardSets, Impl->TestShardSets);
}

void TMemoryChanges::GrabTestShardSet(TSchemeShard* ss, const TPathId& pathId) {
    Grab<TTestShardSetInfo>(pathId, ss->TestShardSets, Impl->TestShardSets);
}

void TMemoryChanges::UnDo(TSchemeShard* ss) {
    // be aware of the order of grab & undo ops
    // stack is the best way to manage it right

    while (Impl->Paths) {
        const auto& [id, elem] = Impl->Paths.top();
        if (elem) {
            ss->PathsById[id] = elem;
        } else {
            ss->PathsById.erase(id);
        }
        Impl->Paths.pop();
    }

    while (Impl->Indexes) {
        const auto& [id, elem] = Impl->Indexes.top();
        if (elem) {
            ss->Indexes[id] = elem;
        } else {
            ss->Indexes.erase(id);
        }
        Impl->Indexes.pop();
    }

    while (Impl->Sequences) {
        const auto& [id, elem] = Impl->Sequences.top();
        if (elem) {
            ss->Sequences[id] = elem;
        } else {
            ss->Sequences.erase(id);
        }
        Impl->Sequences.pop();
    }

    while (Impl->CdcStreams) {
        const auto& [id, elem] = Impl->CdcStreams.top();
        if (elem) {
            ss->CdcStreams[id] = elem;
        } else {
            ss->CdcStreams.erase(id);
        }
        Impl->CdcStreams.pop();
    }

    while (Impl->TablesWithSnapshots) {
        const auto& [id, snapshotTxId] = Impl->TablesWithSnapshots.top();

        ss->TablesWithSnapshots.erase(id);
        auto it = ss->SnapshotTables.find(snapshotTxId);
        if (it != ss->SnapshotTables.end()) {
            it->second.erase(id);
            if (it->second.empty()) {
                ss->SnapshotTables.erase(it);
            }
        }

        Impl->TablesWithSnapshots.pop();
    }

    while (Impl->LockedPaths) {
        const auto& [id, lockTxId] = Impl->LockedPaths.top();
        if (lockTxId != InvalidTxId) {
            ss->LockedPaths[id] = lockTxId;
        } else {
            ss->LockedPaths.erase(id);
        }
        Impl->LockedPaths.pop();
    }

    while (Impl->Tables) {
        const auto& [id, elem] = Impl->Tables.top();
        if (elem) {
            ss->Tables[id] = elem;
        } else {
            ss->Tables.erase(id);
        }
        Impl->Tables.pop();
    }

    while (Impl->ColumnTables) {
        const auto& [id, elem] = Impl->ColumnTables.top();
        // Drop current entry first (if any), then re-create with the saved value (if any)
        ss->ColumnTables.Drop(id);
        if (elem) {
            ss->ColumnTables.BuildNew(id, elem);
        }
        Impl->ColumnTables.pop();
    }

    while (Impl->Shards) {
        const auto& [id, elem] = Impl->Shards.top();
        if (elem) {
            ss->ShardInfos[id] = *elem;
        } else {
            ss->ShardInfos.erase(id);
            ss->OnShardRemoved(id);
        }
        Impl->Shards.pop();
    }

    // Restore ss->SubDomains entries to saved copies of TSubDomainInfo objects.
    // No copy, simple pointer replacement.
    for (const auto& [id, savedState] : Impl->SubDomains) {
        auto& subdomain = ss->SubDomains[id];
        subdomain = savedState;
        if (ss->GetCurrentSubDomainPathId() == id) {
            subdomain->UpdateCounters(ss);
        }
    }
    Impl->SubDomains.clear();

    while (Impl->TxStates) {
        const auto& [id, elem] = Impl->TxStates.top();
        if (!elem) {
            ss->TxInFlight.erase(id);
        } else {
            Y_ABORT("No such cases are exist");
        }
        Impl->TxStates.pop();
    }

    while (Impl->ExternalTables) {
        const auto& [id, elem] = Impl->ExternalTables.top();
        if (elem) {
            ss->ExternalTables[id] = elem;
        } else {
            ss->ExternalTables.erase(id);
        }
        Impl->ExternalTables.pop();
    }

    while (Impl->ExternalDataSources) {
        const auto& [id, elem] = Impl->ExternalDataSources.top();
        if (elem) {
            ss->ExternalDataSources[id] = elem;
        } else {
            ss->ExternalDataSources.erase(id);
        }
        Impl->ExternalDataSources.pop();
    }

    while (Impl->Views) {
        const auto& [id, elem] = Impl->Views.top();
        if (elem) {
            ss->Views[id] = elem;
        } else {
            ss->Views.erase(id);
        }
        Impl->Views.pop();
    }

    while (Impl->ResourcePools) {
        const auto& [id, elem] = Impl->ResourcePools.top();
        if (elem) {
            ss->ResourcePools[id] = elem;
        } else {
            ss->ResourcePools.erase(id);
        }
        Impl->ResourcePools.pop();
    }

    while (Impl->BackupCollections) {
        const auto& [id, elem] = Impl->BackupCollections.top();
        if (elem) {
            ss->BackupCollections[id] = elem;
        } else {
            ss->BackupCollections.erase(id);
        }
        Impl->BackupCollections.pop();
    }

    while (Impl->SysViews) {
        const auto& [id, elem] = Impl->SysViews.top();
        if (elem) {
            ss->SysViews[id] = elem;
        } else {
            ss->SysViews.erase(id);
        }
        Impl->SysViews.pop();
    }

    while (Impl->LongIncrementalRestoreOps) {
        const auto& [id, elem] = Impl->LongIncrementalRestoreOps.top();
        if (elem.has_value()) {
            ss->LongIncrementalRestoreOps[id] = elem.value();
        } else {
            ss->LongIncrementalRestoreOps.erase(id);
        }
        Impl->LongIncrementalRestoreOps.pop();
    }

    while (Impl->IncrementalBackups) {
        const auto& [id, elem] = Impl->IncrementalBackups.top();
        if (elem) {
            ss->IncrementalBackups[id] = elem;
        } else {
            ss->IncrementalBackups.erase(id);
        }
        Impl->IncrementalBackups.pop();
    }

    while (Impl->FullBackups) {
        const auto& [id, elem] = Impl->FullBackups.top();
        if (elem) {
            ss->FullBackups[id] = elem;
        } else {
            ss->FullBackups.erase(id);
        }
        Impl->FullBackups.pop();
    }

    while (Impl->BCPathToFullBackup) {
        const auto& [bcPathId, prevId] = Impl->BCPathToFullBackup.top();
        if (prevId.has_value()) {
            ss->BCPathToFullBackup[bcPathId] = *prevId;
        } else {
            ss->BCPathToFullBackup.erase(bcPathId);
        }
        Impl->BCPathToFullBackup.pop();
    }

    while (Impl->Secrets) {
        const auto& [id, elem] = Impl->Secrets.top();
        if (elem) {
            ss->Secrets[id] = elem;
        } else {
            ss->Secrets.erase(id);
        }
        Impl->Secrets.pop();
    }

    while (Impl->StreamingQueries) {
        const auto& [id, elem] = Impl->StreamingQueries.top();
        if (elem) {
            ss->StreamingQueries[id] = elem;
        } else {
            ss->StreamingQueries.erase(id);
        }
        Impl->StreamingQueries.pop();
    }

    while (Impl->SharedShardEntries) {
        const auto& [shardIdx, pathId, elem] = Impl->SharedShardEntries.top();
        if (elem) {
            ss->SharedShards[shardIdx][pathId] = *elem;
        } else {
            auto shardIt = ss->SharedShards.find(shardIdx);
            if (shardIt != ss->SharedShards.end()) {
                shardIt->second.erase(pathId);
                if (shardIt->second.empty()) {
                    ss->SharedShards.erase(shardIt);
                }
            }
        }
        Impl->SharedShardEntries.pop();
    }

    while (Impl->TestShardSets) {
        const auto& [id, elem] = Impl->TestShardSets.top();
        if (elem) {
            ss->TestShardSets[id] = elem;
        } else {
            ss->TestShardSets.erase(id);
        }
        Impl->TestShardSets.pop();
    }
}

}
